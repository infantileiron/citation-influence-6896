"""运行任务的队列、互斥锁与中断恢复。

职责：
- 同一运行在任意时刻只允许一个执行者（Redis 分布式锁，带 TTL 防死锁）；
- worker 中断后，超过租约未更新心跳的 running 任务自动回到 recovering 并重新入队；
- recovering/非最终 failed 任务若丢失队列消息，扫描时按宽限期补投，不会永久卡住；
- 队列按 run id 去重，重复入队只保留一份，最终结果只发布一次；
- 反复失败达到 MAX_RUN_ATTEMPTS 后停止重试，由 run 服务标记最终失败并写明原因。
所有状态判断均以数据库载荷为准，Redis 仅承担队列与互斥信号量。
"""
import logging
import redis

from src.config.settings import REDIS_URL
from src.constants import domain
from src.constants.enums import RunStatus
from src.utils import repository as repo
from src.utils.identity import now

log = logging.getLogger(__name__)


def _client():
    return redis.Redis.from_url(REDIS_URL)


def acquire_run_lock(run_id: str, owner: str) -> bool:
    """尝试领取运行。成功返回 True；锁已被其他执行者持有时返回 False。"""
    try:
        return bool(_client().set(
            domain.RUN_LOCK_KEY.format(run_id=run_id), owner,
            nx=True, ex=domain.RUN_LOCK_TTL,
        ))
    except redis.RedisError:
        log.exception("acquire run lock failed for %s", run_id)
        return False


def refresh_run_lock(run_id: str, owner: str) -> bool:
    """续租：仅持有者本人可续，锁丢失说明 TTL 已过期并可能被他人领取。"""
    client = _client()
    key = domain.RUN_LOCK_KEY.format(run_id=run_id)
    try:
        if client.get(key) == owner.encode():
            client.set(key, owner, ex=domain.RUN_LOCK_TTL)
            return True
    except redis.RedisError:
        log.exception("refresh run lock failed for %s", run_id)
    return False


def release_run_lock(run_id: str, owner: str) -> None:
    """释放锁，使用 Lua 保证“比对持有者 + 删除”原子，避免误删他人的锁。"""
    lua = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "return redis.call('del', KEYS[1]) else return 0 end"
    )
    try:
        _client().eval(lua, 1, domain.RUN_LOCK_KEY.format(run_id=run_id), owner)
    except redis.RedisError:
        log.exception("release run lock failed for %s", run_id)


def enqueue_run(run_id: str) -> None:
    """按 run id 去重入队：同一运行在队列里只保留一份待处理消息。"""
    client = _client()
    try:
        queued = {item.decode() if isinstance(item, bytes) else item
                  for item in client.lrange(domain.JOB_QUEUE_KEY, 0, -1)}
        if run_id not in queued:
            client.rpush(domain.JOB_QUEUE_KEY, run_id)
    except redis.RedisError as exc:
        raise RuntimeError("队列不可用，请恢复Redis后重新提交") from exc


def requeue_interrupted_runs() -> list[str]:
    """扫描中断/丢失消息的运行并重新入队，返回本次恢复的 run id 列表。

    覆盖三类情况：
    1. running 且心跳超过 RUN_LEASE_SECONDS（worker 被强杀）；
    2. recovering 且队列中已无该消息（Redis 消息丢失/清空），按宽限期避免与刚入队消息竞争；
    3. 非最终 failed（attempts 未达上限且重投失败），队列恢复后补投。
    已完成/最终失败/已在队列中的任务不会重复处理；研究快照不可变，恢复仍按锁定口径计算。
    """
    recovered: list[str] = []
    try:
        client = _client()
        queued = {item.decode() if isinstance(item, bytes) else item
                  for item in client.lrange(domain.JOB_QUEUE_KEY, 0, -1)}
    except redis.RedisError:
        log.exception("recovery sweep: redis unavailable")
        return recovered

    for run in repo.all_rows("runs"):
        status = run["status"]
        run_id = run["id"]
        if status == RunStatus.RUNNING.value:
            if not _lease_expired(run):
                continue
            run["status"] = RunStatus.RECOVERING.value
            run["error"] = run.get("error", "") or "执行中断，等待自动恢复重试"
            repo.put("runs", run)
        elif status == RunStatus.RECOVERING.value:
            if run_id in queued or not _recovery_due(run):
                continue
        elif status == RunStatus.FAILED.value:
            # 仅兜底重投失败、未达尝试上限的任务；最终失败保持停止
            if run_id in queued or run.get("attempts", 0) >= domain.MAX_RUN_ATTEMPTS:
                continue
            run["status"] = RunStatus.RECOVERING.value
            repo.put("runs", run)
        else:
            continue
        # 上次执行者的锁可能因 TTL 已失效；清理残留后再入队，领取仍以 NX 锁为准
        try:
            client.delete(domain.RUN_LOCK_KEY.format(run_id=run_id))
        except redis.RedisError:
            log.exception("recovery sweep: stale lock cleanup failed for %s", run_id)
        if run_id not in queued:
            try:
                client.rpush(domain.JOB_QUEUE_KEY, run_id)
                recovered.append(run_id)
            except redis.RedisError:
                log.exception("recovery sweep: requeue failed for %s", run_id)
    return recovered


def _recovery_due(run: dict) -> bool:
    """recovering 任务等待一个扫描写入宽限期，避免与刚入队的消息重复重投。"""
    heartbeat = run.get("last_heartbeat")
    if not heartbeat:
        return True
    try:
        from datetime import datetime, timezone
        ts = datetime.fromisoformat(heartbeat)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() > domain.RECOVERY_SWEEP_SECONDS
    except ValueError:
        return True


def _lease_expired(run: dict) -> bool:
    heartbeat = run.get("last_heartbeat")
    if not heartbeat:
        return True
    try:
        from datetime import datetime, timezone
        ts = datetime.fromisoformat(heartbeat)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - ts).total_seconds() > domain.RUN_LEASE_SECONDS
    except ValueError:
        return True
