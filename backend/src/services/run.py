import json
import logging
import threading

from src.models.entities import InfluenceRun
from src.utils import repository as repo
from src.utils.identity import new_id, now
from src.services.statistics import analyze
from src.services import dispatch
from src.config.settings import TASK_MODE, ARTIFACT_DIR
from src.constants import domain
from src.constants.enums import RunStatus

log = logging.getLogger(__name__)

# 进程内串行保护（inline 模式与单测场景下没有 Redis 互斥时的兜底）
_LOCAL_LOCKS = threading.local()


def list_runs():
    return repo.all_rows("runs")


def _local_acquire(run_id: str) -> bool:
    held = getattr(_LOCAL_LOCKS, "held", None)
    if held is None:
        held = set()
        _LOCAL_LOCKS.held = held
    if run_id in held:
        return False
    held.add(run_id)
    return True


def _local_release(run_id: str) -> None:
    held = getattr(_LOCAL_LOCKS, "held", None)
    if held:
        held.discard(run_id)


def execute(id: str, owner: str | None = None):
    """执行一次运行尝试。

    并发安全：先抢 Redis 运行锁（inline 无 Redis 时退化为进程内锁），
    同一运行同一时刻只有一个执行者；已完成的运行直接原样返回，结果只发布一份。
    有界重试：attempts 达到 MAX_RUN_ATTEMPTS 仍失败则置为最终失败并写明原因，不再入队。
    """
    run = repo.get("runs", id)
    # 已完成运行及其复核、归档报告保持原样，绝不重算覆盖
    if run["status"] == RunStatus.COMPLETED.value:
        return run

    owner = owner or f"inline-{id}"
    locked_redis = False
    if TASK_MODE == "redis":
        locked_redis = dispatch.acquire_run_lock(id, owner)
        if not locked_redis:
            log.info("run %s already claimed by another worker, skip", id)
            return repo.get("runs", id)
        locked_local = False
    else:
        locked_local = _local_acquire(id)
        if not locked_local:
            return repo.get("runs", id)

    stop_heartbeat = None
    try:
        # 重新读取：可能在等锁期间已被其他执行者完成
        run = repo.get("runs", id)
        if run["status"] == RunStatus.COMPLETED.value:
            return run
        # 达到尝试上限的最终失败任务不再执行
        if run["status"] == RunStatus.FAILED.value and run.get("attempts", 0) >= domain.MAX_RUN_ATTEMPTS:
            return run

        run["attempts"] = run.get("attempts", 0) + 1
        run["status"] = RunStatus.RUNNING.value
        run["locked_by"] = owner
        run["last_heartbeat"] = now()
        run.setdefault("attempts_log", [])
        repo.put("runs", run)

        stop_heartbeat = _start_heartbeat(id, owner, locked_redis)
        try:
            exp = repo.get("studies", run["study_id"])
            # worker 仅读研究锁定快照；数据集后续变化不影响恢复计算
            run["result"] = analyze(exp["snapshot"], exp["checksum"])
            ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
            (ARTIFACT_DIR / (id + ".json")).write_text(
                json.dumps(run["result"], ensure_ascii=False)
            )
            run["status"] = RunStatus.COMPLETED.value
            run["error"] = ""
            run["last_heartbeat"] = now()
            run["attempts_log"].append({
                "attempt": run["attempts"], "status": "completed", "at": now(),
            })
            return repo.put("runs", run)
        except Exception as exc:
            return _handle_failure(id, exc)
    finally:
        if stop_heartbeat:
            stop_heartbeat.set()
        if locked_redis:
            dispatch.release_run_lock(id, owner)
        else:
            _local_release(id)


def _start_heartbeat(run_id: str, owner: str, locked_redis: bool):
    """后台线程按租约周期刷新心跳并续租，中断时心跳停止、租约自然过期。

    心跳只在运行仍由本执行者持有时更新时间戳，绝不覆盖状态/结果等字段，
    避免与失败处理主流程的写回交叉。
    """
    interval = max(5, domain.RUN_LEASE_SECONDS // 3)
    stop = threading.Event()

    def beat():
        def _touch(current):
            if current.get("locked_by") != owner or current["status"] != RunStatus.RUNNING.value:
                raise _HeartbeatStale
            current["last_heartbeat"] = now()

        while not stop.wait(interval):
            try:
                repo.patch("runs", run_id, _touch)
            except _HeartbeatStale:
                return
            except Exception:
                log.exception("heartbeat update failed for %s", run_id)
                continue
            if locked_redis and not dispatch.refresh_run_lock(run_id, owner):
                log.warning("run %s lock lost during execution", run_id)
                return

    threading.Thread(target=beat, name=f"hb-{run_id}", daemon=True).start()
    return stop


class _HeartbeatStale(Exception):
    """运行已易主或不再处于 running，心跳线程应退出。"""


def _handle_failure(run_id: str, exc: Exception):
    run = repo.get("runs", run_id)
    reason = str(exc) or exc.__class__.__name__
    run.setdefault("attempts_log", []).append({
        "attempt": run.get("attempts", 1),
        "status": "failed",
        "at": now(),
        "error": reason,
    })
    run["error"] = reason
    run["last_heartbeat"] = now()
    if run["attempts"] >= domain.MAX_RUN_ATTEMPTS:
        # 反复失败：停止自动重试，明确告知已用尽的尝试次数与最后原因
        run["status"] = RunStatus.FAILED.value
        run["error"] = (
            f"已连续失败 {run['attempts']} 次（上限 {domain.MAX_RUN_ATTEMPTS} 次），"
            f"自动重试已停止；最后原因：{reason}"
        )
        repo.put("runs", run)
        log.error("run %s gave up after %s attempts: %s", run_id, run["attempts"], reason)
        return run

    # 仍有重试额度：回到等待恢复状态并重新入队，由 worker 再次领取
    run["status"] = RunStatus.RECOVERING.value
    repo.put("runs", run)
    if TASK_MODE == "redis":
        try:
            dispatch.enqueue_run(run_id)
        except Exception as qexc:
            run["status"] = RunStatus.FAILED.value
            run["error"] = f"重试任务无法入队：{qexc}；最后计算原因：{reason}"
            repo.put("runs", run)
    return run


def create(data):
    repo.get("studies", data.study_id)
    active = sum(
        r["status"] in (RunStatus.QUEUED.value, RunStatus.RECOVERING.value, RunStatus.RUNNING.value)
        for r in repo.all_rows("runs")
    )
    if active >= domain.MAX_QUEUED:
        raise ValueError("运行队列已满，请等待已有任务完成")
    run = repo.put("runs", InfluenceRun(new_id("run"), data.study_id, created_at=now()))
    if TASK_MODE == "inline":
        # 无独立 worker：在请求内同步重试到完成或最终失败，避免任务停在 recovering
        run = execute(run["id"])
        while run["status"] == RunStatus.RECOVERING.value:
            run = execute(run["id"])
        return run
    try:
        dispatch.enqueue_run(run["id"])
    except Exception as exc:
        run["status"] = RunStatus.FAILED.value
        run["error"] = str(exc)
        repo.put("runs", run)
        raise ValueError(run["error"])
    return run
