"""中断恢复与并发安全的无外部依赖验证（sqlite + fakeredis 思路的最小 Redis 替身）。

运行：cd backend && python -m scripts.verify_recovery
覆盖：
1. running 任务超过租约后被扫描为 recovering 并重新入队，恢复仍按锁定快照；
2. 两个执行者并发领取同一运行，只有一个真正执行，最终只发布一份结果；
3. 反复失败达到上限后标记最终失败并写明原因，不再入队；
4. recovering/running/failed 半成品均不能提交复核，completed 可以；
5. 已完成运行再次领取时原样返回，结果不被覆盖，复核/报告不受影响。
"""
import json
import os
import sys
import tempfile
import time
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
# 允许在未安装依赖的环境通过 PYLIBS 指向隔离依赖目录（生产容器不需要）
_extra = os.environ.get("PYLIBS")
if _extra and _extra not in sys.path:
    sys.path.insert(0, _extra)
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

_tmp = tempfile.mkdtemp(prefix="recover-verify-")
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp}/verify.sqlite"
os.environ["TASK_MODE"] = "redis"
os.environ["ARTIFACT_DIR"] = f"{_tmp}/artifacts"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants import domain  # noqa: E402
from src.constants.enums import RunStatus  # noqa: E402
from src.services import dispatch, run as run_service, review  # noqa: E402
from src.services.seed import seed  # noqa: E402
from src.utils import repository as repo  # noqa: E402
from src.utils.identity import now  # noqa: E402


class FakeRedis:
    def __init__(self):
        self.list = []
        self.strings = {}
        self.lock = threading.Lock()

    def blpop(self, key, timeout=0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                if self.list:
                    return key, self.list.pop(0).encode()
            time.sleep(0.02)
        return None

    def rpush(self, key, value):
        with self.lock:
            if value not in self.list:
                self.list.append(value)
        return len(self.list)

    def lrange(self, key, start, end):
        with self.lock:
            return list(self.list)

    def set(self, key, value, nx=False, ex=None):
        with self.lock:
            if nx and key in self.strings:
                return None
            self.strings[key] = value
            return True

    def get(self, key):
        with self.lock:
            return self.strings.get(key)

    def delete(self, key):
        with self.lock:
            return 1 if self.strings.pop(key, None) is not None else 0

    def eval(self, lua, nkeys, key, owner):
        with self.lock:
            if self.strings.get(key) == owner:
                del self.strings[key]
                return 1
            return 0


fake = FakeRedis()
dispatch._client = lambda: fake  # type: ignore[attr-defined]

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {detail}")


def make_run(status=RunStatus.QUEUED.value, stale=False, attempts=0):
    rid = f"run-verify-{threading.get_ident()}-{time.time_ns()}"
    heartbeat = ""
    if stale:
        heartbeat = (datetime.now(timezone.utc) - timedelta(seconds=domain.RUN_LEASE_SECONDS + 5)).isoformat()
    repo.put("runs", {
        "id": rid, "study_id": "study-index", "status": status,
        "result": {}, "error": "", "attempts": attempts,
        "last_heartbeat": heartbeat, "locked_by": "", "attempts_log": [],
        "created_at": now(),
    })
    return rid


print("== 准备演示数据 ==")
seed()

print("== 1. 中断任务自动恢复并重新入队 ==")
rid = make_run(status=RunStatus.RUNNING.value, stale=True)
recovered = dispatch.requeue_interrupted_runs()
run = repo.get("runs", rid)
check("中断运行转为 recovering", run["status"] == RunStatus.RECOVERING.value, run["status"])
check("恢复任务已重新入队", rid in recovered and rid in fake.list)
# 扫描幂等：recovering 不会被重复扫描，队列不重复
again = dispatch.requeue_interrupted_runs()
check("重复扫描不产生重复入队", again == [] and fake.list.count(rid) == 1)
# 恢复执行：仍读锁定研究快照并完成
done = run_service.execute(rid, owner="w1")
check("恢复后按锁定研究完成", done["status"] == RunStatus.COMPLETED.value and bool(done["result"]))
check("结果只发布一份（artifact 单文件）", (Path(os.environ["ARTIFACT_DIR"]) / f"{rid}.json").exists())
check("完成后锁已释放", fake.get(domain.RUN_LOCK_KEY.format(run_id=rid)) is None)

print("== 2. 并发领取同一运行只有一份最终结果 ==")
rid2 = make_run()
fake.rpush(domain.JOB_QUEUE_KEY, rid2)
outcomes = []
barrier = threading.Barrier(3)

def worker(tag):
    barrier.wait()
    outcomes.append((tag, run_service.execute(rid2, owner=f"w{tag}")["status"]))

threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
for t in threads:
    t.start()
barrier.wait()
for t in threads:
    t.join()
final = repo.get("runs", rid2)
check("并发后恰好一次完成", final["status"] == RunStatus.COMPLETED.value and final["attempts"] == 1,
      f"status={final['status']} attempts={final['attempts']}")
check("两个执行者均有返回且无异常状态", len(outcomes) == 2)
# 失败路径未触发重投，队列中不应产生同一运行的重复消息
check("队列中同一运行无重复消息", fake.list.count(rid2) <= 1)

print("== 3. 反复失败有界重试并最终停止 ==")
rid3 = make_run()
orig_analyze = run_service.analyze

def boom(snapshot, checksum):
    raise RuntimeError("模拟数值计算失败")

run_service.analyze = boom
try:
    for expected_attempt in range(1, domain.MAX_RUN_ATTEMPTS + 1):
        current = repo.get("runs", rid3)
        if current["status"] == RunStatus.FAILED.value:
            break
        run_service.execute(rid3, owner="w-retry")
    final = repo.get("runs", rid3)
    check("达到上限后最终失败", final["status"] == RunStatus.FAILED.value)
    check("尝试次数等于上限", final["attempts"] == domain.MAX_RUN_ATTEMPTS, str(final["attempts"]))
    check("失败原因说明次数与最后错误", "自动重试已停止" in final["error"] and "模拟数值计算失败" in final["error"],
          final["error"])
    check("每次尝试均留痕", len(final["attempts_log"]) == domain.MAX_RUN_ATTEMPTS)
    # 前两次失败各重投一条、最终失败不再重投：去重入队保证至多一条待消费消息
    check("最终失败后不再新增重投消息", fake.list.count(rid3) == 1, f"queue={fake.list.count(rid3)}")
    # 最终失败任务再次领取不会执行
    before = json.dumps(final, sort_keys=True)
    run_service.execute(rid3, owner="w-again")
    after = json.dumps(repo.get("runs", rid3), sort_keys=True)
    check("最终失败任务拒绝再次执行", before == after)
finally:
    run_service.analyze = orig_analyze

print("== 4. 半成品不能进入复核 ==")
from src.schemas.entities import ReviewInput
rid_q = make_run(status=RunStatus.QUEUED.value)
rid_rec = make_run(status=RunStatus.RECOVERING.value)
rid_run = make_run(status=RunStatus.RUNNING.value)
rid_fail = make_run(status=RunStatus.FAILED.value, attempts=domain.MAX_RUN_ATTEMPTS)
user = {"email": "analyst@yanlin.test"}
for label, blocked_id in (("queued", rid_q), ("recovering", rid_rec), ("running", rid_run), ("failed", rid_fail)):
    try:
        review.create(ReviewInput(run_id=blocked_id, comment="不应该被接受的复核说明"), user)
        check(f"{label} 运行被复核拦截", False)
    except ValueError:
        check(f"{label} 运行被复核拦截", True)
ok_review = review.create(ReviewInput(run_id=rid, comment="恢复完成后的运行可以正常提交复核。"), user)
check("完成运行可提交复核", ok_review["status"] == "pending")

print("== 5. 已完成运行及其复核、归档保持原样 ==")
demo = repo.get("runs", "run-demo")
snapshot_before = json.dumps(demo, sort_keys=True)
run_service.analyze = boom
try:
    ret = run_service.execute("run-demo", owner="w-stray")
finally:
    run_service.analyze = orig_analyze
check("已完成运行再次领取直接返回", ret["status"] == RunStatus.COMPLETED.value)
check("完成结果未被覆盖", json.dumps(repo.get("runs", "run-demo"), sort_keys=True) == snapshot_before)
seed_review = repo.get("reviews", "review-demo")
seed_report = repo.get("reports", "report-demo")
check("种子复核仍为批准", seed_review["status"] == "approved")
check("种子归档报告保持原样", seed_report["id"] == "report-demo")

print("== 6. 队列消息丢失后按宽限期补投 ==")
# recovering 且队列消息被清空、心跳已过宽限期 -> 补投
rid_lost = make_run(status=RunStatus.RECOVERING.value, attempts=1)
lost = repo.get("runs", rid_lost)
lost["last_heartbeat"] = (datetime.now(timezone.utc) - timedelta(seconds=domain.RECOVERY_SWEEP_SECONDS + 5)).isoformat()
repo.put("runs", lost)
recovered = dispatch.requeue_interrupted_runs()
check("丢失消息的 recovering 任务被补投", rid_lost in recovered and rid_lost in fake.list)
# 刚进入 recovering、仍在宽限期内 -> 不重复补投
rid_fresh = make_run(status=RunStatus.RECOVERING.value, attempts=1)
fresh = repo.get("runs", rid_fresh)
fresh["last_heartbeat"] = now()
repo.put("runs", fresh)
check("宽限期内不重复补投", rid_fresh not in dispatch.requeue_interrupted_runs())
# 非最终 failed（未达上限、重投失败遗留）-> 恢复后补投
rid_retry = make_run(status=RunStatus.FAILED.value, attempts=1)
check("非最终失败任务被补投", rid_retry in dispatch.requeue_interrupted_runs() and rid_retry in fake.list)
check("补投后状态回到 recovering", repo.get("runs", rid_retry)["status"] == RunStatus.RECOVERING.value)
# 最终 failed -> 保持停止，不补投
rid_final = make_run(status=RunStatus.FAILED.value, attempts=domain.MAX_RUN_ATTEMPTS)
check("最终失败任务不补投", rid_final not in dispatch.requeue_interrupted_runs())
check("最终失败状态保持", repo.get("runs", rid_final)["status"] == RunStatus.FAILED.value)
# 已在队列中的 recovering 不重复入队
rid_queued = make_run(status=RunStatus.RECOVERING.value, attempts=1)
qrun = repo.get("runs", rid_queued)
qrun["last_heartbeat"] = (datetime.now(timezone.utc) - timedelta(seconds=domain.RECOVERY_SWEEP_SECONDS + 5)).isoformat()
repo.put("runs", qrun)
fake.rpush(domain.JOB_QUEUE_KEY, rid_queued)
dispatch.requeue_interrupted_runs()
check("队列已有消息时不重复入队", fake.list.count(rid_queued) == 1)

print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
sys.exit(1 if FAIL else 0)
