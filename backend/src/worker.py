import logging
import time
from pathlib import Path

import redis

from src.config.settings import REDIS_URL
from src.constants import domain
from src.services import dispatch
from src.services.run import execute
from src.utils.identity import new_id
from src.utils.repository import init_db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


def main():
    init_db()
    client = redis.Redis.from_url(REDIS_URL)
    worker_id = new_id("worker")
    last_sweep = 0.0
    # 启动恢复：上次 worker 被强杀后，租约过期的 running 任务立即重新入队
    _safe_sweep()
    while True:
        Path("/tmp/worker-heartbeat").write_text(str(time.time()))
        try:
            item = client.blpop(domain.JOB_QUEUE_KEY, timeout=3)
            if item:
                run_id = item[1].decode()
                log.info("worker %s picked run %s", worker_id, run_id)
                execute(run_id, owner=f"{worker_id}:{run_id}")
        except Exception:
            log.exception("worker failure")
            time.sleep(2)
        if time.time() - last_sweep >= domain.RECOVERY_SWEEP_SECONDS:
            _safe_sweep()
            last_sweep = time.time()


def _safe_sweep():
    try:
        recovered = dispatch.requeue_interrupted_runs()
        if recovered:
            log.info("recovered interrupted runs: %s", ", ".join(recovered))
    except Exception:
        log.exception("recovery sweep failed")


if __name__ == "__main__":
    main()
