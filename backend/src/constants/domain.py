MAX_NODES=200
MAX_EDGES=2000
MAX_QUEUED=10
EDGE_DIRECTION='citing_to_cited'
# 中断恢复：单次运行最多尝试次数（首次执行 + 自动重试），超过即最终失败
MAX_RUN_ATTEMPTS=3
# 运行租约（秒）：超过该时长仍未更新心跳的 running 任务视为中断，可被恢复
RUN_LEASE_SECONDS=120
# worker 启动/轮询时扫描中断任务的间隔（秒）
RECOVERY_SWEEP_SECONDS=15
# 运行级互斥锁与任务队列的 Redis 键
JOB_QUEUE_KEY="citation-influence:jobs"
RUN_LOCK_KEY="citation-influence:run-lock:{run_id}"
RUN_LOCK_TTL=180
