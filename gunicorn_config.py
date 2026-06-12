"""
Gunicorn 配置（用于 Render 云端部署）
关键点：gunicorn 通过 fork 创建 worker 进程，
worker 进程需要在 post_fork 阶段启动监控线程。
"""
import os

bind = "0.0.0.0:" + os.environ.get("PORT", "5000")
workers = 1                   # Render 低配置建议 1 个 worker，避免数据竞争
worker_class = "sync"         # 最简单的 worker 类型
timeout = 120
keepalive = 5
accesslog = "-"
errorlog = "-"
loglevel = "info"


def post_fork(server, worker):
    """gunicorn fork 出新 worker 后调用此函数"""
    try:
        import monitor as mon
        mon.start_monitor_in_background()
        server.log.info(f"✅ Worker PID={worker.pid} 启动，监控线程已就绪")
    except Exception as e:
        server.log.exception(f"❌ post_fork 启动监控线程失败: {e}")


def when_ready(server):
    server.log.info("🚀 ETH EMA 预警系统 Web 服务启动完成")
