"""
ETH EMA 预警系统 - Flask Web 入口
- 页面路由：/ (仪表盘) /settings /history
- API 路由：/api/state /api/save_config /api/save_price_range /api/delete_range /api/test_alert /api/history /api/delete_alert /api/clear_history
- 数据永不阻塞：API 只读缓存，后台线程定时刷新
- Render 兼容：gunicorn 启动时通过 gunicorn_config.py 的 post_fork 启动监控线程
"""
import os
import time
from flask import Flask, render_template, request, jsonify

import monitor as mon

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
)
app.secret_key = "eth-ema-alert-secret-key-v2"

# ========= 页面路由 =========
@app.route("/")
def index():
    return render_template("dashboard.html")

@app.route("/settings")
def settings():
    return render_template("settings.html")

@app.route("/history")
def history():
    return render_template("history.html")

# ========= API: 获取系统状态 =========
@app.route("/api/state")
def api_state():
    try:
        cfg = mon.load_config()
        # 确保监控线程在跑（第一次请求或 gunicorn fork 后启动）
        mon.ensure_monitor_running()

        states = mon.get_all_states()
        last_update = mon.get_last_update_time()
        status = mon.get_connection_status()
        source_health = mon.get_source_health()

        # 计算下一次固定时间点推送
        next_fixed = mon.get_next_fixed_push_time(cfg)

        # 最新价格（从 5m 周期取）
        latest_price = None
        if states.get("5m"):
            latest_price = states["5m"].get("price")

        now = time.time()
        data_age = int(now - last_update) if last_update > 0 else 999999
        update_time_str = ""
        if last_update > 0:
            update_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_update))

        return jsonify({
            "states": states,
            "status": status,
            "source_health": source_health,
            "last_update": last_update,
            "update_time_str": update_time_str,
            "price_push_interval": cfg.get("feishu", {}).get("price_push_interval_seconds", 30),
            "fixed_push_interval": cfg.get("feishu", {}).get("fixed_push_interval_seconds", 0),
            "next_fixed_push_time": next_fixed,
            "latest_price": latest_price,
            "price_ranges": cfg.get("price_ranges", []),
            "config": cfg,
            "data_age_sec": data_age,
            "has_data": latest_price is not None,
        })
    except Exception as e:
        mon.logger.exception(f"[api_state] 异常: {e}")
        return jsonify({"error": str(e), "has_data": False}), 500

# ========= API: 保存整体配置 =========
@app.route("/api/save_config", methods=["POST"])
def api_save_config():
    try:
        data = request.get_json() or {}
        webhook = str(data.get("feishu_webhook", "")).strip()
        ema_short = int(data.get("ema_short", 180))
        ema_long = int(data.get("ema_long", 250))
        push_interval = int(data.get("push_interval", 30))
        fixed_push_interval = int(data.get("fixed_push_interval", 0))
        cooldown_seconds = int(data.get("cooldown_seconds", 600))
        enabled_timeframes = data.get("enabled_timeframes", []) or []

        if ema_short >= ema_long:
            return jsonify({"success": False, "error": "EMA短周期必须小于长周期"}), 400
        if not isinstance(enabled_timeframes, list):
            enabled_timeframes = []

        cfg = mon.load_config()
        if "feishu" not in cfg:
            cfg["feishu"] = {}
        cfg["feishu"]["webhook"] = webhook
        cfg["feishu"]["price_push_interval_seconds"] = push_interval
        cfg["feishu"]["fixed_push_interval_seconds"] = fixed_push_interval

        if "ema_alert" not in cfg:
            cfg["ema_alert"] = {}
        cfg["ema_alert"]["ema_short"] = ema_short
        cfg["ema_alert"]["ema_long"] = ema_long
        cfg["ema_alert"]["enabled_timeframes"] = enabled_timeframes

        if "alert" not in cfg:
            cfg["alert"] = {}
        cfg["alert"]["cooldown_seconds"] = cooldown_seconds

        # 保留原有的 price_ranges（如果前端没带）
        if data.get("price_ranges") is not None:
            cfg["price_ranges"] = data["price_ranges"]

        mon.save_config(cfg)
        return jsonify({"success": True, "message": "配置已保存", "config": cfg})
    except Exception as e:
        mon.logger.exception(f"[api_save_config] 异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 保存价格区间 =========
@app.route("/api/save_price_range", methods=["POST"])
def api_save_price_range():
    try:
        data = request.get_json() or {}
        low = float(data.get("low", 0))
        high = float(data.get("high", 0))
        enabled = bool(data.get("enabled", True))
        note = str(data.get("name", "")).strip()
        if low <= 0 or high <= 0 or low >= high:
            return jsonify({"success": False, "error": "无效的价格区间"}), 400

        cfg = mon.load_config()
        if "price_ranges" not in cfg or not isinstance(cfg.get("price_ranges"), list):
            cfg["price_ranges"] = []

        new_range = {
            "low": round(low, 2),
            "high": round(high, 2),
            "enabled": enabled,
            "name": note or f"区间 {low:.0f}-{high:.0f}",
        }
        cfg["price_ranges"].append(new_range)
        mon.save_config(cfg)
        return jsonify({"success": True, "message": "已添加", "price_ranges": cfg["price_ranges"]})
    except Exception as e:
        mon.logger.exception(f"[api_save_price_range] 异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 删除价格区间 =========
@app.route("/api/delete_price_range", methods=["POST"])
def api_delete_price_range():
    try:
        data = request.get_json() or {}
        idx = int(data.get("index", -1))
        cfg = mon.load_config()
        ranges = cfg.get("price_ranges", []) or []
        if 0 <= idx < len(ranges):
            del ranges[idx]
            cfg["price_ranges"] = ranges
            mon.save_config(cfg)
            return jsonify({"success": True, "price_ranges": ranges})
        return jsonify({"success": False, "error": "无效的索引"}), 400
    except Exception as e:
        mon.logger.exception(f"[api_delete_price_range] 异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 切换价格区间开启状态 =========
@app.route("/api/toggle_price_range", methods=["POST"])
def api_toggle_price_range():
    try:
        data = request.get_json() or {}
        idx = int(data.get("index", -1))
        cfg = mon.load_config()
        ranges = cfg.get("price_ranges", []) or []
        if 0 <= idx < len(ranges):
            ranges[idx]["enabled"] = not bool(ranges[idx].get("enabled", True))
            cfg["price_ranges"] = ranges
            mon.save_config(cfg)
            return jsonify({"success": True, "price_ranges": ranges})
        return jsonify({"success": False, "error": "无效的索引"}), 400
    except Exception as e:
        mon.logger.exception(f"[api_toggle_price_range] 异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 测试飞书推送 =========
@app.route("/api/test_alert")
def api_test_alert():
    try:
        cfg = mon.load_config()
        ok = mon.test_feishu_push(cfg)
        return jsonify({"success": ok, "message": "已发送测试消息到飞书" if ok else "发送失败，请检查 Webhook"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 历史记录列表 =========
@app.route("/api/history")
def api_history():
    try:
        history = mon.load_history()
        return jsonify({"history": history, "total": len(history)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ========= API: 删除单条历史 =========
@app.route("/api/delete_alert", methods=["POST"])
def api_delete_alert():
    try:
        data = request.get_json() or {}
        idx = int(data.get("index", -1))
        history = mon.load_history()
        if 0 <= idx < len(history):
            del history[idx]
            mon.save_history(history)
            return jsonify({"success": True, "total": len(history)})
        return jsonify({"success": False, "error": "无效的索引"}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 清空历史 =========
@app.route("/api/clear_history", methods=["POST"])
def api_clear_history():
    try:
        mon.save_history([])
        return jsonify({"success": True, "total": 0})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# ========= 直接启动 =========
if __name__ == "__main__":
    mon.start_monitor_in_background()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
