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

# ======= 兜底工具：调用 monitor 函数前检查是否存在 =======
def _has(name):
    return hasattr(mon, name) and callable(getattr(mon, name, None))

def _safe_call(fn_name, *args, default=None):
    """安全调用 monitor 里的函数，不存在时返回默认值"""
    if _has(fn_name):
        try:
            return getattr(mon, fn_name)(*args)
        except Exception as e:
            _log(f"[safe_call] {fn_name} 失败: {e}")
    return default

def _log(msg):
    if _has("logger"):
        try:
            mon.logger.warning(msg)
            return
        except Exception:
            pass
    print(msg)

# ========= 默认返回值 =========
def _default_state_response():
    return {
        "states": {},
        "status": "正在连接...",
        "source_health": {},
        "last_update": 0,
        "update_time_str": "",
        "price_push_interval": 30,
        "fixed_push_interval": 0,
        "next_fixed_push_time": "",
        "latest_price": None,
        "price_ranges": [],
        "config": {
            "feishu": {"webhook": "", "price_push_interval_seconds": 30, "fixed_push_interval_seconds": 0},
            "ema_alert": {"ema_short": 180, "ema_long": 250, "enabled_timeframes": ["15m", "1h", "4h"]},
            "alert": {"cooldown_seconds": 600},
            "price_ranges": [],
            "timezone_hours": 8,
        },
        "data_age_sec": 999999,
        "has_data": False,
    }



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
        # 1) 尝试启动/重启监控线程
        _safe_call("ensure_monitor_running")

        # 2) 安全地读取配置（缺字段也不崩）
        cfg = _safe_call("load_config")
        if not isinstance(cfg, dict):
            cfg = {}

        # 3) 获取数据
        states = _safe_call("get_all_states") or {}
        last_update = _safe_call("get_last_update_time") or 0
        status = _safe_call("get_connection_status") or "正在连接..."
        source_health = _safe_call("get_source_health") or {}
        next_fixed = _safe_call("get_next_fixed_push_time", cfg) if isinstance(cfg, dict) else ""

        # 最新价格
        latest_price = None
        if isinstance(states, dict) and states.get("5m"):
            latest_price = states["5m"].get("price")

        now = time.time()
        data_age = int(now - last_update) if last_update > 0 else 999999
        update_time_str = ""
        if last_update > 0:
            update_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_update))

        feishu = cfg.get("feishu", {}) if isinstance(cfg, dict) else {}

        return jsonify({
            "states": states,
            "status": status,
            "source_health": source_health,
            "last_update": last_update,
            "update_time_str": update_time_str,
            "price_push_interval": feishu.get("price_push_interval_seconds", 30),
            "fixed_push_interval": feishu.get("fixed_push_interval_seconds", 0),
            "next_fixed_push_time": next_fixed,
            "latest_price": latest_price,
            "price_ranges": cfg.get("price_ranges", []) if isinstance(cfg, dict) else [],
            "config": cfg,
            "data_age_sec": data_age,
            "has_data": latest_price is not None,
        })
    except Exception as e:
        _log(f"[api_state] 异常: {e}")
        # 兜底：返回一个合法的 JSON，前端不会白屏
        return jsonify(_default_state_response()), 200

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

        cfg = _safe_call("load_config")
        if not isinstance(cfg, dict):
            cfg = {}
        if "feishu" not in cfg or not isinstance(cfg["feishu"], dict):
            cfg["feishu"] = {}
        cfg["feishu"]["webhook"] = webhook
        cfg["feishu"]["price_push_interval_seconds"] = push_interval
        cfg["feishu"]["fixed_push_interval_seconds"] = fixed_push_interval

        if "ema_alert" not in cfg or not isinstance(cfg["ema_alert"], dict):
            cfg["ema_alert"] = {}
        cfg["ema_alert"]["ema_short"] = ema_short
        cfg["ema_alert"]["ema_long"] = ema_long
        cfg["ema_alert"]["enabled_timeframes"] = enabled_timeframes

        if "alert" not in cfg or not isinstance(cfg["alert"], dict):
            cfg["alert"] = {}
        cfg["alert"]["cooldown_seconds"] = cooldown_seconds

        if data.get("price_ranges") is not None:
            cfg["price_ranges"] = data["price_ranges"]

        _safe_call("save_config", cfg)
        return jsonify({"success": True, "message": "配置已保存", "config": cfg})
    except Exception as e:
        _log(f"[api_save_config] 异常: {e}")
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

        cfg = _safe_call("load_config")
        if not isinstance(cfg, dict):
            cfg = {}
        if "price_ranges" not in cfg or not isinstance(cfg.get("price_ranges"), list):
            cfg["price_ranges"] = []

        new_range = {
            "low": round(low, 2),
            "high": round(high, 2),
            "enabled": enabled,
            "name": note or f"区间 {low:.0f}-{high:.0f}",
        }
        cfg["price_ranges"].append(new_range)
        _safe_call("save_config", cfg)
        return jsonify({"success": True, "message": "已添加", "price_ranges": cfg["price_ranges"]})
    except Exception as e:
        _log(f"[api_save_price_range] 异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 删除价格区间 =========
@app.route("/api/delete_price_range", methods=["POST"])
def api_delete_price_range():
    try:
        data = request.get_json() or {}
        idx = int(data.get("index", -1))
        cfg = _safe_call("load_config")
        if not isinstance(cfg, dict):
            cfg = {"price_ranges": []}
        ranges = cfg.get("price_ranges", []) or []
        if 0 <= idx < len(ranges):
            del ranges[idx]
            cfg["price_ranges"] = ranges
            _safe_call("save_config", cfg)
            return jsonify({"success": True, "price_ranges": ranges})
        return jsonify({"success": False, "error": "无效的索引"}), 400
    except Exception as e:
        _log(f"[api_delete_price_range] 异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 切换价格区间开启状态 =========
@app.route("/api/toggle_price_range", methods=["POST"])
def api_toggle_price_range():
    try:
        data = request.get_json() or {}
        idx = int(data.get("index", -1))
        cfg = _safe_call("load_config")
        if not isinstance(cfg, dict):
            cfg = {"price_ranges": []}
        ranges = cfg.get("price_ranges", []) or []
        if 0 <= idx < len(ranges):
            ranges[idx]["enabled"] = not bool(ranges[idx].get("enabled", True))
            cfg["price_ranges"] = ranges
            _safe_call("save_config", cfg)
            return jsonify({"success": True, "price_ranges": ranges})
        return jsonify({"success": False, "error": "无效的索引"}), 400
    except Exception as e:
        _log(f"[api_toggle_price_range] 异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 测试飞书推送 =========
@app.route("/api/test_alert")
def api_test_alert():
    try:
        cfg = _safe_call("load_config")
        if not isinstance(cfg, dict):
            cfg = {}
        ok = bool(_safe_call("test_feishu_push", cfg))
        return jsonify({"success": ok, "message": "已发送测试消息到飞书" if ok else "发送失败，请检查 Webhook"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 历史记录列表 =========
@app.route("/api/history")
def api_history():
    try:
        history = _safe_call("load_history") or []
        return jsonify({"history": history, "total": len(history)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# ========= API: 删除单条历史 =========
@app.route("/api/delete_alert", methods=["POST"])
def api_delete_alert():
    try:
        data = request.get_json() or {}
        idx = int(data.get("index", -1))
        history = _safe_call("load_history") or []
        if 0 <= idx < len(history):
            del history[idx]
            _safe_call("save_history", history)
            return jsonify({"success": True, "total": len(history)})
        return jsonify({"success": False, "error": "无效的索引"}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# ========= API: 清空历史 =========
@app.route("/api/clear_history", methods=["POST"])
def api_clear_history():
    try:
        _safe_call("save_history", [])
        return jsonify({"success": True, "total": 0})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

# ========= 直接启动 =========
if __name__ == "__main__":
    _safe_call("start_monitor_in_background")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
