"""
ETH EMA 预警系统 - 核心监控模块
==================================
架构：后台线程定时从交易所拉数据 → 写入缓存 → API 只读缓存不阻塞
"""
import os
import json
import time
import threading
import logging
from datetime import datetime, timezone, timedelta
from collections import deque

try:
    import requests
except ImportError:
    requests = None
try:
    import yaml
except ImportError:
    yaml = None


# ============================================================
# 基础配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.yaml")
HISTORY_FILE = os.path.join(BASE_DIR, "alert_history.json")

TIMEFRAMES = ["5m", "15m", "30m", "1h", "4h"]
TF_LABELS = {"5m": "5分钟", "15m": "15分钟", "30m": "30分钟", "1h": "1小时", "4h": "4小时"}

# 每个周期需要的 K 线数量（比 EMA 长周期多 50 根，确保计算稳定）
NEED_BARS = {
    "5m": 300, "15m": 300, "30m": 300, "1h": 300, "4h": 300,
}

# 多数据源（按顺序尝试）
APIS = [
    {
        "name": "binance",
        "url": "https://api.binance.com/api/v3/klines",
        "params": lambda tf, limit: {"symbol": "ETHUSDT", "interval": tf, "limit": limit},
        "parse": lambda data: [
            {"t": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
            for k in data
        ],
        "interval_map": {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h"},
    },
    {
        "name": "okx",
        "url": "https://www.okx.com/api/v5/market/history-candles",
        "params": lambda tf, limit: {"instId": "ETH-USDT", "bar": tf, "limit": str(limit)},
        "parse": lambda data: [
            {"t": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
            for k in (data.get("data", []) if isinstance(data, dict) else [])
        ],
        "interval_map": {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "1H", "4h": "4H"},
    },
    {
        "name": "gate",
        "url": "https://api.gateio.ws/api/v4/spot/candlesticks",
        "params": lambda tf, limit: {"currency_pair": "ETH_USDT", "interval": tf, "limit": str(limit)},
        "parse": lambda data: [
            {"t": int(k[0]), "o": float(k[5]), "h": float(k[3]), "l": float(k[4]), "c": float(k[2]), "v": float(k[1])}
            for k in data
        ],
        "interval_map": {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h"},
    },
]


# ============================================================
# 日志
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("eth-ema")


# ============================================================
# 全局缓存 & 锁
# ============================================================
_state_lock = threading.Lock()
_state_cache = {}           # {tf: state_dict}
_last_update_time = 0
_last_source = {}           # {tf: "binance" | "okx" | ...}
_source_fail_count = {}     # {api_name: fail_count}
_monitor_running = False
_monitor_thread = None


# ============================================================
# 配置 & 历史读写
# ============================================================
DEFAULT_CONFIG = {
    "feishu": {
        "webhook": "",
        "price_push_interval_seconds": 30,
        "fixed_push_interval_seconds": 0,
    },
    "ema_alert": {
        "ema_short": 180,
        "ema_long": 250,
        "enabled_timeframes": ["15m", "1h", "4h"],
    },
    "alert": {
        "cooldown_seconds": 600,
    },
    "price_ranges": [],
    "timezone_hours": 8,
}

_alert_cooldown = {}        # {(tf, direction): last_alert_timestamp}
_range_cooldown = {}        # {range_idx: last_alert_timestamp}


def load_config():
    """加载配置文件，不存在则返回默认并写入"""
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                if yaml is not None:
                    loaded = yaml.safe_load(f) or {}
                else:
                    # 退化：用 json 兼容解析
                    content = f.read()
                    try:
                        loaded = json.loads(content)
                    except Exception:
                        loaded = {}
            # 深度合并
            for k, v in loaded.items():
                if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                    cfg[k].update(v)
                else:
                    cfg[k] = v
        except Exception as e:
            logger.warning(f"读取配置文件失败，使用默认配置: {e}")
    else:
        save_config(cfg)
    return cfg


def save_config(cfg):
    try:
        if yaml is not None:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        else:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"写入配置文件失败: {e}")
        return False


def load_history():
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_history(history):
    try:
        # 只保留最近 200 条
        history = list(history[-200:])
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"写入历史记录失败: {e}")
        return False


def append_history(item):
    history = load_history()
    history.append(item)
    save_history(history)


# ============================================================
# 时间工具
# ============================================================
def _now_tz():
    cfg = load_config()
    hours = cfg.get("timezone_hours", 8)
    return datetime.now(timezone(timedelta(hours=hours)))


def format_time_now():
    return _now_tz().strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# K 线获取
# ============================================================
def fetch_klines(tf, limit):
    """拉取指定周期的 K 线。返回 (klines, api_name) 或 (None, None)"""
    if requests is None:
        return None, None

    for api in APIS:
        api_name = api["name"]
        interval = api["interval_map"].get(tf, tf)
        params = api["params"](interval, limit)
        try:
            resp = requests.get(
                api["url"],
                params=params,
                timeout=5,
                headers={"User-Agent": "Mozilla/5.0 eth-ema-alert"},
            )
            resp.raise_for_status()
            data = resp.json()
            parsed = api["parse"](data)
            if len(parsed) >= 50:
                _source_fail_count[api_name] = 0
                return parsed, api_name
            else:
                logger.warning(f"[{tf}] {api_name} 数据不足: {len(parsed)} 根")
                _source_fail_count[api_name] = _source_fail_count.get(api_name, 0) + 1
        except Exception as e:
            logger.warning(f"[{tf}] {api_name} 失败: {e}")
            _source_fail_count[api_name] = _source_fail_count.get(api_name, 0) + 1

    return None, None


# ============================================================
# EMA 计算 & 周期分析
# ============================================================
def compute_ema(values, period):
    if not values or period <= 0:
        return []
    ema = []
    k = 2.0 / (period + 1)
    prev_ema = sum(values[:period]) / period if len(values) >= period else values[0]
    for i, v in enumerate(values):
        if i == 0:
            cur = v
        else:
            cur = v * k + prev_ema * (1 - k)
        ema.append(cur)
        prev_ema = cur
    return ema


def analyze_tf(tf, klines, ema_short, ema_long):
    if not klines or len(klines) < max(ema_short, ema_long) + 10:
        return None
    closes = [k["c"] for k in klines]
    es = compute_ema(closes, ema_short)
    el = compute_ema(closes, ema_long)
    price = float(closes[-1])
    es_val = float(es[-1])
    el_val = float(el[-1])
    low = min(es_val, el_val)
    high = max(es_val, el_val)

    if price > high:
        signal = "long"
    elif price < low:
        signal = "short"
    else:
        signal = "between"

    return {
        "tf": tf,
        "price": round(price, 2),
        "ema_short": round(es_val, 2),
        "ema_long": round(el_val, 2),
        "signal": signal,
    }


# ============================================================
# 飞书推送
# ============================================================
def _send_feishu(webhook, text):
    if not webhook or requests is None:
        return False
    try:
        resp = requests.post(
            webhook,
            json={"msg_type": "text", "content": {"text": text}},
            timeout=5,
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0 or data.get("StatusCode") == 0 or data.get("StatusCode") is None:
                return True
            logger.warning(f"飞书返回异常: {data}")
        return False
    except Exception as e:
        logger.warning(f"飞书推送失败: {e}")
        return False


def test_feishu_push(cfg):
    webhook = cfg.get("feishu", {}).get("webhook", "")
    return _send_feishu(
        webhook,
        f"✅ ETH EMA 预警系统测试消息\n时间: {format_time_now()}",
    )


def send_price_alert(cfg, tf, state, direction="ema"):
    """EMA 触发推送"""
    webhook = cfg.get("feishu", {}).get("webhook", "")
    if not webhook:
        return False
    arrow = "🟢 价格突破区间上沿（看多）" if state["signal"] == "long" else "🔴 价格跌破区间下沿（看空）"
    text = (
        f"ETH EMA 预警 · {TF_LABELS.get(tf, tf)}\n"
        f"{arrow}\n"
        f"当前价格: ${state['price']:.2f}\n"
        f"EMA {cfg['ema_alert']['ema_short']}: ${state['ema_short']:.2f}\n"
        f"EMA {cfg['ema_alert']['ema_long']}: ${state['ema_long']:.2f}\n"
        f"时间: {format_time_now()}"
    )
    return _send_feishu(webhook, text)


def send_range_alert(cfg, rng, price, direction):
    """价格区间触发推送"""
    webhook = cfg.get("feishu", {}).get("webhook", "")
    if not webhook:
        return False
    arrow = "🟢 突破上沿" if direction == "above" else "🔴 跌破下沿"
    text = (
        f"价格区间预警 · {rng.get('name', '区间')}\n"
        f"{arrow}\n"
        f"当前价格: ${price:.2f}\n"
        f"区间: ${rng['low']:.2f} ~ ${rng['high']:.2f}\n"
        f"时间: {format_time_now()}"
    )
    return _send_feishu(webhook, text)


def send_fixed_price_report(cfg, price):
    """定时播报价格"""
    webhook = cfg.get("feishu", {}).get("webhook", "")
    if not webhook:
        return False
    text = f"ETH/USDT 实时价格: ${price:,.2f} · {format_time_now()}"
    return _send_feishu(webhook, text)


# ============================================================
# 预警判定 & 冷却
# ============================================================
def check_ema_alert(tf, state, cfg):
    """检查 EMA 预警，冷却 10 分钟"""
    if tf not in (cfg.get("ema_alert", {}).get("enabled_timeframes", []) or []):
        return
    signal = state.get("signal")
    if signal not in ("long", "short"):
        return
    key = (tf, signal)
    cooldown = cfg.get("alert", {}).get("cooldown_seconds", 600)
    now = time.time()
    if now - _alert_cooldown.get(key, 0) < cooldown:
        return
    ok = send_price_alert(cfg, tf, state)
    if ok:
        _alert_cooldown[key] = now
        append_history({
            "time": format_time_now(),
            "type": "ema",
            "tf": tf,
            "price": state["price"],
            "signal": signal,
            "ema_short": state["ema_short"],
            "ema_long": state["ema_long"],
        })


def check_range_alerts(cfg, price):
    """检查价格区间预警，每个区间独立冷却"""
    ranges = cfg.get("price_ranges", []) or []
    now = time.time()
    cooldown = cfg.get("alert", {}).get("cooldown_seconds", 600)
    for i, rng in enumerate(ranges):
        if not rng.get("enabled", True):
            continue
        # 缓冲带：区间上下各 +10 美元作为缓冲
        buf_low = float(rng["low"]) - 10
        buf_high = float(rng["high"]) + 10
        direction = None
        if price > buf_high:
            direction = "above"
        elif price < buf_low:
            direction = "below"
        if direction is None:
            continue
        key = (i, direction)
        if now - _range_cooldown.get(key, 0) < cooldown:
            continue
        ok = send_range_alert(cfg, rng, price, direction)
        if ok:
            _range_cooldown[key] = now
            append_history({
                "time": format_time_now(),
                "type": "range",
                "price": price,
                "range_low": rng["low"],
                "range_high": rng["high"],
                "range_name": rng.get("name", ""),
                "direction": direction,
            })


# ============================================================
# 定时播报对齐
# ============================================================
_last_fixed_push = 0

def get_next_fixed_push_time(cfg):
    """返回 'HH:MM:SS' 字符串或空"""
    interval = cfg.get("feishu", {}).get("fixed_push_interval_seconds", 0) or 0
    if interval <= 0:
        return ""
    now = _now_tz()
    # 从当日 0:00 开始对齐
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_today = int((now - start_of_day).total_seconds())
    next_slot_seconds = ((seconds_today // interval) + 1) * interval
    if next_slot_seconds >= 86400:
        next_slot_seconds = 0
    h = next_slot_seconds // 3600
    m = (next_slot_seconds % 3600) // 60
    s = next_slot_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def check_fixed_push(cfg, price):
    """检查是否到达定时播报时间点"""
    global _last_fixed_push
    interval = cfg.get("feishu", {}).get("fixed_push_interval_seconds", 0) or 0
    if interval <= 0:
        return
    now = _now_tz()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_today = int((now - start_of_day).total_seconds())
    if seconds_today % interval < 30 and (time.time() - _last_fixed_push) > interval - 10:
        ok = send_fixed_price_report(cfg, price)
        if ok:
            _last_fixed_push = time.time()


# ============================================================
# 核心刷新逻辑
# ============================================================
def update_all_data():
    """拉一次所有周期数据并更新缓存"""
    global _last_update_time
    cfg = load_config()
    enabled = cfg.get("ema_alert", {}).get("enabled_timeframes", [])
    ema_short = cfg.get("ema_alert", {}).get("ema_short", 180)
    ema_long = cfg.get("ema_alert", {}).get("ema_long", 250)

    new_price = None
    for tf in TIMEFRAMES:
        klines, source = fetch_klines(tf, NEED_BARS.get(tf, 300))
        if klines is None:
            continue
        _last_source[tf] = source
        result = analyze_tf(tf, klines, ema_short, ema_long)
        if result:
            result["data_source"] = source
            with _state_lock:
                _state_cache[tf] = result
            if tf == "5m":
                new_price = result["price"]
            # EMA 预警
            if tf in enabled:
                check_ema_alert(tf, result, cfg)

    # 价格区间预警
    if new_price is not None:
        check_range_alerts(cfg, new_price)
        # 定时播报
        check_fixed_push(cfg, new_price)

    _last_update_time = time.time()
    logger.info(f"✅ 数据更新完成 - ETH=${new_price:.2f}" if new_price else "⚠️ 更新完成但未获取到价格")


def _monitor_loop():
    """后台监控循环：每 30 秒刷新一次"""
    while True:
        try:
            update_all_data()
        except Exception as e:
            logger.exception(f"监控循环异常: {e}")
        time.sleep(30)


# ============================================================
# 对外 API（被 web_app.py 调用）
# ============================================================
def start_monitor_in_background():
    """启动后台监控线程（幂等：已启动就不重复启动）"""
    global _monitor_running, _monitor_thread
    with _state_lock:
        if _monitor_running and _monitor_thread and _monitor_thread.is_alive():
            return
        _monitor_thread = threading.Thread(target=_monitor_loop, daemon=True)
        _monitor_thread.start()
        _monitor_running = True
    logger.info("🚀 后台监控线程启动")


def ensure_monitor_running():
    """如果线程死了或未启动，重新启动"""
    global _monitor_running, _monitor_thread
    if not _monitor_running or _monitor_thread is None or not _monitor_thread.is_alive():
        logger.info("🔄 监控线程未运行，正在重新启动")
        start_monitor_in_background()


def get_all_states():
    with _state_lock:
        return dict(_state_cache)


def get_last_update_time():
    return _last_update_time


def get_connection_status():
    """返回 '已连接' / '连接稍慢' / '连接异常'"""
    age = time.time() - _last_update_time
    if _last_update_time == 0:
        return "正在连接..."
    if age < 60:
        return "已连接"
    elif age < 180:
        return "连接稍慢"
    else:
        return "连接异常"


def get_source_health():
    """返回各数据源失败次数"""
    return dict(_source_fail_count)


# ============================================================
# 直接运行（调试）
# ============================================================
if __name__ == "__main__":
    print("ETH EMA 监控模块 - 直接运行会进入循环拉取模式")
    print("按 Ctrl+C 退出\n")
    start_monitor_in_background()
    try:
        while True:
            time.sleep(5)
            states = get_all_states()
            for tf in TIMEFRAMES:
                s = states.get(tf)
                if s:
                    print(f"  [{tf}] ${s['price']:.2f} (EMA{s['ema_short']:.0f}/${s['ema_long']:.0f}) [{s.get('data_source','?')}]")
    except KeyboardInterrupt:
        print("\n已停止")
