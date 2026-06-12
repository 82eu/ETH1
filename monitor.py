"""
ETH EMA 预警系统 - 核心监控模块
功能：多交易所数据获取、EMA计算、信号判定、飞书推送、
      固定时间点价格播报、缓冲带防重复告警
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from copy import deepcopy

# ============== 依赖 ==============
try:
    import yaml
    import requests
except ImportError:
    print("[ERROR] 缺少依赖：pip install pyyaml requests")
    raise


def _new_session():
    """每次请求用全新 Session，避免长连接状态污染。"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    # 关掉默认的 keep-alive，避免 stale connection
    s.keep_alive = False
    return s

# ============== 常量 ==============
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.yaml")
HISTORY_FILE = os.path.join(BASE_DIR, "alert_history.json")

TF_LABELS = {
    "5m": "5分钟",
    "15m": "15分钟",
    "30m": "30分钟",
    "1h": "1小时",
    "4h": "4小时",
}

# 各周期需要的 K 线根数（够算 EMA 250，且在 API 限制内）
TF_NEED_BARS = {
    "5m": 300,
    "15m": 300,
    "30m": 300,
    "1h": 300,
    "4h": 300,
}

# 数据刷新间隔（秒）
DATA_REFRESH_INTERVAL = 30

# 缓冲带宽度（美元）- 告警触发后扩大区间防止重复告警
BUFF_BAND = 10.0

# ============== 默认配置 ==============
DEFAULT_CONFIG = {
    "feishu": {
        "webhook": "",
        "price_push_interval_seconds": 30,   # 简单间隔推送（保留30秒仅测试）
        "fixed_push_interval_seconds": 0,    # 固定时间点推送，0=关
    },
    "ema_alert": {
        "ema_short": 180,
        "ema_long": 250,
        "enabled_timeframes": ["5m", "15m", "30m", "1h", "4h"],
    },
    "alert": {
        "cooldown_seconds": 600,  # 同一周期冷却时间
    },
    "price_ranges": [],  # 例：[{"low":1600, "high":1650, "enabled":true, "name":"1600-1650区间"}]
}

# ============== 日志 ==============
logger = logging.getLogger("eth_monitor")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

# ============== 线程安全的状态 ==============
_state_cache = {}           # 各周期的最新状态
_state_lock = threading.Lock()
_config_lock = threading.Lock()
_last_update_time = 0
_monitor_started = False
_monitor_thread = None
_monitor_stop = threading.Event()

# 各周期的缓冲带状态：{tf: {"env_low": float, "env_high": float}}
_zone_tracker = {}

# 价格区间缓冲带：{idx: {"env_low": float, "env_high": float}}
_price_range_tracker = {}

# 简单间隔推送的上次时间
_last_simple_push_time = 0

# 固定时间点推送的上次推送时间槽："YYYY-MM-DD-slot_index"
_last_fixed_push_slot = None

# 最近一次价格
_last_price_value = 0

# 数据源健康度：{source_name: bool}
_source_health = {}

# 最后使用的数据源
_last_source = {}


# ============================================================
# 配置读写
# ============================================================
def load_config():
    """读取配置，不存在则创建默认"""
    cfg = deepcopy(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
            if isinstance(loaded, dict):
                _deep_merge(cfg, loaded)
        except Exception as e:
            logger.warning(f"读取配置失败，使用默认: {e}")
    else:
        save_config(cfg)
    return cfg


def save_config(cfg):
    try:
        with _config_lock:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)
        return True
    except Exception as e:
        logger.error(f"保存配置失败: {e}")
        return False


def _deep_merge(target, source):
    """深合并 dict"""
    for k, v in source.items():
        if isinstance(v, dict) and isinstance(target.get(k), dict):
            _deep_merge(target[k], v)
        else:
            target[k] = v


# ============================================================
# 数据获取 - Gate.io 主
# ============================================================
def _fetch_gateio_klines(tf_symbol, tf_seconds, limit):
    """从 Gate.io 获取K线。返回 [(ts, open, high, low, close, vol), ...] 按时间升序"""
    interval_map = {
        300: "5m",
        900: "15m",
        1800: "30m",
        3600: "1h",
        14400: "4h",
    }
    interval = interval_map.get(tf_seconds)
    if not interval:
        return None

    url = "https://api.gateio.ws/api/v4/spot/candlesticks"
    # Gate.io 最大支持 500 根；这里用 300 作为上限避免偶发 400
    params = {"currency_pair": tf_symbol, "interval": interval, "limit": min(limit, 300)}
    resp = _new_session().get(url, params=params, timeout=10)
    if resp.status_code != 200:
        logger.debug(f"Gate.io HTTP {resp.status_code}: {resp.text[:200]}")
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or len(data) == 0:
        return None

    bars = []
    for row in data:
        try:
            ts = int(float(row[0]))
            op = float(row[5])
            hi = float(row[3])
            lo = float(row[4])
            cl = float(row[2])
            vo = float(row[6]) if len(row) > 6 else 0
            bars.append((ts, op, hi, lo, cl, vo))
        except (IndexError, ValueError, TypeError):
            continue
    bars.sort(key=lambda x: x[0])
    return bars


def _fetch_okx_klines(tf_symbol, tf_seconds, limit):
    """从 OKX 获取K线（备用）"""
    interval_map = {
        300: "5m",
        900: "15m",
        1800: "30m",
        3600: "1H",
        14400: "4H",
    }
    interval = interval_map.get(tf_seconds)
    if not interval:
        return None

    url = "https://www.okx.com/api/v5/market/candles"
    params = {"instId": tf_symbol, "bar": interval, "limit": min(limit, 300)}
    resp = _new_session().get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("data"):
        return None

    bars = []
    for row in data["data"]:
        try:
            ts = int(float(row[0]) / 1000)
            op = float(row[1])
            hi = float(row[2])
            lo = float(row[3])
            cl = float(row[4])
            vo = float(row[5]) if len(row) > 5 else 0
            bars.append((ts, op, hi, lo, cl, vo))
        except (IndexError, ValueError, TypeError):
            continue
    bars.sort(key=lambda x: x[0])
    return bars


def _fetch_binance_klines(tf_symbol, tf_seconds, limit):
    """从 Binance 获取K线（第三备用，国际访问性好）"""
    interval_map = {
        300: "5m",
        900: "15m",
        1800: "30m",
        3600: "1h",
        14400: "4h",
    }
    interval = interval_map.get(tf_seconds)
    if not interval:
        return None

    # tf_symbol 格式 ETH_USDT -> ETHUSDT
    symbol = tf_symbol.replace("_", "")
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": min(limit, 500)}
    resp = _new_session().get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or len(data) == 0:
        return None

    bars = []
    for row in data:
        try:
            ts = int(row[0] / 1000)
            op = float(row[1])
            hi = float(row[2])
            lo = float(row[3])
            cl = float(row[4])
            vo = float(row[5])
            bars.append((ts, op, hi, lo, cl, vo))
        except (IndexError, ValueError, TypeError):
            continue
    return bars


def fetch_klines(tf_symbol, tf_seconds, limit=300):
    """多源获取 K线：Gate.io -> OKX -> Binance，拉长请求间距避免限流。"""
    sources = [
        ("gateio", lambda: _fetch_gateio_klines(tf_symbol, tf_seconds, limit)),
        ("okx", lambda: _fetch_okx_klines(tf_symbol, tf_seconds, limit)),
        ("binance", lambda: _fetch_binance_klines(tf_symbol, tf_seconds, limit)),
    ]
    for name, fetch_fn in sources:
        for attempt in range(2):
            try:
                bars = fetch_fn()
                if bars and len(bars) >= 50:
                    _source_health[name] = True
                    return bars, name
                logger.warning(f"{name} 数据不足: {len(bars) if bars else 0} 根")
            except Exception as e:
                _source_health[name] = False
                logger.warning(f"{name} 尝试{attempt+1}失败: {e}")
            time.sleep(1.0)

    return None, None


# ============================================================
# EMA 计算
# ============================================================
def compute_ema(values, period):
    """计算 EMA，返回与输入等长的列表"""
    if not values or len(values) == 0:
        return []
    k = 2.0 / (period + 1)
    ema = []
    prev_ema = float(values[0])
    for v in values:
        cur = float(v)
        prev_ema = cur * k + prev_ema * (1 - k)
        ema.append(prev_ema)
    return ema


# ============================================================
# 核心分析
# ============================================================
def analyze_tf(tf, klines, ema_short, ema_long):
    """分析单个周期，返回状态 dict"""
    if not klines or len(klines) < max(ema_short, ema_long) + 10:
        return None

    closes = [k[4] for k in klines]
    es = compute_ema(closes, ema_short)
    el = compute_ema(closes, ema_long)

    price = float(closes[-1])
    ema_s_val = float(es[-1])
    ema_l_val = float(el[-1])
    low = min(ema_s_val, ema_l_val)
    high = max(ema_s_val, ema_l_val)

    # 信号
    if price > high:
        signal = "long"
        signal_text = "🟢 开多"
    elif price < low:
        signal = "short"
        signal_text = "🔴 开空"
    else:
        signal = "between"
        signal_text = "🟡 观望"

    # 均线排列
    if ema_s_val > ema_l_val:
        arrangement = "多头排列"
    elif ema_s_val < ema_l_val:
        arrangement = "空头排列"
    else:
        arrangement = "平行"

    in_zone = (low <= price <= high)

    return {
        "tf": tf,
        "price": price,
        "ema_short": round(ema_s_val, 2),
        "ema_long": round(ema_l_val, 2),
        "ema_low": round(low, 2),
        "ema_high": round(high, 2),
        "signal": signal,
        "signal_text": signal_text,
        "arrangement": arrangement,
        "in_zone": in_zone,
        "position": "EMA下方" if price < low else ("EMA上方" if price > high else "EMA之间"),
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ============================================================
# 飞书推送
# ============================================================
def send_feishu(title, content, cfg):
    webhook = cfg.get("feishu", {}).get("webhook", "").strip()
    if not webhook:
        logger.warning("飞书 Webhook 未配置，跳过推送")
        return False

    try:
        msg = {
            "msg_type": "text",
            "content": {"text": f"{title}\n\n{content}"}
        }
        resp = requests.post(webhook, json=msg, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == 0 or data.get("StatusCode") == 0:
                logger.info(f"✅ 飞书推送成功: {title}")
                return True
            logger.warning(f"飞书返回异常: {data}")
        else:
            logger.warning(f"飞书 HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"飞书推送异常: {e}")
    return False


# ============================================================
# 历史记录
# ============================================================
def load_history():
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"读取历史失败: {e}")
    return []


def save_history(history):
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.warning(f"保存历史失败: {e}")
        return False


def add_alert_record(tf, price, ema_short, ema_long, signal, position, alert_type="ema_alert", note=None):
    history = load_history()
    record = {
        "timestamp": time.time(),
        "time_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "timeframe": tf,
        "price": round(float(price), 2),
        "ema_short": round(float(ema_short), 2),
        "ema_long": round(float(ema_long), 2),
        "signal": signal,
        "position": position,
        "alert_type": alert_type,
        "note": note or "",
    }
    history.insert(0, record)
    # 只保留最近 500 条
    if len(history) > 500:
        history = history[:500]
    save_history(history)


# ============================================================
# 推送时间判定
# ============================================================
def _beijing_now():
    return datetime.utcnow() + timedelta(hours=8)


def _should_simple_push(cfg):
    """简单间隔推送：仅用于测试数据是否刷新"""
    interval = cfg.get("feishu", {}).get("price_push_interval_seconds", 0)
    if not interval or interval <= 0:
        return False
    global _last_simple_push_time
    now = time.time()
    return (now - _last_simple_push_time) >= interval


def _should_fixed_push(cfg):
    """固定时间点推送：从北京时间8:00起对齐时间槽"""
    interval = cfg.get("feishu", {}).get("fixed_push_interval_seconds", 0)
    if not interval or interval <= 0:
        return False

    bj_now = _beijing_now()
    bj_8am = bj_now.replace(hour=8, minute=0, second=0, microsecond=0)
    if bj_now < bj_8am:
        bj_8am = bj_8am - timedelta(days=1)

    seconds_from_8am = (bj_now - bj_8am).total_seconds()
    slot_index = int(seconds_from_8am // interval)

    date_str = bj_8am.strftime("%Y-%m-%d")
    current_slot = f"{date_str}-{slot_index}"

    global _last_fixed_push_slot
    if current_slot != _last_fixed_push_slot:
        # 必须确保真正跨过了时间槽（避免整点前几秒误触发）
        elapsed = seconds_from_8am - slot_index * interval
        if elapsed < 30:  # 允许有30秒内的延迟
            _last_fixed_push_slot = current_slot
            return True
    return False


# ============================================================
# 告警检查
# ============================================================
def check_ema_alert(tf, result, cfg):
    """EMA 区间告警，带缓冲带"""
    if not result:
        return
    cooldown = cfg.get("alert", {}).get("cooldown_seconds", 600)
    price = float(result["price"])
    ema_low = float(result["ema_low"])
    ema_high = float(result["ema_high"])

    tracker = _zone_tracker.setdefault(tf, {"env_low": None, "env_high": None})

    # 1) 如果在缓冲带模式：看是否走出缓冲带
    if tracker.get("env_low") is not None:
        if price < tracker["env_low"] or price > tracker["env_high"]:
            tracker["env_low"] = None
            tracker["env_high"] = None
            logger.info(f"[{tf}] ↔️ 价格已走出缓冲带，恢复预警检测")
        return

    # 2) 正常模式：看价格是否刚进入 EMA 两线之间
    in_zone = (ema_low <= price <= ema_high)
    if not in_zone:
        return

    # 冷却时间检查
    history = load_history()
    last_time = 0
    for r in history:
        if r.get("timeframe") == tf:
            last_time = r.get("timestamp", 0)
            break
    now = time.time()
    if now - last_time < cooldown:
        return

    # 触发告警
    title = f"[ETH EMA 预警] {TF_LABELS.get(tf, tf)} · {result['signal_text']} · ${price:.2f}"
    body = (
        f"{TF_LABELS.get(tf, tf)} K线分析\n"
        f"当前价格: ${price:.2f}\n"
        f"EMA{cfg['ema_alert']['ema_short']}: ${result['ema_short']:.2f}\n"
        f"EMA{cfg['ema_alert']['ema_long']}: ${result['ema_long']:.2f}\n"
        f"排列状态: {result['arrangement']} · {result['position']}\n"
        f"缓冲带: ${ema_low - BUFF_BAND:.2f} - ${ema_high + BUFF_BAND:.2f}（走出后重新开启预警）\n"
        f"时间: {result['update_time']}"
    )
    if send_feishu(title, body, cfg):
        add_alert_record(tf, price, result["ema_short"], result["ema_long"], result["signal"], result["position"])
        # 记录缓冲带
        tracker["env_low"] = ema_low - BUFF_BAND
        tracker["env_high"] = ema_high + BUFF_BAND
        logger.info(f"[{tf}] ✅ EMA预警触发 - ${price:.2f}，缓冲带 ${tracker['env_low']:.2f} - ${tracker['env_high']:.2f}")


def check_price_range_alert(current_price, cfg):
    """价格区间告警"""
    if not current_price or current_price <= 0:
        return
    price_ranges = cfg.get("price_ranges", []) or []
    if not price_ranges:
        return

    for idx, pr in enumerate(price_ranges):
        if not pr.get("enabled", False):
            continue
        try:
            low = float(pr.get("low", 0))
            high = float(pr.get("high", 0))
        except (TypeError, ValueError):
            continue
        if low <= 0 or high <= 0 or low >= high:
            continue

        key = f"pr_{idx}"
        tracker = _price_range_tracker.setdefault(key, {"env_low": None, "env_high": None})

        # 缓冲带模式
        if tracker.get("env_low") is not None:
            if current_price < tracker["env_low"] or current_price > tracker["env_high"]:
                tracker["env_low"] = None
                tracker["env_high"] = None
                logger.info(f"[价格区间{idx+1}] ↔️ 价格走出缓冲带，恢复预警")
            continue

        # 正常模式
        in_range = (low <= current_price <= high)
        if not in_range:
            continue

        # 冷却检查
        cooldown = cfg.get("alert", {}).get("cooldown_seconds", 600)
        history = load_history()
        last_time = 0
        for r in history:
            if r.get("timeframe") == f"price_range_{idx}":
                last_time = r.get("timestamp", 0)
                break
        if time.time() - last_time < cooldown:
            continue

        note = pr.get("name", f"价格区间 {low:.0f}-{high:.0f}")
        title = f"[ETH EMA 预警] 价格区间 · {note} · ${current_price:.2f}"
        body = (
            f"{note}\n"
            f"当前价格: ${current_price:.2f}\n"
            f"区间上限: ${high:.2f}\n"
            f"区间下限: ${low:.2f}\n"
            f"缓冲带: ${low - BUFF_BAND:.2f} - ${high + BUFF_BAND:.2f}（走出后重新开启预警）\n"
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        if send_feishu(title, body, cfg):
            add_alert_record(f"price_range_{idx}", current_price, low, high, "进入区间", "between", alert_type="price_range", note=note)
            tracker["env_low"] = low - BUFF_BAND
            tracker["env_high"] = high + BUFF_BAND
            logger.info(f"[价格区间{idx+1}] ✅ 触发预警 - ${current_price:.2f}，缓冲带 ${tracker['env_low']:.2f} - ${tracker['env_high']:.2f}")


# ============================================================
# 主刷新循环
# ============================================================
def update_all_data():
    """拉一次所有周期数据并更新缓存"""
    global _last_update_time, _last_price_value
    cfg = load_config()
    enabled = cfg.get("ema_alert", {}).get("enabled_timeframes", [])
    ema_short = cfg.get("ema_alert", {}).get("ema_short", 180)
    ema_long = cfg.get("ema_alert", {}).get("ema_long", 250)

    price_changed = False
    new_price = None

    for tf in ["5m", "15m", "30m", "1h", "4h"]:
        tf_seconds = {"5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400}[tf]
        limit = TF_NEED_BARS.get(tf, 300)
        bars, source = fetch_klines("ETH_USDT", tf_seconds, limit=limit)

        if bars is None:
            logger.warning(f"[{tf}] 所有数据源获取失败")
            time.sleep(0.8)
            continue
        # 不同周期间隔 0.8s，避免给数据源压力
        time.sleep(0.8)

        _last_source[tf] = source
        logger.info(f"[{tf}] 使用数据源: {source}")

        result = analyze_tf(tf, bars, ema_short, ema_long)
        if result:
            result["data_source"] = source
            with _state_lock:
                _state_cache[tf] = result
            if tf == "5m":
                new_price = result["price"]

            # 告警检查（仅启用的周期）
            if tf in enabled:
                check_ema_alert(tf, result, cfg)

    # 价格变化检测
    if new_price is not None and abs(new_price - _last_price_value) > 0.01:
        price_changed = True
        if _last_price_value > 0:
            logger.info(f"📈 价格变化: ${_last_price_value:.2f} -> ${new_price:.2f}")
        else:
            logger.info(f"📈 价格变化: $0.00 -> ${new_price:.2f}")
        _last_price_value = new_price

    # 价格区间告警
    if new_price is not None:
        check_price_range_alert(new_price, cfg)

    # 简单间隔推送
    if _should_simple_push(cfg) and new_price is not None:
        title = f"ETH 价格播报 · ${new_price:.2f}"
        body = f"时间: {_beijing_now().strftime('%Y-%m-%d %H:%M:%S')}（北京时间）\n数据源: {_last_source.get('5m','unknown')}"
        if send_feishu(title, body, cfg):
            global _last_simple_push_time
            _last_simple_push_time = time.time()
            logger.info(f"✅ 简单间隔推送成功 - ${new_price:.2f}")

    # 固定时间点推送
    if _should_fixed_push(cfg) and new_price is not None:
        bj_now = _beijing_now()
        slot = bj_now.strftime("%H:%M")
        title = f"[定时播报] {slot} · ETH ${new_price:.2f}"
        body = (
            f"时间点: {slot}（北京时间）\n"
            f"当前价格: ${new_price:.2f}\n"
            f"数据源: {_last_source.get('5m', 'unknown')}"
        )
        if send_feishu(title, body, cfg):
            logger.info(f"✅ 固定时间点推送成功 - {slot} ${new_price:.2f}")

    if new_price is not None:
        _last_update_time = time.time()
        logger.info(f"✅ 数据更新完成 - ETH=${new_price:.2f}, 数据源={_last_source.get('5m','unknown')}, 价格变化={price_changed}")


# ============================================================
# 对外状态读取
# ============================================================
def get_all_states():
    with _state_lock:
        return deepcopy(_state_cache)


def get_last_update_time():
    return _last_update_time


def get_connection_status():
    if not _last_update_time:
        return "正在连接..."
    age = time.time() - _last_update_time
    if age < 60:
        return "已连接"
    elif age < 300:
        return "连接稍慢"
    return "连接异常"


def get_source_health():
    return deepcopy(_source_health)


# ============================================================
# 线程管理
# ============================================================
def _monitor_loop():
    """后台监控线程主循环"""
    logger.info("🚀 后台监控线程启动")
    while not _monitor_stop.is_set():
        try:
            update_all_data()
        except Exception as e:
            logger.exception(f"监控线程异常: {e}")
        # 用 Event 替代 sleep，可立即退出
        _monitor_stop.wait(DATA_REFRESH_INTERVAL)
    logger.info("监控线程已退出")


def start_monitor_in_background():
    """启动后台监控线程（幂等）"""
    global _monitor_started, _monitor_thread
    if _monitor_started:
        return
    with threading.Lock() if False else threading.RLock():
        if _monitor_started:
            return
        _monitor_stop.clear()
        _monitor_thread = threading.Thread(target=_monitor_loop, name="eth-monitor", daemon=True)
        _monitor_thread.start()
        _monitor_started = True


def ensure_monitor_running():
    """API层兜底检查：如果没在跑就启动"""
    if not _monitor_started:
        start_monitor_in_background()


def stop_monitor():
    """停止监控（用于清理）"""
    _monitor_stop.set()
    # _monitor_started = False  # 不重置，防止重启


# ============================================================
# 下一次固定时间点推送时间（供前端显示）
# ============================================================
def get_next_fixed_push_time(cfg):
    """返回形如 '14:30' 的字符串，未开启则返回空串"""
    interval = cfg.get("feishu", {}).get("fixed_push_interval_seconds", 0)
    if not interval or interval <= 0:
        return ""
    bj_now = _beijing_now()
    bj_8am = bj_now.replace(hour=8, minute=0, second=0, microsecond=0)
    if bj_now < bj_8am:
        bj_8am = bj_8am - timedelta(days=1)
    seconds_from_8am = (bj_now - bj_8am).total_seconds()
    slot_index = int(seconds_from_8am // interval)
    next_slot_time = bj_8am + timedelta(seconds=(slot_index + 1) * interval)
    return next_slot_time.strftime("%H:%M")


# ============================================================
# 测试飞书推送
# ============================================================
def test_feishu_push(cfg=None):
    if cfg is None:
        cfg = load_config()
    states = get_all_states()
    price = 0
    if states and states.get("5m"):
        price = states["5m"].get("price", 0)
    title = "[测试] ETH EMA 预警系统"
    body = (
        f"测试消息\n"
        f"当前价格: ${price:.2f}\n"
        f"测试时间: {_beijing_now().strftime('%Y-%m-%d %H:%M:%S')}（北京时间）"
    )
    ok = send_feishu(title, body, cfg)
    return ok


# 启动时自加载一次配置（确保文件存在）
try:
    load_config()
except Exception:
    pass
