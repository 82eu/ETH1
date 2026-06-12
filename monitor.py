"""
ETH 价格播报 —— 每 10 秒发送到飞书
格式：ETH/USDT 实时价格: $3,767.24 · 2025-06-13 21:30:45
"""
import requests
import time
from datetime import datetime, timezone, timedelta

# ============================================================
# 配置（直接在这里改）
# ============================================================
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxxx"  # TODO: 换成你的飞书 webhook
INTERVAL_SECONDS = 10                 # 发送间隔（秒）
TIMEZONE_HOURS = 8                    # 东 8 区

# 备用数据源
APIS = [
    {
        "name": "Binance",
        "url": "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT",
        "path": ["price"],
    },
    {
        "name": "OKX",
        "url": "https://www.okx.com/api/v5/market/ticker?instId=ETH-USDT",
        "path": ["data", 0, "last"],
    },
    {
        "name": "Gate",
        "url": "https://api.gateio.ws/api/v4/spot/tickers?currency_pair=ETH_USDT",
        "path": [0, "last"],
    },
]


# ============================================================
# 工具函数
# ============================================================
def get_price():
    """按顺序尝试数据源，返回 float 价格；全部失败返回 None"""
    for api in APIS:
        try:
            r = requests.get(api["url"], timeout=5)
            r.raise_for_status()
            data = r.json()
            for key in api["path"]:
                if isinstance(data, list) and isinstance(key, int):
                    data = data[key]
                else:
                    data = data[key]
            price = float(data)
            if price > 0:
                return price
        except Exception as e:
            print(f"  [{api['name']}] 失败: {e}")
            continue
    return None


def fmt_price(p):
    """美元千分位格式，如 3,767.24"""
    return f"{p:,.2f}"


def fmt_time_now():
    """格式化当前时间：2025-06-13 21:30:45（东 8 区）"""
    tz = timezone(timedelta(hours=TIMEZONE_HOURS))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def send_to_feishu(price):
    """发送到飞书"""
    text = (
        f"ETH/USDT 实时价格: ${fmt_price(price)} · {fmt_time_now()}"
    )
    payload = {
        "msg_type": "text",
        "content": {"text": text},
    }
    try:
        r = requests.post(
            WEBHOOK_URL,
            json=payload,
            timeout=5,
            headers={"Content-Type": "application/json"},
        )
        r.raise_for_status()
        result = r.json()
        if result.get("code") == 0 or result.get("StatusCode") == 0:
            return True, None
        return False, r.text
    except Exception as e:
        return False, str(e)


# ============================================================
# 主循环
# ============================================================
def main():
    print("=" * 60)
    print(" ETH 价格播报已启动")
    print(f" 发送间隔: 每 {INTERVAL_SECONDS} 秒")
    print(f" 时区: 东 {TIMEZONE_HOURS} 区")
    print(f" 数据源: {' / '.join(a['name'] for a in APIS)}")
    print("=" * 60)

    success_count = 0
    fail_count = 0

    while True:
        print(f"\n[{fmt_time_now()}]", end=" ")
        price = get_price()
        if price is None:
            fail_count += 1
            print(f"⚠️ 获取价格失败 (累计 {fail_count} 次)")
            time.sleep(INTERVAL_SECONDS)
            continue

        ok, err = send_to_feishu(price)
        if ok:
            success_count += 1
            print(f"✅ ${fmt_price(price)} 发送成功 (累计 {success_count} 次)")
        else:
            fail_count += 1
            print(f"❌ 发送失败: {err} (累计 {fail_count} 次)")

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n 已停止。")
