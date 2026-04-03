#!/usr/bin/env python3
"""
CRE Daily Brief — 경제지표 수집 스크립트
=========================================
실행: python collect.py
결과: data/indicators.json 생성
"""

import requests
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List
import warnings
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Windows cp949 환경에서 이모지/유니코드 출력 보장
import sys
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)

KST = timezone(timedelta(hours=9))
NOW = datetime.now(KST)

# ===== API KEYS =====
# ⚠️ 운영 시 환경변수로 전환: os.environ.get("ECOS_API_KEY")
ECOS_API_KEY = "XTQ9T69UVUDG5HWB18P3"
KOREAEXIM_RATE_KEY = "t5cJ35UsbZbr0AFT8IsbCV1tFsIWlmlU"
KMA_API_KEY = "cwUHy65sR2aFB8uubEdmeA"


# ── 한국은행 ECOS ──

def ecos_call(stat_code, cycle, start, end, item_code="?"):
    url = (f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}/json/kr/"
           f"1/20/{stat_code}/{cycle}/{start}/{end}/{item_code}/?/?")
    try:
        resp = requests.get(url, timeout=15, verify=False)
        data = resp.json()
        if "StatisticSearch" in data:
            return data["StatisticSearch"].get("row", [])
    except Exception as e:
        print(f"  ❌ ECOS 실패: {e}")
    return None


def get_base_rate():
    """기준금리"""
    end = NOW.strftime("%Y%m")
    start = (NOW - timedelta(days=180)).strftime("%Y%m")
    rows = ecos_call("722Y001", "M", start, end, "0101000")
    if rows and len(rows) >= 2:
        val = float(rows[-1]["DATA_VALUE"])
        prev = float(rows[-2]["DATA_VALUE"])
        chg = round(val - prev, 2)
        return {
            "name": "기준금리",
            "value": f"{val}%",
            "change": f"{'+' if chg > 0 else ''}{chg}%p",
            "direction": "up" if chg > 0 else ("down" if chg < 0 else "neutral"),
            "sub": f"한국은행 · {rows[-1]['TIME'][:4]}.{rows[-1]['TIME'][4:]}",
            "raw": val
        }
    return None


def get_treasury_bond():
    """국고채 3년"""
    end = NOW.strftime("%Y%m%d")
    start = (NOW - timedelta(days=14)).strftime("%Y%m%d")
    rows = ecos_call("817Y002", "D", start, end, "010200000")
    if rows and len(rows) >= 2:
        val = float(rows[-1]["DATA_VALUE"])
        prev = float(rows[-2]["DATA_VALUE"])
        chg = round(val - prev, 3)
        return {
            "name": "국고채 3년",
            "value": f"{val}%",
            "change": f"{'+' if chg > 0 else ''}{chg}%p",
            "direction": "up" if chg > 0 else ("down" if chg < 0 else "neutral"),
            "sub": f"한국은행 · {rows[-1]['TIME']}",
            "raw": val
        }
    return None


def get_cd_rate():
    """CD금리 91일"""
    end = NOW.strftime("%Y%m%d")
    start = (NOW - timedelta(days=14)).strftime("%Y%m%d")
    rows = ecos_call("817Y002", "D", start, end, "010502000")
    if rows and len(rows) >= 2:
        val = float(rows[-1]["DATA_VALUE"])
        prev = float(rows[-2]["DATA_VALUE"])
        chg = round(val - prev, 3)
        return {
            "name": "CD금리 91일",
            "value": f"{val}%",
            "change": f"{'+' if chg > 0 else ''}{chg}%p",
            "direction": "up" if chg > 0 else ("down" if chg < 0 else "neutral"),
            "sub": f"한국은행 · {rows[-1]['TIME']}",
            "raw": val
        }
    return None


# ── 수출입은행 환율 ──

def get_exchange_rate():
    """원/달러 환율"""
    for delta in range(5):
        target = (NOW - timedelta(days=delta)).strftime("%Y%m%d")
        try:
            resp = requests.get(
                "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON",
                params={"authkey": KOREAEXIM_RATE_KEY, "searchdate": target, "data": "AP01"},
                timeout=15, verify=False
            )
            data = resp.json()
            if not data:
                continue

            usd = next((i for i in data if i.get("cur_unit") == "USD"), None)
            if usd:
                rate = usd["deal_bas_r"].replace(",", "")

                # 전일 환율 조회 (변동폭 계산)
                prev_rate = None
                for pd in range(delta + 1, delta + 6):
                    pt = (NOW - timedelta(days=pd)).strftime("%Y%m%d")
                    try:
                        pr = requests.get(
                            "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON",
                            params={"authkey": KOREAEXIM_RATE_KEY, "searchdate": pt, "data": "AP01"},
                            timeout=10, verify=False
                        ).json()
                        pusd = next((i for i in pr if i.get("cur_unit") == "USD"), None) if pr else None
                        if pusd:
                            prev_rate = float(pusd["deal_bas_r"].replace(",", ""))
                            break
                    except:
                        continue

                chg = round(float(rate) - prev_rate, 2) if prev_rate else 0
                direction = "up" if chg > 0 else ("down" if chg < 0 else "neutral")

                return {
                    "name": "원/달러 환율",
                    "value": f"{float(rate):,.1f}",
                    "change": f"{'+' if chg > 0 else ''}{chg:,.1f}원",
                    "direction": direction,
                    "sub": f"수출입은행 · {target[:4]}.{target[4:6]}.{target[6:]}",
                    "raw": float(rate)
                }
        except Exception as e:
            print(f"  ❌ 환율 실패: {e}")
    return None


# ── 기상청 날씨 ──

NX_SIZE = 149
SEOUL_IDX = 127 * NX_SIZE + 60  # 서울 중구 격자


def kma_grid_value(tmfc, tmef, var):
    """단기예보 격자에서 서울 값 추출 (재시도 2회)"""
    import time as _time
    for attempt in range(2):
        try:
            resp = requests.get(
                "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_shrt_grd",
                params={"tmfc": tmfc, "tmef": tmef, "vars": var, "authKey": KMA_API_KEY},
                timeout=20, verify=False
            )
            if resp.status_code == 200:
                values = [v.strip() for v in resp.text.strip().split(",") if v.strip()]
                if len(values) > SEOUL_IDX:
                    val = float(values[SEOUL_IDX])
                    if val not in [-99.0, -999.0]:
                        return val
        except Exception as e:
            if attempt == 0:
                _time.sleep(2)
            else:
                print(f"  ⚠️ 격자 {var} 실패 (2회 시도): {e}")
    return None


def kma_vilage_fcst_value(base_date, base_time, category):
    """단기예보 JSON API로 TMN/TMX 폴백 조회"""
    try:
        resp = requests.get(
            "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getVilageFcst",
            params={
                "pageNo": 1, "numOfRows": 300, "dataType": "JSON",
                "base_date": base_date, "base_time": base_time,
                "nx": 60, "ny": 127, "authKey": KMA_API_KEY
            }, timeout=20, verify=False
        )
        data = resp.json()
        if data.get("response", {}).get("header", {}).get("resultCode") == "00":
            items = data["response"]["body"]["items"]["item"]
            for item in items:
                if item["category"] == category and item["fcstDate"] == base_date:
                    val = float(item["fcstValue"])
                    if val not in [-99.0, -999.0]:
                        return val
    except Exception as e:
        print(f"  ⚠️ 단기예보 {category} 실패: {e}")
    return None


def kma_ultra_srt():
    """초단기실황 — 현재 기온, 습도"""
    base = NOW - timedelta(hours=1)
    try:
        resp = requests.get(
            "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0/getUltraSrtNcst",
            params={
                "pageNo": 1, "numOfRows": 100, "dataType": "JSON",
                "base_date": base.strftime("%Y%m%d"),
                "base_time": base.strftime("%H") + "00",
                "nx": 60, "ny": 127, "authKey": KMA_API_KEY
            }, timeout=15, verify=False
        )
        data = resp.json()
        if data.get("response", {}).get("header", {}).get("resultCode") == "00":
            items = data["response"]["body"]["items"]["item"]
            w = {item["category"]: item["obsrValue"] for item in items}
            return {
                "temp": float(w.get("T1H", 0)),
                "humidity": int(float(w.get("REH", 0))),
                "wind": float(w.get("WSD", 0)),
                "pty": int(float(w.get("PTY", 0)))
            }
    except Exception as e:
        print(f"  ❌ 초단기실황 실패: {e}")
    return {}


def get_weather():
    """날씨 통합 (초단기실황 + 단기예보 격자 + JSON 폴백)"""
    today = NOW.strftime("%Y%m%d")

    # 초단기실황
    current = kma_ultra_srt()

    # 단기예보 격자 (1차 시도)
    tmn = kma_grid_value(f"{today}02", f"{today}06", "TMN")
    tmx = kma_grid_value(f"{today}05", f"{today}15", "TMX")
    sky_val = kma_grid_value(f"{today}05", f"{today}12", "SKY")
    pop_val = kma_grid_value(f"{today}05", f"{today}12", "POP")

    # TMN/TMX 실패 시 → 단기예보 JSON API 폴백
    if tmn is None:
        print("  🔄 TMN 폴백: 단기예보 JSON API 시도...")
        tmn = kma_vilage_fcst_value(today, "0200", "TMN")
        if tmn is None:
            # 전날 23시 발표분에서 재시도
            yesterday = (NOW - timedelta(days=1)).strftime("%Y%m%d")
            tmn = kma_vilage_fcst_value(today, "2300", "TMN") or kma_vilage_fcst_value(yesterday, "2300", "TMN")

    if tmx is None:
        print("  🔄 TMX 폴백: 단기예보 JSON API 시도...")
        tmx = kma_vilage_fcst_value(today, "0500", "TMX")
        if tmx is None:
            tmx = kma_vilage_fcst_value(today, "0200", "TMX")

    # SKY/POP도 폴백
    if sky_val is None:
        sky_val_fb = kma_vilage_fcst_value(today, "0500", "SKY")
        if sky_val_fb is not None:
            sky_val = sky_val_fb

    if pop_val is None:
        pop_val_fb = kma_vilage_fcst_value(today, "0500", "POP")
        if pop_val_fb is not None:
            pop_val = pop_val_fb

    sky_map = {1: "맑음", 3: "구름많음", 4: "흐림"}
    sky = sky_map.get(int(sky_val), "맑음") if sky_val else "정보없음"
    pop = int(pop_val) if pop_val else 0

    # 강수형태
    pty = current.get("pty", 0)
    pty_map = {0: "없음", 1: "비", 2: "비/눈", 3: "눈", 5: "빗방울", 6: "빗방울눈날림", 7: "눈날림"}

    # 이모지
    if pty in [1, 2, 5, 6]:
        emoji = "🌧️"
    elif pty in [3, 7]:
        emoji = "❄️"
    elif pop >= 60:
        emoji = "🌧️"
    elif sky == "맑음":
        emoji = "☀️"
    elif sky == "구름많음":
        emoji = "⛅"
    else:
        emoji = "☁️"

    temp = current.get("temp", "N/A")

    return {
        "name": "서울 날씨",
        "type": "weather",
        "emoji": emoji,
        "temp": temp,
        "tmn": tmn if tmn is not None else "N/A",
        "tmx": tmx if tmx is not None else "N/A",
        "humidity": current.get("humidity", "N/A"),
        "wind": current.get("wind", "N/A"),
        "sky": sky,
        "pop": pop,
        "rain_type": pty_map.get(pty, "없음"),
        "sub": f"기상청 · {today[:4]}.{today[4:6]}.{today[6:]}"
    }


# ── 메인 수집 ──

def main():
    print(f"🏢 CRE Daily Brief — 경제지표 수집")
    print(f"⏰ {NOW.strftime('%Y-%m-%d %H:%M:%S KST')}")
    print("=" * 50)

    cards = []

    # 1. 기준금리
    print("\n🏛️ 기준금리...")
    r = get_base_rate()
    if r:
        cards.append(r)
        print(f"  ✅ {r['value']} ({r['change']})")
    else:
        print("  ❌ 실패")

    # 2. 환율
    print("\n💱 원/달러 환율...")
    r = get_exchange_rate()
    if r:
        cards.append(r)
        print(f"  ✅ {r['value']}원 ({r['change']})")
    else:
        print("  ❌ 실패")

    # 3. 국고채
    print("\n📊 국고채 3년...")
    r = get_treasury_bond()
    if r:
        cards.append(r)
        print(f"  ✅ {r['value']} ({r['change']})")
    else:
        print("  ❌ 실패")

    # 4. CD금리
    print("\n📊 CD금리 91일...")
    r = get_cd_rate()
    if r:
        cards.append(r)
        print(f"  ✅ {r['value']} ({r['change']})")
    else:
        print("  ❌ 실패")

    # 5. 날씨
    print("\n🌤️ 서울 날씨...")
    r = get_weather()
    if r:
        cards.append(r)
        print(f"  ✅ {r['emoji']} {r['temp']}℃ ({r['tmn']}~{r['tmx']}℃) {r['sky']} 강수{r['pop']}%")
    else:
        print("  ❌ 실패")

    # 저장
    output = {
        "generated_at": NOW.strftime("%Y-%m-%d %H:%M:%S"),
        "date_display": NOW.strftime("%Y년 %m월 %d일"),
        "day_of_week": ["월요일","화요일","수요일","목요일","금요일","토요일","일요일"][NOW.weekday()],
        "cards": cards
    }

    os.makedirs("data", exist_ok=True)
    with open("data/indicators.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 50}")
    print(f"✅ 성공: {len(cards)}/5")
    print(f"💾 data/indicators.json 저장 완료")
    print(f"\n🚀 웹서버 실행: python -m http.server 8000")
    print(f"🌐 브라우저: http://localhost:8000")


if __name__ == "__main__":
    main()
