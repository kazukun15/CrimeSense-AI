# -*- coding: utf-8 -*-
# ============================================================
# 愛媛県 全域：犯罪/異常行動 警戒予測（Streamlit 最終完成版）
#  過去の要件をすべて統合し、地図が"確実に"表示されるよう堅牢化
#
#  機能（会話要件の統合）
#   - 地図: Folium + streamlit-folium。版差(returns/returned_objects)を事前判定し1回だけ呼ぶ
#   - 2019年 愛媛県オープンデータ(CSV)の自動検出・統合（./, ./data, /mnt/data）
#       -> 市町村中心に微ジッターを加えた「概位置」レイヤで可視化（個人特定を避けつつ傾向表示）
#   - CSVアップロード: 住所を Nominatim でジオコーディング (/mnt/data にキャッシュ) し、実点を別レイヤで表示
#   - mgpn 月齢API: v2(JSON)優先→v3 フォールバック、Accept/Retry/30分キャッシュ、ボタン押下時のみ呼出
#   - 気象: WeatherAPI/OpenWeather（キー未設定時はダミー動作）
#   - NET 情報: Overpass APIで駅/バス停/駐輪場/コンビニ/駐車場/公園/ATM を取得・レイヤ表示
#   - リスクスコア: 気温/降雨/時間帯/週末/月齢/2019傾向で 0-100 スコア化
#   - 将来30日で最大リスク日（21:00評価）を月齢中心に概算表示
#   - Gemini 2.5 Flash（任意）で注意喚起コメント（APIキーがあれば）
#
#  可用性の工夫
#   - streamlit-folium の引数名を inspect で事前判定 → 一発呼び出し（フォールバックの多重呼出し禁止）
#   - ウィジェット key は唯一。地図は1枚に集約（key="map_main"）
#   - st.experimental_rerun は未使用。st.rerun のみ
#   - Folium プラグイン(LocateControl)は存在検知してから追加
#   - 解析オーバーレイは例外でも消す（try/finally）
# ============================================================

import os, re, io, glob, time, json, inspect
from datetime import datetime, timedelta, timezone
import chardet, requests, numpy as np, pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_folium import st_folium
import streamlit_folium as stf
import folium
from folium.plugins import MiniMap, MousePosition, MeasureControl, Fullscreen, MarkerCluster
try:
    from folium.plugins import LocateControl  # 存在しない版がある
    HAS_LOCATE = True
except Exception:
    HAS_LOCATE = False

# ---------------- 基本設定 ----------------
JST = timezone(timedelta(hours=9))
APP_TITLE = "愛媛県 警戒予測モニター"
EHIME_CENTER = (33.8416, 132.7661)
INIT_LAT, INIT_LON = 34.27717, 133.20986  # 上島町近辺
EHIME_BBOX = {"min_lat": 32.8, "max_lat": 34.6, "min_lon": 131.8, "max_lon": 134.0}
DATA_GLOBS = ["./ehime_2019*.csv", "./data/ehime_2019*.csv", "/mnt/data/ehime_2019*.csv"]
GEOCODE_CACHE = "/mnt/data/geocode_cache.parquet"

MUNI_CENTER = {
    "松山市": (33.839, 132.765), "今治市": (34.066, 132.997), "新居浜市": (33.960, 133.283),
    "西条市": (33.918, 133.183), "四国中央市": (33.983, 133.550), "宇和島市": (33.223, 132.560),
    "大洲市": (33.503, 132.545), "八幡浜市": (33.461, 132.422), "伊予市": (33.757, 132.709),
    "東温市": (33.792, 132.870), "西予市": (33.360, 132.512), "上島町": (34.244, 133.200),
    "久万高原町": (33.655, 132.901), "砥部町": (33.742, 132.789), "松前町": (33.790, 132.710),
    "内子町": (33.533, 132.658), "伊方町": (33.493, 132.352), "鬼北町": (33.201, 132.746),
    "松野町": (33.146, 132.744), "愛南町": (32.966, 132.567),
}

CTYPE_STYLE = {
    "ひったくり": ("red", "glyphicon-flag"),
    "車上ねらい": ("orange", "glyphicon-wrench"),
    "部品ねらい": ("lightred", "glyphicon-wrench"),
    "自動車盗": ("darkred", "glyphicon-road"),
    "オートバイ盗": ("purple", "glyphicon-road"),
    "自転車盗": ("green", "glyphicon-bicycle"),
    "自動販売機ねらい": ("cadetblue", "glyphicon-usd"),
    "不明": ("gray", "glyphicon-question-sign"),
}

# ---------------- Secrets / API Keys ----------------
try:
    WEATHERAPI_KEY = st.secrets.get("weatherapi", {}).get("api_key", "")
    OPENWEATHER_KEY = st.secrets.get("openweather", {}).get("api_key", "")
    GEMINI_KEY = st.secrets.get("gemini", {}).get("api_key", "")
    GEMINI_MODEL = st.secrets.get("gemini", {}).get("model", "gemini-2.5-flash")
except Exception:
    WEATHERAPI_KEY = OPENWEATHER_KEY = GEMINI_KEY = ""
    GEMINI_MODEL = "gemini-2.5-flash"

# ---------------- CSS ----------------
DRAMA_CSS = """
<style>
.main, .stApp { background: #0b0f14; color: #e6eef7; }
section.main > div.block-container { padding-top: .8rem; padding-bottom: 1.0rem; }
.page-title { margin: 0 0 8px 0; font-weight: 900; letter-spacing: .04em; }
.card { background: linear-gradient(135deg, #121821, #0e141b); border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 14px 16px; margin: 8px 0; }
.score-big { font-size: 72px; font-weight: 900; letter-spacing: 1.5px; text-shadow: 0 0 8px rgba(255,0,0,0.25), 0 0 16px rgba(255,0,0,0.15); margin: 0; line-height: 1.0; }
.badge { display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 700; background: linear-gradient(135deg, #1c2633 0%, #121821 100%); border: 1px solid rgba(255,255,255,0.1); color:#d9e6f2; margin-left: 8px; }
.alert-bar { position: sticky; top: 0; z-index: 1000; background: linear-gradient(90deg, rgba(180,0,0,0.85), rgba(255,50,50,0.85)); color: white; padding: 10px 14px; border-bottom: 2px solid rgba(255,255,255,0.25); box-shadow: 0 4px 24px rgba(255,0,0,0.35); animation: pulse 2s infinite; }
@keyframes pulse { 0% { box-shadow: 0 0 0 rgba(255,0,0,0.4); } 50% { box-shadow: 0 0 24px rgba(255,0,0,0.6); } 100% { box-shadow: 0 0 0 rgba(255,0,0,0.4); } }
.mute { color: #a8b7c7; font-size: 13px; }
.overlay { position: fixed; inset: 0; display: flex; align-items: center; justify-content: center; background: rgba(0, 0, 0, 0.45); z-index: 9999; }
.overlay-content { text-align: center; padding: 28px 36px; border-radius: 18px; backdrop-filter: blur(6px); background: linear-gradient(135deg, rgba(20,40,60,0.85), rgba(8,16,24,0.85)); border: 1px solid rgba(255,255,255,0.15); box-shadow: 0 12px 48px rgba(0,0,0,0.5), 0 0 24px rgba(0,180,255,0.2) inset; }
.overlay-title { font-size: 28px; font-weight: 900; letter-spacing: 0.2em; color: #e8f2ff; text-shadow: 0 0 12px rgba(0,180,255,0.3); }
.overlay-sub { margin-top: 8px; font-size: 13px; color: #bcd0e0; }
.loader { margin: 16px auto 0 auto; width: 72px; height: 72px; border-radius: 50%; border: 4px solid rgba(255,255,255,0.18); border-top-color: #39c0ff; animation: spin 1.1s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
</style>
"""

# ---------------- CSV 読み込み（堅牢） ----------------

def read_csv_robust(path: str) -> pd.DataFrame:
    with open(path, "rb") as f:
        raw = f.read()
    enc_guess = (chardet.detect(raw).get("encoding") or "utf-8").lower()
    for enc in (enc_guess, "utf-8-sig", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, encoding_errors="ignore")


def guess_columns(df: pd.DataFrame) -> dict:
    cols_lower = {c: str(c).lower() for c in df.columns}
    date_col = next((c for c in df.columns if re.search(r"(発生|年月日|日付|日時)", str(c))), None)
    if not date_col:
        date_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["date", "day", "time", "occur"])), None)
    muni_col = next((c for c in df.columns if re.search(r"(市|町|村).*名", str(c)) or re.search(r"(市町村|自治体|地域)", str(c))), None)
    if not muni_col:
        muni_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["municipality", "city", "town", "area", "region"])), None)
    addr_col = next((c for c in df.columns if re.search(r"(住所|所在地|地名|番地)", str(c))), None)
    type_col = next((c for c in df.columns if re.search(r"(手口|罪|罪種|種別|分類)", str(c))), None)
    if not type_col:
        type_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["type", "category", "kind", "crime"])), None)
    return {"date": date_col, "municipality": muni_col, "addr": addr_col, "ctype": type_col}


def parse_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def load_all_crime_2019(data_globs: list[str] | None = None) -> pd.DataFrame | None:
    if data_globs is None:
        data_globs = DATA_GLOBS
    files: list[str] = []
    for g in data_globs:
        files.extend(glob.glob(g))
    files = sorted(set(files))
    if not files:
        return None
    frames = []
    for fp in files:
        df = read_csv_robust(fp)
        g = guess_columns(df)
        if g["date"] is None:
            df["date"] = pd.NaT
        else:
            df.rename(columns={g["date"]: "date"}, inplace=True)
            df["date"] = parse_date_series(df["date"])
        if g["municipality"] is None:
            df["municipality"] = ""
        else:
            df.rename(columns={g["municipality"]: "municipality"}, inplace=True)
            df["municipality"] = df["municipality"].astype(str)
        if g["addr"] is not None:
            df.rename(columns={g["addr"]: "address"}, inplace=True)
        else:
            df["address"] = ""
        if g["ctype"] is None:
            base = os.path.basename(fp)
            mapping = {
                "hittakuri": "ひったくり",
                "syazyounerai": "車上ねらい",
                "buhinnerai": "部品ねらい",
                "zidousyatou": "自動車盗",
                "ootobaitou": "オートバイ盗",
                "zitensyatou": "自転車盗",
                "zidouhanbaikinerai": "自動販売機ねらい",
            }
            ctype = None
            for k, v in mapping.items():
                if k in base:
                    ctype = v
                    break
            df["ctype"] = ctype if ctype else "不明"
        else:
            df.rename(columns={g["ctype"]: "ctype"}, inplace=True)
            df["ctype"] = df["ctype"].astype(str)
        df = df[(df["date"].dt.year == 2019) | (df["date"].isna())]
        frames.append(df[["date", "municipality", "address", "ctype"]])
    return pd.concat(frames, ignore_index=True)

# ---------------- 天気 ----------------

def get_weather_weatherapi(lat, lon):
    try:
        if not WEATHERAPI_KEY:
            return None
        base = "https://api.weatherapi.com/v1/current.json"
        params = {"key": WEATHERAPI_KEY, "q": f"{lat},{lon}", "aqi": "no"}
        r = requests.get(base, params=params, timeout=10)
        r.raise_for_status()
        curr = r.json()
        return {
            "provider": "weatherapi",
            "temp_c": curr["current"]["temp_c"],
            "humidity": curr["current"]["humidity"],
            "condition": curr["current"]["condition"]["text"],
            "precip_mm": curr["current"].get("precip_mm", 0.0),
            "wind_kph": curr["current"].get("wind_kph", 0.0),
        }
    except Exception:
        return None


def get_weather_openweather(lat, lon):
    try:
        if not OPENWEATHER_KEY:
            return None
        url = "https://api.openweathermap.org/data/2.5/weather"
        p = {"lat": lat, "lon": lon, "appid": OPENWEATHER_KEY, "units": "metric", "lang": "ja"}
        r = requests.get(url, params=p, timeout=10)
        r.raise_for_status()
        jd = r.json()
        return {
            "provider": "openweather",
            "temp_c": jd["main"]["temp"],
            "humidity": jd["main"]["humidity"],
            "condition": jd["weather"][0]["description"],
            "precip_mm": 0.0,
            "wind_kph": jd.get("wind", {}).get("speed", 0.0) * 3.6,
        }
    except Exception:
        return None


def get_weather(lat, lon):
    w = get_weather_weatherapi(lat, lon)
    if not w:
        w = get_weather_openweather(lat, lon)
    if not w:
        w = {"provider": "dummy", "temp_c": 26.0, "humidity": 70, "condition": "晴れ", "precip_mm": 0.0, "wind_kph": 8.0}
    return w

# ---------------- mgpn（月齢） v2 JSON → v3 ----------------

def _extract_moonage(payload) -> float | None:
    if payload is None:
        return None
    obj = payload[0] if isinstance(payload, list) and payload else (payload if isinstance(payload, dict) else None)
    if not obj:
        return None
    for k in ["moonage", "moon_age", "moonAge", "age"]:
        if k in obj and obj[k] is not None:
            try:
                return float(obj[k])
            except Exception:
                pass
    return None


def _phase_text_from_age(age: float | None) -> str | None:
    if age is None:
        return None
    a = age % 29.53
    if a < 1.0: return "新月"
    if a < 6.0: return "三日月（若月）"
    if a < 8.9: return "上弦前後"
    if a < 13.5: return "十三夜～満月前"
    if a < 16.0: return "満月前後"
    if a < 21.0: return "満月後～下弦前"
    if a < 23.5: return "下弦前後"
    if a < 28.0: return "有明月（残月）"
    return "新月に近い"


def _mgpn_call(url: str, params: dict) -> dict | None:
    headers = {"Accept": "application/json"}
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            r.raise_for_status()
            ct = r.headers.get("Content-Type", "")
            if "json" in ct:
                payload = r.json()
            else:
                txt = r.text.strip()
                try:
                    payload = json.loads(txt)
                except Exception:
                    payload = None
            if payload is None:
                raise ValueError("mgpn: empty payload")
            age = _extract_moonage(payload)
            obj = payload[0] if isinstance(payload, list) and payload else payload
            alt = float(obj.get("altitude")) if obj and "altitude" in obj else None
            azi = float(obj.get("azimuth")) if obj and "azimuth" in obj else None
            return {"moon_age": age, "phase_text": _phase_text_from_age(age), "altitude": alt, "azimuth": azi}
        except Exception:
            time.sleep(0.5 * (attempt + 1))
            continue
    return None

@st.cache_data(show_spinner=False, ttl=60*30)
def get_mgpn_moon(lat: float, lon: float, dt_jst: datetime) -> dict | None:
    t = dt_jst.strftime("%Y-%m-%dT%H:%M")
    p = {"time": t, "lat": f"{lat:.6f}", "lon": f"{lon:.6f}", "loop": 1, "interval": 0}
    obj = _mgpn_call("https://mgpn.org/api/moon/v2position.cgi", p)
    if obj:
        return obj
    p2 = {"time": t, "lat": f"{lat:.6f}", "lon": f"{lon:.6f}"}
    return _mgpn_call("https://mgpn.org/api/moon/v3position.cgi", p2)


def is_full_moon_like_text(phase_text: str | None, age: float | None) -> bool:
    if phase_text and ("満月" in phase_text):
        return True
    if age is not None:
        a = age % 29.53
        return 13.3 <= a <= 16.3
    return False

# ---------------- リスクスコア ----------------

def compute_risk_score(weather: dict, now_dt: datetime, all_df: pd.DataFrame | None, moon_info: dict | None) -> dict:
    score = 0.0
    reasons: list[str] = []
    temp = float(weather.get("temp_c", 20.0))
    precip = float(weather.get("precip_mm", 0.0))
    humidity = float(weather.get("humidity", 60))
    cond = str(weather.get("condition", ""))
    if temp >= 32: add = 42
    elif temp >= 30: add = 36
    elif temp >= 27: add = 28
    elif temp >= 25: add = 20
    elif temp >= 22: add = 10
    else: add = 0
    score += add
    if add > 0: reasons.append(f"気温{temp:.0f}℃:+{add}")
    if precip >= 10: score -= 20; reasons.append("強い降雨:-20")
    elif precip >= 1: score -= 8; reasons.append("降雨あり:-8")
    hour = now_dt.hour
    if 20 <= hour <= 23 or 0 <= hour <= 4: score += 15; reasons.append("夜間:+15")
    elif 17 <= hour < 20: score += 7; reasons.append("夕方:+7")
    if now_dt.weekday() in (4, 5): score += 6; reasons.append("週末(+金土):+6")
    moon_age = moon_info.get("moon_age") if moon_info else None
    moon_phase_text = moon_info.get("phase_text") if moon_info else None
    if is_full_moon_like_text(moon_phase_text, moon_age): score += 5; reasons.append("満月相当:+5")
    if humidity >= 80: score += 3; reasons.append("高湿度:+3")
    if all_df is not None and not all_df.empty:
        sub = all_df.copy(); sub["month"] = sub["date"].dt.month
        month_ratio = len(sub[sub["month"] == now_dt.month]) / max(1, len(sub))
        if month_ratio >= 0.12: score += 6; reasons.append("2019傾向(同月比 多め):+6")
        elif month_ratio >= 0.08: score += 3; reasons.append("2019傾向(同月比 やや多め):+3")
        if "ctype" in sub.columns:
            top_types = sub["ctype"].value_counts(normalize=True)
            outdoor_like = float(top_types.get("ひったくり",0)+top_types.get("車上ねらい",0)+top_types.get("自転車盗",0)+top_types.get("オートバイ盗",0))
            if outdoor_like >= 0.45: score += 5; reasons.append("2019傾向(屋外系多):+5")
            elif outdoor_like >= 0.30: score += 2; reasons.append("2019傾向(屋外系やや多):+2")
    score = float(np.clip(score, 0, 100))
    if score < 25: level, color = "Low", "#0aa0ff"
    elif score < 50: level, color = "Moderate", "#ffd033"
    elif score < 75: level, color = "High", "#ff7f2a"
    else: level, color = "Very High", "#ff2a2a"
    return {"score": round(score,1), "level": level, "color": color, "reasons": reasons,
            "moon_phase": moon_phase_text, "moon_age": moon_age,
            "temp_c": temp, "humidity": humidity, "precip_mm": precip, "condition": cond}

# ---------------- Geocoding (Nominatim) ----------------

def _load_geocode_cache() -> pd.DataFrame:
    if os.path.exists(GEOCODE_CACHE):
        try:
            return pd.read_parquet(GEOCODE_CACHE)
        except Exception:
            alt = GEOCODE_CACHE.replace(".parquet", ".csv")
            if os.path.exists(alt):
                return pd.read_csv(alt)
    return pd.DataFrame(columns=["q","lat","lon"])


def _save_geocode_cache(df: pd.DataFrame):
    try:
        df.to_parquet(GEOCODE_CACHE, index=False)
    except Exception:
        df.to_csv(GEOCODE_CACHE.replace(".parquet", ".csv"), index=False)


def nominatim_geocode(q: str) -> tuple[float|None, float|None]:
    cache: pd.DataFrame = st.session_state.get("_geocode_cache")
    if cache is None:
        cache = _load_geocode_cache()
        st.session_state._geocode_cache = cache
    hit = cache[cache["q"] == q]
    if not hit.empty:
        row = hit.iloc[0]
        return (float(row["lat"]) if pd.notna(row["lat"]) else None, float(row["lon"]) if pd.notna(row["lon"]) else None)
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": q, "format": "json", "countrycodes": "jp", "limit": 1}
        headers = {"User-Agent": "ehime-crime-sense/1.0 (contact: example@example.com)"}
        r = requests.get(url, params=params, headers=headers, timeout=12)
        r.raise_for_status()
        arr = r.json()
        if arr:
            lat = float(arr[0]["lat"]); lon = float(arr[0]["lon"]) 
        else:
            lat = lon = None
    except Exception:
        lat = lon = None
    cache = pd.concat([cache, pd.DataFrame({"q":[q], "lat":[lat], "lon":[lon]})], ignore_index=True)
    st.session_state._geocode_cache = cache
    _save_geocode_cache(cache)
    return lat, lon


def geocode_df(df: pd.DataFrame, addr_col: str, muni_col: str|None = None, max_rows: int = 1000) -> pd.DataFrame:
    out = df.copy()
    out["lat"] = np.nan; out["lon"] = np.nan
    queries = []
    for _, r in out.iterrows():
        a = str(r.get(addr_col) or "").strip()
        m = str(r.get(muni_col) or "").strip() if muni_col else ""
        if a and m: q = f"愛媛県 {m} {a}"
        elif a: q = f"愛媛県 {a}"
        elif m: q = f"愛媛県 {m}"
        else: q = ""
        queries.append(q)
    pbar = st.progress(0, text="ジオコーディング中…")
    total = min(max_rows, len(out))
    for i in range(total):
        q = queries[i]
        if q:
            lat, lon = nominatim_geocode(q)
            if lat is not None and lon is not None:
                out.at[i, "lat"] = lat; out.at[i, "lon"] = lon
        pbar.progress(int((i+1)/total*100), text=f"{i+1}/{total} 件 処理…")
        time.sleep(1.0)  # レート制御
    pbar.empty()
    return out

# ---------------- Overpass（POI） ----------------
@st.cache_data(show_spinner=False, ttl=60*30)
def query_overpass_poi(lat: float, lon: float, radius_m: int = 1200) -> pd.DataFrame:
    query = f"""
    [out:json][timeout:25];
    (
      node(around:{radius_m},{lat},{lon})["amenity"="bicycle_parking"];
      node(around:{radius_m},{lat},{lon})["amenity"="convenience"];
      node(around:{radius_m},{lat},{lon})["amenity"="bar"];
      node(around:{radius_m},{lat},{lon})["amenity"="nightclub"];
      node(around:{radius_m},{lat},{lon})["amenity"="atm"];
      node(around:{radius_m},{lat},{lon})["shop"="bicycle"];
      node(around:{radius_m},{lat},{lon})["shop"="convenience"];
      node(around:{radius_m},{lat},{lon})["highway"="bus_stop"];
      node(around:{radius_m},{lat},{lon})["railway"="station"];
      node(around:{radius_m},{lat},{lon})["amenity"="parking"];
      node(around:{radius_m},{lat},{lon})["leisure"="park"];
    );
    out center;
    """
    url = "https://overpass-api.de/api/interpreter"
    r = requests.post(url, data=query.encode("utf-8"), timeout=30, headers={"User-Agent":"ehime-crime-sense/1.0"})
    r.raise_for_status()
    js = r.json()
    elems = js.get("elements", [])
    rows = []
    for e in elems:
        tags = e.get("tags", {})
        rows.append({
            "name": tags.get("name") or tags.get("name:ja") or "(名称なし)",
            "type": tags.get("amenity") or tags.get("shop") or tags.get("railway") or tags.get("highway") or tags.get("leisure"),
            "lat": e.get("lat"),
            "lon": e.get("lon"),
        })
    return pd.DataFrame(rows)

# ---------------- Map helpers ----------------

def _add_common_map_ui(m: folium.Map):
    folium.TileLayer("cartodbpositron", name="Light").add_to(m)
    folium.TileLayer("cartodbdark_matter", name="Dark").add_to(m)
    folium.TileLayer("OpenStreetMap", name="OSM").add_to(m)
    folium.TileLayer(tiles="https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png", attr="OSM HOT", name="OSM HOT").add_to(m)
    Fullscreen(position="topleft").add_to(m)
    MiniMap(zoom_level_fixed=5, toggle_display=True).add_to(m)
    MeasureControl(position="topleft", primary_length_unit="meters").add_to(m)
    MousePosition(position="bottomright", separator=" | ", prefix="座標",
                  lat_formatter="function(num){return L.Util.formatNum(num,6);}",
                  lng_formatter="function(num){return L.Util.formatNum(num,6);}"
                 ).add_to(m)
    if HAS_LOCATE:
        try:
            LocateControl(auto_start=False, flyTo=True, keepCurrentZoomLevel=True).add_to(m)
        except Exception:
            pass
    folium.LayerControl(collapsed=True).add_to(m)


def _plot_past_crimes_approx(m: folium.Map, df: pd.DataFrame | None):
    if df is None or df.empty:
        return
    cluster = MarkerCluster(name="2019 概位置").add_to(m)
    rng = np.random.default_rng(42)
    rows = df.dropna(subset=["municipality"]).copy()
    rows = rows[rows["municipality"].str.len() > 0]
    rows = rows.sample(min(3000, len(rows)), random_state=42) if len(rows) > 3000 else rows
    for _, r in rows.iterrows():
        muni = str(r.get("municipality") or "")
        latlon = None
        if muni in MUNI_CENTER:
            latlon = MUNI_CENTER[muni]
        else:
            for k in MUNI_CENTER.keys():
                if muni.startswith(k) or k in muni:
                    latlon = MUNI_CENTER[k]
                    break
        if not latlon:
            continue
        lat0, lon0 = latlon
        lat = lat0 + float(rng.normal(0, 0.0010))  # ~100m
        lon = lon0 + float(rng.normal(0, 0.0012))  # ~120m
        ctype = str(r.get("ctype") or "不明")
        color, icon = CTYPE_STYLE.get(ctype, ("gray", "glyphicon-question-sign"))
        date_txt = "" if pd.isna(r.get("date")) else pd.to_datetime(r.get("date")).strftime("%Y-%m-%d")
        html = f"<b>{ctype}</b><br/>市町村: {muni}<br/>発生日: {date_txt}"
        folium.Marker([lat, lon], popup=folium.Popup(html, max_width=260), icon=folium.Icon(color=color, icon=icon)).add_to(cluster)


def _plot_points(m: folium.Map, df: pd.DataFrame, name: str, color: str, icon: str = "info-sign"):
    if df is None or df.empty:
        return
    cluster = MarkerCluster(name=name).add_to(m)
    for _, r in df.dropna(subset=["lat","lon"]).iterrows():
        nm = str(r.get("name") or r.get("ctype") or r.get("address") or "地点")
        html = folium.Popup(nm, max_width=250)
        folium.Marker([float(r["lat"]), float(r["lon"])], popup=html, icon=folium.Icon(color=color, icon=icon)).add_to(cluster)


def render_map(lat: float, lon: float, snap: dict | None, approx_df: pd.DataFrame | None, geocoded_df: pd.DataFrame | None, poi_df: pd.DataFrame | None):
    m = folium.Map(location=EHIME_CENTER, zoom_start=9, tiles="cartodbpositron")
    _add_common_map_ui(m)
    _plot_past_crimes_approx(m, approx_df)
    if geocoded_df is not None and not geocoded_df.empty:
        _plot_points(m, geocoded_df, "住所ジオコーディング結果", "blue", "ok-sign")
    if poi_df is not None and not poi_df.empty:
        _plot_points(m, poi_df, "リスク指標POI", "purple", "record")

    popup_html = "<div style='color:#111;'>地点をクリックして選択</div>"
    if snap:
        radius = 1500 if snap["score"] < 50 else (2500 if snap["score"] < 75 else 3500)
        folium.Circle([lat, lon], radius=radius, color=snap["color"], fill=True, fill_opacity=0.25, weight=2).add_to(m)
        popup_html = f"""
        <div style=\"color:#111;\"><b>警戒度:</b> {snap['score']} ({snap['level']})<br/>
        <b>月齢:</b> {snap.get('moon_age')}（{snap.get('moon_phase')}）<br/>
        <b>天候:</b> {snap['condition']} / {snap['temp_c']}℃ / 降水{snap['precip_mm']}mm</div>
        """
    folium.Marker([lat, lon], popup=folium.Popup(popup_html, max_width=320), draggable=True,
                  icon=folium.Icon(color="red" if snap and snap["score"]>=75 else ("orange" if snap and snap["score"]>=50 else "blue"), icon="info-sign")).add_to(m)
    return m

# ---------------- 将来最大リスク日 ----------------

def projected_score_for_day(lat: float, lon: float, base_df: pd.DataFrame | None, d: datetime) -> float:
    dt = datetime(d.year, d.month, d.day, 21, 0, tzinfo=JST)
    moon = get_mgpn_moon(lat, lon, dt) or {}
    weather = {"temp_c": 26.0, "humidity": 65, "precip_mm": 0.0, "condition": "—"}
    snap = compute_risk_score(weather, dt, base_df, moon)
    bonus = 2.0 if is_full_moon_like_text(snap.get("moon_phase"), snap.get("moon_age")) else 0.0
    return float(snap["score"]) + bonus


def find_peak_day_next_30(lat: float, lon: float, base_df: pd.DataFrame | None, start: datetime) -> tuple[datetime, float]:
    best_d, best_s = None, -1.0
    for i in range(0, 30):
        d = start + timedelta(days=i)
        s = projected_score_for_day(lat, lon, base_df, d)
        if s > best_s:
            best_s, best_d = s, d
        time.sleep(0.05)
    return best_d, best_s

# ---------------- streamlit-folium 互換（単発呼び出し） ----------------

def st_folium_compat(m: folium.Map, *, height: int, key: str, need_click: bool):
    """引数名の版差を事前判定して *1回だけ* 呼び出す。失敗時は HTML 埋め込みに切替。"""
    try:
        params = inspect.signature(st_folium).parameters
        if "returned_objects" in params:
            return st_folium(m, height=height, returned_objects=["last_clicked"] if need_click else [], key=key)
        elif "returns" in params:
            return st_folium(m, height=height, returns=["last_clicked"] if need_click else None, key=key)
        else:
            return st_folium(m, height=height, key=key)
    except Exception:
        # 最後の保険：HTML直埋め（クリックは拾えないが描画は保証）
        try:
            html = m.get_root().render()
            components.html(html, height=height)
            return {}
        except Exception:
            st.error("地図の描画に失敗しました。ネットワーク（タイルCDN）やブラウザの制限をご確認ください。")
            return {}

# ---------------- Gemini（任意） ----------------

def gemini_explain(snap: dict, now_dt: datetime) -> str | None:
    if not GEMINI_KEY:
        return None
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel(model_name=GEMINI_MODEL, generation_config={"temperature": 0.2}, system_instruction=(
            "あなたは防犯アナリストAIです。与えられた要因から、愛媛県の犯罪/異常行動リスクの根拠を、過度な断定を避けて簡潔に日本語で説明してください。"
            "満月効果は限定的、気温・時間帯・降雨などの影響は統計的示唆あり、を前提に、最後に取るべき具体的行動で締めること。"))
        prompt = (f"現在: {now_dt.strftime('%Y-%m-%d %H:%M JST')}\n"
                  f"スコア: {snap['score']} ({snap['level']})\n"
                  f"要因: 気温{snap['temp_c']}℃, 湿度{snap['humidity']}%, 降水{snap['precip_mm']}mm, 天候:{snap['condition']}, "
                  f"月齢:{snap.get('moon_age')}（{snap.get('moon_phase')}）\n内部理由: {', '.join(snap['reasons'])}\n"
                  "一般向けの注意喚起コメントを120〜200字で。")
        resp = model.generate_content(prompt)
        return (resp.text or "").strip()
    except Exception:
        return None

# ---------------- Main ----------------

def main():
    st.set_page_config(APP_TITLE, page_icon="🚨", layout="wide")
    st.markdown(DRAMA_CSS, unsafe_allow_html=True)
    st.markdown(f"<h1 class='page-title'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.caption("愛媛県全域。地図クリックで地点選択 → 『分析する』。月齢は mgpn v2(JSON)優先（v3フォールバック）。2019年データの概位置は維持、住所CSVは座標化して実点表示。POIもNETから補助表示。")

    # --- State ---
    if "sel_lat" not in st.session_state: st.session_state.sel_lat = INIT_LAT
    if "sel_lon" not in st.session_state: st.session_state.sel_lon = INIT_LON
    if "last_snap" not in st.session_state: st.session_state.last_snap = None
    if "geocoded_df" not in st.session_state: st.session_state.geocoded_df = None
    if "poi_df" not in st.session_state: st.session_state.poi_df = None

    # --- Sidebar ---
    with st.sidebar:
        st.markdown("### 操作")
        st.session_state.sel_lat = st.number_input("選択緯度", value=float(st.session_state.sel_lat), format="%.6f", key="lat_input")
        st.session_state.sel_lon = st.number_input("選択経度", value=float(st.session_state.sel_lon), format="%.6f", key="lon_input")
        analyze = st.button("🔎 分析する", use_container_width=True, key="btn_analyze")
        reset = st.button("📍 初期地点へ", use_container_width=True, key="btn_reset")
        st.divider()

        st.markdown("### CSVアップロード（住所→座標）")
        up = st.file_uploader("住所列を含むCSVを指定", type=["csv"], accept_multiple_files=False, key="uploader")
        addr_col_name = st.text_input("住所列名（例: 住所 / 所在地）", value="住所", key="addr_col")
        muni_col_name = st.text_input("市町村列名（任意: 市町村名 等）", value="", key="muni_col")
        do_geocode = st.button("📌 ジオコーディング実行", use_container_width=True, key="btn_geocode")
        st.caption("※ Nominatim を1秒/件で丁寧に呼びます。結果は /mnt/data にキャッシュ。")
        st.divider()

        st.markdown("### POI（NET）")
        poi_radius = st.slider("POI探索半径(m)", min_value=400, max_value=3000, value=1200, step=100, key="poi_radius")
        fetch_poi = st.button("📍 近傍POIを取得", use_container_width=True, key="btn_poi")
        st.divider()

        st.markdown("### バージョン")
        st.write({"streamlit_folium": getattr(stf, "__version__", "?"), "folium": getattr(folium, "__version__", "?")})

    # --- Load base 2019 data (必須機能) ---
    @st.cache_data(show_spinner=False)
    def _load_2019():
        return load_all_crime_2019(DATA_GLOBS)
    base_df = _load_2019()

    # --- Geocode uploaded CSV ---
    if up is not None and do_geocode:
        try:
            raw = up.read()
            enc = (chardet.detect(raw).get("encoding") or "utf-8").lower()
            df_up = pd.read_csv(io.BytesIO(raw), encoding=enc)
        except Exception:
            df_up = pd.read_csv(io.BytesIO(raw))
        addr_col = addr_col_name if addr_col_name in df_up.columns else None
        muni_col = muni_col_name if muni_col_name and (muni_col_name in df_up.columns) else None
        if addr_col is None and muni_col is None:
            st.error("住所列名が見つかりません。サンプル：'住所' '所在地' 等。")
        else:
            with st.spinner("住所を座標へ変換中…"):
                geocoded_df = geocode_df(df_up, addr_col or muni_col, muni_col if addr_col else None, max_rows=min(1000, len(df_up)))
            st.session_state.geocoded_df = geocoded_df
            st.success(f"ジオコーディング完了: {geocoded_df.dropna(subset=['lat','lon']).shape[0]} / {len(geocoded_df)} 件に座標付与")

    # --- POI fetch ---
    if fetch_poi:
        with st.spinner("Overpassから近傍POIを取得中…"):
            poi_df = query_overpass_poi(st.session_state.sel_lat, st.session_state.sel_lon, radius_m=poi_radius)
        st.session_state.poi_df = poi_df
        if poi_df is not None and not poi_df.empty:
            st.success(f"POI取得: {len(poi_df)} 件")
        else:
            st.info("該当POIが見つかりませんでした。半径を広げてお試しください。")

    geocoded_df = st.session_state.geocoded_df
    poi_df = st.session_state.poi_df

    # --- Map (唯一の st_folium 呼び出し) ---
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    fmap = render_map(st.session_state.sel_lat, st.session_state.sel_lon, st.session_state.last_snap, base_df, geocoded_df, poi_df)
    out = st_folium_compat(fmap, height=600, key="map_main", need_click=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if out and isinstance(out, dict) and out.get("last_clicked"):
        lat = out["last_clicked"].get("lat"); lon = out["last_clicked"].get("lng")
        if lat is not None and lon is not None:
            if (EHIME_BBOX["min_lat"] <= lat <= EHIME_BBOX["max_lat"]) and (EHIME_BBOX["min_lon"] <= lon <= EHIME_BBOX["max_lon"]):
                st.session_state.sel_lat = float(lat); st.session_state.sel_lon = float(lon)
            else:
                st.warning("選択地点が愛媛県の想定範囲外です。")

    # --- Buttons logic ---
    if reset:
        st.session_state.sel_lat = INIT_LAT
        st.session_state.sel_lon = INIT_LON
        st.session_state.last_snap = None
        st.rerun()

    if analyze:
        overlay = st.empty(); p = None
        try:
            overlay.markdown("""
                <div class='overlay'><div class='overlay-content'>
                  <div class='overlay-title'>解析中</div>
                  <div class='overlay-sub'>気象・月齢（mgpn）・2019傾向を統合しています…</div>
                  <div class='loader'></div>
                </div></div>""", unsafe_allow_html=True)
            p = st.progress(0, text="準備中…")
            for i, txt in [(15, "気象の取得…"), (40, "月齢（mgpn）の取得…"), (70, "2019年傾向の補正…"), (100, "スコア集計…")]:
                time.sleep(0.35); p.progress(i, text=txt)
            now_dt = datetime.now(JST)
            lat, lon = st.session_state.sel_lat, st.session_state.sel_lon
            weather = get_weather(lat, lon)
            moon_info = get_mgpn_moon(lat, lon, now_dt)
            snap = compute_risk_score(weather, now_dt, base_df, moon_info)
            st.session_state.last_snap = snap
        finally:
            if p: p.empty()
            overlay.empty()

    # --- Results ---
    snap = st.session_state.last_snap
    if snap:
        if snap["score"] >= 75:
            st.markdown(f"<div class='alert-bar'>警報：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。周囲に注意。</div>", unsafe_allow_html=True)
        elif snap["score"] >= 50:
            st.markdown(f"<div class='alert-bar' style='background:linear-gradient(90deg, rgba(200,120,0,0.85), rgba(255,170,0,0.85));'>注意：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。</div>", unsafe_allow_html=True)

        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**現在のリスク**")
            st.markdown(f"<div class='score-big' style='color:{snap['color']};'>{int(round(snap['score']))}</div>", unsafe_allow_html=True)
            st.markdown(f"<span class='badge'>{snap['level']}</span>", unsafe_allow_html=True)
            st.markdown(f"**月齢**：{snap.get('moon_age')}（{snap.get('moon_phase')}） / **天候**：{snap['condition']} / **気温**：{snap['temp_c']}℃ / **降水**：{snap['precip_mm']}mm / **湿度**：{snap['humidity']}%", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**内部理由（ヒューリスティック + 2019補正 + 月齢）**")
            for r in snap["reasons"]: st.write("・", r)
            st.markdown("</div>", unsafe_allow_html=True)

            if base_df is not None and not base_df.empty:
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown("**2019データ概要（検出件数）**")
                vc = base_df["ctype"].value_counts().rename("件数").to_frame()
                st.dataframe(vc)
                st.markdown("</div>", unsafe_allow_html=True)

        with c2:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**将来30日の最大リスク予測（21:00評価）**")
            with st.spinner("月齢を用いて30日予測を計算中…"):
                best_day, best_score = find_peak_day_next_30(st.session_state.sel_lat, st.session_state.sel_lon, base_df, datetime.now(JST).replace(hour=0,minute=0,second=0,microsecond=0))
            if best_day:
                st.success(f"最も上がりそうな日: {best_day.strftime('%Y-%m-%d (%a)')} ／ 推定スコア: {best_score:.1f}")
            else:
                st.info("予測に失敗しました。時間をおいて再試行してください。")
            st.markdown("</div>", unsafe_allow_html=True)

    # --- Footer ---
    st.caption("※ mgpnはv2(JSON)→v3の順で呼出し、30分キャッシュ。住所のジオコーディングはNominatim(1秒/件)で行い、/mnt/dataにキャッシュします。POIはOverpass APIから取得。将来予測は月齢・曜日・季節ヒューリスティックの概算です。2019年犯罪データの概位置レイヤは維持しています。")


if __name__ == "__main__":
    main()
