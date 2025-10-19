# -*- coding: utf-8 -*-
# ============================================================
# 愛媛セーフティ・プラットフォーム / Ehime Safety Platform
# SIBYL拡張（犯罪係数モード）+ 既存機能（気象・月齢・2019統計・POI・CSVジオコーディング）
# ============================================================

import os, re, io, glob, json, time, math, random, inspect, traceback
from datetime import datetime, timedelta, timezone

import requests
import numpy as np
import pandas as pd
import streamlit as st
import folium
from folium.plugins import MiniMap, MousePosition, MeasureControl, Fullscreen, LocateControl, MarkerCluster
from streamlit_folium import st_folium
import chardet
import streamlit.components.v1 as components

# ---------------------------
# 基本設定
# ---------------------------
JST = timezone(timedelta(hours=9))
APP_TITLE = "愛媛セーフティ・プラットフォーム / Ehime Safety Platform"
EHIME_CENTER_LAT = 33.8416
EHIME_CENTER_LON = 132.7661
EHIME_BBOX = {"min_lat": 32.8, "max_lat": 34.6, "min_lon": 131.8, "max_lon": 134.0}

INIT_LAT = 34.27717   # 上島町付近
INIT_LON = 133.20986

DATA_GLOBS = [
    "./ehime_2019*.csv",
    "./data/ehime_2019*.csv",
    "/mnt/data/ehime_2019*.csv",
]

GEOCODE_CACHE_PATH = "/mnt/data/geocode_cache.parquet"
MUNI_GEOCODE_CACHE_PATH = "/mnt/data/muni_geocode_cache.json"

USER_AGENT = "ESP-SIBYL/1.0 (Nominatim polite; contact: local-app)"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
EHIME_POLICE_URL = "https://www.police.pref.ehime.jp/sokuho/sokuho.htm"

CITY_NAMES = [
    "松山市","今治市","新居浜市","西条市","大洲市","伊予市","四国中央市","西予市","東温市",
    "上島町","久万高原町","松前町","砥部町","内子町","伊方町","松野町","鬼北町","愛南町"
]

# ---------------------------
# スタイル（SIBYL風）
# ---------------------------
DRAMA_CSS = """
<style>
.main, .stApp { background: #0b0f14; color: #e6eef7; }
section[data-testid="stSidebar"] { background: #0e141b; }
.score-big { font-size: 72px; font-weight: 900; letter-spacing: 1.5px;
  text-shadow: 0 0 8px rgba(0,255,200,0.25), 0 0 16px rgba(0,255,200,0.15);
  margin: 0; line-height: 1.0; }
.badge { display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 700;
  background: linear-gradient(135deg, #1c2633 0%, #121821 100%);
  border: 1px solid rgba(255,255,255,0.1); color:#d9e6f2; margin-left: 8px; }
.alert-bar { position: sticky; top: 0; z-index: 1000;
  background: linear-gradient(90deg, rgba(0,180,180,0.85), rgba(0,255,220,0.85));
  color: #001; padding: 10px 14px; border-bottom: 2px solid rgba(0,255,220,0.35);
  box-shadow: 0 4px 24px rgba(0,255,220,0.35); }
.card { background: linear-gradient(135deg, #121821, #0e141b);
  border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 16px 18px; margin: 8px 0;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35); }
button[kind="primary"], .stButton>button {
  background: linear-gradient(135deg, #0b3941, #0a2a30) !important;
  border: 1px solid rgba(0,255,220,0.15) !important;
  color: #dff !important; font-weight: 700 !important;
  border-radius: 12px !important; }
.mute { color: #a8b7c7; font-size: 13px; }
.gauge-wrap { position: relative; height: 12px; background: #0d2230; border-radius: 999px;
  border:1px solid rgba(255,255,255,0.08); overflow:hidden; }
.gauge-bar { position:absolute; left:0; top:0; bottom:0; background: linear-gradient(90deg,#08f7ff,#09fbd3); }
.sybil-num { font-weight:900; font-size:48px; letter-spacing:0.08em; color:#09fbd3;
  text-shadow: 0 0 10px rgba(9,251,211,.4); }
.sybil-tag { display:inline-block; padding:3px 8px; border-radius:10px; margin-left:8px;
  border:1px solid rgba(9,251,211,.35); color:#bafef3; background:rgba(9,251,211,.08); font-weight:700;}
.rank-pill { display:inline-block; padding:2px 8px; border-radius:999px; font-weight:700; margin-left:6px;
  background:#10232c; border:1px solid rgba(9,251,211,.25); color:#aaf3e9; }
</style>
"""

# ---------------------------
# Secrets / API Keys（任意）
# ---------------------------
try:
    WEATHERAPI_KEY = st.secrets.get("weatherapi", {}).get("api_key", "")
    OPENWEATHER_KEY = st.secrets.get("openweather", {}).get("api_key", "")
    GEMINI_KEY = st.secrets.get("gemini", {}).get("api_key", "")
    GEMINI_MODEL = st.secrets.get("gemini", {}).get("model", "gemini-2.5-flash")
except Exception:
    WEATHERAPI_KEY = OPENWEATHER_KEY = GEMINI_KEY = ""
    GEMINI_MODEL = "gemini-2.5-flash"

# ---------------------------
# ユーティリティ
# ---------------------------
def read_csv_robust(path: str) -> pd.DataFrame:
    with open(path, "rb") as f: raw = f.read()
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
    if not date_col: date_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["date","day","time","occur"])), None)
    muni_col = next((c for c in df.columns if re.search(r"(市|町|村).*名", str(c)) or re.search(r"(市町村|自治体|地域)", str(c))), None)
    if not muni_col: muni_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["municipality","city","town","area","region"])), None)
    type_col = next((c for c in df.columns if re.search(r"(手口|罪|罪種|種別|分類)", str(c))), None)
    if not type_col: type_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["type","category","kind","crime"])), None)
    return {"date": date_col, "municipality": muni_col, "ctype": type_col}

def parse_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def jitter_latlon(lat: float, lon: float, meters: float = 110.0) -> tuple[float, float]:
    dlat = (random.random() - 0.5) * (meters / 111000.0) * 2
    scale = math.cos(math.radians(lat))
    dlon = (random.random() - 0.5) * (meters / (111000.0 * max(scale, 1e-6))) * 2
    return lat + dlat, lon + dlon

def clamp(v, lo, hi): return lo if v < lo else (hi if v > hi else v)

# ---------------------------
# 2019 CSV ロード
# ---------------------------
def load_all_crime_2019(globs: list[str]) -> pd.DataFrame | None:
    files = []
    for g in globs: files.extend(glob.glob(g))
    files = sorted(set(files))
    if not files: return None
    frames = []
    for fp in files:
        df = read_csv_robust(fp)
        g = guess_columns(df)
        if g["date"] is None: df["date"] = pd.NaT
        else:
            df.rename(columns={g["date"]: "date"}, inplace=True)
            df["date"] = parse_date_series(df["date"])
        if g["municipality"] is None: df["municipality"] = ""
        else:
            df.rename(columns={g["municipality"]: "municipality"}, inplace=True)
            df["municipality"] = df["municipality"].astype(str)
        if g["ctype"] is None:
            base = os.path.basename(fp)
            mapping = {
                "hittakuri":"ひったくり","syazyounerai":"車上ねらい","buhinnerai":"部品ねらい",
                "zidousyatou":"自動車盗","ootobaitou":"オートバイ盗","zitensyatou":"自転車盗",
                "zidouhanbaikinerai":"自動販売機ねらい",
            }
            guess = None
            for k,v in mapping.items():
                if k in base: guess = v; break
            df["ctype"] = guess if guess else "不明"
        else:
            df.rename(columns={g["ctype"]:"ctype"}, inplace=True)
            df["ctype"] = df["ctype"].astype(str)
        # 2019のみ（欠損は許容）
        df = df[(df["date"].dt.year == 2019) | (df["date"].isna())]
        frames.append(df[["date","municipality","ctype"]])
    return pd.concat(frames, ignore_index=True) if frames else None

# ---------------------------
# 気象
# ---------------------------
def get_weather_weatherapi(lat, lon):
    try:
        if not WEATHERAPI_KEY: return None
        base = "https://api.weatherapi.com/v1"
        p = f"key={WEATHERAPI_KEY}&q={lat},{lon}"
        r = requests.get(f"{base}/current.json?{p}&aqi=no", timeout=10)
        r.raise_for_status()
        curr = r.json()
        return {
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
        if not OPENWEATHER_KEY: return None
        url = "https://api.openweathermap.org/data/2.5/weather"
        p = {"lat": lat, "lon": lon, "appid": OPENWEATHER_KEY, "units": "metric", "lang":"ja"}
        r = requests.get(url, params=p, timeout=10); r.raise_for_status(); jd = r.json()
        return {
            "temp_c": jd["main"]["temp"], "humidity": jd["main"]["humidity"],
            "condition": jd["weather"][0]["description"], "precip_mm": 0.0,
            "wind_kph": jd.get("wind",{}).get("speed",0.0)*3.6,
        }
    except Exception:
        return None

def get_weather(lat, lon):
    w = get_weather_weatherapi(lat, lon) or get_weather_openweather(lat, lon)
    return w or {"temp_c": 26.0, "humidity": 70, "condition": "晴れ", "precip_mm": 0.0, "wind_kph": 8.0}

# ---------------------------
# 月齢（mgpn v2→v3）
# ---------------------------
def _extract_moonage(payload) -> float | None:
    if payload is None: return None
    obj = payload[0] if isinstance(payload, list) and payload else payload
    for k in ["moonage","moon_age","moonAge","age"]:
        if obj and k in obj and obj[k] is not None:
            try: return float(obj[k])
            except: pass
    return None

def _phase_text_from_age(age: float | None) -> str | None:
    if age is None: return None
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

@st.cache_data(show_spinner=False, ttl=60*30)
def get_mgpn_moon(lat: float, lon: float, dt_jst: datetime) -> dict | None:
    t = dt_jst.strftime("%Y-%m-%dT%H:%M")
    headers = {"Accept":"application/json"}
    for base in ["https://mgpn.org/api/moon/v2position.cgi", "https://mgpn.org/api/moon/v3position.cgi"]:
        for _ in range(3):
            try:
                params = {"time": t, "lat": f"{lat:.6f}", "lon": f"{lon:.6f}"}
                if "v2" in base: params.update({"loop":1,"interval":0})
                r = requests.get(base, params=params, headers=headers, timeout=8)
                r.raise_for_status(); payload = r.json()
                age = _extract_moonage(payload)
                obj = payload[0] if isinstance(payload,list) and payload else payload
                alt = float(obj.get("altitude")) if obj and "altitude" in obj else None
                azi = float(obj.get("azimuth")) if obj and "azimuth" in obj else None
                return {"moon_age":age, "phase_text":_phase_text_from_age(age), "altitude":alt, "azimuth":azi}
            except Exception:
                time.sleep(0.6)
    return None

def is_full_moon_like_text(phase_text: str | None, age: float | None) -> bool:
    if phase_text and ("満月" in phase_text): return True
    if age is not None:
        a = age % 29.53
        return 13.3 <= a <= 16.3
    return False

# ---------------------------
# リスクスコア（0–100）
# ---------------------------
def compute_risk_score(weather: dict, now_dt: datetime, all_df: pd.DataFrame | None, moon_info: dict | None) -> dict:
    score = 0.0; reasons = []
    temp = float(weather.get("temp_c", 20.0))
    precip = float(weather.get("precip_mm", 0.0))
    humidity = float(weather.get("humidity", 60))
    cond = str(weather.get("condition", ""))

    if   temp >= 32: add = 42
    elif temp >= 30: add = 36
    elif temp >= 27: add = 28
    elif temp >= 25: add = 20
    elif temp >= 22: add = 10
    else: add = 0
    score += add
    if add>0: reasons.append(f"気温{temp:.0f}℃:+{add}")

    if precip >= 10: score -= 20; reasons.append("強い降雨:-20")
    elif precip >= 1: score -= 8; reasons.append("降雨あり:-8")

    hour = now_dt.hour
    if 20 <= hour <= 23 or 0 <= hour <= 4: score += 15; reasons.append("夜間:+15")
    elif 17 <= hour < 20: score += 7; reasons.append("夕方:+7")

    if now_dt.weekday() in (4,5): score += 6; reasons.append("週末(+金土):+6")

    moon_age = moon_info.get("moon_age") if moon_info else None
    phase_tx = moon_info.get("phase_text") if moon_info else None
    if is_full_moon_like_text(phase_tx, moon_age): score += 5; reasons.append("満月相当:+5")

    if humidity >= 80: score += 3; reasons.append("高湿度:+3")

    if all_df is not None and not all_df.empty:
        sub = all_df.copy(); sub["month"] = sub["date"].dt.month
        month_ratio = len(sub[sub["month"]==now_dt.month]) / max(1,len(sub))
        if   month_ratio >= 0.12: score += 6; reasons.append("2019傾向(同月比 多め):+6")
        elif month_ratio >= 0.08: score += 3; reasons.append("2019傾向(同月比 やや多め):+3")
        if "ctype" in sub.columns:
            vc = sub["ctype"].value_counts(normalize=True)
            outdoor_like = float(vc.get("ひったくり",0)+vc.get("車上ねらい",0)+vc.get("自転車盗",0)+vc.get("オートバイ盗",0))
            if   outdoor_like >= 0.45: score += 5; reasons.append("2019傾向(屋外系多):+5")
            elif outdoor_like >= 0.30: score += 2; reasons.append("2019傾向(屋外系やや多):+2")

    score = float(np.clip(score, 0, 100))
    level = "Low" if score<25 else ("Moderate" if score<50 else ("High" if score<75 else "Very High"))
    color = {"Low":"#0aa0ff","Moderate":"#ffd033","High":"#ff7f2a","Very High":"#ff2a2a"}[level]
    return {"score": round(score,1), "level": level, "color": color, "reasons": reasons,
            "moon_phase": phase_tx, "moon_age": moon_age,
            "temp_c": temp, "humidity": humidity, "precip_mm": precip, "condition": cond}

# ---------------------------
# 県警速報スクレイピング（最新記事 → 市町出現回数）
# ---------------------------
@st.cache_data(show_spinner=False, ttl=10*60)
def fetch_police_muni_counts() -> dict:
    try:
        r = requests.get(EHIME_POLICE_URL, headers={"User-Agent": USER_AGENT}, timeout=12)
        r.raise_for_status()
        r.encoding = r.apparent_encoding or r.encoding or "utf-8"
        text = re.sub(r"\s+", " ", r.text)
        text = re.sub(r"<[^>]+>", " ", text)
    except Exception:
        return {}

    counts = {c: 0 for c in CITY_NAMES}
    for c in CITY_NAMES:
        counts[c] = len(re.findall(re.escape(c), text))

    mx = max(counts.values()) if counts else 0
    if mx > 0:
        for k,v in counts.items():
            counts[k] = int(min(v, max(1, mx)))
    return counts

# ---------------------------
# CC（Crime Coefficient 0–300）
# ---------------------------
def compute_cc_from_risk_and_news(risk_score_0_100: float, recent_count: int) -> int:
    cc = int(round(risk_score_0_100 * 2.4 + 30 * min(int(recent_count), 5)))
    return int(clamp(cc, 0, 300))

# ---------------------------
# st_folium 互換ラッパ
# ---------------------------
def call_st_folium_with_fallback(m: folium.Map, height: int, key: str, return_last_clicked: bool = False):
    args = inspect.signature(st_folium).parameters
    kwargs = {"height": height, "key": key}
    try:
        if "returned_objects" in args and return_last_clicked:
            kwargs["returned_objects"] = ["last_clicked"]
        return st_folium(m, **kwargs)
    except TypeError:
        try:
            kwargs.pop("returned_objects", None)
            return st_folium(m, **kwargs)
        except Exception:
            pass
    except Exception:
        pass
    try:
        html = m.get_root().render()
        components.html(html, height=height, scrolling=False)
    except Exception:
        st.error("地図描画に失敗しました。ネットワークやタイルの到達性をご確認ください。")
    return {}

# ---------------------------
# Map UI
# ---------------------------
def _add_common_map_ui(m: folium.Map):
    folium.TileLayer("cartodbpositron", name="Light").add_to(m)
    folium.TileLayer("cartodbdark_matter", name="Dark").add_to(m)
    folium.TileLayer("OpenStreetMap", name="OSM").add_to(m)
    folium.TileLayer(
        tiles="https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
        attr="OSM HOT", name="OSM HOT", control=True
    ).add_to(m)
    Fullscreen(position="topleft").add_to(m)
    MiniMap(zoom_level_fixed=5, toggle_display=True).add_to(m)
    MeasureControl(position="topleft", primary_length_unit="meters").add_to(m)
    MousePosition(
        position="bottomright", separator=" | ", prefix="座標",
        lat_formatter="function(num) {return L.Util.formatNum(num, 6);}",
        lng_formatter="function(num) {return L.Util.formatNum(num, 6);}"
    ).add_to(m)
    try:
        LocateControl(auto_start=False, flyTo=True, keepCurrentZoomLevel=True).add_to(m)
    except Exception:
        pass
    folium.LayerControl(collapsed=True).add_to(m)

def render_map_selectable(lat: float, lon: float, snap: dict | None):
    m = folium.Map(location=[EHIME_CENTER_LAT, EHIME_CENTER_LON], zoom_start=9, tiles="cartodbpositron")
    _add_common_map_ui(m)
    popup_html = "<div style='color:#111;'>地点をクリックして選択</div>"
    if snap:
        r = 1500 if snap["score"] < 50 else (2500 if snap["score"] < 75 else 3500)
        folium.Circle(location=[lat, lon], radius=r, color=snap["color"], fill=True, fill_opacity=0.25, weight=2).add_to(m)
        popup_html = f"<div style='color:#111;'><b>現在リスク:</b> {snap['score']} ({snap['level']})</div>"
    folium.Marker([lat, lon],
        popup=folium.Popup(popup_html, max_width=320), draggable=True,
        icon=folium.Icon(color="lightgray" if not snap else ("red" if snap["score"]>=75 else "orange" if snap["score"]>=50 else "blue"),
                         icon="info-sign"),
    ).add_to(m)
    return m

# ---------------------------
# Nominatim（市町村重心キャッシュ）
# ---------------------------
def load_json_if_exists(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except Exception: pass
    return {}

def save_json(obj: dict, path: str):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path,"w",encoding="utf-8") as f: json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception: pass

def nominatim_search(q: str) -> tuple[float | None, float | None]:
    try:
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        params = {"q": q, "format": "jsonv2", "limit": 1, "countrycodes": "jp", "addressdetails": 0}
        r = requests.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers, timeout=12)
        r.raise_for_status()
        items = r.json()
        if items:
            return float(items[0]["lat"]), float(items[0]["lon"])
    except Exception:
        return None, None
    return None, None

def geocode_municipality(muni: str) -> tuple[float | None, float | None]:
    if not muni: return None, None
    cache = load_json_if_exists(MUNI_GEOCODE_CACHE_PATH)
    if muni in cache: v = cache[muni]; return v.get("lat"), v.get("lon")
    time.sleep(0.6)  # polite
    lat, lon = nominatim_search(f"{muni} 愛媛県 日本")
    cache[muni] = {"lat": lat, "lon": lon}; save_json(cache, MUNI_GEOCODE_CACHE_PATH)
    return lat, lon

# ---------------------------
# 2019概位置レイヤ
# ---------------------------
def add_2019_layer(m: folium.Map, all_df: pd.DataFrame | None, max_points: int = 800):
    if all_df is None or all_df.empty: return
    df = all_df.copy().sample(frac=1.0, random_state=42).head(max_points)
    fg = folium.FeatureGroup(name="2019概位置（重心＋微ジッター）"); cl = MarkerCluster(name="2019クラスタ").add_to(fg)
    color_map = {
        "ひったくり":"red","車上ねらい":"orange","部品ねらい":"lightred",
        "自動車盗":"darkred","オートバイ盗":"cadetblue","自転車盗":"blue",
        "自動販売機ねらい":"purple","不明":"gray"
    }
    muni_cache = {}
    for _, r in df.iterrows():
        muni = str(r.get("municipality") or "").strip()
        ctype = str(r.get("ctype") or "不明")
        if not muni: continue
        if muni in muni_cache: lat0, lon0 = muni_cache[muni]
        else: lat0, lon0 = geocode_municipality(muni); muni_cache[muni]=(lat0,lon0)
        if not lat0 or not lon0: continue
        lat, lon = jitter_latlon(lat0, lon0, meters=120.0)
        ic = color_map.get(ctype, "gray")
        html = f"<b>{muni}</b><br>種別: {ctype}<br>（概位置）"
        folium.CircleMarker([lat, lon], radius=5, color=ic, fill=True, fill_opacity=0.6,
                            popup=folium.Popup(html, max_width=260)).add_to(cl)
    fg.add_to(m)

# ---------------------------
# SIBYL：犯罪係数レイヤ
# ---------------------------
def add_sybil_cc_layer(m: folium.Map, muni_counts: dict, base_dt: datetime, all_df: pd.DataFrame):
    if not muni_counts: return
    fg = folium.FeatureGroup(name="犯罪係数（SIBYL）")
    ranks = []
    for muni in CITY_NAMES:
        lat0, lon0 = geocode_municipality(muni)
        if not lat0 or not lon0: continue
        weather = get_weather(lat0, lon0)
        moon = get_mgpn_moon(lat0, lon0, base_dt)
        risk = compute_risk_score(weather, base_dt, all_df, moon)["score"]
        recent = int(muni_counts.get(muni, 0))
        cc = compute_cc_from_risk_and_news(risk, recent)
        if   cc >= 250: color = "#ff1a1a"
        elif cc >= 150: color = "#ff9f2a"
        elif cc >= 100: color = "#ffd033"
        else:           color = "#0aa0ff"
        radius = 400 + int(cc*3)
        html = (f"<b>{muni}</b><br>CC: {cc} / recent:{recent}"
                f"<br><span style='color:#555'>基礎リスク:{risk}（気象/時間帯/週末/月齢/2019）</span>"
                f"<br><a href='{EHIME_POLICE_URL}' target='_blank'>出典: 県警速報</a>")
        folium.Circle([lat0, lon0], radius=radius, color=color, fill=True, fill_opacity=0.25,
                      weight=2, popup=folium.Popup(html, max_width=320)).add_to(fg)
        ranks.append((muni, cc, recent))
    fg.add_to(m)
    return sorted(ranks, key=lambda x: x[1], reverse=True)

# ---------------------------
# POI
# ---------------------------
@st.cache_data(show_spinner=False, ttl=60*30)
def fetch_pois_overpass(lat: float, lon: float, radius_m: int = 1200) -> list[dict]:
    q = f"""
    [out:json][timeout:25];
    (
      node(around:{radius_m},{lat},{lon})["railway"="station"];
      node(around:{radius_m},{lat},{lon})["public_transport"~"stop_position|platform"];
      node(around:{radius_m},{lat},{lon})["amenity"="bicycle_parking"];
      node(around:{radius_m},{lat},{lon})["amenity"="convenience"];
      node(around:{radius_m},{lat},{lon})["amenity"="parking"];
      node(around:{radius_m},{lat},{lon})["leisure"="park"];
      node(around:{radius_m},{lat},{lon})["amenity"="atm"];
      node(around:{radius_m},{lat},{lon})["amenity"~"bar|nightclub|pub"];
    );
    out center 200;
    """
    try:
        r = requests.post(OVERPASS_URL, data=q.encode("utf-8"), headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        js = r.json(); return js.get("elements", [])
    except Exception:
        return []

def add_poi_layer(m: folium.Map, pois: list[dict]):
    if not pois: return
    fg = folium.FeatureGroup(name="近傍POI"); cl = MarkerCluster(name="POIクラスタ").add_to(fg)
    for e in pois:
        lat, lon = e.get("lat"), e.get("lon")
        if lat is None or lon is None: continue
        tags = e.get("tags", {})
        name = tags.get("name") or tags.get("brand") or ""
        cat = tags.get("railway") or tags.get("public_transport") or tags.get("amenity") or tags.get("leisure") or ""
        html = f"<b>{name or '(名称未設定)'}</b><br>種別: {cat}"
        folium.Marker([lat, lon], popup=folium.Popup(html, max_width=280),
                      icon=folium.Icon(color="green", icon="ok")).add_to(cl)
    fg.add_to(m)

# ---------------------------
# CSVアップロード（住所→座標）
# ---------------------------
def geocode_address_rows(df: pd.DataFrame, addr_col: str, muni_col: str | None) -> pd.DataFrame:
    res = []
    for _, r in df.iterrows():
        addr = str(r.get(addr_col,"")).strip()
        muni = str(r.get(muni_col,"")).strip() if (muni_col and muni_col in df.columns) else ""
        if not addr:
            res.append({"lat": None, "lon": None}); continue
        q = f"愛媛県 {muni} {addr}".strip()
        lat, lon = nominatim_search(q)
        time.sleep(0.8)
        res.append({"lat": lat, "lon": lon})
    geo = pd.DataFrame(res)
    return pd.concat([df.reset_index(drop=True), geo], axis=1)

# ---------------------------
# メイン
# ---------------------------
def main():
    st.set_page_config(APP_TITLE, page_icon="🧭", layout="wide")
    st.markdown(DRAMA_CSS, unsafe_allow_html=True)

    st.markdown(f"<h1 style='margin:0 0 8px 0;'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.caption("クリックで地点選択 →『分析する』。SIBYLモードで“犯罪係数(CC)”を市町単位に可視化（速報と現在条件に基づく相対指標）。")

    if "sel_lat" not in st.session_state: st.session_state.sel_lat = INIT_LAT
    if "sel_lon" not in st.session_state: st.session_state.sel_lon = INIT_LON
    if "last_snap" not in st.session_state: st.session_state.last_snap = None
    if "pois" not in st.session_state: st.session_state.pois = []
    if "user_geo_df" not in st.session_state: st.session_state.user_geo_df = None

    with st.sidebar:
        st.markdown("### 設定")
        st.session_state.sel_lat = st.number_input("選択緯度", value=float(st.session_state.sel_lat), format="%.6f")
        st.session_state.sel_lon = st.number_input("選択経度", value=float(st.session_state.sel_lon), format="%.6f")
        sibyl_on = st.toggle("SIBYL（犯罪係数）モード", value=True)
        st.divider()
        st.markdown("#### データ検出（2019）")
        files = sorted(set(sum([glob.glob(g) for g in DATA_GLOBS], [])))
        if files: [st.write("・", os.path.basename(fp), f"〔{os.path.dirname(fp) or '.'}〕") for fp in files]
        else: st.warning("データが見つかりません: " + ", ".join(DATA_GLOBS))
        st.divider()
        st.markdown("#### APIキー")
        st.write(f"- WeatherAPI: {'✅' if WEATHERAPI_KEY else '—'}")
        st.write(f"- OpenWeather: {'✅' if OPENWEATHER_KEY else '—'}")
        st.write(f"- Gemini: {'✅' if GEMINI_KEY else '—'}")

    @st.cache_data(show_spinner=False)
    def _load2019():
        return load_all_crime_2019(DATA_GLOBS)
    all_df = _load2019()

    st.markdown("<div class='card'>**地図：クリックで任意地点を選択（ドラッグ可）**</div>", unsafe_allow_html=True)
    fmap = render_map_selectable(st.session_state.sel_lat, st.session_state.sel_lon, st.session_state.last_snap)
    out = call_st_folium_with_fallback(fmap, height=540, key="map_select", return_last_clicked=True)
    if out and isinstance(out, dict) and out.get("last_clicked"):
        lat = out["last_clicked"].get("lat"); lon = out["last_clicked"].get("lng")
        if lat is not None and lon is not None:
            if (EHIME_BBOX["min_lat"] <= lat <= EHIME_BBOX["max_lat"]) and (EHIME_BBOX["min_lon"] <= lon <= EHIME_BBOX["max_lon"]):
                st.session_state.sel_lat = float(lat); st.session_state.sel_lon = float(lon)
            else:
                st.warning("選択地点が愛媛県の想定範囲外です。")

    colb1, colb2, colb3 = st.columns([1,1,2])
    with colb1:
        analyze = st.button("🔎 分析する", use_container_width=True)
    with colb2:
        reset = st.button("📍 初期地点へ戻す", use_container_width=True)

    if reset:
        st.session_state.sel_lat = INIT_LAT; st.session_state.sel_lon = INIT_LON
        st.session_state.last_snap = None; st.rerun()

    if analyze:
        with st.spinner("解析中（気象・月齢・2019傾向…）"):
            now_dt = datetime.now(JST)
            lat, lon = st.session_state.sel_lat, st.session_state.sel_lon
            weather = get_weather(lat, lon); moon = get_mgpn_moon(lat, lon, now_dt)
            snap = compute_risk_score(weather, now_dt, all_df, moon)
            st.session_state.last_snap = snap

    snap = st.session_state.last_snap
    c1, c2 = st.columns([1,1])

    with c1:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<div class='mute'>CURRENT RISK</div>", unsafe_allow_html=True)
        if snap:
            st.markdown(f"<div class='score-big' style='color:{snap['color']};'>{int(round(snap['score']))}</div>", unsafe_allow_html=True)
            st.markdown(f"<span class='badge'>{snap['level']}</span>", unsafe_allow_html=True)
            cc_local = compute_cc_from_risk_and_news(snap["score"], recent_count=0)
            st.write("")
            st.markdown("**SIBYL: 犯罪係数（参考・速報加点なし）**", unsafe_allow_html=True)
            st.markdown(f"<div class='sybil-num'>{cc_local}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='gauge-wrap'><div class='gauge-bar' style='width:{cc_local/3:.1f}%;'></div></div>", unsafe_allow_html=True)
            st.caption("※ 選択地点の現在条件のみで概算（速報出現回数は市町層で加味）")
        else:
            st.info("『分析する』ボタンで現在リスクを評価できます。")
        st.markdown("</div>", unsafe_allow_html=True)

        if snap:
            st.markdown("<div class='card'>**内部理由（気象/時間帯/週末/月齢/2019）**</div>", unsafe_allow_html=True)
            for r in snap["reasons"]: st.write("・", r)

    with c2:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("**SIBYL：犯罪係数レイヤ（市町単位）**")
        fmap2 = folium.Map(location=[EHIME_CENTER_LAT, EHIME_CENTER_LON], zoom_start=9, tiles="cartodbdark_matter")
        _add_common_map_ui(fmap2)

        ranks = None
        if sibyl_on:
            with st.spinner("県警速報から最近の出現回数を推定…"):
                muni_counts = fetch_police_muni_counts()
            now_dt = datetime.now(JST)
            # ←←← ここが修正点：DataFrameの真偽値評価を避ける
            safe_all_df = all_df if (all_df is not None) else pd.DataFrame({"date": pd.to_datetime([])})
            ranks = add_sybil_cc_layer(fmap2, muni_counts, now_dt, safe_all_df)

        add_2019_layer(fmap2, all_df)
        call_st_folium_with_fallback(fmap2, height=520, key="map_result", return_last_clicked=False)
        st.markdown("</div>", unsafe_allow_html=True)

        if ranks:
            st.markdown("<div class='card'>**市町別 犯罪係数（上位）**</div>", unsafe_allow_html=True)
            top = ranks[:5]
            for i,(muni, cc, rc) in enumerate(top, start=1):
                lvl = "⚠︎ENFORCE" if cc>=150 else ("CAUTION" if cc>=100 else "CLEAR")
                st.markdown(f"{i}. **{muni}**  —  **{cc}** <span class='rank-pill'>{lvl}</span>  <span class='mute'>(速報:{rc})</span>", unsafe_allow_html=True)
            st.caption("※ CCは相対指標。速報ページの“記載頻度”を加点として使用し、断定を避けています。")

    st.markdown("<div class='card'>**CSVアップロード（住所→座標）**</div>", unsafe_allow_html=True)
    up = st.file_uploader("住所CSVを選択（UTF-8/CP932等自動判別）", type=["csv"])
    colu1, colu2, colu3 = st.columns([2,2,1])
    with colu1: addr_col = st.text_input("住所列名（必須）", value="住所")
    with colu2: muni_col = st.text_input("市町村列名（任意）", value="市町村")
    with colu3: geo_run = st.button("ジオコーディング実行", use_container_width=True)

    if up is not None and geo_run:
        try:
            raw = up.read()
            enc_guess = (chardet.detect(raw).get("encoding") or "utf-8").lower()
            df_tmp = pd.read_csv(io.BytesIO(raw), encoding=enc_guess, engine="python")
            with st.spinner("Nominatimで住所を座標化中（礼節1秒/件）…"):
                udf = geocode_address_rows(df_tmp, addr_col, muni_col if muni_col in df_tmp.columns else None)
                st.session_state.user_geo_df = udf
            ok = udf[["lat","lon"]].notna().all(axis=1).sum()
            st.success(f"ジオコーディング完了：{ok}/{len(udf)} 行で座標取得")
        except Exception as e:
            st.error(f"CSV読込/ジオコーディングに失敗: {e}")

    st.markdown("<div class='card'>**近傍POI（Overpass）**</div>", unsafe_allow_html=True)
    pr = st.slider("探索半径[m]", 400, 3000, 1200, 100)
    colp1, colp2 = st.columns([1,3])
    with colp1: poi_btn = st.button("取得", use_container_width=True)
    with colp2: st.caption("駅・停留所・駐輪場・コンビニ・駐車場・公園・ATM・夜間娯楽")
    if poi_btn:
        with st.spinner("POI取得中…"):
            st.session_state.pois = fetch_pois_overpass(st.session_state.sel_lat, st.session_state.sel_lon, pr)
        st.success(f"取得: {len(st.session_state.pois)} 件")

    st.markdown("---")
    st.caption(
        "※ 県警速報の出現回数は“最近の記載頻度”の**近似指標**。個別事件の真偽・詳細は必ず出典を参照。"
        " CCは注意喚起のための相対値であり、断定・差別・排除に用いるものではありません。"
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error("致命的エラーが発生しました：\n" + "".join(traceback.format_exception(e)))
