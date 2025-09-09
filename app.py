# -*- coding: utf-8 -*-
# ============================================================
# 愛媛県 全域：犯罪/異常行動 警戒予測（Streamlit 完成版）
# - 2019 愛媛県オープンデータ（手口別CSV）を自動統合（./, ./data, /mnt/data）
# - 地図クリック＆ドラッグで任意地点を選択（愛媛全域）
# - 地図の直下に操作UI（分析/リセット/座標編集/Gemini切替）を配置（サイドバー不使用）
# - 「分析する」→ 進捗バー + 画面中央オーバーレイ「解析中」
# - 気象（WeatherAPI/OpenWeather） + mgpn 月齢API（ver2/3） + 2019傾向補正
# - mgpn はボタン押下時のみ呼出し・30分キャッシュ・ver1は使用しない
# - 見やすい地図UI：複数タイル/フルスクリーン/ミニマップ/測距/座標表示
# - 過去の犯罪箇所（おおよそ）：市町村中心の近傍にプロット（MarkerCluster）
# - DuplicateWidgetID 回避：st_folium に一意 key を付与
# ============================================================

import os
import re
import glob
import time
from datetime import datetime, timedelta, timezone
import chardet
import requests
import numpy as np
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import folium
from folium.plugins import MiniMap, MousePosition, MeasureControl, Fullscreen, LocateControl, MarkerCluster

# ---------------------------
# 基本設定
# ---------------------------
JST = timezone(timedelta(hours=9))
APP_TITLE = "愛媛県 警戒予測モニター"

# 愛媛県の中心付近（県庁付近）
EHIME_CENTER_LAT = 33.8416
EHIME_CENTER_LON = 132.7661
# 愛媛のざっくり範囲（範囲外クリックの誤選択を防止）
EHIME_BBOX = {"min_lat": 32.8, "max_lat": 34.6, "min_lon": 131.8, "max_lon": 134.0}

# 初期地点（上島町周辺）
INIT_LAT = 34.27717
INIT_LON = 133.20986

# CSV 自動検出グロブ（順に探索）
DATA_GLOBS = [
    "./ehime_2019*.csv",
    "./data/ehime_2019*.csv",
    "/mnt/data/ehime_2019*.csv",
]

# 市町村の代表座標（おおよそ）
MUNI_CENTER = {
    "松山市": (33.839, 132.765),
    "今治市": (34.066, 132.997),
    "新居浜市": (33.960, 133.283),
    "西条市": (33.918, 133.183),
    "四国中央市": (33.983, 133.550),
    "宇和島市": (33.223, 132.560),
    "大洲市": (33.503, 132.545),
    "八幡浜市": (33.461, 132.422),
    "伊予市": (33.757, 132.709),
    "東温市": (33.792, 132.870),
    "西予市": (33.360, 132.512),
    "上島町": (34.244, 133.200),
    "久万高原町": (33.655, 132.901),
    "砥部町": (33.742, 132.789),
    "松前町": (33.790, 132.710),
    "内子町": (33.533, 132.658),
    "伊方町": (33.493, 132.352),
    "鬼北町": (33.201, 132.746),
    "松野町": (33.146, 132.744),
    "愛南町": (32.966, 132.567),
}

# 犯罪種別→色/アイコン
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

# ---------------------------
# UI スタイル（ドラマ風 + 間延び防止の余白調整）
# ---------------------------
DRAMA_CSS = """
<style>
.main, .stApp { background: #0b0f14; color: #e6eef7; }
/* 余白の間延びを抑える */
section.main > div.block-container { padding-top: 1.0rem; padding-bottom: 1.0rem; }

/* 見出し */
.page-title { margin: 0 0 8px 0; font-weight: 900; letter-spacing: .04em; }

/* カード */
.card { background: linear-gradient(135deg, #121821, #0e141b);
  border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 14px 16px; margin: 8px 0; }

/* ボタン行（地図直下）を詰める */
.btn-row { margin-top: 6px; }
.stButton>button { background: linear-gradient(135deg, #143957, #0d2a41) !important;
  border: 1px solid rgba(255,255,255,0.15) !important; color: #e6eef7 !important; font-weight: 700 !important;
  border-radius: 10px !important; padding: .5rem .75rem !important; }

/* 数字カウンタ */
.score-big { font-size: 72px; font-weight: 900; letter-spacing: 1.5px;
  text-shadow: 0 0 8px rgba(255,0,0,0.25), 0 0 16px rgba(255,0,0,0.15); margin: 0; line-height: 1.0; }
.badge { display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 700;
  background: linear-gradient(135deg, #1c2633 0%, #121821 100%);
  border: 1px solid rgba(255,255,255,0.1); color:#d9e6f2; margin-left: 8px; }

/* 警報バー */
.alert-bar { position: sticky; top: 0; z-index: 1000;
  background: linear-gradient(90deg, rgba(180,0,0,0.85), rgba(255,50,50,0.85));
  color: white; padding: 10px 14px; border-bottom: 2px solid rgba(255,255,255,0.25);
  box-shadow: 0 4px 24px rgba(255,0,0,0.35); animation: pulse 2s infinite; }
@keyframes pulse { 0% { box-shadow: 0 0 0 rgba(255,0,0,0.4); }
 50% { box-shadow: 0 0 24px rgba(255,0,0,0.6); } 100% { box-shadow: 0 0 0 rgba(255,0,0,0.4); } }

/* サブテキスト */
.mute { color: #a8b7c7; font-size: 13px; }

/* 画面中央オーバーレイ（解析中） */
.overlay { position: fixed; inset: 0; display: flex; align-items: center; justify-content: center;
  background: rgba(0, 0, 0, 0.45); z-index: 9999; }
.overlay-content { text-align: center; padding: 28px 36px; border-radius: 18px; backdrop-filter: blur(6px);
  background: linear-gradient(135deg, rgba(20,40,60,0.85), rgba(8,16,24,0.85));
  border: 1px solid rgba(255,255,255,0.15); box-shadow: 0 12px 48px rgba(0,0,0,0.5), 0 0 24px rgba(0,180,255,0.2) inset; }
.overlay-title { font-size: 28px; font-weight: 900; letter-spacing: 0.2em; color: #e8f2ff;
  text-shadow: 0 0 12px rgba(0,180,255,0.3); }
.overlay-sub { margin-top: 8px; font-size: 13px; color: #bcd0e0; }
.loader { margin: 16px auto 0 auto; width: 72px; height: 72px; border-radius: 50%;
  border: 4px solid rgba(255,255,255,0.18); border-top-color: #39c0ff; animation: spin 1.1s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
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
    GEMINI_THINKING_BUDGET = int(st.secrets.get("gemini", {}).get("thinking_budget", 0))
except Exception:
    WEATHERAPI_KEY = OPENWEATHER_KEY = GEMINI_KEY = ""
    GEMINI_MODEL = "gemini-2.5-flash"
    GEMINI_THINKING_BUDGET = 0

# ---------------------------
# CSV 読み込み（堅牢）
# ---------------------------

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
    type_col = next((c for c in df.columns if re.search(r"(手口|罪|罪種|種別|分類)", str(c))), None)
    if not type_col:
        type_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["type", "category", "kind", "crime"])), None)
    return {"date": date_col, "municipality": muni_col, "ctype": type_col}


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
        frames.append(df[["date", "municipality", "ctype"]])
    return pd.concat(frames, ignore_index=True)

# ---------------------------
# 気象（任意API：キー無ければダミー）
# ---------------------------

def get_weather_weatherapi(lat, lon):
    try:
        if not WEATHERAPI_KEY:
            return None
        base = "https://api.weatherapi.com/v1"
        common = f"key={WEATHERAPI_KEY}&q={lat},{lon}"
        r = requests.get(f"{base}/current.json?{common}&aqi=no", timeout=10)
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

# ---------------------------
# mgpn 月齢API（ver2→ver3フォールバック）※30分キャッシュ
# ---------------------------

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
    if a < 1.0:
        return "新月"
    if a < 6.0:
        return "三日月（若月）"
    if a < 8.9:
        return "上弦前後"
    if a < 13.5:
        return "十三夜～満月前"
    if a < 16.0:
        return "満月前後"
    if a < 21.0:
        return "満月後～下弦前"
    if a < 23.5:
        return "下弦前後"
    if a < 28.0:
        return "有明月（残月）"
    return "新月に近い"


@st.cache_data(show_spinner=False, ttl=60 * 30)
def get_mgpn_moon(lat: float, lon: float, dt_jst: datetime) -> dict | None:
    t = dt_jst.strftime("%Y-%m-%dT%H:%M")  # JST指定
    try:
        url = "https://mgpn.org/api/moon/v2position.cgi"
        params = {"time": t, "lat": f"{lat:.6f}", "lon": f"{lon:.6f}", "loop": 1, "interval": 0}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        payload = r.json()
        age = _extract_moonage(payload)
        obj = payload[0] if isinstance(payload, list) and payload else payload
        alt = float(obj.get("altitude")) if obj and "altitude" in obj else None
        azi = float(obj.get("azimuth")) if obj and "azimuth" in obj else None
        return {"moon_age": age, "phase_text": _phase_text_from_age(age), "altitude": alt, "azimuth": azi}
    except Exception:
        pass
    try:
        url = "https://mgpn.org/api/moon/v3position.cgi"
        params = {"time": t, "lat": f"{lat:.6f}", "lon": f"{lon:.6f}"}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        payload = r.json()
        age = _extract_moonage(payload)
        obj = payload[0] if isinstance(payload, list) and payload else payload
        alt = float(obj.get("altitude")) if obj and "altitude" in obj else None
        azi = float(obj.get("azimuth")) if obj and "azimuth" in obj else None
        return {"moon_age": age, "phase_text": _phase_text_from_age(age), "altitude": alt, "azimuth": azi}
    except Exception:
        return None


def is_full_moon_like_text(phase_text: str | None, age: float | None) -> bool:
    if phase_text and ("満月" in phase_text):
        return True
    if age is not None:
        a = age % 29.53
        return 13.3 <= a <= 16.3
    return False

# ---------------------------
# スコア計算（愛媛全域／2019傾向＋月齢）
# ---------------------------

def compute_risk_score(weather: dict, now_dt: datetime, all_df: pd.DataFrame | None, moon_info: dict | None) -> dict:
    score = 0.0
    reasons: list[str] = []

    temp = float(weather.get("temp_c", 20.0))
    precip = float(weather.get("precip_mm", 0.0))
    humidity = float(weather.get("humidity", 60))
    cond = str(weather.get("condition", ""))

    if temp >= 32:
        add = 42
    elif temp >= 30:
        add = 36
    elif temp >= 27:
        add = 28
    elif temp >= 25:
        add = 20
    elif temp >= 22:
        add = 10
    else:
        add = 0
    score += add
    if add > 0:
        reasons.append(f"気温{temp:.0f}℃:+{add}")

    if precip >= 10:
        score -= 20
        reasons.append("強い降雨:-20")
    elif precip >= 1:
        score -= 8
        reasons.append("降雨あり:-8")

    hour = now_dt.hour
    if 20 <= hour <= 23 or 0 <= hour <= 4:
        score += 15
        reasons.append("夜間:+15")
    elif 17 <= hour < 20:
        score += 7
        reasons.append("夕方:+7")

    if now_dt.weekday() in (4, 5):
        score += 6
        reasons.append("週末(+金土):+6")

    moon_age = moon_info.get("moon_age") if moon_info else None
    moon_phase_text = moon_info.get("phase_text") if moon_info else None
    if is_full_moon_like_text(moon_phase_text, moon_age):
        score += 5
        reasons.append("満月相当:+5")

    if humidity >= 80:
        score += 3
        reasons.append("高湿度:+3")

    if all_df is not None and not all_df.empty:
        sub = all_df.copy()
        sub["month"] = sub["date"].dt.month
        month_ratio = len(sub[sub["month"] == now_dt.month]) / max(1, len(sub))
        if month_ratio >= 0.12:
            score += 6
            reasons.append("2019傾向(同月比 多め):+6")
        elif month_ratio >= 0.08:
            score += 3
            reasons.append("2019傾向(同月比 やや多め):+3")
        if "ctype" in sub.columns:
            top_types = sub["ctype"].value_counts(normalize=True)
            outdoor_like = float(
                top_types.get("ひったくり", 0)
                + top_types.get("車上ねらい", 0)
                + top_types.get("自転車盗", 0)
                + top_types.get("オートバイ盗", 0)
            )
            if outdoor_like >= 0.45:
                score += 5
                reasons.append("2019傾向(屋外系多):+5")
            elif outdoor_like >= 0.30:
                score += 2
                reasons.append("2019傾向(屋外系やや多):+2")

    score = float(np.clip(score, 0, 100))
    if score < 25:
        level, color = "Low", "#0aa0ff"
    elif score < 50:
        level, color = "Moderate", "#ffd033"
    elif score < 75:
        level, color = "High", "#ff7f2a"
    else:
        level, color = "Very High", "#ff2a2a"

    return {
        "score": round(score, 1),
        "level": level,
        "color": color,
        "reasons": reasons,
        "moon_phase": moon_phase_text,
        "moon_age": moon_age,
        "temp_c": temp,
        "humidity": humidity,
        "precip_mm": precip,
        "condition": cond,
    }

# ---------------------------
# Gemini 説明（任意）
# ---------------------------

def gemini_explain(snap: dict, now_dt: datetime) -> str | None:
    if not GEMINI_KEY:
        return None
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel(
            model_name=GEMINI_MODEL,
            generation_config={"temperature": 0.2},
            system_instruction=(
                "あなたは防犯アナリストAIです。与えられた要因から、愛媛県の犯罪/異常行動リスクの"
                "根拠を、過度な断定を避けて簡潔に日本語で説明してください。"
                "満月効果は限定的、気温・時間帯・降雨などの影響は統計的示唆あり、を前提に、"
                "最後に取るべき具体的行動（人通りの少ない場所を避ける等）で締めること。"
            ),
        )
        prompt = (
            f"現在: {now_dt.strftime('%Y-%m-%d %H:%M JST')}\n"
            f"スコア: {snap['score']} ({snap['level']})\n"
            f"要因: 気温{snap['temp_c']}℃, 湿度{snap['humidity']}%, 降水{snap['precip_mm']}mm, 天候:{snap['condition']}, "
            f"月齢:{snap.get('moon_age')}（{snap.get('moon_phase')}）\n"
            f"内部理由: {', '.join(snap['reasons'])}\n"
            "一般向けの注意喚起コメントを120〜200字で。"
        )
        resp = model.generate_content(prompt)
        return (resp.text or "").strip()
    except Exception:
        return None

# ---------------------------
# 地図部品
# ---------------------------

def _add_common_map_ui(m: folium.Map):
    folium.TileLayer("cartodbpositron", name="Light").add_to(m)
    folium.TileLayer("cartodbdark_matter", name="Dark").add_to(m)
    folium.TileLayer("OpenStreetMap", name="OSM").add_to(m)
    folium.TileLayer(
        tiles="https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
        attr="OSM HOT",
        name="OSM HOT",
        control=True,
    ).add_to(m)
    Fullscreen(position="topleft").add_to(m)
    MiniMap(zoom_level_fixed=5, toggle_display=True).add_to(m)
    MeasureControl(position="topleft", primary_length_unit="meters").add_to(m)
    MousePosition(
        position="bottomright",
        separator=" | ",
        prefix="座標",
        lat_formatter="function(num) {return L.Util.formatNum(num, 6);}",
        lng_formatter="function(num) {return L.Util.formatNum(num, 6);}"
    ).add_to(m)
    try:
        LocateControl(auto_start=False, flyTo=True, keepCurrentZoomLevel=True).add_to(m)
    except Exception:
        pass
    folium.LayerControl(collapsed=True).add_to(m)


def _plot_past_crimes(m: folium.Map, df: pd.DataFrame | None):
    if df is None or df.empty:
        return
    # 市町村中心の近傍（微小ジッター）に点を打つ
    cluster = MarkerCluster(name="2019 犯罪（概位置）").add_to(m)
    rng = np.random.default_rng(42)
    rows = df.dropna(subset=["municipality"]).copy()
    rows = rows[rows["municipality"].str.len() > 0]
    # 簡易絞り込み（性能）：最大3000点
    rows = rows.sample(min(3000, len(rows)), random_state=42) if len(rows) > 3000 else rows
    for _, r in rows.iterrows():
        muni = str(r.get("municipality") or "")
        # キーに含まれていない場合は末尾の「市/町/村」を使って前方一致で粗くマッチ
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
        # 100〜300m程度のジッター（概位置を表現）
        jitter_lat = float(rng.normal(0, 0.0010))  # ~100m
        jitter_lon = float(rng.normal(0, 0.0012))  # ~120m
        lat = lat0 + jitter_lat
        lon = lon0 + jitter_lon
        ctype = str(r.get("ctype") or "不明")
        color, icon = CTYPE_STYLE.get(ctype, ("gray", "glyphicon-question-sign"))
        date_txt = "" if pd.isna(r.get("date")) else pd.to_datetime(r.get("date")).strftime("%Y-%m-%d")
        html = f"<b>{ctype}</b><br/>市町村: {muni}<br/>発生日: {date_txt}"
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(html, max_width=260),
            icon=folium.Icon(color=color, icon=icon)
        ).add_to(cluster)


def render_map_selectable(lat: float, lon: float, snap: dict | None, past_df: pd.DataFrame | None):
    m = folium.Map(location=[EHIME_CENTER_LAT, EHIME_CENTER_LON], zoom_start=9, tiles="cartodbpositron")
    _add_common_map_ui(m)

    # 過去犯罪（概位置）
    _plot_past_crimes(m, past_df)

    popup_html = "<div style='color:#111;'>地点をクリックして選択</div>"
    if snap:
        radius = 1500 if snap["score"] < 50 else (2500 if snap["score"] < 75 else 3500)
        folium.Circle(
            location=[lat, lon], radius=radius, color=snap["color"], fill=True, fill_opacity=0.25, weight=2
        ).add_to(m)
        popup_html = f"""
        <div style=\"color:#111;\">
          <b>警戒度:</b> {snap['score']} ({snap['level']})<br/>
          <b>月齢:</b> {snap.get('moon_age')}（{snap.get('moon_phase')}）<br/>
          <b>天候:</b> {snap['condition']} / {snap['temp_c']}℃ / 降水{snap['precip_mm']}mm
        </div>
        """

    folium.Marker(
        [lat, lon],
        popup=folium.Popup(popup_html, max_width=320),
        draggable=True,
        icon=folium.Icon(color="lightgray" if not snap else ("red" if snap["score"] >= 75 else "orange" if snap["score"] >= 50 else "blue"), icon="info-sign"),
    ).add_to(m)

    return m

# ---------------------------
# Streamlit 本体
# ---------------------------

def main():
    st.set_page_config(APP_TITLE, page_icon="🚨", layout="wide")
    st.markdown(DRAMA_CSS, unsafe_allow_html=True)

    st.markdown(f"<h1 class='page-title'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.caption("愛媛県全域。地図クリックで地点選択 → 『分析する』でスコア算出。月齢は mgpn API（ver2/3）を安全に利用。2019年オープンデータを概位置で可視化。")

    # セッションステート
    if "sel_lat" not in st.session_state:
        st.session_state.sel_lat = INIT_LAT
    if "sel_lon" not in st.session_state:
        st.session_state.sel_lon = INIT_LON
    if "last_snap" not in st.session_state:
        st.session_state.last_snap = None
    if "run_gemini" not in st.session_state:
        st.session_state.run_gemini = False

    # 2019データ（キャッシュ）
    @st.cache_data(show_spinner=False)
    def _load_2019():
        return load_all_crime_2019(DATA_GLOBS)

    all_df = _load_2019()

    # === 地図（クリックで地点更新） ===
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    fmap = render_map_selectable(st.session_state.sel_lat, st.session_state.sel_lon, st.session_state.last_snap, all_df)
    out = st_folium(fmap, height=560, returned_objects=["last_clicked"], key="map_select")

    # 地図直下：操作行（空白を作らない）
    col_a, col_b, col_c, col_d, col_e = st.columns([1.1, 1.1, 1.2, 1.2, 1.0])
    with col_a:
        analyze = st.button("🔎 分析する", use_container_width=True)
    with col_b:
        reset = st.button("📍 初期地点へ", use_container_width=True)
    with col_c:
        st.session_state.sel_lat = st.number_input("緯度", value=float(st.session_state.sel_lat), format="%.6f", key="lat_input")
    with col_d:
        st.session_state.sel_lon = st.number_input("経度", value=float(st.session_state.sel_lon), format="%.6f", key="lon_input")
    with col_e:
        st.session_state.run_gemini = st.toggle("Gemini", value=st.session_state.run_gemini)
    st.markdown("</div>", unsafe_allow_html=True)

    # クリック位置の反映（分析ボタンの近くに置く）
    if out and out.get("last_clicked"):
        lat = out["last_clicked"]["lat"]
        lon = out["last_clicked"]["lng"]
        if (EHIME_BBOX["min_lat"] <= lat <= EHIME_BBOX["max_lat"]) and (EHIME_BBOX["min_lon"] <= lon <= EHIME_BBOX["max_lon"]):
            st.session_state.sel_lat = float(lat)
            st.session_state.sel_lon = float(lon)
        else:
            st.warning("選択地点が愛媛県の想定範囲外です。")

    # 追加情報（折りたたみ）
    with st.expander("データ検出（クリックで表示）", expanded=False):
        files = []
        for g in DATA_GLOBS:
            files.extend(glob.glob(g))
        files = sorted(set(files))
        if files:
            for fp in files:
                st.write("・", os.path.basename(fp), f" 〔{os.path.dirname(fp) or '.'}〕")
        else:
            st.warning("データが見つかりませんでした: " + ", ".join(DATA_GLOBS))
        if st.button("🔄 データ再スキャン/キャッシュ削除"):
            st.cache_data.clear(); st.experimental_rerun()

    # リセット
    if reset:
        st.session_state.sel_lat = INIT_LAT
        st.session_state.sel_lon = INIT_LON
        st.session_state.last_snap = None
        st.experimental_rerun()

    # 分析（ボタン押下時のみ API 呼出し）
    if analyze:
        overlay = st.empty()
        overlay.markdown(
            """
            <div class=\"overlay\">
              <div class=\"overlay-content\">
                <div class=\"overlay-title\">解析中</div>
                <div class=\"overlay-sub\">気象・月齢（mgpn）・2019傾向を統合しています…</div>
                <div class=\"loader\"></div>
              </div>
            </div>
        """,
            unsafe_allow_html=True,
        )
        p = st.progress(0, text="準備中…")
        for i, txt in [(15, "気象の取得…"), (40, "月齢（mgpn）の取得…"), (70, "2019年傾向の補正…"), (100, "スコア集計…")]:
            time.sleep(0.35); p.progress(i, text=txt)

        now_dt = datetime.now(JST)
        lat, lon = st.session_state.sel_lat, st.session_state.sel_lon
        weather = get_weather(lat, lon)
        moon_info = get_mgpn_moon(lat, lon, now_dt)  # 30分キャッシュ
        snap = compute_risk_score(weather, now_dt, all_df, moon_info)
        st.session_state.last_snap = snap

        overlay.empty(); p.empty()

    # 結果表示
    snap = st.session_state.last_snap
    if snap:
        if snap["score"] >= 75:
            st.markdown(
                f"<div class='alert-bar'>警報：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。周囲に注意。</div>",
                unsafe_allow_html=True,
            )
        elif snap["score"] >= 50:
            st.markdown(
                f"<div class='alert-bar' style='background:linear-gradient(90deg, rgba(200,120,0,0.85), rgba(255,170,0,0.85));'>注意：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。</div>",
                unsafe_allow_html=True,
            )

        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("<div class='mute'>CURRENT RISK</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='score-big' style='color:{snap['color']};'>{int(round(snap['score']))}</div>", unsafe_allow_html=True)
            st.markdown(f"<span class='badge'>{snap['level']}</span>", unsafe_allow_html=True)
            st.write("")
            st.markdown(
                f"**月齢**：{snap.get('moon_age')}（{snap.get('moon_phase')}） / "
                f"**天候**：{snap['condition']} / **気温**：{snap['temp_c']}℃ / "
                f"**降水**：{snap['precip_mm']}mm / **湿度**：{snap['humidity']}%",
                unsafe_allow_html=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**内部理由（ヒューリスティック + 2019補正 + 月齢）**")
            for r in snap["reasons"]:
                st.write("・", r)
            st.markdown("</div>", unsafe_allow_html=True)

            if all_df is not None and not all_df.empty:
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown("**2019データ概要（検出件数）**")
                vc = all_df["ctype"].value_counts().rename("件数").to_frame()
                st.dataframe(vc)
                st.markdown("</div>", unsafe_allow_html=True)

        with c2:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**警戒ゾーン（選択地点に対する可視化）**")
            fmap2 = render_map_selectable(st.session_state.sel_lat, st.session_state.sel_lon, snap, all_df)
            st_folium(fmap2, height=500, returned_objects=[], key="map_result")
            st.markdown("</div>", unsafe_allow_html=True)

            if st.session_state.run_gemini:
                with st.spinner("Gemini 2.5 Flash が説明を生成中..."):
                    msg = gemini_explain(snap, datetime.now(JST))
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown("**Gemini 2.5 Flash の見解（任意）**")
                st.write(msg if msg else "（Gemini未設定または生成失敗）")
                st.markdown("</div>", unsafe_allow_html=True)

    st.caption(
        "※ 月齢は mgpn API（ver2/3）をボタン押下時にのみ呼出し、30分キャッシュで負荷を抑制。"
        " 気象APIキーが無い場合はダミー値で動作。2019年オープンデータは ./, ./data, /mnt/data を自動検出。"
        " 犯罪箇所は市町村中心の概位置（100〜300mジッター）で表示し、個人を特定する情報は含みません。"
    )


if __name__ == "__main__":
    main()
