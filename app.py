# -*- coding: utf-8 -*-
# ============================================================
# 愛媛県 全域：犯罪/異常行動 警戒予測（Streamlit 完成版：仕様最終統合準拠）
# - 2019 愛媛県オープンデータ（手口別CSV）を自動統合（./, ./data, /mnt/data）
# - 地図クリック＆ドラッグで任意地点を選択（愛媛全域）※地図表示の方式/見た目は従来通り
# - 「分析する」：中央オーバーレイ + 進捗バー（finallyで必ず消去）
# - 気象（WeatherAPI/OpenWeather） + mgpn 月齢API（v2優先→v3）30分キャッシュ・3リトライ
# - POI（Overpass）取得＋レイヤ表示、ユーザーCSVの住所ジオコーディング（Nominatim・永続キャッシュ）
# - 2019概位置レイヤ（市町村重心＋微ジッター）＋手口別色
# - 将来30日（各日21:00 JST）の最高日表示（気象はダミー標準値）
# - st_folium引数互換：事前シグネチャ検査→単回呼び出し、失敗時はHTML直埋め
# - セキュリティ/プライバシー配慮（概位置・APIキーはsecrets）
# ============================================================

import os
import re
import io
import glob
import json
import time
import math
import random
import inspect
from datetime import datetime, timedelta, timezone

import requests
import numpy as np
import pandas as pd
import streamlit as st
import folium
from folium.plugins import (
    MiniMap, MousePosition, MeasureControl, Fullscreen, LocateControl, MarkerCluster
)
from streamlit_folium import st_folium
import chardet
import traceback

# HTML直埋めフォールバック
import streamlit.components.v1 as components

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

GEOCODE_CACHE_PATH = "/mnt/data/geocode_cache.parquet"
MUNI_GEOCODE_CACHE_PATH = "/mnt/data/muni_geocode_cache.json"

USER_AGENT = "EhimeCrimeRisk/1.0 (Nominatim polite; contact: local-app)"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# ---------------------------
# UI スタイル（ドラマ風 + 中央オーバーレイ）
# ---------------------------
DRAMA_CSS = """
<style>
.main, .stApp { background: #0b0f14; color: #e6eef7; }
section[data-testid="stSidebar"] { background: #0e141b; }
/* 数字カウンタ */
.score-big { font-size: 72px; font-weight: 900; letter-spacing: 1.5px;
  text-shadow: 0 0 8px rgba(255,0,0,0.25), 0 0 16px rgba(255,0,0,0.15);
  margin: 0; line-height: 1.0; }
/* レベルタグ */
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
/* カード */
.card { background: linear-gradient(135deg, #121821, #0e141b);
  border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 16px 18px; margin: 8px 0;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35); }
/* ボタン */
button[kind="primary"], .stButton>button {
  background: linear-gradient(135deg, #143957, #0d2a41) !important;
  border: 1px solid rgba(255,255,255,0.15) !important;
  color: #e6eef7 !important; font-weight: 700 !important;
  border-radius: 12px !important; }
/* サブテキスト */
.mute { color: #a8b7c7; font-size: 13px; }
/* 画面中央オーバーレイ（解析中） */
.overlay { position: fixed; inset: 0; display: flex; align-items: center; justify-content: center;
  background: rgba(0, 0, 0, 0.45); z-index: 9999; }
.overlay-content { text-align: center; padding: 28px 36px; border-radius: 18px; backdrop-filter: blur(6px);
  background: linear-gradient(135deg, rgba(20,40,60,0.85), rgba(8,16,24,0.85));
  border: 1px solid rgba(255,255,255,0.15);
  box-shadow: 0 12px 48px rgba(0,0,0,0.5), 0 0 24px rgba(0,180,255,0.2) inset; }
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
# ユーティリティ
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
    # 発生日
    date_col = next((c for c in df.columns if re.search(r"(発生|年月日|日付|日時)", str(c))), None)
    if not date_col:
        date_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["date", "day", "time", "occur"])), None)
    # 市町村
    muni_col = next((c for c in df.columns if re.search(r"(市|町|村).*名", str(c)) or re.search(r"(市町村|自治体|地域)", str(c))), None)
    if not muni_col:
        muni_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["municipality", "city", "town", "area", "region"])), None)
    # 手口
    type_col = next((c for c in df.columns if re.search(r"(手口|罪|罪種|種別|分類)", str(c))), None)
    if not type_col:
        type_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["type", "category", "kind", "crime"])), None)
    return {"date": date_col, "municipality": muni_col, "ctype": type_col}

def parse_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def jitter_latlon(lat: float, lon: float, meters: float = 110.0) -> tuple[float, float]:
    # 約metersの微ジッター（緯度方向：1度≒111km）
    dlat = (random.random() - 0.5) * (meters / 111000.0) * 2
    # 経度は緯度による
    scale = math.cos(math.radians(lat))
    dlon = (random.random() - 0.5) * (meters / (111000.0 * max(scale, 1e-6))) * 2
    return lat + dlat, lon + dlon

# ---------------------------
# 2019 CSV ロード
# ---------------------------
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
        # 標準化
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
        # 2019のみ（欠損は許容）
        df = df[(df["date"].dt.year == 2019) | (df["date"].isna())]
        frames.append(df[["date", "municipality", "ctype"]])
    if frames:
        return pd.concat(frames, ignore_index=True)
    return None

# ---------------------------
# 気象API（フォールバック）
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
# mgpn 月齢API（v2→v3、30分キャッシュ、3リトライ）
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
    if a < 1.0: return "新月"
    if a < 6.0: return "三日月（若月）"
    if a < 8.9: return "上弦前後"
    if a < 13.5: return "十三夜～満月前"
    if a < 16.0: return "満月前後"
    if a < 21.0: return "満月後～下弦前"
    if a < 23.5: return "下弦前後"
    if a < 28.0: return "有明月（残月）"
    return "新月に近い"

@st.cache_data(show_spinner=False, ttl=60 * 30)
def get_mgpn_moon(lat: float, lon: float, dt_jst: datetime) -> dict | None:
    t = dt_jst.strftime("%Y-%m-%dT%H:%M")
    headers = {"Accept": "application/json"}
    # v2優先→v3、3回まで各リトライ
    for base in ["https://mgpn.org/api/moon/v2position.cgi", "https://mgpn.org/api/moon/v3position.cgi"]:
        for _ in range(3):
            try:
                params = {"time": t, "lat": f"{lat:.6f}", "lon": f"{lon:.6f}"}
                if "v2" in base:
                    params.update({"loop": 1, "interval": 0})
                r = requests.get(base, params=params, headers=headers, timeout=8)
                r.raise_for_status()
                payload = r.json()
                age = _extract_moonage(payload)
                obj = payload[0] if isinstance(payload, list) and payload else payload
                alt = float(obj.get("altitude")) if obj and "altitude" in obj else None
                azi = float(obj.get("azimuth")) if obj and "azimuth" in obj else None
                return {"moon_age": age, "phase_text": _phase_text_from_age(age), "altitude": alt, "azimuth": azi}
            except Exception:
                time.sleep(0.6)
    return None

def is_full_moon_like_text(phase_text: str | None, age: float | None) -> bool:
    if phase_text and ("満月" in phase_text):
        return True
    if age is not None:
        a = age % 29.53
        return 13.3 <= a <= 16.3
    return False

# ---------------------------
# スコア計算（0–100）
# ---------------------------
def compute_risk_score(weather: dict, now_dt: datetime, all_df: pd.DataFrame | None, moon_info: dict | None) -> dict:
    score = 0.0
    reasons: list[str] = []

    temp = float(weather.get("temp_c", 20.0))
    precip = float(weather.get("precip_mm", 0.0))
    humidity = float(weather.get("humidity", 60))
    cond = str(weather.get("condition", ""))

    # 気温
    if temp >= 32: add = 42
    elif temp >= 30: add = 36
    elif temp >= 27: add = 28
    elif temp >= 25: add = 20
    elif temp >= 22: add = 10
    else: add = 0
    score += add
    if add > 0: reasons.append(f"気温{temp:.0f}℃:+{add}")

    # 降雨
    if precip >= 10:
        score -= 20; reasons.append("強い降雨:-20")
    elif precip >= 1:
        score -= 8; reasons.append("降雨あり:-8")

    # 時間帯
    hour = now_dt.hour
    if 20 <= hour <= 23 or 0 <= hour <= 4:
        score += 15; reasons.append("夜間:+15")
    elif 17 <= hour < 20:
        score += 7; reasons.append("夕方:+7")

    # 週末（金・土）
    if now_dt.weekday() in (4, 5):
        score += 6; reasons.append("週末(+金土):+6")

    # 月齢
    moon_age = moon_info.get("moon_age") if moon_info else None
    moon_phase_text = moon_info.get("phase_text") if moon_info else None
    if is_full_moon_like_text(moon_phase_text, moon_age):
        score += 5; reasons.append("満月相当:+5")

    # 湿度
    if humidity >= 80:
        score += 3; reasons.append("高湿度:+3")

    # 2019傾向（県全域）
    if all_df is not None and not all_df.empty:
        sub = all_df.copy()
        sub["month"] = sub["date"].dt.month
        month_ratio = len(sub[sub["month"] == now_dt.month]) / max(1, len(sub))
        if month_ratio >= 0.12:
            score += 6; reasons.append("2019傾向(同月比 多め):+6")
        elif month_ratio >= 0.08:
            score += 3; reasons.append("2019傾向(同月比 やや多め):+3")
        if "ctype" in sub.columns:
            top_types = sub["ctype"].value_counts(normalize=True)
            outdoor_like = float(
                top_types.get("ひったくり", 0)
                + top_types.get("車上ねらい", 0)
                + top_types.get("自転車盗", 0)
                + top_types.get("オートバイ盗", 0)
            )
            if outdoor_like >= 0.45:
                score += 5; reasons.append("2019傾向(屋外系多):+5")
            elif outdoor_like >= 0.30:
                score += 2; reasons.append("2019傾向(屋外系やや多):+2")

    score = float(np.clip(score, 0, 100))
    if score < 25: level, color = "Low", "#0aa0ff"
    elif score < 50: level, color = "Moderate", "#ffd033"
    elif score < 75: level, color = "High", "#ff7f2a"
    else: level, color = "Very High", "#ff2a2a"

    return {
        "score": round(score, 1), "level": level, "color": color, "reasons": reasons,
        "moon_phase": moon_phase_text, "moon_age": moon_age,
        "temp_c": temp, "humidity": humidity, "precip_mm": precip, "condition": cond,
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
# st_folium 互換ラッパ（失敗時HTML直埋め）
# ---------------------------
def call_st_folium_with_fallback(m: folium.Map, height: int, key: str, return_last_clicked: bool = False):
    args = inspect.signature(st_folium).parameters
    kwargs = {"height": height, "key": key}
    try:
        if "returned_objects" in args:
            if return_last_clicked:
                kwargs["returned_objects"] = ["last_clicked"]
            out = st_folium(m, **kwargs)
        else:
            # 古い版：returned_objects未サポート
            out = st_folium(m, **kwargs)
        return out
    except TypeError:
        # 引数食い違いは returned_objects 不指定で再試行
        try:
            kwargs.pop("returned_objects", None)
            out = st_folium(m, **kwargs)
            return out
        except Exception:
            pass
    except Exception:
        pass
    # 最終保険：HTML直埋め（クリック取得は不可）
    try:
        html = m.get_root().render()
        components.html(html, height=height, scrolling=False)
    except Exception:
        st.error("地図描画に失敗しました。ネットワークやタイルの到達性をご確認ください。")
    return {}

# ---------------------------
# マップUI（ベースは従来の見た目・操作感）
# ---------------------------
def _add_common_map_ui(m: folium.Map):
    # 追加タイル（昼/淡色/衛星/OSM）
    folium.TileLayer("cartodbpositron", name="Light").add_to(m)
    folium.TileLayer("cartodbdark_matter", name="Dark").add_to(m)
    folium.TileLayer("OpenStreetMap", name="OSM").add_to(m)
    folium.TileLayer(
        tiles="https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
        attr="OSM HOT", name="OSM HOT", control=True
    ).add_to(m)
    # プラグイン
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
        radius = 1500 if snap["score"] < 50 else (2500 if snap["score"] < 75 else 3500)
        folium.Circle(
            location=[lat, lon], radius=radius, color=snap["color"], fill=True, fill_opacity=0.25, weight=2
        ).add_to(m)
        popup_html = f"""
        <div style="color:#111;">
          <b>警戒度:</b> {snap['score']} ({snap['level']})<br/>
          <b>月齢:</b> {snap.get('moon_age')}（{snap.get('moon_phase')}）<br/>
          <b>天候:</b> {snap['condition']} / {snap['temp_c']}℃ / 降水{snap['precip_mm']}mm
        </div>
        """

    folium.Marker(
        [lat, lon],
        popup=folium.Popup(popup_html, max_width=320),
        draggable=True,
        icon=folium.Icon(
            color="lightgray" if not snap else ("red" if snap["score"] >= 75 else "orange" if snap["score"] >= 50 else "blue"),
            icon="info-sign"
        ),
    ).add_to(m)
    return m

# ---------------------------
# Nominatim ジオコーディング（住所 / 市町村）
# ---------------------------
def load_parquet_if_exists(path: str) -> pd.DataFrame | None:
    try:
        if os.path.exists(path):
            return pd.read_parquet(path)
    except Exception:
        pass
    return None

def save_parquet(df: pd.DataFrame, path: str):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
    except Exception:
        pass

def load_json_if_exists(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def save_json(obj: dict, path: str):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def nominatim_search(q: str) -> tuple[float | None, float | None]:
    try:
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        params = {"q": q, "format": "jsonv2", "limit": 1, "countrycodes": "jp", "addressdetails": 0}
        r = requests.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers, timeout=12)
        r.raise_for_status()
        items = r.json()
        if items:
            lat = float(items[0]["lat"]); lon = float(items[0]["lon"])
            return lat, lon
    except Exception:
        return None, None
    return None, None

def geocode_address_rows(df: pd.DataFrame, addr_col: str, muni_col: str | None) -> pd.DataFrame:
    cache = load_parquet_if_exists(GEOCODE_CACHE_PATH)
    if cache is None:
        cache = pd.DataFrame(columns=["raw", "lat", "lon"])
    cache_dict = {r["raw"]: (r["lat"], r["lon"]) for _, r in cache.iterrows()}

    results = []
    last_ts = 0.0
    for _, row in df.iterrows():
        addr_raw = str(row.get(addr_col, "")).strip()
        muni_raw = str(row.get(muni_col, "")).strip() if (muni_col and muni_col in df.columns) else ""
        if not addr_raw:
            results.append({"lat": None, "lon": None}); continue
        query = f"愛媛県 {muni_raw} {addr_raw}".strip()
        if query in cache_dict:
            lat, lon = cache_dict[query]
        else:
            # レート制御 1秒/件
            now = time.time()
            if now - last_ts < 1.0:
                time.sleep(1.0 - (now - last_ts))
            lat, lon = nominatim_search(query)
            cache_dict[query] = (lat, lon)
            # キャッシュ追記
            cache = pd.concat([cache, pd.DataFrame([{"raw": query, "lat": lat, "lon": lon}])], ignore_index=True)
            save_parquet(cache, GEOCODE_CACHE_PATH)
            last_ts = time.time()
        results.append({"lat": lat, "lon": lon})
    geo = pd.DataFrame(results)
    return pd.concat([df.reset_index(drop=True), geo], axis=1)

def geocode_municipality(muni: str) -> tuple[float | None, float | None]:
    if not muni:
        return None, None
    cache = load_json_if_exists(MUNI_GEOCODE_CACHE_PATH)
    if muni in cache:
        v = cache[muni]; return v.get("lat"), v.get("lon")
    # レート制御（市町村は件数少）
    time.sleep(0.8)
    lat, lon = nominatim_search(f"{muni} 愛媛県 日本")
    cache[muni] = {"lat": lat, "lon": lon}
    save_json(cache, MUNI_GEOCODE_CACHE_PATH)
    return lat, lon

# ---------------------------
# Overpass POI 取得（30分キャッシュ）
# ---------------------------
@st.cache_data(show_spinner=False, ttl=60 * 30)
def fetch_pois_overpass(lat: float, lon: float, radius_m: int = 1200) -> list[dict]:
    # 駅・停留所、自転車駐輪、コンビニ、駐車場、公園、ATM、夜間娯楽
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
        js = r.json()
        return js.get("elements", [])
    except Exception:
        return []

def add_poi_layer(m: folium.Map, pois: list[dict]):
    if not pois:
        return
    fg = folium.FeatureGroup(name="近傍POI")
    cl = MarkerCluster(name="POIクラスタ").add_to(fg)
    for e in pois:
        lat, lon = e.get("lat"), e.get("lon")
        if lat is None or lon is None: continue
        tags = e.get("tags", {})
        name = tags.get("name") or tags.get("brand") or ""
        cat = (
            tags.get("railway") or tags.get("public_transport") or tags.get("amenity") or tags.get("leisure") or ""
        )
        html = f"<b>{name or '(名称未設定)'}</b><br>種別: {cat}"
        folium.Marker([lat, lon], popup=folium.Popup(html, max_width=280), icon=folium.Icon(color="green", icon="ok")).add_to(cl)
    fg.add_to(m)

# ---------------------------
# 2019概位置（市町村重心＋微ジッター）
# ---------------------------
def add_2019_layer(m: folium.Map, all_df: pd.DataFrame | None, max_points: int = 800):
    if all_df is None or all_df.empty:
        return
    # 市町村ごとに軽くサンプルしつつ概位置生成
    df = all_df.copy()
    df = df.sample(frac=1.0, random_state=42)  # シャッフル
    df = df.head(max_points)
    fg = folium.FeatureGroup(name="2019概位置（重心＋微ジッター）")
    cl = MarkerCluster(name="2019クラスタ").add_to(fg)

    color_map = {
        "ひったくり": "red", "車上ねらい": "orange", "部品ねらい": "lightred",
        "自動車盗": "darkred", "オートバイ盗": "cadetblue", "自転車盗": "blue",
        "自動販売機ねらい": "purple", "不明": "gray"
    }
    muni_cache = {}

    for _, r in df.iterrows():
        muni = str(r.get("municipality") or "").strip()
        ctype = str(r.get("ctype") or "不明")
        if not muni:
            continue
        if muni in muni_cache:
            lat0, lon0 = muni_cache[muni]
        else:
            lat0, lon0 = geocode_municipality(muni)
            muni_cache[muni] = (lat0, lon0)
        if not lat0 or not lon0:
            continue
        lat, lon = jitter_latlon(lat0, lon0, meters=120.0)
        ic_color = color_map.get(ctype, "gray")
        html = f"<b>{muni}</b><br>種別: {ctype}<br>（概位置）"
        folium.CircleMarker([lat, lon], radius=5, color=ic_color, fill=True, fill_opacity=0.6, popup=folium.Popup(html, max_width=260)).add_to(cl)
    fg.add_to(m)

# ---------------------------
# ユーザーCSV（アップロード）レイヤ
# ---------------------------
def add_userpoints_layer(m: folium.Map, udf: pd.DataFrame | None):
    if udf is None or udf.empty:
        return
    if not {"lat", "lon"}.issubset(set(udf.columns)):
        return
    fg = folium.FeatureGroup(name="アップロード地点")
    cl = MarkerCluster(name="アップロード地点クラスタ").add_to(fg)
    for _, r in udf.iterrows():
        lat, lon = r.get("lat"), r.get("lon")
        if pd.isna(lat) or pd.isna(lon):
            continue
        label = ""
        for c in ["住所", "address", "名称", "name"]:
            if c in udf.columns and pd.notna(r.get(c)):
                label = str(r.get(c)); break
        html = f"<b>{label or '地点'}</b><br>({lat:.6f}, {lon:.6f})"
        folium.Marker([lat, lon], popup=folium.Popup(html, max_width=280), icon=folium.Icon(color="lightblue", icon="star")).add_to(cl)
    fg.add_to(m)

# ---------------------------
# 将来30日最高日
# ---------------------------
def evaluate_next_30days(lat: float, lon: float, base_dt: datetime, all_df: pd.DataFrame | None) -> dict:
    # ダミー標準気象で評価（将来は予報API連携）
    best = {"date": None, "score": -1.0}
    for d in range(1, 31):
        target = (base_dt + timedelta(days=d)).replace(hour=21, minute=0, second=0, microsecond=0)
        weather = {"temp_c": 26.0, "humidity": 65, "precip_mm": 0.0, "condition": "—", "wind_kph": 5.0}
        moon = get_mgpn_moon(lat, lon, target)
        snap = compute_risk_score(weather, target, all_df, moon)
        if snap["score"] > best["score"]:
            best = {"date": target, "score": snap["score"], "level": snap["level"]}
    return best

# ---------------------------
# Streamlit 本体
# ---------------------------
def main():
    st.set_page_config(APP_TITLE, page_icon="🚨", layout="wide")
    st.markdown(DRAMA_CSS, unsafe_allow_html=True)

    st.markdown(f"<h1 style='margin:0 0 8px 0;'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.caption("愛媛県全域。地図クリックで地点選択 → 『分析する』でスコア算出。月齢は mgpn API（v2/3）を安全に利用。")

    # セッションステート
    if "sel_lat" not in st.session_state:
        st.session_state.sel_lat = INIT_LAT
    if "sel_lon" not in st.session_state:
        st.session_state.sel_lon = INIT_LON
    if "last_snap" not in st.session_state:
        st.session_state.last_snap = None
    if "user_geo_df" not in st.session_state:
        st.session_state.user_geo_df = None
    if "poi_radius" not in st.session_state:
        st.session_state.poi_radius = 1200
    if "pois" not in st.session_state:
        st.session_state.pois = []
    if "csv_preview" not in st.session_state:
        st.session_state.csv_preview = None

    # サイドバー
    with st.sidebar:
        st.markdown("### 設定")
        st.write("地図をクリックすると地点が選択されます。")
        st.session_state.sel_lat = st.number_input("選択緯度", value=float(st.session_state.sel_lat), format="%.6f")
        st.session_state.sel_lon = st.number_input("選択経度", value=float(st.session_state.sel_lon), format="%.6f")
        run_gemini = st.toggle("Gemini 2.5 Flashで説明を生成（任意）", value=False)

        st.divider()
        st.markdown("#### データ検出")
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
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.markdown("#### API キー状態")
        st.write(f"- WeatherAPI: {'✅' if WEATHERAPI_KEY else '—'}")
        st.write(f"- OpenWeather: {'✅' if OPENWEATHER_KEY else '—'}")
        st.write(f"- Gemini: {'✅' if GEMINI_KEY else '—'}")
        st.markdown("（mgpn 月齢APIはキー不要・v2/v3のみ使用）")

        st.divider()
        st.markdown("#### バージョン")
        try:
            import streamlit_folium as _sf
            st.write(f"streamlit_folium: {_sf.__version__}")
        except Exception:
            st.write("streamlit_folium: 不明")
        try:
            st.write(f"folium: {folium.__version__}")
        except Exception:
            st.write("folium: 不明")

    # 2019データ（キャッシュ）
    @st.cache_data(show_spinner=False)
    def _load_2019():
        return load_all_crime_2019(DATA_GLOBS)

    all_df = _load_2019()

    # 地図（クリックで地点更新）—【従来の表示を維持】
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("**地図：クリックで任意地点を選択（ドラッグで微調整）**")
    fmap = render_map_selectable(st.session_state.sel_lat, st.session_state.sel_lon, st.session_state.last_snap)
    out = call_st_folium_with_fallback(fmap, height=540, key="map_select", return_last_clicked=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if out and isinstance(out, dict) and out.get("last_clicked"):
        lat = out["last_clicked"].get("lat")
        lon = out["last_clicked"].get("lng")
        if lat is not None and lon is not None:
            if (EHIME_BBOX["min_lat"] <= lat <= EHIME_BBOX["max_lat"]) and (EHIME_BBOX["min_lon"] <= lon <= EHIME_BBOX["max_lon"]):
                st.session_state.sel_lat = float(lat)
                st.session_state.sel_lon = float(lon)
            else:
                st.warning("選択地点が愛媛県の想定範囲外です。")

    # 操作ボタン
    colb1, colb2, _ = st.columns([1, 1, 2])
    with colb1:
        analyze = st.button("🔎 分析する", use_container_width=True)
    with colb2:
        reset = st.button("📍 初期地点へ戻す", use_container_width=True)

    if reset:
        st.session_state.sel_lat = INIT_LAT
        st.session_state.sel_lon = INIT_LON
        st.session_state.last_snap = None
        st.rerun()

    # CSVアップロード & ジオコーディング
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("**CSVアップロード（住所→座標化）**")
    up = st.file_uploader("住所CSVを選択（UTF-8/CP932等自動判別）", type=["csv"])
    colu1, colu2, colu3 = st.columns([2, 2, 1])
    with colu1:
        addr_col = st.text_input("住所列名（必須）", value="住所")
    with colu2:
        muni_col = st.text_input("市町村列名（任意）", value="市町村")
    with colu3:
        geo_run = st.button("ジオコーディング実行", use_container_width=True)

    if up is not None and st.session_state.csv_preview is None:
        # プレビュー（先頭200行）
        try:
            raw = up.read()
            enc_guess = (chardet.detect(raw).get("encoding") or "utf-8").lower()
            df_tmp = pd.read_csv(io.BytesIO(raw), encoding=enc_guess, engine="python")
            st.session_state.csv_preview = df_tmp.head(200)
            st.info(f"プレビューを表示（推定エンコード: {enc_guess}）")
        except Exception as e:
            st.error(f"CSV読込に失敗: {e}")

    if st.session_state.csv_preview is not None:
        st.dataframe(st.session_state.csv_preview, use_container_width=True, height=200)

    if geo_run:
        try:
            if st.session_state.csv_preview is None:
                st.warning("先にCSVを選択してください。")
            elif addr_col not in st.session_state.csv_preview.columns:
                st.warning("住所列名が一致しません。正しい列名を指定してください。")
            else:
                with st.spinner("Nominatimで住所を座標化中（1秒/件・キャッシュあり）..."):
                    udf = geocode_address_rows(st.session_state.csv_preview, addr_col, muni_col if muni_col in st.session_state.csv_preview.columns else None)
                    st.session_state.user_geo_df = udf
                ok = udf[["lat", "lon"]].notna().all(axis=1).sum()
                st.success(f"ジオコーディング完了：{ok}/{len(udf)} 行で座標取得")
        except Exception as e:
            st.error(f"ジオコーディング中にエラー: {e}")
    st.markdown("</div>", unsafe_allow_html=True)

    # POI 取得
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("**近傍POI（Overpass）**")
    pr = st.slider("探索半径 [m]", min_value=400, max_value=3000, step=100, value=int(st.session_state.poi_radius))
    colp1, colp2 = st.columns([1, 3])
    with colp1:
        poi_btn = st.button("近傍POIを取得", use_container_width=True)
    with colp2:
        st.caption("駅・停留所・駐輪場・コンビニ・駐車場・公園・ATM・夜間娯楽 などを簡易抽出")
    if poi_btn:
        st.session_state.poi_radius = pr
        with st.spinner("Overpass API からPOIを取得中..."):
            st.session_state.pois = fetch_pois_overpass(st.session_state.sel_lat, st.session_state.sel_lon, pr)
        st.success(f"POI取得：{len(st.session_state.pois)} 件")
    st.markdown("</div>", unsafe_allow_html=True)

    # 分析フロー（ボタン押下時のみ API 呼出し）＋ finally でオーバーレイ消去
    if analyze:
        overlay = st.empty()
        prog = st.empty()
        try:
            overlay.markdown(
                """
                <div class="overlay">
                  <div class="overlay-content">
                    <div class="overlay-title">解析中</div>
                    <div class="overlay-sub">気象・月齢（mgpn）・2019傾向を統合しています…</div>
                    <div class="loader"></div>
                  </div>
                </div>
                """, unsafe_allow_html=True
            )
            p = prog.progress(0, text="準備中…")
            steps = [(15, "気象の取得…"), (40, "月齢（mgpn）の取得…"), (70, "2019年傾向の補正…"), (100, "スコア集計…")]
            for v, txt in steps:
                time.sleep(0.35)
                p.progress(v, text=txt)

            now_dt = datetime.now(JST)
            lat, lon = st.session_state.sel_lat, st.session_state.sel_lon
            weather = get_weather(lat, lon)
            moon_info = get_mgpn_moon(lat, lon, now_dt)
            snap = compute_risk_score(weather, now_dt, all_df, moon_info)
            st.session_state.last_snap = snap
        finally:
            overlay.empty(); prog.empty()

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
                st.dataframe(vc, use_container_width=True, height=220)
                st.markdown("</div>", unsafe_allow_html=True)

            # 将来30日の最大リスク日
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**将来30日の最大リスク日（各日21:00評価・仮天候）**")
            best = evaluate_next_30days(st.session_state.sel_lat, st.session_state.sel_lon, datetime.now(JST), all_df)
            if best["date"] is not None:
                st.write(f"最も高いのは **{best['date'].strftime('%Y-%m-%d %H:%M')}** 、推定スコア **{best['score']} ({best['level']})**")
            else:
                st.write("評価不能（月齢API等の到達性）")
            st.markdown("</div>", unsafe_allow_html=True)

        with c2:
            # 結果マップ（レイヤ追加）—【従来の2枚構成/見た目を維持】
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**警戒ゾーン＆レイヤ（選択地点基準）**")
            fmap2 = render_map_selectable(st.session_state.sel_lat, st.session_state.sel_lon, snap)
            # 追加レイヤ：2019概位置／アップロード地点／POI
            add_2019_layer(fmap2, all_df)
            add_userpoints_layer(fmap2, st.session_state.user_geo_df)
            add_poi_layer(fmap2, st.session_state.pois)
            call_st_folium_with_fallback(fmap2, height=460, key="map_result", return_last_clicked=False)
            st.markdown("</div>", unsafe_allow_html=True)

            if run_gemini:
                with st.spinner("Gemini 2.5 Flash が説明を生成中..."):
                    msg = gemini_explain(snap, datetime.now(JST))
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown("**Gemini 2.5 Flash の見解（任意）**")
                st.write(msg if msg else "（Gemini未設定または生成失敗）")
                st.markdown("</div>", unsafe_allow_html=True)

    # フッター（注意書き）
    st.caption(
        "※ 月齢は mgpn v2→v3の順でボタン押下時のみ呼出し、30分キャッシュ＆最大3リトライします。"
        " 気象APIキー未設定時はダミー値。2019年オープンデータは ./, ./data, /mnt/data の順で検出。"
        " st_foliumが不一致/失敗時はHTML直埋めで地図表示（クリック取得不可）。"
        " アップロード/2019は概位置表示で個人特定リスクを抑制しています。"
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error("致命的エラーが発生しました。ログ：\n" + "".join(traceback.format_exception(e)))
