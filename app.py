# -*- coding: utf-8 -*-
# ============================================================
# 上島町 周辺：犯罪者・異常者 警戒予測（Streamlit）
# - 気象/月齢 API（WeatherAPI/OpenWeatherMap） + 月相/気温/降雨/時間帯/曜日
# - 愛媛県警等オープンデータ（CSV任意）を加点補正
# - Gemini 2.5 Flash（任意）でスコア説明を生成（Thinking Budget切替）
# - マップ（Folium）にヒートサークル、ドラマ風UI
# Python 3.12 互換
# ============================================================

import os
import json
import time
from datetime import datetime, timedelta, timezone
from dateutil import tz
import requests
import numpy as np
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import folium

JST = timezone(timedelta(hours=9))
APP_TITLE = "上島町 警戒予測モニター"

# ---------------------------
# 既定座標：上島町役場（生名開発総合センター付近）
# ---------------------------
DEFAULT_LAT = 34.27717
DEFAULT_LON = 133.20986

# ---------------------------
# スタイル（ドラマ風）
# ---------------------------
DRAMA_CSS = """
<style>
/* フルスクリーン系ダーク */
.main, .stApp { background: #0b0f14; color: #e6eef7; }
section[data-testid="stSidebar"] { background: #0e141b; }

/* 数字カウンタ */
.score-big {
  font-size: 72px; font-weight: 900; letter-spacing: 1.5px;
  text-shadow: 0 0 8px rgba(255,0,0,0.25), 0 0 16px rgba(255,0,0,0.15);
  margin: 0; line-height: 1.0;
}

/* レベルタグ */
.badge {
  display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 700;
  background: linear-gradient(135deg, #1c2633 0%, #121821 100%);
  border: 1px solid rgba(255,255,255,0.1); color:#d9e6f2; margin-left: 8px;
}

/* 警報バー（上スライドイン） */
.alert-bar {
  position: sticky; top: 0; z-index: 1000;
  background: linear-gradient(90deg, rgba(180,0,0,0.85), rgba(255,50,50,0.85));
  color: white; padding: 10px 14px; border-bottom: 2px solid rgba(255,255,255,0.25);
  box-shadow: 0 4px 24px rgba(255,0,0,0.35);
  animation: pulse 2s infinite;
}
@keyframes pulse {
  0% { box-shadow: 0 0 0 rgba(255,0,0,0.4); }
  50% { box-shadow: 0 0 24px rgba(255,0,0,0.6); }
  100% { box-shadow: 0 0 0 rgba(255,0,0,0.4); }
}

/* カード */
.card {
  background: linear-gradient(135deg, #121821, #0e141b);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px; padding: 16px 18px; margin: 8px 0;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35);
}

/* 主要ボタン */
button[kind="primary"], .stButton>button {
  background: linear-gradient(135deg, #143957, #0d2a41) !important;
  border: 1px solid rgba(255,255,255,0.15) !important;
  color: #e6eef7 !important; font-weight: 700 !important;
  border-radius: 12px !important;
}

/* サブテキスト */
.mute { color: #a8b7c7; font-size: 13px; }
</style>
"""

# ---------------------------
# Secrets / API Keys
# ---------------------------
WEATHERAPI_KEY = st.secrets.get("weatherapi", {}).get("api_key", "")
OPENWEATHER_KEY = st.secrets.get("openweather", {}).get("api_key", "")
GEMINI_KEY = st.secrets.get("gemini", {}).get("api_key", "")
GEMINI_MODEL = st.secrets.get("gemini", {}).get("model", "gemini-2.5-flash")
GEMINI_THINKING_BUDGET = st.secrets.get("gemini", {}).get("thinking_budget", 0)

# ---------------------------
# 補助：天気・月齢の取得（フォールバック連鎖）
# ---------------------------
def get_weather_weatherapi(lat, lon):
    """WeatherAPI.com：現在・予報・天文（月齢）"""
    try:
        if not WEATHERAPI_KEY:
            return None
        base = "https://api.weatherapi.com/v1"
        # current + forecast（当日分）＋ astronomy
        params_common = f"key={WEATHERAPI_KEY}&q={lat},{lon}"
        r_curr = requests.get(f"{base}/current.json?{params_common}&aqi=no", timeout=10)
        r_astr = requests.get(f"{base}/astronomy.json?{params_common}", timeout=10)
        r_curr.raise_for_status(); r_astr.raise_for_status()
        curr = r_curr.json()
        astr = r_astr.json()
        return {
            "provider": "weatherapi",
            "temp_c": curr["current"]["temp_c"],
            "humidity": curr["current"]["humidity"],
            "condition": curr["current"]["condition"]["text"],
            "precip_mm": curr["current"].get("precip_mm", 0.0),
            "wind_kph": curr["current"].get("wind_kph", 0.0),
            "moon_phase": astr["astronomy"]["astro"]["moon_phase"],  # e.g., "Full Moon"
        }
    except Exception:
        return None

def get_weather_openweather(lat, lon):
    """OpenWeatherMap：現在（+OneCallでmoon_phaseあり）"""
    try:
        if not OPENWEATHER_KEY:
            return None
        # OneCall 3.0 ではAPIキー契約により制限あり。簡易：現在天気＋月相はdailyで補完
        url_now = "https://api.openweathermap.org/data/2.5/weather"
        p_now = {"lat": lat, "lon": lon, "appid": OPENWEATHER_KEY, "units": "metric", "lang": "ja"}
        r_now = requests.get(url_now, params=p_now, timeout=10); r_now.raise_for_status()
        jd = r_now.json()
        # 月相は daily の OneCall が理想だが、キー制限を考慮し省略。moon_phaseはNoneに。
        return {
            "provider": "openweather",
            "temp_c": jd["main"]["temp"],
            "humidity": jd["main"]["humidity"],
            "condition": jd["weather"][0]["description"],
            "precip_mm": 0.0,  # 現在APIからは取得しにくいので0扱い（必要なら OneCall に変更）
            "wind_kph": jd.get("wind", {}).get("speed", 0.0) * 3.6,
            "moon_phase": None,
        }
    except Exception:
        return None

def get_weather(lat, lon):
    """優先：WeatherAPI -> OpenWeather -> ダミー"""
    w = get_weather_weatherapi(lat, lon)
    if not w:
        w = get_weather_openweather(lat, lon)
    if not w:
        # ダミー（オフライン時用）
        w = {
            "provider": "dummy",
            "temp_c": 26.0,
            "humidity": 70,
            "condition": "晴れ",
            "precip_mm": 0.0,
            "wind_kph": 8.0,
            "moon_phase": "Full Moon" if datetime.now(JST).day % 29 in [14,15] else "Waxing Gibbous"
        }
    return w

def is_full_moon_like(phase_text: str | None) -> bool:
    if not phase_text:
        return False
    pt = phase_text.lower()
    # 満月に近い表現を広めに拾う
    return ("full" in pt) or ("満月" in pt)

# ---------------------------
# 犯罪オープンデータの読み込み（任意）
# ---------------------------
def load_crime_history(csv_path: str = "data/ehime_crime_2023.csv") -> pd.DataFrame | None:
    try:
        if not os.path.exists(csv_path):
            return None
        df = pd.read_csv(csv_path, encoding="utf-8")
        # 想定：列に「発生日」「市町村名」「罪種/手口」などがあるケース
        # 列名を標準化（手持ちのCSVに合わせて調整）
        cols = {c: c for c in df.columns}
        # ざっくり推定
        for c in list(df.columns):
            if "日" in c and "発" in c:
                cols[c] = "date"
            if ("市" in c or "町" in c) and ("名" in c):
                cols[c] = "municipality"
            if ("手口" in c) or ("罪" in c):
                cols[c] = "type"
        df = df.rename(columns=cols)
        # 日付標準化
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        else:
            df["date"] = pd.NaT
        return df
    except Exception:
        return None

# ---------------------------
# スコア計算（ルールベース + 補正）
# ---------------------------
def compute_risk_score(weather: dict, now_dt: datetime, crime_df: pd.DataFrame | None,
                       lat: float, lon: float, municipality_hint: str = "上島町") -> dict:
    """
    0-100 スコア。
    主要因：気温(重), 降雨(減), 夜間(加), 週末(加), 月相(軽い加点), 過去件数(補正)
    ※学術的に満月効果は限定的 → 加点は控えめ
    """
    score = 0.0
    reasons = []

    temp = float(weather.get("temp_c", 20.0))
    precip = float(weather.get("precip_mm", 0.0))
    humidity = float(weather.get("humidity", 60))
    cond = str(weather.get("condition", ""))
    moon = weather.get("moon_phase")

    # 気温：25℃超で逓増（最大 +40）
    if temp >= 30:
        add = 40
    elif temp >= 27:
        add = 30
    elif temp >= 25:
        add = 20
    elif temp >= 22:
        add = 10
    else:
        add = 0
    score += add
    if add > 0:
        reasons.append(f"気温{temp:.0f}℃（高温）:+{add}")

    # 降雨：強い降雨は街頭活動低下 → 減点（最大 -20）
    if precip >= 10:
        score -= 20
        reasons.append("強い降雨:-20")
    elif precip >= 1:
        score -= 8
        reasons.append("降雨あり:-8")

    # 時間帯：夜間(20-24,0-4) +15、夕方(17-20) +7
    hour = now_dt.hour
    if 20 <= hour <= 23 or 0 <= hour <= 4:
        score += 15; reasons.append("夜間:+15")
    elif 17 <= hour < 20:
        score += 7; reasons.append("夕方:+7")

    # 曜日：金土に +6（人出増）
    weekday = now_dt.weekday()  # Mon=0
    if weekday in (4, 5):
        score += 6; reasons.append("週末(+金土):+6")

    # 月齢：満月近似なら +5（控えめ）
    if is_full_moon_like(moon):
        score += 5; reasons.append("満月:+5")

    # 湿度極端：不快指数上昇で +3
    if humidity >= 80:
        score += 3; reasons.append("高湿度:+3")

    # 過去データ補正（任意）：同市町/夏夜の発生が多いなど → +0〜10
    if crime_df is not None and "municipality" in crime_df.columns and "date" in crime_df.columns:
        recent = crime_df.copy()
        if "municipality" in recent.columns:
            msk = recent["municipality"].astype(str).str.contains(municipality_hint, na=False)
            recent = recent[msk]
        # 直近2年分
        recent = recent.sort_values("date").dropna(subset=["date"])
        recent = recent[recent["date"] >= (now_dt - timedelta(days=730))]
        # 夏夜に偏るか簡易判定
        if not recent.empty:
            recent["mo"] = recent["date"].dt.month
            recent["hr"] = recent["date"].dt.hour if "hour" in recent.columns else np.random.randint(0, 24, len(recent))
            summer_night = recent[(recent["mo"].isin([6,7,8,9])) & ((recent["hr"]>=20)|(recent["hr"]<=4))]
            ratio = len(summer_night) / max(1, len(recent))
            if ratio >= 0.4:
                score += 8; reasons.append("過去傾向(夏夜に偏り):+8")
            elif ratio >= 0.25:
                score += 4; reasons.append("過去傾向(やや夏夜偏り):+4")

    # 正規化＆クリップ
    score = max(0.0, min(100.0, score))

    # レベル化
    if score < 25:
        level = "Low"
        color = "#0aa0ff"
    elif score < 50:
        level = "Moderate"
        color = "#ffd033"
    elif score < 75:
        level = "High"
        color = "#ff7f2a"
    else:
        level = "Very High"
        color = "#ff2a2a"

    return {
        "score": round(score, 1),
        "level": level,
        "color": color,
        "reasons": reasons,
        "moon_phase": moon,
        "temp_c": temp,
        "humidity": humidity,
        "precip_mm": precip,
        "condition": cond,
    }

# ---------------------------
# Gemini 2.5 Flash（任意）
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
                "あなたは防犯アナリストAIです。与えられた要因から、上島町周辺の犯罪/異常行動リスクの"
                "根拠を、過度な断定を避けて簡潔に日本語で説明してください。"
                "満月効果は限定的であること、気温・時間帯・降雨の影響は実証研究で示唆があることを踏まえ、"
                "誇張表現は避け、具体的な注意点（人通りの少ない場所を避ける等）で締めてください。"
            )
        )
        thinking_cfg = {"thinking": {"budget_tokens": int(GEMINI_THINKING_BUDGET)}} if GEMINI_THINKING_BUDGET else {}
        prompt = (
            f"現在: {now_dt.strftime('%Y-%m-%d %H:%M JST')}\n"
            f"スコア: {snap['score']} ({snap['level']})\n"
            f"要因: 気温{snap['temp_c']}℃, 湿度{snap['humidity']}%, 降水{snap['precip_mm']}mm, 天候:{snap['condition']}, 月相:{snap['moon_phase']}\n"
            f"内部理由: {', '.join(snap['reasons'])}\n"
            "これらを踏まえた上で、一般向けの注意喚起コメントを100〜180字で。"
        )
        resp = model.generate_content(prompt, **thinking_cfg)
        return resp.text.strip()
    except Exception:
        return None

# ---------------------------
# マップ描画（Folium）
# ---------------------------
def render_map(lat: float, lon: float, snap: dict):
    m = folium.Map(location=[lat, lon], zoom_start=12, tiles="cartodb dark_matter")
    # 中心サークル（色はスコア色）
    folium.Circle(
        location=[lat, lon],
        radius=1500 if snap["score"] < 50 else (2500 if snap["score"] < 75 else 3500),
        color=snap["color"], fill=True, fill_opacity=0.25, weight=2
    ).add_to(m)

    # ポップアップ
    popup_html = f"""
    <div style="color:#fff;">
      <b>警戒度:</b> {snap['score']} ({snap['level']})<br/>
      <b>月相:</b> {snap['moon_phase']}<br/>
      <b>天候:</b> {snap['condition']} / {snap['temp_c']}℃ / 降水{snap['precip_mm']}mm
    </div>
    """
    folium.Marker([lat, lon],
        popup=folium.Popup(popup_html, max_width=280),
        icon=folium.Icon(color="red" if snap["score"]>=75 else "orange" if snap["score"]>=50 else "blue", icon="info-sign")
    ).add_to(m)
    return m

# ---------------------------
# Streamlit App
# ---------------------------
def main():
    st.set_page_config(APP_TITLE, page_icon="🚨", layout="wide")
    st.markdown(DRAMA_CSS, unsafe_allow_html=True)

    # タイトル
    st.markdown(f"<h1 style='margin:0 0 8px 0;'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.caption("満月/気象/時刻/曜日 + 過去傾向（任意）で警戒スコアを推定。Gemini 2.5 Flash による説明（任意）。")

    # サイドバー：設定
    with st.sidebar:
        st.markdown("### 設定")
        lat = st.number_input("緯度 (Lat)", value=DEFAULT_LAT, format="%.6f")
        lon = st.number_input("経度 (Lon)", value=DEFAULT_LON, format="%.6f")
        municipality = st.text_input("市町村名ヒント（過去データ抽出）", value="上島町")
        csv_path = st.text_input("過去データCSV（任意）", value="data/ehime_crime_2023.csv")
        run_gemini = st.toggle("Gemini 2.5 Flashで説明を生成（任意）", value=False)
        st.divider()
        st.markdown("#### API キー状態")
        st.write(f"- WeatherAPI: {'✅' if WEATHERAPI_KEY else '—'}")
        st.write(f"- OpenWeather: {'✅' if OPENWEATHER_KEY else '—'}")
        st.write(f"- Gemini: {'✅' if GEMINI_KEY else '—'}")

    # データ取得
    now_dt = datetime.now(JST)
    weather = get_weather(lat, lon)
    crime_df = load_crime_history(csv_path)

    # スコア計算
    snap = compute_risk_score(weather, now_dt, crime_df, lat, lon, municipality)

    # 警報バー
    if snap["score"] >= 75:
        st.markdown(f"<div class='alert-bar'>警報：現在の警戒レベルは <b>{snap['level']}</b>（{snap['score']}）です。周囲に注意してください。</div>", unsafe_allow_html=True)
    elif snap["score"] >= 50:
        st.markdown(f"<div class='alert-bar' style='background:linear-gradient(90deg, rgba(200,120,0,0.85), rgba(255,170,0,0.85));'>注意：現在の警戒レベルは <b>{snap['level']}</b>（{snap['score']}）。不審な状況に備えてください。</div>", unsafe_allow_html=True)

    # 上段：スコア／要因
    c1, c2 = st.columns([1,1])
    with c1:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("<div class='mute'>CURRENT RISK</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='score-big' style='color:{snap['color']};'>{int(round(snap['score']))}</div>", unsafe_allow_html=True)
        st.markdown(f"<span class='badge'>{snap['level']}</span>", unsafe_allow_html=True)
        st.write("")
        st.markdown(f"**月相**：{snap['moon_phase']}  /  **天候**：{snap['condition']}  /  **気温**：{snap['temp_c']}℃  /  **降水**：{snap['precip_mm']}mm  /  **湿度**：{snap['humidity']}%", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("**内部理由（ヒューリスティック）**")
        if snap["reasons"]:
            for r in snap["reasons"]:
                st.write("・", r)
        else:
            st.write("—")
        st.markdown("</div>", unsafe_allow_html=True)

    with c2:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markdown("**マップ（警戒ゾーン）**")
        fmap = render_map(lat, lon, snap)
        st_folium(fmap, height=420, returned_objects=[])
        st.markdown("</div>", unsafe_allow_html=True)

        # Gemini 説明
        if run_gemini:
            with st.spinner("Gemini 2.5 Flash が説明を生成中..."):
                msg = gemini_explain(snap, now_dt)
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**Gemini 2.5 Flash の見解（任意）**")
            st.write(msg if msg else "（Gemini未設定または生成に失敗）")
            st.markdown("</div>", unsafe_allow_html=True)

    # 下段：通知と運用
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("### 通知（擬似）・運用メモ")
    st.write("- 本番ではサーバ側で定期評価 → しきい値到達時に FCM/APNs で端末へプッシュ通知。")
    st.write("- 本サンプルでは擬似的にスコア75以上で画面上部の**赤バー**、50以上で**橙バー**を表示。")
    st.write("- CSVを年度更新すると、過去傾向補正が自動更新されます（ETLは本番でスケジューラ化）。")
    st.markdown("</div>", unsafe_allow_html=True)

    # フッター
    st.caption("※ 満月効果は学術的に限定的です。気温・時間帯・人出・降雨などの複合要因で警戒度を示します。"
               " データは正確性向上のため随時更新してください。")


if __name__ == "__main__":
    main()
