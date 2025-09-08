# -*- coding: utf-8 -*-
# ============================================================
# 愛媛県 全域対応：犯罪者・異常者 警戒予測（Streamlit）
# - 2019 愛媛県オープンデータ（手口別CSV）を自動統合
# - 地図クリック＆ドラッグで任意地点を選択（愛媛内を想定）
# - 「分析する」ボタン → 進捗バー + 中央オーバーレイ「解析中」
# - 気象・月齢API（任意） + 2019傾向補正 + ドラマ風UI
# - Gemini 2.5 Flash（任意）で説明生成
# ============================================================

import os
import re
import glob
import time
import json
from datetime import datetime, timedelta, timezone
import chardet
import requests
import numpy as np
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium
import folium

# ---------------------------
# 基本設定
# ---------------------------
JST = timezone(timedelta(hours=9))
APP_TITLE = "愛媛県 警戒予測モニター"

# 愛媛県の中心付近（県庁付近）
EHIME_CENTER_LAT = 33.8416
EHIME_CENTER_LON = 132.7661
# 愛媛のざっくりバウンディングボックス（簡易な入力制約用）
EHIME_BBOX = {
    "min_lat": 32.8, "max_lat": 34.6,
    "min_lon": 131.8, "max_lon": 134.0
}

# 旧：上島町の便宜中心（初期選択の参考）
UEJIMA_LAT = 34.27717
UEJIMA_LON = 133.20986

DATA_GLOB = "/mnt/data/ehime_2019*.csv"  # ご提供のCSV群
MUNICIPALITY_DEFAULT = "愛媛県"

# ---------------------------
# UI スタイル（ドラマ風 + 中央オーバーレイ）
# ---------------------------
DRAMA_CSS = """
<style>
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
/* 警報バー */
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
/* ボタン */
button[kind="primary"], .stButton>button {
  background: linear-gradient(135deg, #143957, #0d2a41) !important;
  border: 1px solid rgba(255,255,255,0.15) !important;
  color: #e6eef7 !important; font-weight: 700 !important;
  border-radius: 12px !important;
}
/* サブテキスト */
.mute { color: #a8b7c7; font-size: 13px; }

/* 画面中央オーバーレイ（解析中） */
.overlay {
  position: fixed;
  inset: 0;
  display: flex;
  align-items: center; justify-content: center;
  background: rgba(0, 0, 0, 0.45);
  z-index: 9999;
}
.overlay-content {
  text-align: center;
  padding: 28px 36px;
  border-radius: 18px;
  backdrop-filter: blur(6px);
  background: linear-gradient(135deg, rgba(20,40,60,0.85), rgba(8,16,24,0.85));
  border: 1px solid rgba(255,255,255,0.15);
  box-shadow: 0 12px 48px rgba(0,0,0,0.5), 0 0 24px rgba(0,180,255,0.2) inset;
}
.overlay-title {
  font-size: 28px; font-weight: 900; letter-spacing: 0.2em; color: #e8f2ff;
  text-shadow: 0 0 12px rgba(0,180,255,0.3);
}
.overlay-sub {
  margin-top: 8px; font-size: 13px; color: #bcd0e0;
}
.loader {
  margin: 16px auto 0 auto;
  width: 72px; height: 72px; border-radius: 50%;
  border: 4px solid rgba(255,255,255,0.18);
  border-top-color: #39c0ff;
  animation: spin 1.1s linear infinite;
}
@keyframes spin {
  to { transform: rotate(360deg); }
}
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
# 文字コード推定＋CSV読込（堅牢）
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
        date_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["date","day","time","occur"])), None)
    # 市町村
    muni_col = next((c for c in df.columns if re.search(r"(市|町|村).*名", str(c)) or re.search(r"(市町村|自治体|地域)", str(c))), None)
    if not muni_col:
        muni_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["municipality","city","town","area","region"])), None)
    # 手口
    type_col = next((c for c in df.columns if re.search(r"(手口|罪|罪種|種別|分類)", str(c))), None)
    if not type_col:
        type_col = next((c for c in df.columns if any(k in cols_lower[c] for k in ["type","category","kind","crime"])), None)
    return {"date": date_col, "municipality": muni_col, "ctype": type_col}

def parse_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def load_all_crime_2019(data_glob: str = DATA_GLOB) -> pd.DataFrame | None:
    files = sorted(glob.glob(data_glob))
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
                    ctype = v; break
            df["ctype"] = ctype if ctype else "不明"
        else:
            df.rename(columns={g["ctype"]: "ctype"}, inplace=True)
            df["ctype"] = df["ctype"].astype(str)
        # 2019だけ（念のため）
        df = df[(df["date"].dt.year == 2019) | (df["date"].isna())]
        frames.append(df[["date","municipality","ctype"]])
    all_df = pd.concat(frames, ignore_index=True)
    return all_df

# ---------------------------
# 天気・月齢（任意API） ※キー無ければダミー
# ---------------------------
def get_weather_weatherapi(lat, lon):
    try:
        if not WEATHERAPI_KEY:
            return None
        base = "https://api.weatherapi.com/v1"
        common = f"key={WEATHERAPI_KEY}&q={lat},{lon}"
        r_curr = requests.get(f"{base}/current.json?{common}&aqi=no", timeout=10)
        r_ast  = requests.get(f"{base}/astronomy.json?{common}", timeout=10)
        r_curr.raise_for_status(); r_ast.raise_for_status()
        curr = r_curr.json(); astr = r_ast.json()
        return {
            "provider": "weatherapi",
            "temp_c": curr["current"]["temp_c"],
            "humidity": curr["current"]["humidity"],
            "condition": curr["current"]["condition"]["text"],
            "precip_mm": curr["current"].get("precip_mm", 0.0),
            "wind_kph": curr["current"].get("wind_kph", 0.0),
            "moon_phase": astr["astronomy"]["astro"]["moon_phase"],
        }
    except Exception:
        return None

def get_weather_openweather(lat, lon):
    try:
        if not OPENWEATHER_KEY:
            return None
        url = "https://api.openweathermap.org/data/2.5/weather"
        p = {"lat": lat, "lon": lon, "appid": OPENWEATHER_KEY, "units": "metric", "lang": "ja"}
        r = requests.get(url, params=p, timeout=10); r.raise_for_status()
        jd = r.json()
        return {
            "provider": "openweather",
            "temp_c": jd["main"]["temp"],
            "humidity": jd["main"]["humidity"],
            "condition": jd["weather"][0]["description"],
            "precip_mm": 0.0,
            "wind_kph": jd.get("wind", {}).get("speed", 0.0) * 3.6,
            "moon_phase": None,
        }
    except Exception:
        return None

def get_weather(lat, lon):
    w = get_weather_weatherapi(lat, lon)
    if not w:
        w = get_weather_openweather(lat, lon)
    if not w:
        # ダミー（オフライン）
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
    pt = str(phase_text).lower()
    return ("full" in pt) or ("満月" in pt)

# ---------------------------
# スコア計算（愛媛全域を想定／2019傾向補正）
# ---------------------------
def compute_risk_score(weather: dict, now_dt: datetime, all_df: pd.DataFrame | None) -> dict:
    score = 0.0
    reasons = []

    temp = float(weather.get("temp_c", 20.0))
    precip = float(weather.get("precip_mm", 0.0))
    humidity = float(weather.get("humidity", 60))
    cond = str(weather.get("condition", ""))
    moon = weather.get("moon_phase")

    # 気温（重）
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
    if add > 0: reasons.append(f"気温{temp:.0f}℃:+{add}")

    # 降雨（減）
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

    # 週末
    if now_dt.weekday() in (4, 5):
        score += 6; reasons.append("週末(+金土):+6")

    # 月齢（控えめ）
    if is_full_moon_like(moon):
        score += 5; reasons.append("満月:+5")

    # 湿度（わずかに加点）
    if humidity >= 80:
        score += 3; reasons.append("高湿度:+3")

    # 2019年の発生傾向（県全域）
    if all_df is not None and not all_df.empty:
        sub = all_df.copy()
        sub["month"] = sub["date"].dt.month
        month = now_dt.month
        month_ratio = len(sub[sub["month"] == month]) / max(1, len(sub))
        if month_ratio >= 0.12:
            score += 6; reasons.append("2019傾向(同月比 多め):+6")
        elif month_ratio >= 0.08:
            score += 3; reasons.append("2019傾向(同月比 やや多め):+3")

        if "ctype" in sub.columns:
            top_types = sub["ctype"].value_counts(normalize=True)
            outdoor_like = float(top_types.get("ひったくり", 0) + top_types.get("車上ねらい", 0)
                                 + top_types.get("自転車盗", 0) + top_types.get("オートバイ盗", 0))
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
            )
        )
        thinking_cfg = {"thinking": {"budget_tokens": GEMINI_THINKING_BUDGET}} if GEMINI_THINKING_BUDGET else {}
        prompt = (
            f"現在: {now_dt.strftime('%Y-%m-%d %H:%M JST')}\n"
            f"スコア: {snap['score']} ({snap['level']})\n"
            f"要因: 気温{snap['temp_c']}℃, 湿度{snap['humidity']}%, 降水{snap['precip_mm']}mm, 天候:{snap['condition']}, 月相:{snap['moon_phase']}\n"
            f"内部理由: {', '.join(snap['reasons'])}\n"
            "一般向けの注意喚起コメントを120〜200字で。"
        )
        resp = model.generate_content(prompt, **thinking_cfg)
        return (resp.text or "").strip()
    except Exception:
        return None

# ---------------------------
# マップ描画（Folium）＋クリック選択
# ---------------------------
def render_map_selectable(lat: float, lon: float, snap: dict | None):
    # 愛媛全体が入るようズーム（9〜10程度）
    m = folium.Map(location=[EHIME_CENTER_LAT, EHIME_CENTER_LON],
                   zoom_start=9, tiles="cartodb dark_matter")
    # 現在選択点（サークル＋マーカー）
    if snap:
        radius = 1500 if snap["score"] < 50 else (2500 if snap["score"] < 75 else 3500)
        folium.Circle(location=[lat, lon], radius=radius,
                      color=snap["color"], fill=True, fill_opacity=0.25, weight=2).add_to(m)
        popup_html = f"""
        <div style="color:#fff;">
          <b>警戒度:</b> {snap['score']} ({snap['level']})<br/>
          <b>月相:</b> {snap['moon_phase']}<br/>
          <b>天候:</b> {snap['condition']} / {snap['temp_c']}℃ / 降水{snap['precip_mm']}mm
        </div>
        """
    else:
        popup_html = "<div style='color:#fff;'>地点をクリックして選択</div>"

    folium.Marker(
        [lat, lon],
        popup=folium.Popup(popup_html, max_width=280),
        draggable=True,
        icon=folium.Icon(color="lightgray" if not snap else ("red" if snap["score"]>=75 else "orange" if snap["score"]>=50 else "blue"),
                         icon="info-sign")
    ).add_to(m)

    # クリックで地点選択（ClickForMarkerよりst_foliumのlast_clickedを使用）
    return m

# ---------------------------
# Streamlit 本体
# ---------------------------
def main():
    st.set_page_config(APP_TITLE, page_icon="🚨", layout="wide")
    st.markdown(DRAMA_CSS, unsafe_allow_html=True)

    st.markdown(f"<h1 style='margin:0 0 8px 0;'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.caption("愛媛県全域に拡張。地図クリックで地点選択 → 「分析する」でスコア算出。Gemini説明は任意。")

    # セッションステート：選択地点・スナップ
    if "sel_lat" not in st.session_state: st.session_state.sel_lat = UEJIMA_LAT
    if "sel_lon" not in st.session_state: st.session_state.sel_lon = UEJIMA_LON
    if "last_snap" not in st.session_state: st.session_state.last_snap = None

    # サイドバー：設定
    with st.sidebar:
        st.markdown("### 設定")
        st.write("地図をクリックすると地点が選択されます。")
        # 参考表示（直接編集も可）
        st.session_state.sel_lat = st.number_input("選択緯度", value=float(st.session_state.sel_lat), format="%.6f")
        st.session_state.sel_lon = st.number_input("選択経度", value=float(st.session_state.sel_lon), format="%.6f")
        muni_hint = st.text_input("市町村名ヒント（任意・抽出に使用）", value=MUNICIPALITY_DEFAULT)
        run_gemini = st.toggle("Gemini 2.5 Flashで説明を生成（任意）", value=False)

        st.divider()
        st.markdown("#### データ検出")
        files = sorted(glob.glob(DATA_GLOB))
        if files:
            for fp in files:
                st.write("・", os.path.basename(fp))
        else:
            st.warning("データが見つかりませんでした: " + DATA_GLOB)

        st.divider()
        st.markdown("#### API キー状態")
        st.write(f"- WeatherAPI: {'✅' if WEATHERAPI_KEY else '—'}")
        st.write(f"- OpenWeather: {'✅' if OPENWEATHER_KEY else '—'}")
        st.write(f"- Gemini: {'✅' if GEMINI_KEY else '—'}")

    # 2019データ読み込み（初回のみ重い想定）
    @st.cache_data(show_spinner=False)
    def _load_2019():
        return load_all_crime_2019(DATA_GLOB)
    all_df = _load_2019()

    # 地図（クリックで地点更新）
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.markdown("**地図：クリックで任意地点を選択（ドラッグで微調整）**")
    fmap = render_map_selectable(st.session_state.sel_lat, st.session_state.sel_lon, st.session_state.last_snap)
    out = st_folium(fmap, height=520, returned_objects=["last_clicked", "last_object_clicked", "last_object_clicked_popup"])
    st.markdown("</div>", unsafe_allow_html=True)

    # クリック位置の反映
    if out and out.get("last_clicked"):
        lat = out["last_clicked"]["lat"]
        lon = out["last_clicked"]["lng"]
        # 愛媛の範囲チェック（緩め）
        if (EHIME_BBOX["min_lat"] <= lat <= EHIME_BBOX["max_lat"]) and (EHIME_BBOX["min_lon"] <= lon <= EHIME_BBOX["max_lon"]):
            st.session_state.sel_lat = float(lat)
            st.session_state.sel_lon = float(lon)
        else:
            st.warning("選択地点が愛媛県の想定範囲外です。")

    # 操作ボタン群
    colb1, colb2, colb3 = st.columns([1,1,2])
    with colb1:
        analyze = st.button("🔎 分析する", use_container_width=True)
    with colb2:
        reset = st.button("📍 上島町に戻す", use_container_width=True)

    if reset:
        st.session_state.sel_lat = UEJIMA_LAT
        st.session_state.sel_lon = UEJIMA_LON
        st.session_state.last_snap = None
        st.experimental_rerun()

    # 分析フロー
    if analyze:
        # 中央オーバーレイ + プログレスバー
        overlay = st.empty()
        overlay.markdown("""
            <div class="overlay">
              <div class="overlay-content">
                <div class="overlay-title">解析中</div>
                <div class="overlay-sub">気象・月齢・2019傾向を統合しています…</div>
                <div class="loader"></div>
              </div>
            </div>
        """, unsafe_allow_html=True)

        p = st.progress(0, text="準備中…")
        for i, txt in [(10,"気象/月齢の取得…"), (40,"時刻・曜日要因の評価…"),
                       (70,"2019年傾向の補正…"), (100,"スコア集計…")]:
            time.sleep(0.35)
            p.progress(i, text=txt)

        # 実計算
        now_dt = datetime.now(JST)
        weather = get_weather(st.session_state.sel_lat, st.session_state.sel_lon)
        snap = compute_risk_score(weather, now_dt, all_df)
        st.session_state.last_snap = snap

        # オーバーレイ除去
        overlay.empty()
        p.empty()

    # スナップがあれば表示
    snap = st.session_state.last_snap
    if snap:
        # 警報バー
        if snap["score"] >= 75:
            st.markdown(f"<div class='alert-bar'>警報：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。周囲に注意。</div>", unsafe_allow_html=True)
        elif snap["score"] >= 50:
            st.markdown(f"<div class='alert-bar' style='background:linear-gradient(90deg, rgba(200,120,0,0.85), rgba(255,170,0,0.85));'>注意：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。</div>", unsafe_allow_html=True)

        c1, c2 = st.columns([1,1])
        with c1:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("<div class='mute'>CURRENT RISK</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='score-big' style='color:{snap['color']};'>{int(round(snap['score']))}</div>", unsafe_allow_html=True)
            st.markdown(f"<span class='badge'>{snap['level']}</span>", unsafe_allow_html=True)
            st.write("")
            st.markdown(
                f"**月相**：{snap['moon_phase']}  /  **天候**：{snap['condition']}  /  "
                f"**気温**：{snap['temp_c']}℃  /  **降水**：{snap['precip_mm']}mm  /  **湿度**：{snap['humidity']}%",
                unsafe_allow_html=True
            )
            st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**内部理由（ヒューリスティック + 2019補正）**")
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
            fmap2 = render_map_selectable(st.session_state.sel_lat, st.session_state.sel_lon, snap)
            st_folium(fmap2, height=420, returned_objects=[])
            st.markdown("</div>", unsafe_allow_html=True)

            if run_gemini:
                with st.spinner("Gemini 2.5 Flash が説明を生成中..."):
                    msg = gemini_explain(snap, datetime.now(JST))
                st.markdown("<div class='card'>", unsafe_allow_html=True)
                st.markdown("**Gemini 2.5 Flash の見解（任意）**")
                st.write(msg if msg else "（Gemini未設定または生成失敗）")
                st.markdown("</div>", unsafe_allow_html=True)

    # フッター
    st.caption(
        "※ 満月効果は学術的に限定的です。気温・時間帯・人出・降雨などの複合要因で警戒度を表示します。"
        " 本アプリは2019年の愛媛県オープンデータを参考傾向として利用し、個人を特定する情報は扱いません。"
    )

if __name__ == "__main__":
    main()
