# -*- coding: utf-8 -*-
# ============================================================
# 上島町 周辺：犯罪者・異常者 警戒予測（Streamlit / 2019愛媛オープンデータ対応）
# - /mnt/data/ehime_2019*.csv を自動検出し、手口別CSVを統合
# - 日本語列の揺れを自動推定（発生日/市町村/手口）
# - 気象・月齢API（任意） + 過去傾向（2019）でスコア補正
# - マップ（Folium）表示、ドラマ風UI
# - Gemini 2.5 Flash（任意）で説明生成
# ============================================================

import os
import re
import glob
import json
from datetime import datetime, timedelta, timezone
from dateutil import tz
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
APP_TITLE = "上島町 警戒予測モニター（2019オープンデータ対応）"

# 既定座標：上島町役場（生名開発総合センター付近）
DEFAULT_LAT = 34.27717
DEFAULT_LON = 133.20986

DATA_GLOB = "/mnt/data/ehime_2019*.csv"  # 今回ご提供のCSV群
MUNICIPALITY_DEFAULT = "上島町"

# ---------------------------
# UI スタイル（ドラマ風）
# ---------------------------
DRAMA_CSS = """
<style>
.main, .stApp { background: #0b0f14; color: #e6eef7; }
section[data-testid="stSidebar"] { background: #0e141b; }

.score-big {
  font-size: 72px; font-weight: 900; letter-spacing: 1.5px;
  text-shadow: 0 0 8px rgba(255,0,0,0.25), 0 0 16px rgba(255,0,0,0.15);
  margin: 0; line-height: 1.0;
}
.badge {
  display: inline-block; padding: 4px 10px; border-radius: 12px; font-weight: 700;
  background: linear-gradient(135deg, #1c2633 0%, #121821 100%);
  border: 1px solid rgba(255,255,255,0.1); color:#d9e6f2; margin-left: 8px;
}
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
.card {
  background: linear-gradient(135deg, #121821, #0e141b);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px; padding: 16px 18px; margin: 8px 0;
  box-shadow: 0 8px 28px rgba(0,0,0,0.35);
}
button[kind="primary"], .stButton>button {
  background: linear-gradient(135deg, #143957, #0d2a41) !important;
  border: 1px solid rgba(255,255,255,0.15) !important;
  color: #e6eef7 !important; font-weight: 700 !important;
  border-radius: 12px !important;
}
.mute { color: #a8b7c7; font-size: 13px; }
</style>
"""

# ---------------------------
# Secrets / API Keys（任意）
# ---------------------------
WEATHERAPI_KEY = st.secrets.get("weatherapi", {}).get("api_key", "")
OPENWEATHER_KEY = st.secrets.get("openweather", {}).get("api_key", "")
GEMINI_KEY = st.secrets.get("gemini", {}).get("api_key", "")
GEMINI_MODEL = st.secrets.get("gemini", {}).get("model", "gemini-2.5-flash")
GEMINI_THINKING_BUDGET = int(st.secrets.get("gemini", {}).get("thinking_budget", 0))

# ---------------------------
# 文字コード推定＋CSV読込（堅牢）
# ---------------------------
def read_csv_robust(path: str) -> pd.DataFrame:
    """
    UTF-8 / CP932 などの揺れに対応。BOM も吸収。
    """
    with open(path, "rb") as f:
        raw = f.read()
    enc_guess = chardet.detect(raw).get("encoding") or "utf-8"
    for enc in (enc_guess, "utf-8-sig", "cp932", "shift_jis"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    # 最後の手段
    return pd.read_csv(path, encoding_errors="ignore")

# ---------------------------
# 列名の推定（発生日/市町村/手口）
# ---------------------------
def guess_columns(df: pd.DataFrame) -> dict:
    cols = {c: c for c in df.columns}
    lower_cols = {c: str(c).lower() for c in df.columns}

    # 発生日
    date_col = None
    for c in df.columns:
        n = str(c)
        if re.search(r"(発生|年月日|日付|日時)", n):
            date_col = c; break
    if not date_col:
        # 英数字系
        for c in df.columns:
            n = lower_cols[c]
            if any(k in n for k in ["date", "day", "time", "occur"]):
                date_col = c; break

    # 市町村
    muni_col = None
    for c in df.columns:
        n = str(c)
        if re.search(r"(市|町|村).*名", n):
            muni_col = c; break
        if re.search(r"(市町村|自治体|地域)", n):
            muni_col = c; break
    if not muni_col:
        for c in df.columns:
            n = lower_cols[c]
            if any(k in n for k in ["municipality", "city", "town", "area", "region"]):
                muni_col = c; break

    # 手口/罪種（無い場合は後でファイル名から補う）
    type_col = None
    for c in df.columns:
        n = str(c)
        if re.search(r"(手口|罪|罪種|種別|分類)", n):
            type_col = c; break
    if not type_col:
        for c in df.columns:
            n = lower_cols[c]
            if any(k in n for k in ["type", "category", "kind", "crime"]):
                type_col = c; break

    return {"date": date_col, "municipality": muni_col, "ctype": type_col}

# ---------------------------
# 日付の正規化
# ---------------------------
def parse_date_series(s: pd.Series) -> pd.Series:
    def parse_one(x):
        if pd.isna(x):
            return pd.NaT
        tx = str(x).strip()
        # yyyy/mm/dd, yyyy-mm-dd, 和暦や連番にも可能な限り対応
        # pandasのto_datetimeに丸投げ + errors='coerce'
        try:
            return pd.to_datetime(tx, errors="coerce")
        except Exception:
            return pd.NaT
    return s.apply(parse_one)

# ---------------------------
# 2019 CSV群の統合
# ---------------------------
def load_all_crime_2019(data_glob: str = DATA_GLOB) -> pd.DataFrame | None:
    files = sorted(glob.glob(data_glob))
    if not files:
        return None

    frames = []
    for fp in files:
        df = read_csv_robust(fp)
        guessed = guess_columns(df)

        # 列を標準化
        if guessed["date"] is None:
            # 最低限、行番号から2019-01-01起点で擬似日付付与（苦肉）
            df["date"] = pd.Timestamp("2019-01-01") + pd.to_timedelta(range(len(df)), unit="D")
        else:
            df.rename(columns={guessed["date"]: "date"}, inplace=True)
            df["date"] = parse_date_series(df["date"])

        if guessed["municipality"] is None:
            # 市町村不明は空
            df["municipality"] = ""
        else:
            df.rename(columns={guessed["municipality"]: "municipality"}, inplace=True)
            df["municipality"] = df["municipality"].astype(str)

        # 手口：列なければファイル名から推定
        if guessed["ctype"] is None:
            base = os.path.basename(fp)
            # ファイル名からざっくり和名に
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
            df.rename(columns={guessed["ctype"]: "ctype"}, inplace=True)
            df["ctype"] = df["ctype"].astype(str)

        # 2019年だけに限定（CSV自体が2019だが念のため）
        df = df[(df["date"].dt.year == 2019) | (df["date"].isna())]
        frames.append(df[["date", "municipality", "ctype"]].copy())

    if not frames:
        return None

    all_df = pd.concat(frames, ignore_index=True)
    # 欠損は落としすぎない
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
        r_ast = requests.get(f"{base}/astronomy.json?{common}", timeout=10)
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
# スコア計算（2019傾向の補正強化版）
# ---------------------------
def compute_risk_score(
    weather: dict,
    now_dt: datetime,
    all_df: pd.DataFrame | None,
    municipality_hint: str = MUNICIPALITY_DEFAULT
) -> dict:
    """0-100 スコア。気温、降雨、時間帯、週末、月齢 + 2019傾向で補正"""
    score = 0.0
    reasons = []

    temp = float(weather.get("temp_c", 20.0))
    precip = float(weather.get("precip_mm", 0.0))
    humidity = float(weather.get("humidity", 60))
    cond = str(weather.get("condition", ""))
    moon = weather.get("moon_phase")

    # 気温（重み大）
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

    # 降雨（減点）
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

    # 湿度（僅かに加点）
    if humidity >= 80:
        score += 3; reasons.append("高湿度:+3")

    # 2019年の発生傾向による補正（上島町に近い市町村含む）
    if all_df is not None and not all_df.empty:
        df = all_df.copy()
        # 上島町 or 島しょ部を想起する “島/上島/伯方/弓削/因島/生名 など” をゆるくヒット
        hint_pattern = r"(上島|上島町|弓削|生名|魚島|岩城|伯方|因島|大三島|今治|越智|島)"
        msk = df["municipality"].astype(str).str.contains(hint_pattern, na=False)
        sub = df[msk] if msk.any() else df

        # 月別・曜日別の粗い偏り
        sub["month"] = sub["date"].dt.month
        month = now_dt.month
        month_count = len(sub[sub["month"] == month])
        total = max(1, len(sub))
        month_ratio = month_count / total

        # 7-9月が相対的に高いなら加点
        if month_ratio >= 0.12:
            score += 6; reasons.append("2019傾向(同月比 多め):+6")
        elif month_ratio >= 0.08:
            score += 3; reasons.append("2019傾向(同月比 やや多め):+3")

        # 手口偏り（ひったくり/車上ねらい/自転車盗 等）
        if "ctype" in sub.columns:
            top_types = sub["ctype"].value_counts(normalize=True)
            # 屋外系が多い→夜間・週末シナジー
            outdoor_like = 0.0
            for k in ["ひったくり", "車上ねらい", "自転車盗", "オートバイ盗"]:
                outdoor_like += float(top_types.get(k, 0))
            if outdoor_like >= 0.45:
                score += 5; reasons.append("2019傾向(屋外系手口が多い):+5")
            elif outdoor_like >= 0.30:
                score += 2; reasons.append("2019傾向(屋外系やや多い):+2")

    # クリップ & レベル
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
                "あなたは防犯アナリストAIです。与えられた要因から、上島町周辺の犯罪/異常行動リスクの"
                "根拠を、過度な断定を避けて簡潔に日本語で説明してください。"
                "満月効果は限定的、気温・時間帯・降雨などの影響は統計的示唆あり、を前提に、"
                "誇張表現は避け、最後に取るべき具体的行動（人通りの少ない場所を避ける等）で締めること。"
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
# マップ描画（Folium）
# ---------------------------
def render_map(lat: float, lon: float, snap: dict):
    m = folium.Map(location=[lat, lon], zoom_start=12, tiles="cartodb dark_matter")
    folium.Circle(
        location=[lat, lon],
        radius=1500 if snap["score"] < 50 else (2500 if snap["score"] < 75 else 3500),
        color=snap["color"], fill=True, fill_opacity=0.25, weight=2
    ).add_to(m)

    popup_html = f"""
    <div style="color:#fff;">
      <b>警戒度:</b> {snap['score']} ({snap['level']})<br/>
      <b>月相:</b> {snap['moon_phase']}<br/>
      <b>天候:</b> {snap['condition']} / {snap['temp_c']}℃ / 降水{snap['precip_mm']}mm
    </div>
    """
    folium.Marker(
        [lat, lon],
        popup=folium.Popup(popup_html, max_width=280),
        icon=folium.Icon(color="red" if snap["score"]>=75 else "orange" if snap["score"]>=50 else "blue", icon="info-sign")
    ).add_to(m)
    return m

# ---------------------------
# Streamlit アプリ本体
# ---------------------------
def main():
    st.set_page_config(APP_TITLE, page_icon="🚨", layout="wide")
    st.markdown(DRAMA_CSS, unsafe_allow_html=True)

    st.markdown(f"<h1 style='margin:0 0 8px 0;'>{APP_TITLE}</h1>", unsafe_allow_html=True)
    st.caption("2019年 愛媛県オープンデータ（手口別CSV）を自動統合。月齢/気象/時間帯/週末+2019傾向でスコア推定。Geminiは任意。")

    # サイドバー：設定
    with st.sidebar:
        st.markdown("### 設定")
        lat = st.number_input("緯度 (Lat)", value=DEFAULT_LAT, format="%.6f")
        lon = st.number_input("経度 (Lon)", value=DEFAULT_LON, format="%.6f")
        muni_hint = st.text_input("市町村名ヒント（抽出ワード）", value=MUNICIPALITY_DEFAULT)
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

    # データ読込
    all_df = load_all_crime_2019(DATA_GLOB)

    # 現在情報
    now_dt = datetime.now(JST)
    weather = get_weather(lat, lon)

    # スコア
    snap = compute_risk_score(weather, now_dt, all_df, municipality_hint=muni_hint)

    # 警報バー
    if snap["score"] >= 75:
        st.markdown(f"<div class='alert-bar'>警報：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。周囲に注意。</div>", unsafe_allow_html=True)
    elif snap["score"] >= 50:
        st.markdown(f"<div class='alert-bar' style='background:linear-gradient(90deg, rgba(200,120,0,0.85), rgba(255,170,0,0.85));'>注意：現在の警戒レベル <b>{snap['level']}</b>（{snap['score']}）。</div>", unsafe_allow_html=True)

    # 上段：スコア/要因
    c1, c2 = st.columns([1,1])
    with c1:
        st.markdown("<div class='card'>", unsafe_allow_html=True)
        st.markown = st.markdown  # alias
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
        if snap["reasons"]:
            for r in snap["reasons"]:
                st.write("・", r)
        else:
            st.write("—")
        st.markdown("</div>", unsafe_allow_html=True)

        if all_df is not None and not all_df.empty:
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("**2019データ概要（検出件数）**")
            st.write(f"- 総件数: {len(all_df)}")
            vc = all_df["ctype"].value_counts()
            st.dataframe(vc.rename("件数").to_frame())
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
            st.write(msg if msg else "（Gemini未設定または生成失敗）")
            st.markdown("</div>", unsafe_allow_html=True)

    # フッター
    st.caption(
        "※ 満月効果は学術的に限定的です。気温・時間帯・人出・降雨などの複合要因で警戒度を表示します。"
        " 本アプリは2019年の愛媛県オープンデータを参考傾向として利用し、個人を特定する情報は扱いません。"
    )

if __name__ == "__main__":
    main()
