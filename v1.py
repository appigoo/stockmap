import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import os

st.set_page_config(
    page_title="Market Pulse · 市場熱力圖",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={"Get Help": None, "Report a bug": None, "About": None}
)

# ── GLOBAL STYLES ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');

  /* ── Base reset ── */
  html, body, [class*="css"] {
    font-family: 'Syne', sans-serif;
    background: #0a0c10 !important;
    color: #c8ccd6 !important;
  }

  /* ── Main container ── */
  .main .block-container {
    background: #0a0c10 !important;
    padding: 2rem 2.5rem !important;
    max-width: 1600px !important;
  }

  /* ── Sidebar ── */
  [data-testid="stSidebar"] {
    background: #0d1017 !important;
    border-right: 1px solid #1e2530 !important;
  }
  [data-testid="stSidebar"] * { color: #8b95a5 !important; }
  [data-testid="stSidebar"] label { font-size: 0.72rem !important; letter-spacing: .12em !important; text-transform: uppercase !important; color: #556070 !important; }
  [data-testid="stSidebar"] input,
  [data-testid="stSidebar"] .stTextInput > div > div {
    background: #13181f !important;
    border: 1px solid #1e2530 !important;
    border-radius: 4px !important;
    color: #c8ccd6 !important;
    font-family: 'Space Mono', monospace !important;
  }

  /* ── Hero header ── */
  .hero-wrap {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    border-bottom: 1px solid #1a2030;
    padding-bottom: 1.4rem;
    margin-bottom: 2rem;
  }
  .hero-title {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 2.6rem;
    letter-spacing: -0.03em;
    line-height: 1;
    color: #e8ecf2 !important;
  }
  .hero-title span { color: #4af0a0; }
  .hero-sub {
    font-family: 'Space Mono', monospace;
    font-size: 0.72rem;
    color: #3d4d5c;
    letter-spacing: 0.1em;
    margin-top: 0.5rem;
  }
  .hero-badge {
    font-family: 'Space Mono', monospace;
    font-size: 0.68rem;
    background: #0f1a12;
    border: 1px solid #1d3d28;
    color: #4af0a0;
    padding: 4px 10px;
    border-radius: 3px;
    letter-spacing: .1em;
  }

  /* ── Stat chips ── */
  .stat-row { display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }
  .stat-chip {
    background: #0e1319;
    border: 1px solid #1a2230;
    border-radius: 6px;
    padding: 0.8rem 1.2rem;
    flex: 1;
    min-width: 120px;
  }
  .stat-label {
    font-family: 'Space Mono', monospace;
    font-size: 0.62rem;
    color: #3d4d5c;
    letter-spacing: .12em;
    text-transform: uppercase;
    margin-bottom: 4px;
  }
  .stat-value {
    font-family: 'Space Mono', monospace;
    font-size: 1.35rem;
    font-weight: 700;
    color: #e8ecf2;
  }
  .stat-up   { color: #4af0a0 !important; }
  .stat-down { color: #f05a4a !important; }

  /* ── Primary button ── */
  [data-testid="stButton"] > button {
    background: #4af0a0 !important;
    color: #050709 !important;
    border: none !important;
    border-radius: 4px !important;
    font-family: 'Space Mono', monospace !important;
    font-size: 0.78rem !important;
    font-weight: 700 !important;
    letter-spacing: .12em !important;
    text-transform: uppercase !important;
    padding: 0.6rem 1.8rem !important;
    transition: all .15s ease !important;
  }
  [data-testid="stButton"] > button:hover {
    background: #62f5ae !important;
    box-shadow: 0 0 18px rgba(74,240,160,.35) !important;
  }

  /* ── Download button ── */
  [data-testid="stDownloadButton"] > button {
    background: transparent !important;
    border: 1px solid #1e2d20 !important;
    color: #4af0a0 !important;
    font-family: 'Space Mono', monospace !important;
    font-size: 0.72rem !important;
    letter-spacing: .1em !important;
    border-radius: 4px !important;
    padding: 0.5rem 1.4rem !important;
  }
  [data-testid="stDownloadButton"] > button:hover {
    border-color: #4af0a0 !important;
    background: rgba(74,240,160,.06) !important;
  }

  /* ── Dataframe ── */
  [data-testid="stDataFrame"] {
    border: 1px solid #1a2230 !important;
    border-radius: 8px !important;
    overflow: hidden !important;
  }
  [data-testid="stDataFrame"] iframe {
    background: #0a0c10 !important;
  }

  /* ── Section title ── */
  .section-title {
    font-family: 'Space Mono', monospace;
    font-size: 0.68rem;
    letter-spacing: .18em;
    text-transform: uppercase;
    color: #3d4d5c;
    margin-bottom: .8rem;
  }

  /* ── Spinner ── */
  [data-testid="stSpinner"] { color: #4af0a0 !important; }

  /* ── Alert / info ── */
  [data-testid="stAlert"] {
    background: #0d1017 !important;
    border: 1px solid #1a2230 !important;
    border-radius: 6px !important;
    color: #8b95a5 !important;
    font-family: 'Space Mono', monospace !important;
    font-size: .78rem !important;
  }

  /* ── Success ── */
  .stSuccess {
    background: #0f1a12 !important;
    border: 1px solid #1d3d28 !important;
    color: #4af0a0 !important;
    font-family: 'Space Mono', monospace !important;
    font-size: .72rem !important;
    border-radius: 4px !important;
  }

  /* ── Footer ── */
  .footer-bar {
    margin-top: 3rem;
    padding-top: 1rem;
    border-top: 1px solid #111820;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .footer-text {
    font-family: 'Space Mono', monospace;
    font-size: 0.62rem;
    color: #2a3540;
    letter-spacing: .08em;
  }
  .dot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: #4af0a0; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse {
    0%,100% { opacity:1; box-shadow: 0 0 0 0 rgba(74,240,160,.4); }
    50%      { opacity:.7; box-shadow: 0 0 0 5px rgba(74,240,160,0); }
  }

  /* heatmap-wrap container */
  .heatmap-wrap {
    overflow-x: auto;
    border: 1px solid #141c28;
    border-radius: 10px;
    box-shadow: 0 4px 40px rgba(0,0,0,.6), 0 0 0 1px #0d1522;
    background: #0c0f14;
    margin-top: .5rem;
  }
  .heatmap-wrap table { margin: 0 !important; }
  .heatmap-wrap::-webkit-scrollbar { height: 6px; }
  .heatmap-wrap::-webkit-scrollbar-track { background: #080b0f; }
  .heatmap-wrap::-webkit-scrollbar-thumb { background: #1e2d40; border-radius: 3px; }
  .heatmap-wrap::-webkit-scrollbar-thumb:hover { background: #2e4060; }

  /* ── Hide Streamlit chrome ── */
  #MainMenu, footer, header { visibility: hidden; }
  .reportview-container .main footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── ASSETS ─────────────────────────────────────────────────────────────────────
ASSETS = {
    "TESLA":            "TSLA",
    "APPLE":            "AAPL",
    "NVIDIA":           "NVDA",
    "GOOGLE":           "GOOGL",
    "META":             "META",
    "MSFT":             "MSFT",
    "AMAZON":           "AMZN",
    "NIO":              "NIO",
    "XPEV":             "XPEV",
    "美元指數":          "DX-Y.NYB",
    "2年美債收益率":      "^IRX",
    "10年美債收益率":     "^TNX",
    "TLT(長期美債ETF)":  "TLT",
    "S&P500":           "^GSPC",
    "那斯達克":          "^IXIC",
    "道瓊工業":          "^DJI",
    "羅素2000":          "^RUT",
    "VIX(恐慌指數)":     "^VIX",
    "黃金期貨":          "GC=F",
    "WTI原油":           "CL=F",
    "REITs指數ETF":      "VNQ",
    "科技ETF":           "XLK",
    "醫療ETF":           "XLV",
    "金融ETF":           "XLF",
    "能源ETF":           "XLE",
    "非必需消費ETF":      "XLY",
    "公用事業ETF":        "XLU",
    "必需消費ETF":        "XLP",
    "標普成長ETF":        "SPYG",
    "標普價值ETF":        "SPYV",
    "標普500 ETF":        "SPY",
    "那斯達克100(大型科技)": "QQQ",
}

# ── SIDEBAR ─────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="font-family:'Space Mono',monospace;font-size:.62rem;
                letter-spacing:.18em;text-transform:uppercase;
                color:#3d4d5c;margin-bottom:1.4rem;padding-bottom:.8rem;
                border-bottom:1px solid #1a2230;">
        ⚙ 設定
    </div>
    """, unsafe_allow_html=True)

    start_of_fetch = st.date_input(
        "最早抓取日期",
        value=date.today() - timedelta(days=400),
        min_value=date(2000, 1, 1),
        max_value=date.today(),
    )
    save_folder = st.text_input("CSV 存放資料夾", value="data")
    os.makedirs(save_folder, exist_ok=True)

    st.markdown("""
    <div style="margin-top:2rem;font-family:'Space Mono',monospace;
                font-size:.6rem;color:#2a3540;line-height:1.8;">
      資料來源：Yahoo Finance<br>
      時間基準：美東時間 (ET)<br>
      快取：每小時更新
    </div>
    """, unsafe_allow_html=True)


# ── HERO ────────────────────────────────────────────────────────────────────────
today_str = date.today().strftime("%Y · %m · %d")
st.markdown(f"""
<div class="hero-wrap">
  <div>
    <div class="hero-title">Market <span>Pulse</span></div>
    <div class="hero-sub">GLOBAL ASSET HEATMAP · {today_str} · DATA: YAHOO FINANCE</div>
  </div>
  <div class="hero-badge"><span class="dot"></span>LIVE DATA</div>
</div>
""", unsafe_allow_html=True)


# ── TIMEZONE UTIL ───────────────────────────────────────────────────────────────
NY_TZ = 'America/New_York'

def to_ny_aware(dt):
    if dt is None:
        return None
    try:
        ts = pd.to_datetime(dt)
        return ts.tz_localize(NY_TZ) if ts.tz is None else ts.tz_convert(NY_TZ)
    except Exception as e:
        st.warning(f"時間轉換錯誤: {e}")
        return None


# ── DATA FETCH ──────────────────────────────────────────────────────────────────
import time, random

def _fetch_one(tk, start_date, end_date, retries=4):
    """Fetch single ticker with exponential back-off on rate-limit errors."""
    for attempt in range(retries):
        try:
            hist = yf.Ticker(tk).history(
                start=start_date, end=end_date, raise_errors=False
            )
            return hist
        except Exception as e:
            msg = str(e).lower()
            if "too many requests" in msg or "rate limit" in msg or "429" in msg:
                wait = (2 ** attempt) + random.uniform(0.5, 2.0)
                time.sleep(wait)
            else:
                raise
    return pd.DataFrame()   # give up after retries


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_history(tickers, start_date):
    data = {}
    start_ny = to_ny_aware(start_date)
    end_ny   = to_ny_aware(date.today() + timedelta(days=1))
    s = start_ny.date() if start_ny else None
    e = end_ny.date()   if end_ny   else None

    for i, tk in enumerate(tickers):
        # polite delay between every request (0.3-0.8 s)
        if i > 0:
            time.sleep(random.uniform(0.3, 0.8))
        try:
            hist = _fetch_one(tk, s, e)
            if not hist.empty:
                hist.index = (hist.index.tz_localize(NY_TZ)
                              if hist.index.tz is None
                              else hist.index.tz_convert(NY_TZ))
            data[tk] = hist
        except Exception as ex:
            st.warning(f"抓取 {tk} 失敗：{ex}")
            data[tk] = pd.DataFrame()
    return data


def nearest_price(hist_df, target_date):
    if hist_df is None or hist_df.empty:
        return np.nan
    target = to_ny_aware(target_date)
    if target is None:
        return np.nan
    idx = (hist_df.index.tz_localize(NY_TZ)
           if hist_df.index.tz is None
           else hist_df.index.tz_convert(NY_TZ))
    past = idx[idx <= target]
    return hist_df.loc[past.max(), "Close"] if not past.empty else np.nan


def quarter_start(today):
    q = (today.month - 1) // 3 + 1
    return date(today.year, 3*(q-1)+1, 1)


def compute_changes(hist_map):
    rows   = []
    today  = date.today()
    targets = {
        "1日":  today - timedelta(days=1),
        "1週":  today - timedelta(days=7),
        "1月":  today - timedelta(days=30),
        "1年":  today - timedelta(days=365),
        "QTD":  quarter_start(today),
        "YTD":  date(today.year, 1, 1),
    }
    for name, tk in ASSETS.items():
        hist   = hist_map.get(tk, pd.DataFrame())
        latest = hist["Close"].iloc[-1] if (not hist.empty and "Close" in hist.columns) else np.nan
        changes = {}
        for label, base_date in targets.items():
            prev = nearest_price(hist, base_date)
            changes[label] = (
                round((latest / prev - 1) * 100, 2)
                if pd.notna(latest) and pd.notna(prev) and prev != 0
                else np.nan
            )
        rows.append({
            "資產": name, "Ticker": tk,
            "收盤": round(latest, 4) if pd.notna(latest) else np.nan,
            **{k: changes[k] for k in targets},
        })
    return pd.DataFrame(rows).set_index("資產")


# ── COLOURING ───────────────────────────────────────────────────────────────────
def colorize(val):
    if pd.isna(val):
        return "background-color:#0e1319; color:#2a3540;"
    v = float(val)
    if v > 0:
        intensity = min(abs(v) / 15, 1)
        alpha     = 0.08 + 0.28 * intensity
        return (f"background-color:rgba(74,240,160,{alpha:.2f});"
                f"color:#4af0a0;font-weight:700;"
                f"font-family:'Space Mono',monospace;")
    elif v < 0:
        intensity = min(abs(v) / 15, 1)
        alpha     = 0.08 + 0.28 * intensity
        return (f"background-color:rgba(240,90,74,{alpha:.2f});"
                f"color:#f05a4a;font-weight:700;"
                f"font-family:'Space Mono',monospace;")
    return "background-color:#0e1319; color:#556070;"


# ── SUMMARY CHIPS ────────────────────────────────────────────────────────────────
def render_chips(df):
    style_cols = ["1日", "1週", "1月", "1年", "QTD", "YTD"]
    chips_html = '<div class="stat-row">'
    for col in style_cols:
        vals    = df[col].dropna()
        adv     = (vals > 0).sum()
        dec     = (vals < 0).sum()
        avg     = vals.mean()
        sign    = "stat-up" if avg >= 0 else "stat-down"
        prefix  = "+" if avg >= 0 else ""
        chips_html += f"""
        <div class="stat-chip">
          <div class="stat-label">{col} 均漲跌</div>
          <div class="stat-value {sign}">{prefix}{avg:.2f}%</div>
          <div style="font-family:'Space Mono',monospace;font-size:.62rem;
                      color:#3d4d5c;margin-top:4px;">
            ▲{adv} &nbsp; ▼{dec}
          </div>
        </div>"""
    chips_html += "</div>"
    st.markdown(chips_html, unsafe_allow_html=True)


# ── MAIN ─────────────────────────────────────────────────────────────────────────
col_btn, col_dl = st.columns([1, 5])

with col_btn:
    run = st.button("▶  生成熱力圖", type="primary")

if run:
    with st.spinner("拉取市場資料中…"):
        hist_map = fetch_history(list(ASSETS.values()), start_of_fetch)
        df       = compute_changes(hist_map)

    # summary chips
    render_chips(df)

    # ── build pure HTML table ──
    style_cols = ["1日", "1週", "1月", "1年", "QTD", "YTD"]
    all_cols   = ["Ticker", "收盤"] + style_cols

    def cell_style(val, col):
        base = "padding:10px 18px;border-bottom:1px solid #0d1018;border-right:1px solid #0d1018;white-space:nowrap;font-family:'Space Mono',monospace;font-size:0.8rem;"
        if col in style_cols:
            if pd.isna(val):
                return base + "color:#2a3540;text-align:center;"
            v = float(val)
            if v > 0:
                intensity = min(abs(v)/15, 1)
                a = 0.08 + 0.28*intensity
                return base + f"background:rgba(74,240,160,{a:.2f});color:#4af0a0;font-weight:700;text-align:center;"
            elif v < 0:
                intensity = min(abs(v)/15, 1)
                a = 0.08 + 0.28*intensity
                return base + f"background:rgba(240,90,74,{a:.2f});color:#f05a4a;font-weight:700;text-align:center;"
            return base + "color:#556070;text-align:center;"
        if col == "Ticker":
            return base + "color:#dde2ea;font-weight:700;font-family:'Syne',sans-serif;font-size:0.88rem;"
        if col == "收盤":
            return base + "color:#c8ccd6;text-align:right;"
        return base + "color:#8b95a5;"

    def fmt(val, col):
        if pd.isna(val): return "—"
        if col == "收盤":  return f"{val:.4f}"
        if col in style_cols: return f"{val:+.2f}%"
        return str(val)

    rows_html = ""
    for i, (asset, row) in enumerate(df.iterrows()):
        bg = "#0a0d12" if i % 2 == 1 else "#0c0f14"
        # index cell (asset name)
        idx_style = (f"padding:10px 18px;background:{bg};color:#c8ccd6;"
                     "font-family:'Syne',sans-serif;font-size:0.84rem;font-weight:600;"
                     "border-bottom:1px solid #0d1018;border-right:2px solid #1a2535;"
                     "white-space:nowrap;min-width:150px;")
        cells = f'<td style="{idx_style}">{asset}</td>'
        for col in all_cols:
            val = row[col]
            s   = cell_style(val, col)
            s  += f"background-color:{bg};" if col not in style_cols or pd.isna(val) or float(val if not pd.isna(val) else 0) == 0 else ""
            cells += f'<td style="{s}">{fmt(val, col)}</td>'
        rows_html += f'<tr style="background:{bg};">{cells}</tr>\n'

    th_style = ("background:#080b0f;color:#3d5060;font-family:'Space Mono',monospace;"
                "font-size:0.6rem;letter-spacing:.15em;text-transform:uppercase;"
                "padding:13px 18px;border-bottom:2px solid #1a2535;"
                "border-right:1px solid #0f1620;white-space:nowrap;")
    th_idx   = th_style + "min-width:150px;border-right:2px solid #1a2535;"
    headers  = f'<th style="{th_idx}">資產</th>'
    for col in all_cols:
        headers += f'<th style="{th_style}">{col}</th>'

    table_html = f"""
    <div class="heatmap-wrap">
      <table style="border-collapse:collapse;width:100%;table-layout:auto;">
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """

    st.markdown('<div class="section-title">資產表現一覽</div>', unsafe_allow_html=True)
    st.markdown(table_html, unsafe_allow_html=True)

    # save & download
    csv_file = f"market_heatmap_{date.today().isoformat()}.csv"
    csv_path = os.path.join(save_folder, csv_file)
    df.to_csv(csv_path, encoding="utf-8-sig", float_format="%.4f")
    st.success(f"✓ 已儲存至 `{csv_path}`")

    with open(csv_path, "rb") as f:
        col_btn.download_button("↓ 下載 CSV", f, csv_file, "text/csv")

else:
    st.markdown("""
    <div style="text-align:center;padding:5rem 0;color:#1e2a38;">
      <div style="font-size:3rem;margin-bottom:1rem;">◈</div>
      <div style="font-family:'Space Mono',monospace;font-size:.75rem;
                  letter-spacing:.2em;text-transform:uppercase;">
        點擊按鈕生成熱力圖
      </div>
    </div>
    """, unsafe_allow_html=True)


# ── FOOTER ───────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="footer-bar">
  <div class="footer-text">綠漲 · 紅跌 · 資料來源：Yahoo Finance · 快取 1 小時</div>
  <div class="footer-text">{date.today().strftime("%Y-%m-%d")} · 美東時間基準</div>
</div>
""", unsafe_allow_html=True)
