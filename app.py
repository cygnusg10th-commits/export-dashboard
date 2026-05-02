"""
수출 통계 모니터링 대시보드
실행: streamlit run app.py
"""
import sqlite3
import subprocess
import sys
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

DB_PATH      = Path(__file__).parent / "export_data.db"
PARSER_PATH  = Path(__file__).parent / "parser.py"
VISITOR_DB   = Path(__file__).parent / "visitors.db"

# 로컬 환경 여부 (Excel 파싱 기능 활성화 조건)
IS_LOCAL = Path(r"C:\수출입 통계").exists()

st.set_page_config(
    page_title="수출 통계 대시보드",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="metric-container"] {
    background: #1a1f2e; border-radius: 8px; padding: 12px;
}
.stMetric label { font-size: 12px; color: #718096; }
div[data-testid="stSidebarContent"] { padding-top: 1rem; }

/* Streamlit 하단 브랜딩 완전 제거 */
#MainMenu                              { visibility: hidden !important; }
footer                                 { visibility: hidden !important; }
header                                 { visibility: hidden !important; }
[data-testid="stToolbar"]              { display: none !important; }
[data-testid="stDecoration"]           { display: none !important; }
[data-testid="stStatusWidget"]         { display: none !important; }
[data-testid="manage-app-button"]      { display: none !important; }
[data-testid="stAppViewerBadge"]       { display: none !important; }
[class*="viewerBadge"]                 { display: none !important; }
[class*="styles_viewerBadge"]          { display: none !important; }
[class*="badge_container"]             { display: none !important; }
a[href="https://streamlit.io/cloud"]   { display: none !important; }
a[href*="streamlit.io"]                { display: none !important; }
</style>
""", unsafe_allow_html=True)

COLORS = dict(
    blue="#2b6cb0", orange="#f6ad55", purple="#9f7aea",
    teal="#38b2ac", up="#48bb78", down="#fc8181", gray="#718096",
)
PALETTE = px.colors.qualitative.Set2 + px.colors.qualitative.Pastel
PLOT_BG = dict(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")


# ─── DB 헬퍼 ─────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=300, show_spinner=False)
def load_items() -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql("SELECT * FROM items ORDER BY sheet_name", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_summary() -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql("""
        WITH latest AS (
            SELECT sheet_name, MAX(period) AS period
            FROM export_data GROUP BY sheet_name
        )
        SELECT e.sheet_name, e.period,
               e.export_dollar, e.export_avg,
               e.mom, e.yoy,
               e.unit_price, e.price_yoy, e.weight_kg,
               i.company, i.hs_code, i.source_file
        FROM export_data e
        JOIN latest l USING (sheet_name)
        JOIN items   i USING (sheet_name)
        WHERE e.period = l.period
        ORDER BY e.sheet_name
    """, conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_item_data(sheet_name: str, months: int = 60) -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    df = pd.read_sql("""
        SELECT period, export_avg, mom, yoy,
               unit_price, price_yoy,
               export_dollar, export_won, weight_kg
        FROM export_data
        WHERE sheet_name = ?
        ORDER BY period DESC LIMIT ?
    """, conn, params=(sheet_name, months))
    return df.sort_values("period").reset_index(drop=True)


@st.cache_data(ttl=300, show_spinner=False)
def load_heatmap_data(n_months: int = 18, n_items: int = 40) -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql(f"""
        WITH top_items AS (
            SELECT sheet_name FROM (
                SELECT e.sheet_name, e.export_dollar,
                       ROW_NUMBER() OVER (PARTITION BY e.sheet_name ORDER BY e.period DESC) rn
                FROM export_data e
            ) WHERE rn = 1
            ORDER BY export_dollar DESC NULLS LAST
            LIMIT {n_items}
        ),
        recent AS (
            SELECT DISTINCT period FROM export_data
            ORDER BY period DESC LIMIT {n_months}
        )
        SELECT e.sheet_name, e.period, e.mom, e.yoy, e.export_dollar
        FROM export_data e
        JOIN top_items USING (sheet_name)
        JOIN recent    USING (period)
        ORDER BY e.sheet_name, e.period
    """, conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_aggregate_monthly() -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql("""
        SELECT period,
               SUM(export_dollar) AS total_dollar,
               AVG(mom)           AS avg_mom,
               AVG(yoy)           AS avg_yoy,
               COUNT(*)           AS item_count
        FROM export_data
        GROUP BY period
        ORDER BY period
    """, conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_4m_growth() -> pd.DataFrame:
    """각 품목의 최신월 vs 4개월 전 수출액 비교로 성장률 산출"""
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql("""
        WITH ranked AS (
            SELECT sheet_name, period, export_dollar, export_avg,
                   mom, yoy, unit_price,
                   ROW_NUMBER() OVER (PARTITION BY sheet_name ORDER BY period DESC) rn
            FROM export_data
        ),
        latest  AS (SELECT sheet_name, period AS latest_period,
                           export_dollar AS latest_dollar, export_avg AS latest_avg,
                           mom AS latest_mom, yoy AS latest_yoy, unit_price AS latest_price
                    FROM ranked WHERE rn = 1),
        four_mo AS (SELECT sheet_name, period AS period_4mo,
                           export_dollar AS dollar_4mo
                    FROM ranked WHERE rn = 4)
        SELECT l.sheet_name, l.latest_period, l.latest_dollar, l.latest_avg,
               l.latest_mom, l.latest_yoy, l.latest_price,
               f.period_4mo, f.dollar_4mo,
               CASE WHEN f.dollar_4mo > 0
                    THEN (l.latest_dollar - f.dollar_4mo) / f.dollar_4mo
                    ELSE NULL END AS growth_4m,
               i.company, i.hs_code
        FROM latest l
        LEFT JOIN four_mo f USING (sheet_name)
        LEFT JOIN items   i USING (sheet_name)
        WHERE l.latest_dollar IS NOT NULL
        ORDER BY growth_4m DESC NULLS LAST
    """, conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_recent_all(months: int = 7) -> pd.DataFrame:
    """전체 품목 최근 N개월 데이터 (4개월 성장 분석용 추이 차트)"""
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    return pd.read_sql(f"""
        WITH recent AS (
            SELECT DISTINCT period FROM export_data
            ORDER BY period DESC LIMIT {months}
        )
        SELECT e.sheet_name, e.period, e.export_dollar, e.mom, e.yoy, e.unit_price
        FROM export_data e
        JOIN recent USING (period)
        ORDER BY e.sheet_name, e.period
    """, conn)


def load_last_parse() -> str:
    conn = get_conn()
    if conn is None:
        return ""
    row = conn.execute(
        "SELECT parsed_at, file_path FROM parse_log ORDER BY parsed_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return ""
    dt = row[0][:16].replace("T", " ")
    fn = Path(row[1]).name if row[1] else ""
    return f"{dt}  |  {fn}"


# ─── 방문자 카운터 ────────────────────────────────────────────────────────────

def _visitor_conn():
    conn = sqlite3.connect(str(VISITOR_DB), check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS visits (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            visited_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def increment_visitor():
    """세션 당 1회만 카운트"""
    if st.session_state.get("_visited"):
        return
    try:
        conn = _visitor_conn()
        conn.execute("INSERT INTO visits(visited_at) VALUES(?)",
                     (datetime.now().isoformat(),))
        conn.commit()
        conn.close()
    except Exception:
        pass
    st.session_state["_visited"] = True


def get_visitor_count() -> int:
    try:
        conn = _visitor_conn()
        n = conn.execute("SELECT COUNT(*) FROM visits").fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0


# ─── 포맷터 ──────────────────────────────────────────────────────────────────

def fmt_pct(v, nd=1) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v*100:.{nd}f}%"


def fmt_dollar(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    if abs(v) >= 1e9:
        return f"${v/1e9:.2f}B"
    if abs(v) >= 1e6:
        return f"${v/1e6:.1f}M"
    return f"${v:,.0f}"


def mom_icon(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "⚪"
    return "🟢" if v > 0 else "🔴"


def run_parser(force: bool = False):
    args = [sys.executable, str(PARSER_PATH)]
    if force:
        args.append("--force")
    subprocess.run(args, capture_output=True, text=True)
    for fn in [get_conn, load_items, load_summary, load_item_data,
               load_heatmap_data, load_aggregate_monthly,
               load_4m_growth, load_recent_all]:
        fn.clear()


# ─── 사이드바 ─────────────────────────────────────────────────────────────────

increment_visitor()

with st.sidebar:
    st.markdown("## 📦 수출 통계")

    # 방문자 카운터
    visitors = get_visitor_count()
    st.markdown(
        f"<div style='text-align:center; padding:6px 0 2px;'>"
        f"<span style='font-size:11px; color:#718096;'>누적 방문자</span><br>"
        f"<span style='font-size:22px; font-weight:700; color:#f6ad55;'>"
        f"{visitors:,}</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    st.divider()

    # 갱신 버튼 (로컬 환경에서만 표시)
    if IS_LOCAL:
        col_a, col_b = st.columns(2)
        if col_a.button("🔄 갱신", use_container_width=True, help="변경 파일만 재파싱"):
            with st.spinner("파싱 중…"):
                run_parser()
            st.rerun()
        if col_b.button("강제 갱신", use_container_width=True, help="전체 강제 재파싱"):
            with st.spinner("전체 파싱 중…"):
                run_parser(force=True)
            st.rerun()
    else:
        st.caption("📡 클라우드 모드 — 로컬에서 push_update.bat 실행 후 자동 반영")

    last_parse = load_last_parse()
    if last_parse:
        st.caption(f"최종 갱신: {last_parse}")

    st.divider()

    if not DB_PATH.exists():
        st.warning("⚠️ 데이터 없음")
        st.stop()

    view = st.radio(
        "보기 모드",
        ["📋 전체 현황", "🔍 종목별 상세", "📈 4개월 성장 분석"],
        label_visibility="collapsed",
    )
    st.divider()

    summary_df = load_summary()
    if summary_df.empty:
        st.info("데이터가 없습니다.")
        st.stop()

    items_df = load_items()
    mom_map  = dict(zip(summary_df["sheet_name"], summary_df["mom"]))
    co_map   = dict(zip(items_df["sheet_name"], items_df["company"].fillna("")))
    all_names = items_df["sheet_name"].tolist()

    def item_label(n):
        mom    = mom_map.get(n)
        icon   = mom_icon(mom)
        pct    = fmt_pct(mom) if mom is not None and not pd.isna(mom) else " - "
        co     = co_map.get(n, "")
        co_str = f" [{co}]" if co else ""
        return f"{icon} {pct}  {n}{co_str}"

    selected_item = None

    if view == "🔍 종목별 상세":
        st.caption("종목 선택 — 클릭 후 바로 타이핑해 검색")
        prev      = st.session_state.get("selected")
        def_idx   = all_names.index(prev) if prev in all_names else 0
        selected_item = st.selectbox(
            "종목 선택",
            all_names,
            index=def_idx,
            format_func=item_label,
            label_visibility="collapsed",
        )
        st.session_state["selected"] = selected_item
        co = co_map.get(selected_item, "")
        if co:
            st.caption(f"🏢 {co}")


# ─── 전체 현황 ────────────────────────────────────────────────────────────────

if view == "📋 전체 현황":
    st.title("📋 전체 수출 현황")

    latest_period = summary_df["period"].max()
    st.caption(f"기준: {latest_period}  |  총 {len(summary_df)}개 품목")

    # KPI
    has_mom = summary_df["mom"].notna()
    n_up = int((summary_df.loc[has_mom, "mom"] > 0).sum())
    n_dn = int((summary_df.loc[has_mom, "mom"] < 0).sum())
    avg_yoy = summary_df["yoy"].dropna().mean()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("총 품목 수",        f"{len(summary_df)}개")
    k2.metric("MoM 상승 품목",     f"{n_up}개")
    k3.metric("MoM 하락 품목",     f"{n_dn}개")
    k4.metric("평균 YoY",          fmt_pct(avg_yoy) if pd.notna(avg_yoy) else "-")

    st.divider()

    # ── 1. 월별 전체 합산 수출액 + 평균 MoM ──────────────────────────────────
    agg = load_aggregate_monthly()
    if not agg.empty:
        st.subheader("① 전체 품목 합산 — 월별 수출액 추이")
        agg_r = agg.tail(36).copy()

        fig_agg = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.62, 0.38],
            vertical_spacing=0.04,
            subplot_titles=["월간 수출 합계 (10억 달러)", "전 품목 평균 MoM (%)"],
        )
        # 수출액 막대
        fig_agg.add_trace(
            go.Bar(
                x=agg_r["period"], y=agg_r["total_dollar"] / 1e9,
                name="월간 합계", marker_color=COLORS["blue"], opacity=0.82,
                hovertemplate="%{x}<br>합계: $%{y:.2f}B<extra></extra>",
            ), row=1, col=1
        )
        # 12개월 이동평균 선
        agg_r["ma12"] = agg_r["total_dollar"].rolling(12, min_periods=3).mean()
        fig_agg.add_trace(
            go.Scatter(
                x=agg_r["period"], y=agg_r["ma12"] / 1e9,
                name="12M 이동평균", mode="lines",
                line=dict(color=COLORS["orange"], width=2.5, dash="dot"),
            ), row=1, col=1
        )
        # 평균 MoM 막대
        mom_vals = agg_r["avg_mom"].fillna(0)
        fig_agg.add_trace(
            go.Bar(
                x=agg_r["period"], y=mom_vals * 100,
                name="평균 MoM",
                marker_color=[COLORS["up"] if v >= 0 else COLORS["down"] for v in mom_vals],
                hovertemplate="%{x}<br>MoM: %{y:.1f}%<extra></extra>",
            ), row=2, col=1
        )
        fig_agg.add_hline(y=0, line_color=COLORS["gray"], line_dash="dash",
                          line_width=1, row=2, col=1)
        fig_agg.update_yaxes(title_text="10억$ (B$)",   row=1, col=1)
        fig_agg.update_yaxes(title_text="%", ticksuffix="%", row=2, col=1)
        fig_agg.update_layout(
            height=520,
            legend=dict(orientation="h", y=1.04),
            **PLOT_BG,
        )
        st.plotly_chart(fig_agg, use_container_width=True)

    # ── 2. MoM 히트맵 ─────────────────────────────────────────────────────────
    st.subheader("② 품목별 월별 MoM 히트맵 (수출액 상위 40개 품목 · 최근 18개월)")
    hm = load_heatmap_data(n_months=18, n_items=40)
    if not hm.empty:
        pivot = hm.pivot(index="sheet_name", columns="period", values="mom")
        latest_col = pivot.columns.max()
        pivot = pivot.sort_values(latest_col, ascending=False, na_position="last")

        z = (pivot.values * 100)

        fig_hm = go.Figure(go.Heatmap(
            z=z.tolist(),
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[
                [0.00, "#9b2335"],
                [0.30, "#fc8181"],
                [0.50, "#1e2433"],
                [0.70, "#68d391"],
                [1.00, "#1a6644"],
            ],
            zmid=0, zmin=-60, zmax=60,
            colorbar=dict(title="MoM (%)", ticksuffix="%", len=0.85),
            hovertemplate="<b>%{y}</b><br>%{x}<br>MoM: %{z:.1f}%<extra></extra>",
        ))
        fig_hm.update_layout(
            height=max(420, len(pivot) * 18 + 90),
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis=dict(side="top", tickangle=-30),
            **PLOT_BG,
        )
        st.plotly_chart(fig_hm, use_container_width=True)

    # ── 3. 수출액 Top 15 / Bottom 15 ─────────────────────────────────────────
    st.subheader("③ 최신 수출액 Top / Bottom 15")
    sub_d = summary_df.dropna(subset=["export_dollar"]).copy()
    top15 = sub_d.nlargest(15, "export_dollar")[["sheet_name", "export_dollar", "mom", "yoy"]]
    bot15 = sub_d.nsmallest(15, "export_dollar")[["sheet_name", "export_dollar", "mom", "yoy"]]

    c_top, c_bot = st.columns(2)
    for frame, title, col_obj in [
        (top15, "수출액 상위 15", c_top),
        (bot15, "수출액 하위 15", c_bot),
    ]:
        mom_colors = [COLORS["up"] if (v is not None and not pd.isna(v) and v > 0)
                      else COLORS["down"] for v in frame["mom"]]
        fig_d = go.Figure(go.Bar(
            x=frame["export_dollar"] / 1e6,
            y=frame["sheet_name"],
            orientation="h",
            marker_color=mom_colors,
            text=[fmt_pct(m) for m in frame["mom"]],
            textposition="outside",
            hovertemplate="<b>%{y}</b><br>수출: $%{x:.0f}M<extra></extra>",
        ))
        fig_d.update_layout(
            title=title,
            xaxis_title="백만 달러 (M$)",
            yaxis=dict(autorange="reversed"),
            height=430,
            margin=dict(l=0, r=60, t=35, b=0),
            **PLOT_BG,
        )
        col_obj.plotly_chart(fig_d, use_container_width=True)

    # ── 4. MoM / YoY 상위·하위 ───────────────────────────────────────────────
    st.subheader("④ MoM · YoY 상위 / 하위 15개")
    tab_mom, tab_yoy = st.tabs(["MoM", "YoY"])

    def mover_chart(col: str, n: int = 15):
        sub = summary_df.dropna(subset=[col])
        for frame, title, clr in [
            (sub.nlargest(n, col),  f"상위 {n}", COLORS["up"]),
            (sub.nsmallest(n, col), f"하위 {n}", COLORS["down"]),
        ]:
            frame = frame[["sheet_name", col]]
            fig = px.bar(
                frame, x=col, y="sheet_name", orientation="h",
                title=title, color_discrete_sequence=[clr],
                labels={col: "", "sheet_name": ""},
                text=frame[col].apply(fmt_pct),
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(
                height=430,
                yaxis=dict(autorange="reversed"),
                margin=dict(l=0, r=60, t=35, b=0),
                **PLOT_BG,
            )
            fig.update_xaxes(tickformat=".0%")
            yield fig

    with tab_mom:
        ca, cb = st.columns(2)
        gen = mover_chart("mom")
        ca.plotly_chart(next(gen), use_container_width=True)
        cb.plotly_chart(next(gen), use_container_width=True)
    with tab_yoy:
        ca, cb = st.columns(2)
        gen = mover_chart("yoy")
        ca.plotly_chart(next(gen), use_container_width=True)
        cb.plotly_chart(next(gen), use_container_width=True)

    # ── 5. 테이블 ─────────────────────────────────────────────────────────────
    st.subheader("⑤ 최신 데이터 전체 테이블")
    c1, c2, c3 = st.columns([2, 2, 2])
    sort_opt  = c1.selectbox("정렬", ["수출액 높은 순", "YoY 높은 순", "YoY 낮은 순",
                                       "MoM 높은 순", "MoM 낮은 순", "종목명"])
    mom_filt  = c2.selectbox("MoM 필터", ["전체", "상승만", "하락만"])
    search_tb = c3.text_input("테이블 검색", placeholder="종목명 검색…")

    df_tbl = summary_df.copy()
    if mom_filt == "상승만":
        df_tbl = df_tbl[df_tbl["mom"] > 0]
    elif mom_filt == "하락만":
        df_tbl = df_tbl[df_tbl["mom"] < 0]
    if search_tb:
        df_tbl = df_tbl[df_tbl["sheet_name"].str.contains(search_tb, case=False, na=False)]

    sort_map = {
        "수출액 높은 순": ("export_dollar", False),
        "YoY 높은 순":   ("yoy",          False),
        "YoY 낮은 순":   ("yoy",          True),
        "MoM 높은 순":   ("mom",          False),
        "MoM 낮은 순":   ("mom",          True),
        "종목명":         ("sheet_name",   True),
    }
    sc, sa = sort_map[sort_opt]
    df_tbl = df_tbl.sort_values(sc, ascending=sa, na_position="last").reset_index(drop=True)

    tbl = df_tbl[["sheet_name","company","hs_code","period",
                  "export_dollar","mom","yoy","unit_price","weight_kg"]].copy()
    tbl.columns = ["종목","기업","HS코드","기준월","수출액(달러)","MoM","YoY","단가($/kg)","중량(kg)"]
    tbl["수출액(달러)"] = tbl["수출액(달러)"].apply(fmt_dollar)
    tbl["MoM"]         = tbl["MoM"].apply(fmt_pct)
    tbl["YoY"]         = tbl["YoY"].apply(fmt_pct)
    tbl["단가($/kg)"]  = tbl["단가($/kg)"].apply(
        lambda x: f"${x:,.1f}" if pd.notna(x) else "-")
    tbl["중량(kg)"]    = tbl["중량(kg)"].apply(
        lambda x: f"{x/1e3:,.1f}t" if pd.notna(x) and x > 0 else "-")
    st.dataframe(tbl, use_container_width=True, height=500, hide_index=True)


# ─── 종목별 상세 ──────────────────────────────────────────────────────────────

elif view == "🔍 종목별 상세":
    if not selected_item:
        st.info("👈 사이드바에서 종목을 선택하세요.")
        st.stop()

    df = load_item_data(selected_item, months=60)
    row = summary_df[summary_df["sheet_name"] == selected_item]
    if df.empty or row.empty:
        st.warning("데이터 없음")
        st.stop()

    latest = row.iloc[0]

    # 헤더
    st.title(f"📦 {selected_item}")
    meta = []
    if pd.notna(latest.get("company")):
        meta.append(f"🏢 {latest['company']}")
    if pd.notna(latest.get("hs_code")):
        meta.append(f"HS: {latest['hs_code']}")
    if pd.notna(latest.get("source_file")):
        meta.append(f"📁 {latest['source_file']}")
    if meta:
        st.caption("  |  ".join(meta))

    # KPI
    st.subheader(f"📅 최신 기준월: {latest['period']}")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("월간 수출액",   fmt_dollar(latest["export_dollar"]))
    k2.metric("일평균 수출액", fmt_dollar(latest["export_avg"]))
    k3.metric("MoM",  fmt_pct(latest["mom"]),
              delta=fmt_pct(latest["mom"]), delta_color="normal")
    k4.metric("YoY",  fmt_pct(latest["yoy"]),
              delta=fmt_pct(latest["yoy"]), delta_color="normal")
    up = latest["unit_price"]
    k5.metric("단가 ($/kg)", f"${up:,.2f}" if pd.notna(up) else "-")

    st.divider()

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 수출액·단가 추이",
        "📊 MoM · YoY",
        "📅 연도별 비교",
        "💰 단가 · 중량",
        "📋 원본 데이터",
    ])

    # ── Tab 1: 수출액 + 일평균 + 단가 (이중 Y축) ────────────────────────────
    with tab1:
        d = df.dropna(subset=["export_dollar"])
        if d.empty:
            st.info("데이터 없음")
        else:
            fig1 = make_subplots(specs=[[{"secondary_y": True}]])

            # 월간 수출액 막대
            fig1.add_trace(
                go.Bar(
                    x=d["period"], y=d["export_dollar"] / 1e6,
                    name="월간 수출액 (M$)",
                    marker_color=COLORS["blue"], opacity=0.75,
                    hovertemplate="%{x}<br>월간: $%{y:.1f}M<extra></extra>",
                ),
                secondary_y=False,
            )
            # 일평균 선
            d_avg = df.dropna(subset=["export_avg"])
            if not d_avg.empty:
                fig1.add_trace(
                    go.Scatter(
                        x=d_avg["period"], y=d_avg["export_avg"] / 1e6,
                        name="일평균 (M$)",
                        mode="lines+markers",
                        line=dict(color=COLORS["orange"], width=2.5),
                        marker=dict(size=5),
                        hovertemplate="%{x}<br>일평균: $%{y:.2f}M<extra></extra>",
                    ),
                    secondary_y=False,
                )
            # 단가 선 (우축)
            d_up = df.dropna(subset=["unit_price"])
            if not d_up.empty:
                fig1.add_trace(
                    go.Scatter(
                        x=d_up["period"], y=d_up["unit_price"],
                        name="단가 ($/kg)",
                        mode="lines+markers",
                        line=dict(color=COLORS["purple"], width=2, dash="dot"),
                        marker=dict(size=5, symbol="diamond"),
                        hovertemplate="%{x}<br>단가: $%{y:,.2f}/kg<extra></extra>",
                    ),
                    secondary_y=True,
                )

            fig1.update_yaxes(title_text="백만 달러 (M$)", secondary_y=False)
            fig1.update_yaxes(title_text="단가 ($/kg)", secondary_y=True, showgrid=False)
            fig1.update_layout(
                title="월간 수출액 · 일평균 · 단가 추이",
                height=460,
                legend=dict(orientation="h", y=1.06),
                **PLOT_BG,
            )
            st.plotly_chart(fig1, use_container_width=True)

            # 6개월 이동평균 추가 미니 차트
            d2 = d.copy()
            d2["ma6"] = d2["export_dollar"].rolling(6, min_periods=2).mean()
            d2["ma12"] = d2["export_dollar"].rolling(12, min_periods=3).mean()
            if d2["ma6"].notna().sum() >= 2:
                fig1b = go.Figure()
                fig1b.add_trace(go.Bar(
                    x=d2["period"], y=d2["export_dollar"] / 1e6,
                    name="월간", marker_color=COLORS["blue"], opacity=0.45,
                ))
                fig1b.add_trace(go.Scatter(
                    x=d2["period"], y=d2["ma6"] / 1e6,
                    name="6M 이동평균", mode="lines",
                    line=dict(color=COLORS["orange"], width=2.5),
                ))
                if d2["ma12"].notna().sum() >= 2:
                    fig1b.add_trace(go.Scatter(
                        x=d2["period"], y=d2["ma12"] / 1e6,
                        name="12M 이동평균", mode="lines",
                        line=dict(color=COLORS["teal"], width=2, dash="dash"),
                    ))
                fig1b.update_layout(
                    title="수출액 이동평균",
                    yaxis_title="M$",
                    height=320,
                    legend=dict(orientation="h", y=1.05),
                    **PLOT_BG,
                )
                st.plotly_chart(fig1b, use_container_width=True)

    # ── Tab 2: MoM / YoY ────────────────────────────────────────────────────
    with tab2:
        fig2 = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.06,
            row_heights=[0.5, 0.5],
            subplot_titles=["MoM 전월 대비 (%)", "YoY 전년 동월 대비 (%)"],
        )

        d_mom = df.dropna(subset=["mom"])
        if not d_mom.empty:
            mom_clrs = [COLORS["up"] if v >= 0 else COLORS["down"] for v in d_mom["mom"]]
            fig2.add_trace(
                go.Bar(
                    x=d_mom["period"], y=d_mom["mom"] * 100,
                    name="MoM", marker_color=mom_clrs,
                    text=[fmt_pct(v) for v in d_mom["mom"]],
                    textposition="outside",
                    hovertemplate="%{x}<br>MoM: %{y:.1f}%<extra></extra>",
                ), row=1, col=1
            )
            fig2.add_hline(y=0, line_color=COLORS["gray"],
                           line_dash="dash", line_width=1, row=1, col=1)

        d_yoy = df.dropna(subset=["yoy"])
        if not d_yoy.empty:
            yoy_clrs = [COLORS["up"] if v >= 0 else COLORS["down"] for v in d_yoy["yoy"]]
            fig2.add_trace(
                go.Bar(
                    x=d_yoy["period"], y=d_yoy["yoy"] * 100,
                    name="YoY", marker_color=yoy_clrs,
                    text=[fmt_pct(v) for v in d_yoy["yoy"]],
                    textposition="outside",
                    hovertemplate="%{x}<br>YoY: %{y:.1f}%<extra></extra>",
                ), row=2, col=1
            )
            fig2.add_hline(y=0, line_color=COLORS["gray"],
                           line_dash="dash", line_width=1, row=2, col=1)

        fig2.update_yaxes(title_text="%", ticksuffix="%")
        fig2.update_layout(
            height=560, showlegend=False,
            **PLOT_BG,
        )
        st.plotly_chart(fig2, use_container_width=True)

        # 최근 12개월 누적 비교 (가로 막대)
        if not d_mom.empty:
            last12 = d_mom.tail(12).copy()
            fig2b = go.Figure()
            fig2b.add_trace(go.Bar(
                x=last12["mom"] * 100, y=last12["period"],
                orientation="h",
                name="MoM",
                marker_color=[COLORS["up"] if v >= 0 else COLORS["down"]
                              for v in last12["mom"]],
                text=[fmt_pct(v) for v in last12["mom"]],
                textposition="outside",
            ))
            fig2b.add_vline(x=0, line_color=COLORS["gray"], line_dash="dash")
            fig2b.update_layout(
                title="최근 12개월 MoM (%)",
                xaxis=dict(title="%", ticksuffix="%"),
                yaxis=dict(autorange="reversed"),
                height=380,
                **PLOT_BG,
            )
            st.plotly_chart(fig2b, use_container_width=True)

    # ── Tab 3: 연도별 비교 ───────────────────────────────────────────────────
    with tab3:
        df_y = df.copy()
        df_y["year"]  = df_y["period"].str[:4]
        df_y["month"] = df_y["period"].str[5:]

        month_labels = {
            "01":"1월","02":"2월","03":"3월","04":"4월",
            "05":"5월","06":"6월","07":"7월","08":"8월",
            "09":"9월","10":"10월","11":"11월","12":"12월",
        }

        # 연도별 수출액 선 그래프
        d_yr = df_y.dropna(subset=["export_dollar"])
        if not d_yr.empty:
            fig3 = go.Figure()
            for i, yr in enumerate(sorted(d_yr["year"].unique())):
                sub = d_yr[d_yr["year"] == yr].sort_values("month")
                fig3.add_trace(go.Scatter(
                    x=sub["month"], y=sub["export_dollar"] / 1e6,
                    name=f"{yr}년",
                    mode="lines+markers",
                    line=dict(color=PALETTE[i % len(PALETTE)], width=2.5),
                    marker=dict(size=7),
                    hovertemplate=f"{yr}년 %{{x}}월<br>수출액: $%{{y:.1f}}M<extra></extra>",
                ))
            fig3.update_layout(
                title="연도별 월간 수출액 비교",
                xaxis=dict(
                    title="월",
                    tickvals=list(month_labels.keys()),
                    ticktext=list(month_labels.values()),
                ),
                yaxis_title="백만 달러 (M$)",
                legend=dict(orientation="h", y=1.05),
                height=440,
                **PLOT_BG,
            )
            st.plotly_chart(fig3, use_container_width=True)

        # 연도별 YoY 선 그래프
        d_yoy_yr = df_y.dropna(subset=["yoy"])
        if not d_yoy_yr.empty:
            fig3b = go.Figure()
            for i, yr in enumerate(sorted(d_yoy_yr["year"].unique())):
                sub = d_yoy_yr[d_yoy_yr["year"] == yr].sort_values("month")
                fig3b.add_trace(go.Scatter(
                    x=sub["month"], y=sub["yoy"] * 100,
                    name=f"{yr}년",
                    mode="lines+markers",
                    line=dict(color=PALETTE[i % len(PALETTE)], width=2),
                    marker=dict(size=6),
                    hovertemplate=f"{yr}년 %{{x}}월<br>YoY: %{{y:.1f}}%<extra></extra>",
                ))
            fig3b.add_hline(y=0, line_color=COLORS["gray"], line_dash="dash", line_width=1)
            fig3b.update_layout(
                title="연도별 YoY 비교",
                xaxis=dict(
                    title="월",
                    tickvals=list(month_labels.keys()),
                    ticktext=list(month_labels.values()),
                ),
                yaxis=dict(title="%", ticksuffix="%"),
                legend=dict(orientation="h", y=1.05),
                height=380,
                **PLOT_BG,
            )
            st.plotly_chart(fig3b, use_container_width=True)

        # 연도별 단가 선 그래프
        d_up_yr = df_y.dropna(subset=["unit_price"])
        if not d_up_yr.empty and d_up_yr["unit_price"].notna().sum() > 2:
            fig3c = go.Figure()
            for i, yr in enumerate(sorted(d_up_yr["year"].unique())):
                sub = d_up_yr[d_up_yr["year"] == yr].sort_values("month")
                fig3c.add_trace(go.Scatter(
                    x=sub["month"], y=sub["unit_price"],
                    name=f"{yr}년",
                    mode="lines+markers",
                    line=dict(color=PALETTE[i % len(PALETTE)], width=2, dash="dot"),
                    marker=dict(size=6, symbol="diamond"),
                ))
            fig3c.update_layout(
                title="연도별 단가 비교 ($/kg)",
                xaxis=dict(
                    title="월",
                    tickvals=list(month_labels.keys()),
                    ticktext=list(month_labels.values()),
                ),
                yaxis_title="$/kg",
                legend=dict(orientation="h", y=1.05),
                height=340,
                **PLOT_BG,
            )
            st.plotly_chart(fig3c, use_container_width=True)

    # ── Tab 4: 단가 · 중량 ───────────────────────────────────────────────────
    with tab4:
        fig4 = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.38, 0.27, 0.35],
            subplot_titles=["단가 추이 ($/kg)", "단가 YoY (%)", "중량 추이 (ton)"],
            specs=[[{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": False}]],
        )

        # 단가 선
        d_price = df.dropna(subset=["unit_price"])
        if not d_price.empty:
            fig4.add_trace(
                go.Scatter(
                    x=d_price["period"], y=d_price["unit_price"],
                    name="단가 ($/kg)",
                    mode="lines+markers",
                    line=dict(color=COLORS["purple"], width=2.5),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor="rgba(159,122,234,0.08)",
                    hovertemplate="%{x}<br>단가: $%{y:,.2f}/kg<extra></extra>",
                ), row=1, col=1
            )

        # 단가 YoY 막대
        d_pyoy = df.dropna(subset=["price_yoy"])
        if not d_pyoy.empty:
            fig4.add_trace(
                go.Bar(
                    x=d_pyoy["period"], y=d_pyoy["price_yoy"] * 100,
                    name="단가 YoY",
                    marker_color=[COLORS["up"] if v >= 0 else COLORS["down"]
                                  for v in d_pyoy["price_yoy"]],
                    hovertemplate="%{x}<br>단가YoY: %{y:.1f}%<extra></extra>",
                ), row=2, col=1
            )
            fig4.add_hline(y=0, line_color=COLORS["gray"],
                           line_dash="dash", line_width=1, row=2, col=1)

        # 중량 면적 차트
        d_wt = df.dropna(subset=["weight_kg"])
        if not d_wt.empty:
            fig4.add_trace(
                go.Scatter(
                    x=d_wt["period"], y=d_wt["weight_kg"] / 1e3,
                    name="중량 (ton)",
                    mode="lines+markers",
                    line=dict(color=COLORS["teal"], width=2.5),
                    marker=dict(size=5),
                    fill="tozeroy",
                    fillcolor="rgba(56,178,172,0.12)",
                    hovertemplate="%{x}<br>중량: %{y:,.0f}ton<extra></extra>",
                ), row=3, col=1
            )

        fig4.update_yaxes(title_text="$/kg",  row=1, col=1)
        fig4.update_yaxes(title_text="%", ticksuffix="%", row=2, col=1)
        fig4.update_yaxes(title_text="ton",   row=3, col=1)
        fig4.update_layout(
            height=600,
            showlegend=False,
            **PLOT_BG,
        )
        st.plotly_chart(fig4, use_container_width=True)

    # ── Tab 5: 원본 데이터 ───────────────────────────────────────────────────
    with tab5:
        disp = df.rename(columns={
            "period":       "기준월",
            "export_dollar":"수출액($)",
            "export_avg":   "일평균($)",
            "mom":          "MoM",
            "yoy":          "YoY",
            "unit_price":   "단가($/kg)",
            "price_yoy":    "단가YoY",
            "weight_kg":    "중량(kg)",
        }).sort_values("기준월", ascending=False)

        for col in ["MoM", "YoY", "단가YoY"]:
            disp[col] = disp[col].apply(fmt_pct)
        disp["수출액($)"]  = disp["수출액($)"].apply(
            lambda x: f"${x:,.0f}" if pd.notna(x) else "-")
        disp["일평균($)"]  = disp["일평균($)"].apply(
            lambda x: f"${x:,.0f}" if pd.notna(x) else "-")
        disp["단가($/kg)"] = disp["단가($/kg)"].apply(
            lambda x: f"${x:,.2f}" if pd.notna(x) else "-")
        disp["중량(kg)"]   = disp["중량(kg)"].apply(
            lambda x: f"{x:,.0f}" if pd.notna(x) else "-")

        st.dataframe(disp, use_container_width=True, height=550, hide_index=True)

        csv = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            "⬇️ CSV 다운로드",
            data=csv.encode("utf-8-sig"),
            file_name=f"{selected_item}_{date.today()}.csv",
            mime="text/csv",
        )


# ─── 4개월 성장 분석 ──────────────────────────────────────────────────────────

elif view == "📈 4개월 성장 분석":
    st.title("📈 4개월 성장 분석")

    gdf = load_4m_growth()
    if gdf.empty:
        st.info("데이터 없음")
        st.stop()

    gdf = gdf.dropna(subset=["growth_4m"]).copy()
    latest_period = gdf["latest_period"].max()
    period_4mo    = gdf["period_4mo"].min()

    st.caption(
        f"비교 기간: {period_4mo} → {latest_period}  |  "
        f"분석 품목: {len(gdf)}개"
    )

    # ── KPI ───────────────────────────────────────────────────────────────────
    n_grow = int((gdf["growth_4m"] > 0).sum())
    n_drop = int((gdf["growth_4m"] < 0).sum())
    top1   = gdf.iloc[0]
    avg_g  = gdf["growth_4m"].mean()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("성장 품목 (양(+))",  f"{n_grow}개")
    k2.metric("감소 품목 (음(-))",  f"{n_drop}개")
    k3.metric("최고 성장",
              f"{top1['sheet_name']}",
              delta=fmt_pct(top1["growth_4m"]))
    k4.metric("전체 평균 성장률",   fmt_pct(avg_g))

    st.divider()

    # ── 1. 성장률 순위 가로 막대 ──────────────────────────────────────────────
    n_show = st.slider("표시할 품목 수 (상위·하위 각각)", 10, 40, 20, step=5)

    top_n = gdf.head(n_show).copy()
    bot_n = gdf.tail(n_show).copy()

    st.subheader(f"① 4개월 성장률 상위 / 하위 {n_show}개")
    ca, cb = st.columns(2)

    for frame, title, clr, col_obj in [
        (top_n, f"성장 상위 {n_show}", COLORS["up"],   ca),
        (bot_n, f"성장 하위 {n_show}", COLORS["down"], cb),
    ]:
        frame = frame.sort_values("growth_4m", ascending=(clr == COLORS["down"]))
        fig = go.Figure(go.Bar(
            x=frame["growth_4m"] * 100,
            y=frame["sheet_name"],
            orientation="h",
            marker_color=clr,
            text=[fmt_pct(v) for v in frame["growth_4m"]],
            textposition="outside",
            customdata=frame[["latest_dollar", "latest_mom", "latest_yoy"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "4M 성장: %{x:.1f}%<br>"
                "최신 수출: $%{customdata[0]:,.0f}<br>"
                "MoM: %{customdata[1]:.1%}<br>"
                "YoY: %{customdata[2]:.1%}<extra></extra>"
            ),
        ))
        fig.update_layout(
            title=title,
            xaxis=dict(title="%", ticksuffix="%"),
            yaxis=dict(autorange="reversed"),
            height=max(400, n_show * 22 + 60),
            margin=dict(l=0, r=70, t=35, b=0),
            **PLOT_BG,
        )
        col_obj.plotly_chart(fig, use_container_width=True)

    # ── 2. 버블 차트: 4M 성장률 × YoY × 수출액 규모 ─────────────────────────
    st.subheader("② 4M 성장률 vs YoY 버블 차트  (버블 크기 = 수출액)")

    bub = gdf.dropna(subset=["growth_4m", "latest_yoy", "latest_dollar"]).copy()
    bub["size_px"] = (bub["latest_dollar"] / bub["latest_dollar"].max() * 50 + 5).clip(5, 55)
    bub["mom_clr"] = bub["latest_mom"].apply(
        lambda v: COLORS["up"] if (pd.notna(v) and v > 0) else COLORS["down"])

    fig_bub = go.Figure(go.Scatter(
        x=bub["growth_4m"] * 100,
        y=bub["latest_yoy"] * 100,
        mode="markers+text",
        marker=dict(
            size=bub["size_px"],
            color=bub["latest_dollar"],
            colorscale="Blues",
            showscale=True,
            colorbar=dict(title="수출액($)", tickformat=".2s"),
            line=dict(width=1, color="white"),
            opacity=0.85,
        ),
        text=bub["sheet_name"],
        textposition="top center",
        textfont=dict(size=9),
        customdata=bub[["sheet_name", "latest_dollar",
                         "latest_mom", "latest_yoy", "growth_4m"]].values,
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "4M 성장: %{x:.1f}%<br>"
            "YoY: %{y:.1f}%<br>"
            "MoM: %{customdata[2]:.1%}<br>"
            "수출액: $%{customdata[1]:,.0f}<extra></extra>"
        ),
    ))
    fig_bub.add_vline(x=0, line_color=COLORS["gray"], line_dash="dash", line_width=1)
    fig_bub.add_hline(y=0, line_color=COLORS["gray"], line_dash="dash", line_width=1)

    # 사분면 레이블
    x_max = bub["growth_4m"].max() * 100 * 0.85
    y_max = bub["latest_yoy"].max() * 100 * 0.85
    x_min = bub["growth_4m"].min() * 100 * 0.85
    y_min = bub["latest_yoy"].min() * 100 * 0.85
    for txt, ax, ay, clr in [
        ("🚀 모멘텀",    x_max, y_max,  COLORS["up"]),
        ("📉 회복",      x_max, y_min,  COLORS["orange"]),
        ("⬆️ 반등",     x_min, y_max,  COLORS["teal"]),
        ("⚠️ 부진",     x_min, y_min,  COLORS["down"]),
    ]:
        fig_bub.add_annotation(
            x=ax, y=ay, text=txt, showarrow=False,
            font=dict(size=11, color=clr), opacity=0.5,
        )

    fig_bub.update_layout(
        xaxis=dict(title="4개월 성장률 (%)", ticksuffix="%", zeroline=False),
        yaxis=dict(title="YoY (%)", ticksuffix="%", zeroline=False),
        height=560,
        **PLOT_BG,
    )
    st.plotly_chart(fig_bub, use_container_width=True)
    st.caption("🚀 모멘텀(우상): 4M·YoY 모두 상승  |  📉 회복(우하): 최근 반등 중  |  "
               "⬆️ 반등(좌상): 연간 성장하나 최근 둔화  |  ⚠️ 부진(좌하): 전방위 하락")

    # ── 3. 상위 품목 월별 추이 (정규화 인덱스) ────────────────────────────────
    st.subheader("③ 상위 성장 품목 월별 수출액 추이 (4개월 전 = 100 기준 인덱스)")

    n_trend = st.slider("추이 비교 품목 수", 5, 20, 10, step=5, key="trend_n")
    top_names = gdf.head(n_trend)["sheet_name"].tolist()

    trend_all = load_recent_all(months=7)
    trend = trend_all[trend_all["sheet_name"].isin(top_names)].copy()

    if not trend.empty:
        # 각 품목의 기준월(4개월 전) 수출액으로 나눠 인덱스화
        base_period = trend.groupby("sheet_name")["period"].min().reset_index()
        base_period.columns = ["sheet_name", "base_period"]
        trend = trend.merge(base_period, on="sheet_name")
        base_vals = (
            trend[trend["period"] == trend["base_period"]]
            [["sheet_name", "export_dollar"]]
            .rename(columns={"export_dollar": "base_dollar"})
        )
        trend = trend.merge(base_vals, on="sheet_name", how="left")
        trend["index_val"] = trend.apply(
            lambda r: r["export_dollar"] / r["base_dollar"] * 100
            if pd.notna(r["base_dollar"]) and r["base_dollar"] > 0 else None,
            axis=1,
        )

        fig_trend = go.Figure()
        for i, name in enumerate(top_names):
            sub = trend[trend["sheet_name"] == name].sort_values("period")
            growth_val = gdf.loc[gdf["sheet_name"] == name, "growth_4m"].values
            g_str = fmt_pct(growth_val[0]) if len(growth_val) else ""
            fig_trend.add_trace(go.Scatter(
                x=sub["period"],
                y=sub["index_val"],
                name=f"{name} ({g_str})",
                mode="lines+markers",
                line=dict(color=PALETTE[i % len(PALETTE)], width=2.5),
                marker=dict(size=7),
                hovertemplate=(
                    f"<b>{name}</b><br>"
                    "%{x}<br>인덱스: %{y:.1f}<extra></extra>"
                ),
            ))

        fig_trend.add_hline(y=100, line_color=COLORS["gray"],
                            line_dash="dash", line_width=1.5,
                            annotation_text="기준(4개월 전)",
                            annotation_position="bottom right")
        fig_trend.update_layout(
            xaxis_title="기준월",
            yaxis=dict(title="인덱스 (4개월 전 = 100)"),
            height=460,
            legend=dict(orientation="h", y=-0.18, font=dict(size=11)),
            **PLOT_BG,
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    # ── 4. 수출액 절대값 추이 (상위 품목) ────────────────────────────────────
    st.subheader("④ 상위 성장 품목 — 실제 수출액 추이 (M$)")
    if not trend.empty:
        fig_abs = go.Figure()
        for i, name in enumerate(top_names):
            sub = trend[trend["sheet_name"] == name].sort_values("period")
            fig_abs.add_trace(go.Bar(
                name=name,
                x=sub["period"],
                y=sub["export_dollar"] / 1e6,
                marker_color=PALETTE[i % len(PALETTE)],
                hovertemplate=(
                    f"<b>{name}</b><br>"
                    "%{x}<br>수출: $%{y:.1f}M<extra></extra>"
                ),
            ))
        fig_abs.update_layout(
            barmode="group",
            xaxis_title="기준월",
            yaxis_title="백만 달러 (M$)",
            height=420,
            legend=dict(orientation="h", y=-0.22, font=dict(size=11)),
            **PLOT_BG,
        )
        st.plotly_chart(fig_abs, use_container_width=True)

    # ── 5. 전체 성장률 테이블 ─────────────────────────────────────────────────
    st.subheader("⑤ 전체 품목 4개월 성장률 테이블")

    show_only = st.radio(
        "필터", ["전체", "성장 품목만 (+)", "감소 품목만 (-)"],
        horizontal=True, key="growth_filter",
    )
    tbl_g = gdf.copy()
    if show_only == "성장 품목만 (+)":
        tbl_g = tbl_g[tbl_g["growth_4m"] > 0]
    elif show_only == "감소 품목만 (-)":
        tbl_g = tbl_g[tbl_g["growth_4m"] < 0]

    tbl_g = tbl_g[[
        "sheet_name", "company", "period_4mo", "latest_period",
        "dollar_4mo", "latest_dollar", "growth_4m",
        "latest_mom", "latest_yoy", "latest_price",
    ]].copy()
    tbl_g.columns = [
        "종목", "기업", "4개월전 기준월", "최신 기준월",
        "4개월전 수출($)", "최신 수출($)", "4M 성장률",
        "MoM", "YoY", "단가($/kg)",
    ]
    tbl_g["4개월전 수출($)"] = tbl_g["4개월전 수출($)"].apply(fmt_dollar)
    tbl_g["최신 수출($)"]   = tbl_g["최신 수출($)"].apply(fmt_dollar)
    tbl_g["4M 성장률"]      = tbl_g["4M 성장률"].apply(fmt_pct)
    tbl_g["MoM"]             = tbl_g["MoM"].apply(fmt_pct)
    tbl_g["YoY"]             = tbl_g["YoY"].apply(fmt_pct)
    tbl_g["단가($/kg)"]     = tbl_g["단가($/kg)"].apply(
        lambda x: f"${x:,.1f}" if pd.notna(x) else "-")

    st.dataframe(tbl_g, use_container_width=True, height=500, hide_index=True)

    csv_g = gdf.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        "⬇️ CSV 다운로드",
        data=csv_g.encode("utf-8-sig"),
        file_name=f"4m_growth_{date.today()}.csv",
        mime="text/csv",
    )
