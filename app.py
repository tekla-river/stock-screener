"""
《投资策略实战分析》A股自动选股系统 - Streamlit Web界面
启动方式：streamlit run app.py
"""

import sys
import os
import re
import shutil
from datetime import datetime

import pandas as pd
import numpy as np
import streamlit as st

# 确保项目目录在搜索路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_fetcher import prepare_data
from strategies import run_strategy
from factors import compute_all_factors
from config import STRATEGIES, CACHE_DIR
from strategy_details import STRATEGY_DETAILS


@st.cache_data(ttl=4 * 3600, show_spinner="⏳ 正在获取市场数据...")
def cached_prepare_data():
    """Streamlit 缓存版数据获取，4小时过期"""
    return prepare_data(force_refresh=False)


# ============================================================
# 页面配置
# ============================================================

st.set_page_config(
    page_title="A股价值选股系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""<style>
.block-container { padding-top: 2rem; }
</style>""", unsafe_allow_html=True)

# 筛选漏斗步骤条样式
st.markdown("""<style>
.funnel-bar {
    display: flex;
    align-items: center;
    gap: 0;
    padding: 16px 20px;
    background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
    border-radius: 12px;
    border: 1px solid #e2e8f0;
    margin-bottom: 8px;
    flex-wrap: wrap;
}
.funnel-step {
    display: flex;
    flex-direction: column;
    align-items: center;
    min-width: 80px;
    flex-shrink: 0;
}
.funnel-icon {
    width: 36px;
    height: 36px;
    border-radius: 50%;
    background: linear-gradient(135deg, #10b981, #059669);
    color: white;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    font-weight: 700;
    margin-bottom: 6px;
    box-shadow: 0 2px 6px rgba(16, 185, 129, 0.3);
}
.funnel-icon.active {
    background: linear-gradient(135deg, #3b82f6, #2563eb);
    box-shadow: 0 2px 6px rgba(59, 130, 246, 0.3);
}
.funnel-name {
    font-size: 12px;
    font-weight: 600;
    color: #1e293b;
    text-align: center;
    line-height: 1.3;
    max-width: 100px;
}
.funnel-count {
    font-size: 11px;
    color: #64748b;
    margin-top: 2px;
}
.funnel-arrow {
    display: flex;
    align-items: center;
    padding: 0 12px;
    color: #94a3b8;
    font-size: 18px;
    flex-shrink: 0;
}
</style>""", unsafe_allow_html=True)


# ============================================================
# 工具函数
# ============================================================

def parse_info_steps(info: str) -> list[dict]:
    """将策略返回的 info 字符串解析为步骤列表"""
    parts = [p.strip() for p in info.split("→")]
    steps = []
    for p in parts:
        match = re.search(r"(.+?)\((\d+)只\)", p)
        if match:
            steps.append({"name": match.group(1).strip(), "count": int(match.group(2))})
        else:
            steps.append({"name": p.strip(), "count": None})
    return steps


def render_funnel_bar(info: str):
    """将 info 字符串渲染为可视化筛选漏斗步骤条"""
    steps = parse_info_steps(info)
    if not steps:
        return

    # 构建 HTML 步骤条
    html_parts = []
    for i, step in enumerate(steps):
        is_last = (i == len(steps) - 1)
        icon_class = "funnel-icon active" if is_last else "funnel-icon"
        icon_text = str(i + 1)
        count_text = f"{step['count']}只" if step["count"] is not None else ""

        html_parts.append(f"""
            <div class="funnel-step">
                <div class="{icon_class}">{icon_text}</div>
                <div class="funnel-name">{step['name']}</div>
                <div class="funnel-count">{count_text}</div>
            </div>""")

        if not is_last:
            html_parts.append('<div class="funnel-arrow">→</div>')

    st.markdown(
        f'<div class="funnel-bar">{"".join(html_parts)}</div>',
        unsafe_allow_html=True,
    )


# 结果表格展示列（按顺序）: (源列名, 显示名, 格式, tooltip说明)
DISPLAY_COLUMNS = [
    ("代码",         "代码",     None,
     "股票代码"),
    ("名称",         "名称",     None,
     "股票简称"),
    ("最新价",       "现价",     "{:.2f}",
     "最新收盘价（元）"),
    ("pe",           "PE",       "{:.1f}",
     "市盈率 = 股价 ÷ 每股收益。  \n越低代表估值越便宜。（越低越好）"),
    ("pb",           "PB",       "{:.2f}",
     "市净率 = 股价 ÷ 每股净资产。  \n越低代表相对于净资产越便宜。（越低越好）"),
    ("ps",           "PS",       "{:.2f}",
     "市销率 = 市值 ÷ 营业收入。  \n越低代表相对于收入越便宜。（越低越好）"),
    ("ebitda_ev",    "EBITDA/EV","{:.1f}",
     "EBITDA/EV = 息税折旧摊销前利润 ÷ 企业价值 × 100%。  \n越高代表企业创造现金流能力越强。（越高越好）"),
    ("roe",          "ROE(%)",   "{:.1f}",
     "净资产收益率 = 净利润 ÷ 净资产 × 100%。  \n越高代表股东资金使用效率越高。（越高越好）"),
    ("total_mv_yi",  "市值(亿)", "{:.0f}",
     "总市值 = 股价 × 总股本（亿元）"),
    ("return_3m",    "近3月%",   "{:.1f}",
     "近3个月涨跌幅（%）。  \n反映短期动量强弱。（越高越好）"),
    ("return_6m",    "近6月%",   "{:.1f}",
     "近6个月涨跌幅（%）。  \n反映中期动量强弱。（越高越好）"),
    ("net_margin",   "净利率%",  "{:.1f}",
     "销售净利率 = 净利润 ÷ 营业收入 × 100%。  \n越高代表盈利能力越强。（越高越好）"),
    ("profit_growth","利润增长%","{:.1f}",
     "净利润同比增长率（%）。  \n正值表示利润同比增长，负值表示同比下降。（越高越好）"),
    ("vc1_score",    "VC1",      "{:.0f}",
     "复合价值因子一 = PE+PB+PS+市现率 四因子百分位排名之和（0~400）。  \n分数越高=综合估值越低=越便宜。（越高越好）"),
    ("vc2_score",    "VC2",      "{:.0f}",
     "复合价值因子二 = VC1 + 股东收益率百分位排名（0~500）。  \n分数越高=估值越低+股东回报越好。（越高越好）"),
]


def format_result_table(df: pd.DataFrame) -> pd.DataFrame:
    """将原始结果DataFrame格式化为展示表格（保留数值类型以支持正确排序）"""
    if df.empty:
        return pd.DataFrame(), {}, {}

    display = pd.DataFrame()
    col_config = {}   # 列格式配置：{显示名: 小数位数}
    col_help = {}     # 列tooltip：{显示名: 帮助文本}

    for src_col, dst_label, fmt, help_text in DISPLAY_COLUMNS:
        if src_col in df.columns:
            col_data = pd.to_numeric(df[src_col], errors="coerce")
            if fmt:
                # 保留数值类型，仅四舍五入到格式对应的小数位
                decimals = len(fmt.split(".")[1].rstrip("f")) if "." in fmt else 0
                display[dst_label] = col_data.round(decimals)
                col_config[dst_label] = decimals
            else:
                display[dst_label] = df[src_col]
            col_help[dst_label] = help_text

    return display, col_config, col_help


def show_factor_charts(market_df: pd.DataFrame, result_df: pd.DataFrame):
    """显示选中股票在全市场中的因子分布位置"""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial"]
    plt.rcParams["axes.unicode_minus"] = False

    charts = [
        ("pe",        "PE (市盈率)",   "PE",   0.95, 0.05),
        ("pb",        "PB (市净率)",   "PB",   0.95, 0.01),
        ("roe",       "ROE (净资产收益率%)", "ROE", 0.98, 0.02),
        ("ps",        "PS (市销率)",   "PS",   0.95, 0.05),
        ("ebitda_ev", "EBITDA/EV (%)","%",    0.98, 0.02),
    ]

    cols = st.columns(1)
    for idx, (factor, title, xlabel, clip_hi, clip_lo) in enumerate(charts):
        with cols[0]:
            if factor not in market_df.columns:
                st.caption(f"{title}: 数据缺失")
                continue

            valid_market = market_df[market_df[factor].notna() & (market_df[factor] != 0)][factor]
            if valid_market.empty:
                st.caption(f"{title}: 无有效数据")
                continue

            lower = valid_market.quantile(clip_lo)
            upper = valid_market.quantile(clip_hi)
            clipped = valid_market.clip(lower=lower, upper=upper)

            fig, ax = plt.subplots(figsize=(10, 3.2))

            # 全市场分布
            ax.hist(
                clipped,
                bins=60, alpha=0.5, color="steelblue", label="全市场", edgecolor="none"
            )

            # 选中股票：叠加柱状图 + 竖线标记
            if not result_df.empty and factor in result_df.columns:
                selected = result_df[result_df[factor].notna() & (result_df[factor] != 0)][factor]
                if not selected.empty:
                    sel_clipped = selected.clip(lower=lower, upper=upper)
                    ax.hist(
                        sel_clipped,
                        bins=20, alpha=0.85, color="coral", label="选中", edgecolor="none"
                    )
                    # 每只选中股票画竖线
                    for v in sel_clipped:
                        ax.axvline(v, color="#d4a0a0", alpha=0.6, linewidth=0.8)

            ax.set_title(title, fontsize=12)
            ax.set_xlabel(xlabel, fontsize=10)
            ax.legend(fontsize=9, loc="upper right")
            ax.grid(alpha=0.2)
            ax.tick_params(labelsize=9)

            st.pyplot(fig, bbox_inches="tight")
            plt.close(fig)


# ============================================================
# 主页面
# ============================================================

def main():
    st.title("📊 A股价值选股系统")
    st.caption("基于《投资策略实战分析》（What Works on Wall Street） — 詹姆斯·奥肖内西  |  行情: 腾讯  财务: 东方财富数据中心")

    # ---- 侧边栏 ----
    with st.sidebar:
        st.header("策略选择")

        strategy_labels = {cfg["name"]: sid for sid, cfg in STRATEGIES.items()}

        def _on_strategy_change():
            st.session_state["_strategy_changed"] = True

        selected_label = st.selectbox(
            "选股策略",
            options=list(strategy_labels.keys()),
            index=2,  # 默认"趋势+价值组合（25只）"
            on_change=_on_strategy_change,
            key="strategy_select",
        )
        strategy_id = strategy_labels[selected_label]

        # 策略说明（第一层：一句话描述）
        st.caption(STRATEGIES[strategy_id]["description"])

        # 策略详情（第三层：按需展开的完整算法说明）
        detail = STRATEGY_DETAILS.get(strategy_id)
        if detail:
            with st.expander("📖 策略详情", expanded=False):
                st.markdown(f"**📖 书中位置：**{detail['chapter']}")
                st.write(detail["summary"])

                st.markdown("**核心因子：**")
                for f in detail["factors"]:
                    st.markdown(f"- {f}")

                st.info(detail["formula"])

                if detail.get("adaptation"):
                    st.markdown("**A股适配说明：**")
                    for a in detail["adaptation"]:
                        st.caption(f"• {a}")

        st.divider()

        force_refresh = st.checkbox("🔄 强制刷新数据（忽略缓存）", value=False)

        st.divider()
        st.subheader("使用说明")
        st.markdown("""
- **VC1/VC2**：分数越高，说明股票越便宜（估值越低），值得买入
- **动量过滤**：只选近3个月或6个月涨得比市场平均多的股票（趋势向好）
- **龙头股**：只选市值大、成交活跃的大盘股（行业龙头）

> 行情数据缓存4小时，财务数据缓存12小时
        """)

        if st.button("🗑 清除全部缓存"):
            if os.path.exists(CACHE_DIR):
                shutil.rmtree(CACHE_DIR)
            st.session_state.pop("_raw_data", None)
            st.session_state.pop("_strategy_changed", None)
            st.success("缓存已清除，请重新运行选股")
            st.rerun()

    # ---- 主区域 ----
    auto_run = st.session_state.get("_strategy_changed", False) and "_raw_data" in st.session_state
    run_btn = st.button("🚀 开始选股", type="primary", use_container_width=True)

    if not run_btn and not auto_run:
        st.info("👆 选择策略后点击「开始选股」")
        return

    # 数据获取
    try:
        if force_refresh:
            st.cache_data.clear()
            raw_data = prepare_data(force_refresh=True)
        elif run_btn or "_raw_data" not in st.session_state:
            raw_data = cached_prepare_data()
        else:
            raw_data = st.session_state["_raw_data"]
            st.toast(f"使用缓存数据：{len(raw_data)} 只股票")

        st.session_state["_raw_data"] = raw_data
        if not (run_btn is False and auto_run):
            st.success(f"✅ 数据就绪：**{len(raw_data)}** 只股票")
    except Exception as e:
        st.error(f"数据获取失败: {e}")
        return

    st.session_state["_strategy_changed"] = False

    # 运行策略
    with st.status(f"运行策略: {selected_label}"):
        try:
            result, info = run_strategy(strategy_id, raw_data)
        except Exception as e:
            st.error(f"策略执行失败: {e}")
            return

    # 展示结果
    st.divider()

    # 筛选漏斗步骤条（第二层：结果页自动可见）
    render_funnel_bar(info)
    st.caption(f"📋 {info}")

    if result.empty:
        st.warning("未找到符合条件的股票，请尝试其他策略或刷新数据。")
        return

    # 统计卡片
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("选股数量", f"{len(result)} 只")
    with m2:
        avg_pe = result["pe"].mean() if "pe" in result.columns else np.nan
        st.metric("平均PE", f"{avg_pe:.1f}" if pd.notna(avg_pe) else "N/A")
    with m3:
        avg_pb = result["pb"].mean() if "pb" in result.columns else np.nan
        st.metric("平均PB", f"{avg_pb:.2f}" if pd.notna(avg_pb) else "N/A")
    with m4:
        avg_roe = result["roe"].mean() if "roe" in result.columns else np.nan
        st.metric("平均ROE", f"{avg_roe:.1f}%" if pd.notna(avg_roe) else "N/A")

    # 结果表格
    st.subheader("选股结果")
    display, col_config, col_help = format_result_table(result)
    # 构建列格式：数值列限制小数位 + tooltip帮助
    column_cfg = {}
    for col_name, decimals in col_config.items():
        column_cfg[col_name] = st.column_config.NumberColumn(
            format=f"%.{decimals}f",
            help=col_help.get(col_name),
        )
    # 非数值列也加上帮助文本
    for col_name, help_text in col_help.items():
        if col_name not in column_cfg and help_text:
            column_cfg[col_name] = st.column_config.TextColumn(help=help_text)
    st.dataframe(display, column_config=column_cfg, use_container_width=True,
                 height=max(400, min(800, len(result) * 35 + 40)))

    # 导出
    csv_data = display.to_csv(index=False).encode("utf-8-sig")
    safe_name = selected_label.replace("/", "_").replace("（", "(").replace("）", ")")
    st.download_button(
        label="📥 导出为 CSV",
        data=csv_data,
        file_name=f"选股结果_{safe_name}_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

    # 因子分布图（需要先计算因子）
    with st.expander("📊 因子分布对比（全市场 vs 选中）", expanded=False):
        show_factor_charts(compute_all_factors(raw_data), result)


if __name__ == "__main__":
    main()
