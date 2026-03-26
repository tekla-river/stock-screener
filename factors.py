"""
《投资策略实战分析》A股自动选股系统 - 因子计算模块
实现书中核心选股因子：PE、PB、PS、市现率、ROE、动量等，以及VC1/VC2复合价值因子
"""

import pandas as pd
import numpy as np
from typing import Optional


# ============================================================
# 工具函数
# ============================================================

def percentile_rank(series: pd.Series, ascending: bool = True) -> pd.Series:
    """
    百分位排名（0~100）
    ascending=True: 值越小排名分数越高（适用于PE、PB等，越小越便宜）
    ascending=False: 值越大排名分数越高（适用于ROE等，越大越好）
    """
    valid = series.dropna()
    if len(valid) == 0:
        return pd.Series(np.nan, index=series.index)

    ranks = valid.rank(pct=True, ascending=ascending) * 100
    result = pd.Series(np.nan, index=series.index)
    result.update(ranks)
    return result


def safe_divide(numerator: pd.Series, denominator: pd.Series,
                fill_value: float = np.nan) -> pd.Series:
    """安全除法，除零返回fill_value"""
    result = pd.Series(fill_value, index=numerator.index, dtype=float)
    mask = denominator.notna() & (denominator != 0) & numerator.notna()
    result[mask] = numerator[mask] / denominator[mask]
    return result


def _find_column(df: pd.DataFrame, candidates: list) -> Optional[str]:
    """
    在DataFrame中查找列名（优先精确匹配，其次包含匹配）
    用于兼容不同版本akshare返回的不同列名
    """
    # 精确匹配
    for col in candidates:
        if col in df.columns:
            return col
    # 包含匹配
    for candidate in candidates:
        for c in df.columns:
            if candidate in str(c):
                return c
    return None


# ============================================================
# 基础因子（仅依赖市场行情数据）
# ============================================================

def compute_basic_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算基础因子（来自市场行情数据）

    新增列:
        pe, pb           - 市盈率、市净率（原始值）
        total_mv_yi      - 总市值（亿元）
        circ_mv_yi       - 流通市值（亿元）
        return_3m        - 近3月涨跌幅(%)
        return_6m        - 近6月涨跌幅(%)
        pe_rank, pb_rank - PE、PB百分位排名
    """
    df = df.copy()

    # 市值（亿元）
    if "总市值" in df.columns:
        df["total_mv_yi"] = pd.to_numeric(df["总市值"], errors="coerce") / 1e8
    if "流通市值" in df.columns:
        df["circ_mv_yi"] = pd.to_numeric(df["流通市值"], errors="coerce") / 1e8

    # PE / PB
    pe_col = _find_column(df, ["市盈率-动态", "市盈率"])
    pb_col = _find_column(df, ["市净率"])
    df["pe"] = pd.to_numeric(df[pe_col], errors="coerce") if pe_col else np.nan
    if pb_col:
        df["pb"] = pd.to_numeric(df[pb_col], errors="coerce")
    else:
        # PB 将在 compute_financial_factors 中用 BPS 补充计算
        df["pb"] = np.nan

    # 动量因子：优先使用近3月/6月列，回退到60日列
    for target, candidates in [
        ("return_3m", ["近3月涨跌幅", "近三月涨跌幅", "60日涨跌幅"]),
        ("return_6m", ["近6月涨跌幅", "近六月涨跌幅", "年初至今涨跌幅"]),
    ]:
        col = _find_column(df, candidates)
        if col:
            df[target] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[target] = np.nan

    # 百分位排名
    df["pe_rank"] = percentile_rank(df["pe"], ascending=True)   # PE越低分越高
    df["pb_rank"] = percentile_rank(df["pb"], ascending=True)

    return df


# ============================================================
# 财务因子（依赖财务报表数据）
# ============================================================

def compute_financial_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算财务因子（来自业绩报表数据）

    新增列:
        roe            - 净资产收益率(%)
        eps            - 每股收益(元)
        ocfps          - 每股经营现金流净额(元)
        gross_margin   - 销售毛利率(%)
        net_margin     - 销售净利率(%)
        revenue_growth - 营业收入同比增长(%)
        profit_growth  - 净利润同比增长(%)
        bps            - 每股净资产(元)
        ps             - 市销率（近似 = PE × 净利率/100）
        price_cf       - 价格/现金流比
        ebitda         - 息税折旧摊销前利润(元)
        ev             - 企业价值(元)
        ebitda_ev      - EBITDA/EV (%)
        roe_rank       - ROE百分位排名
        net_margin_rank - 净利率百分位排名
    """
    df = df.copy()

    # 提取各财务指标（兼容不同列名）
    def _get(col_candidates):
        col = _find_column(df, col_candidates)
        return pd.to_numeric(df[col], errors="coerce") if col else pd.Series(np.nan, index=df.index)

    df["roe"]            = _get(["净资产收益率", "加权净资产收益率", "ROE"])
    df["eps"]            = _get(["每股收益", "摊薄每股收益", "基本每股收益"])
    df["ocfps"]          = _get(["每股经营现金流净额", "每股经营现金流"])
    df["gross_margin"]   = _get(["销售毛利率", "毛利率"])
    df["net_margin"]     = _get(["销售净利率", "净利率"])
    df["revenue_growth"] = _get(["营业收入-同比增长", "营收同比增长"])
    df["profit_growth"]  = _get(["净利润-同比增长", "净利润同比增长"])
    df["bps"]            = _get(["每股净资产"])

    # PB = 最新价 / 每股净资产（若行情未提供则用 BPS 计算）
    if df["pb"].isna().all() and df["bps"].notna().any():
        price_col = _find_column(df, ["最新价"])
        if price_col:
            price = pd.to_numeric(df[price_col], errors="coerce")
            df["pb"] = safe_divide(price, df["bps"])
            print(f"[因子] PB 由 BPS 计算补充，有效 {df['pb'].notna().sum()} 只")

    # 派生因子
    # PS ≈ PE × 净利率/100 （PS = Price / Revenue_per_share = PE × (Net_Profit/Revenue)）
    df["ps"] = df["pe"] * df["net_margin"] / 100

    # 价格/现金流比 = 股价 / 每股经营现金流
    price_col = _find_column(df, ["最新价"])
    if price_col:
        price = pd.to_numeric(df[price_col], errors="coerce")
        df["price_cf"] = safe_divide(price, df["ocfps"])
    else:
        df["price_cf"] = np.nan

    # EBITDA / EV 计算
    # EBITDA = 营业利润 + 财务费用（财务费用≈利息支出，折旧摊销在简化版报表中不可得）
    # 注意：此处折旧摊销未单独获取，EBITDA会偏低，但在全市场百分位排名中仍然有效
    operate_profit_report = _get(["营业利润_报表", "OPERATE_PROFIT"])
    finance_expense = _get(["财务费用", "FINANCE_EXPENSE"])
    total_liabilities = _get(["总负债", "TOTAL_LIABILITIES"])
    monetary_funds = _get(["货币资金", "MONETARYFUNDS"])
    total_mv = pd.to_numeric(df.get("总市值"), errors="coerce")  # 已经是元

    df["ebitda"] = operate_profit_report + finance_expense.abs()
    # EV = 总市值 + 总负债 - 货币资金
    df["ev"] = total_mv + total_liabilities - monetary_funds
    # EBITDA/EV (%)
    df["ebitda_ev"] = safe_divide(df["ebitda"], df["ev"]) * 100
    ebitda_valid = df["ebitda_ev"].notna().sum()
    print(f"[因子] EBITDA/EV 计算完成，有效 {ebitda_valid} 只")

    # 排名因子
    df["roe_rank"]        = percentile_rank(df["roe"], ascending=False)
    df["net_margin_rank"] = percentile_rank(df["net_margin"], ascending=False)

    return df


# ============================================================
# 复合价值因子 VC1 / VC2
# ============================================================

def compute_vc1(df: pd.DataFrame) -> pd.DataFrame:
    """
    复合价值因子一（VC1）

    书中原始因子：市净率 + 市盈率 + 市销率 + EBITDA/EV + 市现率
    A股实现：    PB    + PE    + PS     + EBITDA/EV + 市现率

    每个因子按百分位排序（值越小→分数越高），加总为VC1分数。
    VC1_score越高 = 综合估值越低 = 越便宜。
    """
    df = df.copy()

    # 优先使用 EBITDA/EV，若不可用则回退到 price_cf
    vc1_factors = ["pe", "pb", "ps"]
    if "ebitda_ev" in df.columns and df["ebitda_ev"].notna().sum() > 10:
        vc1_factors.append("ebitda_ev")
        print("[VC1] 使用真正的 EBITDA/EV 因子，不额外加入市现率（书中VC1共5个因子）")
    else:
        vc1_factors.append("price_cf")
        print("[VC1] EBITDA/EV 数据不足，回退使用市现率替代")

    # 各因子百分位：值越小 → 越便宜 → 分数越高（用100-pct反转）
    score = pd.Series(0.0, index=df.index)

    for f in vc1_factors:
        if f in df.columns:
            pct = percentile_rank(df[f], ascending=True)  # 值越小pct越高
            df[f"vc1_{f}"] = 100 - pct.fillna(50)
            score += df[f"vc1_{f}"]
        else:
            score += 50  # 缺失因子按中位值计

    df["vc1_score"] = score
    df["vc1_rank"] = percentile_rank(df["vc1_score"], ascending=False)

    return df


def compute_vc2(df: pd.DataFrame) -> pd.DataFrame:
    """
    复合价值因子二（VC2）= VC1 + 股东收益率

    书中：股东收益率 = 股息率 + 回购收益率
    A股：回购数据难以批量获取，简化为股息率（若有），否则仅使用VC1
    """
    df = df.copy()

    if "vc1_score" not in df.columns:
        df = compute_vc1(df)

    # 尝试获取股息率
    div_col = _find_column(df, ["股息率", "股息收益率"])
    if div_col:
        df["dividend_yield"] = pd.to_numeric(df[div_col], errors="coerce")
        sy_pct = percentile_rank(df["dividend_yield"], ascending=False)
        df["vc2_sy"] = 100 - sy_pct.fillna(50)
    else:
        df["dividend_yield"] = np.nan
        df["vc2_sy"] = 50  # 缺失按中位值

    df["vc2_score"] = df["vc1_score"] + df["vc2_sy"]
    df["vc2_rank"] = percentile_rank(df["vc2_score"], ascending=False)

    return df


# ============================================================
# 复合评分因子（财务实力、收益质量）
# ============================================================

def compute_composite_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算复合评分（简化版）

    financial_strength - 财务实力 = ROE排名 + 净利率排名
    earnings_quality   - 收益质量 = 经营现金流/市值排名
    """
    df = df.copy()

    # 财务实力
    df["financial_strength"] = (
        df.get("roe_rank", pd.Series(50, index=df.index)).fillna(50)
        + df.get("net_margin_rank", pd.Series(50, index=df.index)).fillna(50)
    )

    # 收益质量：每股经营现金流 / 股价（越高说明现金流越充裕）
    price_col = _find_column(df, ["最新价"])
    if price_col and "ocfps" in df.columns:
        price = pd.to_numeric(df[price_col], errors="coerce")
        cf_yield = safe_divide(df["ocfps"], price) * 100  # 转为百分比
        df["earnings_quality"] = percentile_rank(cf_yield, ascending=False).fillna(50)
    else:
        df["earnings_quality"] = 50.0

    return df


# ============================================================
# 主入口：计算全部因子
# ============================================================

def compute_all_factors(df: pd.DataFrame) -> pd.DataFrame:
    """计算所有因子（按依赖顺序）"""
    df = compute_basic_factors(df)
    df = compute_financial_factors(df)
    df = compute_vc1(df)
    df = compute_vc2(df)
    df = compute_composite_scores(df)
    return df
