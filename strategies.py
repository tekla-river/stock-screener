"""
《投资策略实战分析》A股自动选股系统 - 策略引擎
实现config中定义的全部9个选股策略，统一接口 run_strategy(strategy_id, raw_data)
"""

import pandas as pd
import numpy as np
from typing import Tuple

from factors import compute_all_factors
from config import MARKET_CAP_LEVELS, STRATEGIES


# ============================================================
# 公共筛选工具
# ============================================================

def _get_market_cap_threshold(strategy_id: str) -> float:
    """根据策略ID获取对应的市值阈值"""
    level = STRATEGIES.get(strategy_id, {}).get("market_cap_level", "all_stocks")
    threshold = MARKET_CAP_LEVELS.get(level, MARKET_CAP_LEVELS["all_stocks"])
    return threshold


def _apply_market_cap_filter(df: pd.DataFrame, strategy_id: str = None, threshold: float = None) -> pd.DataFrame:
    """
    市值过滤。
    优先使用显式传入的 threshold，否则根据 strategy_id 从配置中获取对应级别的阈值。
    threshold=None 表示不进行过滤（龙头股策略由 _is_turtle 动态筛选）。
    """
    if threshold is None and strategy_id is not None:
        threshold = _get_market_cap_threshold(strategy_id)
    if threshold is not None and "总市值" in df.columns:
        return df[df["总市值"] >= threshold].copy()
    return df


def _apply_momentum_filter(df: pd.DataFrame) -> pd.DataFrame:
    """动量过滤：3月和6月涨幅均 > 中位值"""
    mask = pd.Series(True, index=df.index)
    for col in ["return_3m", "return_6m"]:
        if col in df.columns and df[col].notna().any():
            mask &= df[col] > df[col].median()
    return df[mask].copy()


def _is_turtle(df: pd.DataFrame) -> pd.DataFrame:
    """
    龙头股筛选（书中定义的"龙头"）
    原始定义：市值>平均值 + 流通股>平均值 + 现金流>平均值 + 销售额>平均值的150%
    A股适配：总市值>平均值 + 流通市值>平均值 + 营业收入>平均值×150%（替代销售额）
             + 现金流>平均值（若有） + 排除公用事业
    """
    required = ["总市值", "流通市值"]
    for c in required:
        if c not in df.columns:
            return df.head(0)
    mask = (
        (df["总市值"] > df["总市值"].mean())
        & (df["流通市值"] > df["流通市值"].mean())
    )
    # 营业收入 > 平均值 × 150%（书中：销售额 > 平均值的150%）
    revenue_col = None
    for col_name in ["营业收入", "营业总收入", "OPERATE_INCOME"]:
        if col_name in df.columns:
            revenue_col = col_name
            break
    if revenue_col:
        revenue = pd.to_numeric(df[revenue_col], errors="coerce")
        mask &= revenue > revenue.mean() * 1.5

    # 成交额 > 平均值（替代现金流的A股适配）
    if "成交额" in df.columns:
        mask &= df["成交额"] > df["成交额"].mean()

    # 排除公用事业（书中明确要求）
    if "行业" in df.columns:
        mask &= ~df["行业"].astype(str).str.contains("电力|燃气|水务|环保|公用", na=False)
    elif "所属行业" in df.columns:
        mask &= ~df["所属行业"].astype(str).str.contains("电力|燃气|水务|环保|公用", na=False)

    return df[mask].copy()


def _valid_valuation(df: pd.DataFrame) -> pd.DataFrame:
    """排除估值异常的股票（PE≤0 或 PB≤0 或 缺失）"""
    valid = df[(df["pe"] > 0) & (df["pb"] > 0)].copy()
    # 额外标记 EBITDA/EV 为负的股票（不排除，但VC1排名会受影响）
    if "ebitda_ev" in valid.columns:
        valid.loc[valid["ebitda_ev"].isna(), "ebitda_ev"] = np.nan
    return valid


# ============================================================
# 单因素策略
# ============================================================

def strategy_low_pe_top10(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    低市盈率前10%
    书中结论：低PE组长期显著跑赢高PE组
    """
    df = _apply_market_cap_filter(df, "low_pe_top10")
    df = compute_all_factors(df)
    valid = df[df["pe"].notna() & (df["pe"] > 0)].copy()

    n = max(int(len(valid) * 0.1), 10)
    result = valid.nsmallest(n, "pe")

    info = f"从 {len(valid)} 只有效股票中选出 PE 最低 {n} 只"
    return result, info


def strategy_low_pb_top20(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    低市净率前20%（书中"第2组"）
    书中结论：PB最低的10%包含价值陷阱，第2组（10%~30%）更优
    """
    df = _apply_market_cap_filter(df, "low_pb_top20")
    df = compute_all_factors(df)
    valid = df[df["pb"].notna() & (df["pb"] > 0)].copy()

    q10 = valid["pb"].quantile(0.10)
    q30 = valid["pb"].quantile(0.30)
    group2 = valid[(valid["pb"] >= q10) & (valid["pb"] < q30)].copy()

    if len(group2) == 0:
        n = max(int(len(valid) * 0.2), 10)
        result = valid.nsmallest(n, "pb")
        info = f"第二组为空，回退到PB最低 {n} 只"
    else:
        result = group2
        info = f"PB第2组（10%~30%分位），共 {len(result)} 只"

    return result, info


# ============================================================
# 复合价值因子策略
# ============================================================

def strategy_vc1_top10(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    复合价值因子一（VC1）前10%
    整合 PE + PB + PS + 市现率，综合得分最高的10% = 最便宜的10%
    """
    df = _apply_market_cap_filter(df, "vc1_top10")
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    n = max(int(len(valid) * 0.1), 10)
    result = valid.nlargest(n, "vc1_score")

    info = f"从 {len(valid)} 只有效股票中选出 VC1 前 {n} 只"
    return result, info


def strategy_vc2_top10(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    复合价值因子二（VC2）前10%
    VC1 + 股东收益率（A股简化为股息率）
    """
    df = _apply_market_cap_filter(df, "vc2_top10")
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    n = max(int(len(valid) * 0.1), 10)
    result = valid.nlargest(n, "vc2_score")

    info = f"从 {len(valid)} 只有效股票中选出 VC2 前 {n} 只"
    return result, info


# ============================================================
# 动量+价值组合策略（书中第25章"潜力股"）
# ============================================================

def _strategy_momentum_value(df: pd.DataFrame, n: int, strategy_id: str) -> Tuple[pd.DataFrame, str]:
    """
    趋势+价值组合（通用）
    书中规则（第27章）：VC2前10% → 6月价格增值最佳N只
    """
    df = _apply_market_cap_filter(df, strategy_id)
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    # 步骤1：VC2前10%（第1组十分位）
    vc2_cutoff = valid["vc2_score"].quantile(0.90)
    vc2_pool = valid[valid["vc2_score"] >= vc2_cutoff].copy()

    # 步骤2：动量过滤（书中仅按6月涨幅排序，但添加动量预过滤可提升实际效果）
    momentum_pool = _apply_momentum_filter(vc2_pool)
    if len(momentum_pool) == 0:
        momentum_pool = vc2_pool  # 回退：无股票通过动量过滤时使用VC2池

    # 步骤3：按6月涨幅排序，取前N只
    sort_col = "return_6m" if "return_6m" in momentum_pool.columns else "return_3m"
    actual_n = min(n, len(momentum_pool))
    result = momentum_pool.nlargest(actual_n, sort_col)

    info = (
        f"VC2前10%({len(vc2_pool)}只) → "
        f"动量过滤({len(momentum_pool)}只) → "
        f"涨幅最佳{actual_n}只"
    )
    return result, info


def strategy_momentum_value_25(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    return _strategy_momentum_value(df, n=25, strategy_id="momentum_value_25")


def strategy_momentum_value_50(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    return _strategy_momentum_value(df, n=50, strategy_id="momentum_value_50")


# ============================================================
# 价值+增长组合策略（书中第26章）
# ============================================================

def strategy_value_growth_25(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    价值+增长组合（25只）
    书中规则：
      1. EPS增长>0（或净利润增长>0）
      2. 3月和6月涨幅>中位值
      3. 财务实力+收益质量+VC2 综合排名前50%
      4. 按VC2排序取最佳25只
    """
    df = _apply_market_cap_filter(df, "value_growth_25")
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    # 步骤1：盈利增长过滤
    growth_col = "profit_growth" if "profit_growth" in valid.columns else "revenue_growth"
    if growth_col in valid.columns and valid[growth_col].notna().any():
        valid = valid[valid[growth_col] > 0].copy()

    # 步骤2：动量过滤
    pool = _apply_momentum_filter(valid)
    if len(pool) == 0:
        pool = valid

    # 步骤3：综合评分前50%
    pool["combined"] = (
        pool["financial_strength"].fillna(50)
        + pool["earnings_quality"].fillna(50)
        + pool["vc2_score"].fillna(0)
    )
    cutoff = pool["combined"].quantile(0.50)
    top_half = pool[pool["combined"] >= cutoff].copy()

    # 步骤4：VC2最佳25只
    actual_n = min(25, len(top_half))
    result = top_half.nlargest(actual_n, "vc2_score")

    info = (
        f"盈利增长过滤 → 动量过滤({len(pool)}只) → "
        f"综合前50%({len(top_half)}只) → VC2最佳{actual_n}只"
    )
    return result, info


# ============================================================
# 龙头股策略（书中第22章）
# ============================================================

def strategy_turtle_hare(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    龙头股策略
    书中规则：龙头股 → 3/6月涨幅>中位值 → ROE最高25只
    """
    df = _apply_market_cap_filter(df, "turtle_hare")
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    turtles = _is_turtle(valid)
    pool = _apply_momentum_filter(turtles)
    if len(pool) == 0:
        pool = turtles

    actual_n = min(25, len(pool))
    result = pool.nlargest(actual_n, "roe")

    info = (
        f"龙头股({len(turtles)}只) → "
        f"动量过滤({len(pool)}只) → "
        f"ROE最高{actual_n}只"
    )
    return result, info


# ============================================================
# 股息增强型策略
# ============================================================

def strategy_dividend_enhanced(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """
    股息增强型策略（书中规则）
    书中规则：
      1. 从龙头股中选出
      2. 按 EBITDA/EV 排序，剔除后50%（保留前50%）
      3. 选出股息率最高的50只
      4. 按股息率分层加权（代码中暂用等权）
    A股适配：若 EBITDA/EV 不可用则回退到低PE，若股息率不可用则回退到ROE
    """
    df = _apply_market_cap_filter(df, "dividend_enhanced")
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    turtles = _is_turtle(valid)

    if len(turtles) == 0:
        info = "龙头股筛选后为空，无法继续"
        return turtles, info

    # 步骤2：按 EBITDA/EV 排序，保留前50%（书中原文）
    ebitda_available = "ebitda_ev" in turtles.columns and turtles["ebitda_ev"].notna().sum() > 10
    if ebitda_available:
        turtles = turtles[turtles["ebitda_ev"] > 0].copy()  # 排除负值
        if len(turtles) > 0:
            cutoff = turtles["ebitda_ev"].quantile(0.50)
            value_pool = turtles[turtles["ebitda_ev"] >= cutoff].copy()
        else:
            value_pool = turtles
        pool_label = f"EBITDA/EV前50%"
    else:
        # 回退：使用PE替代（A股适配）
        pe_cutoff = turtles["pe"].quantile(0.50)
        value_pool = turtles[turtles["pe"] <= pe_cutoff].copy()
        pool_label = "低PE前50%(回退)"

    if len(value_pool) == 0:
        value_pool = turtles
        pool_label = "全部龙头股(回退)"

    # 步骤3：股息率最高50只
    div_available = "dividend_yield" in value_pool.columns and value_pool["dividend_yield"].notna().sum() > 10
    if div_available:
        actual_n = min(50, len(value_pool))
        result = value_pool.nlargest(actual_n, "dividend_yield")
        sort_label = "股息率最高"
    else:
        # 回退：ROE最高
        actual_n = min(50, len(value_pool))
        result = value_pool.nlargest(actual_n, "roe")
        sort_label = "ROE最高(回退)"

    info = (
        f"龙头股({len(turtles)}只) → "
        f"{pool_label}({len(value_pool)}只) → "
        f"{sort_label}{actual_n}只"
    )
    return result, info


# ============================================================
# 策略注册表 & 调度
# ============================================================

STRATEGY_FUNCS = {
    "vc1_top10":            strategy_vc1_top10,
    "vc2_top10":            strategy_vc2_top10,
    "momentum_value_25":    strategy_momentum_value_25,
    "momentum_value_50":    strategy_momentum_value_50,
    "value_growth_25":      strategy_value_growth_25,
    "turtle_hare":          strategy_turtle_hare,
    "dividend_enhanced":    strategy_dividend_enhanced,
    "low_pe_top10":         strategy_low_pe_top10,
    "low_pb_top20":         strategy_low_pb_top20,
}


def run_strategy(strategy_id: str, raw_data: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """运行指定策略，返回 (结果DataFrame, 说明文字)"""
    if strategy_id not in STRATEGY_FUNCS:
        raise ValueError(f"未知策略: {strategy_id}，可选: {list(STRATEGY_FUNCS.keys())}")
    return STRATEGY_FUNCS[strategy_id](raw_data)


def run_all_strategies(raw_data: pd.DataFrame) -> dict:
    """运行全部策略，返回 {strategy_id: {name, data, info, count}}"""
    results = {}
    for sid, cfg in STRATEGIES.items():
        name = cfg["name"]
        print(f"\n>>> 运行策略: {name} ...")
        try:
            data, info = run_strategy(sid, raw_data)
            results[sid] = {"name": name, "data": data, "info": info, "count": len(data)}
        except Exception as e:
            results[sid] = {"name": name, "data": pd.DataFrame(), "info": f"失败: {e}", "count": 0}
            print(f"    [错误] {e}")
    return results
