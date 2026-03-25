"""
《投资策略实战分析》A股自动选股系统 - 策略引擎
实现config中定义的全部9个选股策略，统一接口 run_strategy(strategy_id, raw_data)
"""

import pandas as pd
import numpy as np
from typing import Tuple

from factors import compute_all_factors
from config import MARKET_CAP_MIN, STRATEGIES


# ============================================================
# 公共筛选工具
# ============================================================

def _apply_market_cap_filter(df: pd.DataFrame) -> pd.DataFrame:
    """最低市值过滤（默认20亿人民币）"""
    if MARKET_CAP_MIN and "总市值" in df.columns:
        return df[df["总市值"] >= MARKET_CAP_MIN].copy()
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
    原始定义：市值>平均 + 现金流>平均 + 流通股>平均 + 销售额>平均的50%
    A股适配：总市值>中位值 + 流通市值>中位值 + 成交额>中位值（替代现金流/销售额）
    """
    required = ["总市值", "流通市值"]
    for c in required:
        if c not in df.columns:
            return df.head(0)
    mask = (
        (df["总市值"] > df["总市值"].median())
        & (df["流通市值"] > df["流通市值"].median())
    )
    if "成交额" in df.columns:
        mask &= df["成交额"] > df["成交额"].median()
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
    df = _apply_market_cap_filter(df)
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
    df = _apply_market_cap_filter(df)
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
    df = _apply_market_cap_filter(df)
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
    df = _apply_market_cap_filter(df)
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    n = max(int(len(valid) * 0.1), 10)
    result = valid.nlargest(n, "vc2_score")

    info = f"从 {len(valid)} 只有效股票中选出 VC2 前 {n} 只"
    return result, info


# ============================================================
# 动量+价值组合策略（书中第25章"潜力股"）
# ============================================================

def _strategy_momentum_value(df: pd.DataFrame, n: int) -> Tuple[pd.DataFrame, str]:
    """
    趋势+价值组合（通用）
    书中规则：VC2前30% → 3/6月涨幅>中位值 → 6月涨幅最佳N只
    """
    df = _apply_market_cap_filter(df)
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    # 步骤1：VC2前30%
    vc2_cutoff = valid["vc2_score"].quantile(0.70)
    vc2_pool = valid[valid["vc2_score"] >= vc2_cutoff].copy()

    # 步骤2：动量过滤
    momentum_pool = _apply_momentum_filter(vc2_pool)
    if len(momentum_pool) == 0:
        momentum_pool = vc2_pool  # 回退：无股票通过动量过滤时使用VC2池

    # 步骤3：按6月涨幅排序，取前N只
    sort_col = "return_6m" if "return_6m" in momentum_pool.columns else "return_3m"
    actual_n = min(n, len(momentum_pool))
    result = momentum_pool.nlargest(actual_n, sort_col)

    info = (
        f"VC2前30%({len(vc2_pool)}只) → "
        f"动量过滤({len(momentum_pool)}只) → "
        f"涨幅最佳{actual_n}只"
    )
    return result, info


def strategy_momentum_value_25(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    return _strategy_momentum_value(df, n=25)


def strategy_momentum_value_50(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    return _strategy_momentum_value(df, n=50)


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
    df = _apply_market_cap_filter(df)
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
    df = _apply_market_cap_filter(df)
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
    股息增强型策略
    书中规则：大盘龙头 + 高EBITDA/EV前50% + 股息率最高50只
    A股适配：龙头股 + 低PE前50%（替代EBITDA/EV） + ROE最高50只
    """
    df = _apply_market_cap_filter(df)
    df = compute_all_factors(df)
    valid = _valid_valuation(df)

    turtles = _is_turtle(valid)

    # 低PE前50%
    if len(turtles) > 0:
        pe_cutoff = turtles["pe"].quantile(0.50)
        pe_pool = turtles[turtles["pe"] <= pe_cutoff].copy()
    else:
        pe_pool = turtles

    # ROE最高50只
    actual_n = min(50, len(pe_pool))
    result = pe_pool.nlargest(actual_n, "roe")

    info = (
        f"龙头股({len(turtles)}只) → "
        f"低PE前50%({len(pe_pool)}只) → "
        f"ROE最高{actual_n}只"
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
