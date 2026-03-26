# 《投资策略实战分析》A股自动选股系统 配置文件

# ==================== 数据缓存（过期后自动重新获取，也可手动勾选"强制刷新"立即更新） ====================
CACHE_DIR = "cache"
CACHE_HOURS = 4            # 行情数据缓存时间（小时）
FINANCIAL_CACHE_HOURS = 4  # 财务数据缓存时间（小时）

# ==================== 基准参数 ====================
# 书中使用的基准，A股适配
# 原书"所有股票"基准仅排除微型股（~2亿美元），A股对应约20亿人民币
# 原书"大盘股"基准为市值>数据库平均值（前17%），A股对应约500亿以上
# "龙头股"策略内部通过 _is_turtle() 动量筛选，不依赖固定阈值
MARKET_CAP_LEVELS = {
    "all_stocks":   2e9,   # "所有股票"基准：20亿（排除微型股，适用于单因子/复合价值因子策略）
    "large_cap":    5e10,  # "大盘股"基准：500亿（适用于需要大盘股的策略）
    "turtle":       None,  # 龙头股策略不使用固定阈值，由 _is_turtle() 动态筛选
}
MARKET_CAP_MIN = MARKET_CAP_LEVELS["all_stocks"]  # 默认值（向后兼容）

# ==================== 因子计算参数 ====================
# 排除ST、退市股等
EXCLUDE_TAGS = ["ST", "*ST", "退"]

# ==================== 策略默认参数 ====================
# market_cap_level: 每个策略使用的市值级别，对应 MARKET_CAP_LEVELS 中的键
STRATEGIES = {
    "vc1_top10": {
        "name": "复合价值因子一（VC1）前10%",
        "description": "整合市净率、市盈率、市销率、EV/EBITDA、市现率，选综合得分前10%",
        "market_cap_level": "all_stocks",
    },
    "vc2_top10": {
        "name": "复合价值因子二（VC2）前10%",
        "description": "VC1 + 股东收益率（股息率+回购收益率），选综合得分前10%",
        "market_cap_level": "all_stocks",
    },
    "momentum_value_25": {
        "name": "趋势+价值组合（25只）",
        "description": "VC2前10% + 3/6月涨幅>中位值 + 买入6月涨幅最佳25只",
        "market_cap_level": "all_stocks",
    },
    "momentum_value_50": {
        "name": "趋势+价值组合（50只）",
        "description": "VC2前10% + 3/6月涨幅>中位值 + 买入6月涨幅最佳50只",
        "market_cap_level": "all_stocks",
    },
    "value_growth_25": {
        "name": "价值+增长组合（25只）",
        "description": "EPS增长>0 + 3/6月涨幅>中位值 + 财务质量+VC2前50% + VC2最佳25只",
        "market_cap_level": "all_stocks",
    },
    "turtle_hare": {
        "name": "龙头股策略",
        "description": "大盘龙头 + 3/6月涨幅>中位值 + ROE最高25只",
        "market_cap_level": "turtle",
    },
    "dividend_enhanced": {
        "name": "股息增强型策略",
        "description": "大盘龙头 + 高EBITDA/EV前50% + 股息率最高50只",
        "market_cap_level": "turtle",
    },
    "low_pe_top10": {
        "name": "低市盈率前10%",
        "description": "PE最低的前10%股票",
        "market_cap_level": "all_stocks",
    },
    "low_pb_top20": {
        "name": "低市净率前20%",
        "description": "PB第二低的前10%（书中第2组优于第1组）",
        "market_cap_level": "all_stocks",
    },
}
