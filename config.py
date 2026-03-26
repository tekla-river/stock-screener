# 《投资策略实战分析》A股自动选股系统 配置文件

# ==================== 数据缓存（过期后自动重新获取，也可手动勾选"强制刷新"立即更新） ====================
CACHE_DIR = "cache"
CACHE_HOURS = 4            # 行情数据缓存时间（小时）
FINANCIAL_CACHE_HOURS = 4  # 财务数据缓存时间（小时）

# ==================== 基准参数 ====================
# 书中使用的基准，A股适配
MARKET_CAP_MIN = 2e10       # 最低市值200亿人民币
MARKET_CAP_AVG = None        # None表示动态计算数据库平均值

# ==================== 因子计算参数 ====================
# 排除ST、退市股等
EXCLUDE_TAGS = ["ST", "*ST", "退"]

# ==================== 策略默认参数 ====================
STRATEGIES = {
    "vc1_top10": {
        "name": "复合价值因子一（VC1）前10%",
        "description": "整合市净率、市盈率、市销率、EV/EBITDA、市现率，选综合得分前10%",
    },
    "vc2_top10": {
        "name": "复合价值因子二（VC2）前10%",
        "description": "VC1 + 股东收益率（股息率+回购收益率），选综合得分前10%",
    },
    "momentum_value_25": {
        "name": "趋势+价值组合（25只）",
        "description": "VC2前30% + 3/6月涨幅>中位值 + 买入6月涨幅最佳25只",
    },
    "momentum_value_50": {
        "name": "趋势+价值组合（50只）",
        "description": "VC2前30% + 3/6月涨幅>中位值 + 买入6月涨幅最佳50只",
    },
    "value_growth_25": {
        "name": "价值+增长组合（25只）",
        "description": "EPS增长>0 + 3/6月涨幅>中位值 + 财务质量+VC2前50% + VC2最佳25只",
    },
    "turtle_hare": {
        "name": "龙头股策略",
        "description": "大盘龙头 + 3/6月涨幅>中位值 + ROE最高25只",
    },
    "dividend_enhanced": {
        "name": "股息增强型策略",
        "description": "大盘龙头 + 高EBITDA/EV前50% + 股息率最高50只",
    },
    "low_pe_top10": {
        "name": "低市盈率前10%",
        "description": "PE最低的前10%股票",
    },
    "low_pb_top20": {
        "name": "低市净率前20%",
        "description": "PB第二低的前10%（书中第2组优于第1组）",
    },
}
