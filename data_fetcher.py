"""
《投资策略实战分析》A股自动选股系统 - 数据获取模块
数据源架构：
  - 股票列表：东方财富数据中心（批量获取全部A股代码）
  - 行情数据：腾讯行情接口 qt.gtimg.cn（主，稳定快速，含动量数据）
              → 东方财富push2（回退）
  - 财务数据：东方财富数据中心（批量，免费无限制）
  - 动量数据：腾讯行情接口自带（近3月/6月涨跌幅）
"""

import pandas as pd
import numpy as np
import requests
import os
import time
import concurrent.futures
import threading
from typing import Optional, List, Dict, Any

from config import (
    CACHE_DIR, CACHE_HOURS, FINANCIAL_CACHE_HOURS, EXCLUDE_TAGS,
)


# ============================================================
# 网络请求配置
# ============================================================

MAX_RETRIES = 3
RETRY_DELAY = 5
REQUEST_TIMEOUT = 30
FINANCIAL_MAX_WORKERS = 10
FINANCIAL_RATE_LIMIT = 8

# 长连接session（减少TCP握手，降低被限流概率）
_http_session = requests.Session()
_http_session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/",
})


class _RateLimiter:
    """线程安全的速率限制器"""
    def __init__(self, calls_per_second: float):
        self.min_interval: float = 1.0 / calls_per_second
        self._lock = threading.Lock()
        self._last: float = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            gap = self._last + self.min_interval - now
            if gap > 0:
                time.sleep(gap)
            self._last = time.time()


_rate_limiter = _RateLimiter(FINANCIAL_RATE_LIMIT)


def _http_get(url: str, params: dict = None, retries: int = MAX_RETRIES) -> Any:
    """通用HTTP GET，带重试"""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = _http_session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            # 429 额度超限，不再重试
            if resp.status_code == 429:
                msg = resp.text.strip() if resp.text else "429 Too Many Requests"
                raise RuntimeError(f"API额度已用完: {msg}")
            resp.raise_for_status()
            return resp.json()
        except RuntimeError:
            raise  # 429等明确错误直接抛出
        except (requests.ConnectionError, requests.Timeout, OSError) as e:
            last_err = e
            wait = RETRY_DELAY * attempt
            time.sleep(wait)
        except requests.HTTPError as e:
            raise RuntimeError(f"HTTP错误 {e.response.status_code}: {e.response.text[:200]}")
    raise RuntimeError(f"请求失败({retries}次重试): {url[:80]} → {last_err}")








# ============================================================
# 缓存工具
# ============================================================

def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_age_hours(filepath: str) -> float:
    if not os.path.exists(filepath):
        return float("inf")
    return (time.time() - os.path.getmtime(filepath)) / 3600


def _is_cache_valid(filepath: str, max_hours: float) -> bool:
    if not os.path.exists(filepath):
        return False
    # max_hours=0 表示永不过期（仅手动刷新）
    if max_hours == 0:
        return True
    return _cache_age_hours(filepath) < max_hours


def _save_cache(df: pd.DataFrame, filename: str):
    _ensure_cache_dir()
    df.to_csv(os.path.join(CACHE_DIR, filename), index=False)


def _load_cache(filename: str, dtype_cols: dict = None) -> Optional[pd.DataFrame]:
    path = os.path.join(CACHE_DIR, filename)
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, dtype=dtype_cols)
    except Exception:
        return None


# ============================================================
# 市场行情数据
# 主数据源：腾讯行情接口 qt.gtimg.cn（稳定、快速、含动量数据）
# 回退方案：东方财富push2（可能被IP限流）
# ============================================================

# 腾讯行情接口字段映射（88字段版本）
# 索引: 1=名称, 2=代码, 3=当前价, 4=昨收, 5=今开, 6=成交量(手),
#        31=涨跌幅(%), 32=涨跌额, 33=最高, 34=最低,
#        37=成交额(万), 38=换手率(%), 39=PE(动态),
#        44=流通市值(亿), 45=总市值(亿), 46=股息率(%),
#        62=近3月涨跌幅(%), 63=近6月涨跌幅(%)
QT_FIELD_INDEX = {
    "名称": 1, "代码": 2, "最新价": 3, "昨收": 4, "开盘": 5,
    "成交量": 6, "涨跌幅": 31, "涨跌额": 32, "最高": 33, "最低": 34,
    "成交额": 37, "换手率": 38, "市盈率-动态": 39,
    "流通市值": 44, "总市值": 45, "股息率": 46,
    "近3月涨跌幅": 62, "近6月涨跌幅": 63,
}

QT_NUMERIC_FIELDS = [
    "最新价", "昨收", "开盘", "成交量", "涨跌幅", "涨跌额",
    "最高", "最低", "成交额", "换手率", "市盈率-动态",
    "流通市值", "总市值", "股息率",
    "近3月涨跌幅", "近6月涨跌幅",
]

# 成交额单位转换：(万→元)，市值单位转换：(亿→元)
QT_UNIT_SCALE = {
    "成交额": 1e4,        # 万→元
    "流通市值": 1e8,      # 亿→元
    "总市值": 1e8,        # 亿→元
}


def _code_to_qt(code: str) -> Optional[str]:
    """A股代码转腾讯格式: 0/3开头用sz，6开头用sh"""
    code = str(code).zfill(6)
    if code.startswith(("0", "3")):
        return f"sz{code}"
    elif code.startswith("6"):
        return f"sh{code}"
    return None


def _parse_qt_line(line: str) -> Optional[Dict]:
    """解析单条腾讯行情返回"""
    import re
    m = re.match(r'v_(\w+)=(.+)', line)
    if not m:
        return None
    fields = m.group(2).strip('"').split("~")
    if len(fields) < 64 or not fields[2]:
        return None

    record = {}
    for col, idx in QT_FIELD_INDEX.items():
        if idx < len(fields):
            record[col] = fields[idx]
    return record


def _generate_a_share_codes() -> List[str]:
    """生成全部A股可能的代码范围（腾讯接口会自动忽略无效代码）"""
    codes = []
    # 深圳主板: 000001-004999
    codes.extend(f"{i:06d}" for i in range(1, 5000))
    # 中小板: 002001-002999
    codes.extend(f"{i:06d}" for i in range(2001, 3000))
    # 创业板: 300001-301999
    codes.extend(f"{i:06d}" for i in range(300001, 302000))
    # 上海主板: 600000-605999
    codes.extend(f"{i:06d}" for i in range(600000, 606000))
    # 科创板: 688001-688999
    codes.extend(f"{i:06d}" for i in range(688001, 689000))
    return codes


def _get_market_from_tencent(codes: List[str] = None) -> pd.DataFrame:
    """
    通过腾讯行情接口批量获取A股行情
    特点：稳定（腾讯系接口）、快速（每批800条，全量~7批=~2秒）
    额外提供：近3月/6月涨跌幅（动量数据）
    """
    print("正在获取A股行情（腾讯行情接口）...")
    start_time = time.time()

    # 获取股票代码列表
    if codes is None:
        codes = _generate_a_share_codes()

    # 过滤有效代码并转换格式
    qt_codes = []
    for c in codes:
        qt = _code_to_qt(c)
        if qt:
            qt_codes.append(qt)

    if not qt_codes:
        raise RuntimeError("无有效股票代码")

    # 分批请求（每批800条）
    BATCH_SIZE = 800
    all_records = []

    for i in range(0, len(qt_codes), BATCH_SIZE):
        batch = qt_codes[i:i + BATCH_SIZE]
        try:
            url = "https://qt.gtimg.cn/q=" + ",".join(batch)
            resp = _http_session.get(url, timeout=REQUEST_TIMEOUT)
            resp.encoding = "gbk"
            for line in resp.text.strip().split("\n"):
                record = _parse_qt_line(line)
                if record:
                    all_records.append(record)
        except Exception as e:
            print(f"  [警告] 批次请求失败: {e}")

    if not all_records:
        raise RuntimeError("腾讯行情接口返回空数据")

    df = pd.DataFrame(all_records)

    # 标准化代码
    if "代码" in df.columns:
        df["代码"] = df["代码"].astype(str).str.zfill(6)

    # 数值化
    for col in QT_NUMERIC_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 单位转换（万/亿→元）
    for col, scale in QT_UNIT_SCALE.items():
        if col in df.columns:
            df[col] = df[col] * scale

    elapsed = time.time() - start_time
    print(f"[完成] 腾讯接口获取 {len(df)} 只股票行情（{elapsed:.1f}s）")
    return df


# ---- 东方财富push2接口（回退方案）----

EM_FIELD_MAP = {
    "f2": "最新价", "f3": "涨跌幅", "f4": "涨跌额", "f5": "成交量",
    "f6": "成交额", "f7": "振幅", "f8": "换手率", "f9": "市盈率-动态",
    "f10": "量比", "f12": "代码", "f14": "名称", "f15": "最高",
    "f16": "最低", "f17": "开盘", "f18": "昨收", "f20": "总市值",
    "f21": "流通市值", "f23": "市净率", "f62": "流通股本", "f136": "股息率",
}

EM_NUMERIC_FIELDS = [
    "最新价", "涨跌幅", "涨跌额", "成交量", "成交额",
    "振幅", "换手率", "市盈率-动态", "量比",
    "最高", "最低", "开盘", "昨收",
    "总市值", "流通市值", "市净率",
    "流通股本", "股息率",
]


def _get_market_from_eastmoney() -> pd.DataFrame:
    """东方财富push2接口（回退方案，可能被IP限流）"""
    print("正在获取A股行情（东方财富push2回退）...")
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "6000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f12",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": ",".join(EM_FIELD_MAP.keys()),
    }

    data = _http_get(url, params=params)
    items = data.get("data", {}).get("diff", [])
    if not items:
        raise RuntimeError("东方财富push2返回空数据")

    df = pd.DataFrame(items)
    df = df.rename(columns=EM_FIELD_MAP)

    if "代码" in df.columns:
        df["代码"] = df["代码"].astype(str).str.zfill(6)

    for col in EM_NUMERIC_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"[完成] 东方财富获取 {len(df)} 只股票行情")
    return df





def get_market_data(force_refresh: bool = False) -> pd.DataFrame:
    """
    获取全A股行情数据
    优先级：缓存 > 腾讯行情接口(~2秒,含动量) > 东方财富push2(~1秒)
    """
    _ensure_cache_dir()
    cache_path = os.path.join(CACHE_DIR, "market_data.csv")

    if not force_refresh:
        cached = _load_cache("market_data.csv", dtype_cols={"代码": str})
        if cached is not None and _is_cache_valid(cache_path, CACHE_HOURS):
            age = _cache_age_hours(cache_path)
            print(f"[缓存] 行情数据 {len(cached)} 只（{age:.1f}h前）")
            return cached

    # 策略1: 腾讯行情接口（稳定，含动量数据）
    try:
        df = _get_market_from_tencent()
        _save_cache(df, "market_data.csv")
        return df
    except Exception as e:
        print(f"[警告] 腾讯接口失败({e})，尝试东方财富push2...")

    # 策略2: 东方财富push2
    try:
        df = _get_market_from_eastmoney()
        _save_cache(df, "market_data.csv")
        return df
    except Exception as e:
        raise RuntimeError(f"行情数据获取失败（腾讯+东方财富均不可用）: {e}")


# ============================================================
# 动量数据（腾讯行情接口已自带近3月/6月涨跌幅，无需额外获取）
# ============================================================
# ============================================================

def get_momentum_data(codes: List[str] = None, force_refresh: bool = False,
                      progress_callback=None) -> None:
    """
    动量数据已由腾讯行情接口提供（近3月/6月涨跌幅），无需额外获取。
    保留此函数签名以兼容调用方。
    """
    return None


# ============================================================
# 财务数据 — 东方财富数据中心（批量获取，免费无限制）
# ============================================================

# 东方财富数据中心接口地址
EM_DATA_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

# 东方财富数据中心字段映射 → factors.py 需要的列名
EM_FIN_FIELD_MAP = {
    "SECURITY_CODE":    "code",
    "SECURITY_NAME_ABBR": "em_name",
    "REPORT_DATE":      "报告期",
    "EPSJB":            "每股收益",          # 基本每股收益
    "BPS":              "每股净资产",
    "MGJYXJJE":         "每股经营现金流净额",
    "ROEJQ":            "加权净资产收益率",
    "ROEKCJQ":          "扣非加权净资产收益率",
    "XSJLL":            "销售净利率",
    "PARENTNETPROFITTZ": "净利润-同比增长",
    "TOTALOPERATEREVETZ": "营业收入-同比增长",
    "ZCFZL":            "资产负债率",
    "OPERATE_INCOME_PK": "营业收入",         # 营业收入(元)
    "OPERATE_PROFIT_PK": "营业利润",          # 营业利润(元)
    "PARENTNETPROFIT":  "净利润",            # 归母净利润(元)
    "TOTAL_ASSETS_PK":  "总资产",
    "TOTAL_EQUITY_PK":  "净资产",
    "NETCASH_OPERATE_PK": "经营现金流净额",
    "NETCASH_INVEST_PK": "投资现金流净额",     # 用于估算CAPEX
    "TOTAL_SHARE":      "总股本",
    "A_FREE_SHARE":     "流通股本_em",       # 流通股本(股)
    "INTEREST_DEBT_RATIO": "有息负债率",
    "INTEREST_COVERAGE_RATIO": "利息保障倍数",  # EBIT/利息支出
    "TAXRATE":          "所得税率",
    "FCFF_BACK":        "公司自由现金流",
    "ROIC":             "投入资本回报率",
    # EBITDA/EV 计算用字段（来自利润表和资产负债表）
    "OPERATE_PROFIT":   "营业利润_报表",          # 利润表: 营业利润(元)
    "FINANCE_EXPENSE":  "财务费用",               # 利润表: 财务费用(元)≈利息支出
    "TOTAL_LIABILITIES": "总负债",               # 资产负债表: 总负债(元)
    "MONETARYFUNDS":    "货币资金",               # 资产负债表: 货币资金(元)
}

EM_FIN_NUMERIC_FIELDS = [
    "每股收益", "每股净资产", "每股经营现金流净额",
    "加权净资产收益率", "扣非加权净资产收益率",
    "销售净利率", "净利润-同比增长", "营业收入-同比增长",
    "资产负债率", "营业收入", "营业利润", "净利润",
    "总资产", "净资产", "经营现金流净额", "投资现金流净额",
    "总股本", "流通股本_em",
    "有息负债率", "利息保障倍数", "所得税率",
    "公司自由现金流", "投入资本回报率",
    # EBITDA/EV 计算用
    "营业利润_报表", "财务费用", "总负债", "货币资金",
]


def _fetch_em_report_page(report_name: str, page: int, page_size: int,
                          extra_filter: str = "") -> tuple:
    """通用：获取东方财富数据中心一页报表数据"""
    filter_str = '(REPORT_DATE_NAME="2024年报")'
    if extra_filter:
        filter_str = f'({filter_str[1:-1]}){extra_filter})'
    params = {
        "reportName": report_name,
        "columns": "ALL",
        "filter": filter_str,
        "pageNumber": str(page),
        "pageSize": str(page_size),
        "sortTypes": "1",
        "sortColumns": "SECURITY_CODE",
        "source": "WEB",
        "client": "WEB",
    }
    data = _http_get(EM_DATA_URL, params=params)
    result = data.get("result", {})
    if not result:
        return [], 0
    items = result.get("data", [])
    total_count = result.get("count", 0)
    return items, total_count


def _fetch_report_all_pages(report_name: str, page_size: int = 5000,
                            label: str = "") -> pd.DataFrame:
    """批量分页获取某报表全量数据"""
    all_items = []
    items, total = _fetch_em_report_page(report_name, 1, page_size)
    all_items.extend(items)
    print(f"  [{label} 第1页] 获取 {len(items)} 条，共 {total} 条")
    page = 2
    while len(all_items) < total:
        try:
            items, _ = _fetch_em_report_page(report_name, page, page_size)
            all_items.extend(items)
            print(f"  [{label} 第{page}页] 获取 {len(items)} 条，累计 {len(all_items)} 条")
            page += 1
        except Exception as e:
            print(f"  [{label} 第{page}页获取失败: {e}]")
            break
    if not all_items:
        return pd.DataFrame()
    df = pd.DataFrame(all_items)
    # 只保留A股
    sc_col = "SECURITY_CODE"
    if sc_col in df.columns:
        df = df[df[sc_col].astype(str).str.match(r"^[036]\d{5}")].copy()
    if "SECURITY_CODE" in df.columns:
        df["SECURITY_CODE"] = df["SECURITY_CODE"].astype(str).str.zfill(6)
    return df


# 利润表和资产负债表字段映射（补充 EBITDA/EV 计算所需字段）
EM_EBITDA_FIELD_MAP = {
    "SECURITY_CODE": "code",
    "OPERATE_PROFIT": "营业利润_报表",
    "FINANCE_EXPENSE": "财务费用",
    "TOTAL_LIABILITIES": "总负债",
    "MONETARYFUNDS": "货币资金",
}

EM_EBITDA_NUMERIC_FIELDS = ["营业利润_报表", "财务费用", "总负债", "货币资金"]


def _fetch_report_latest_pages(report_name: str, max_pages: int = 20,
                                label: str = "") -> pd.DataFrame:
    """
    获取报表最近的数据（按REPORT_DATE降序），无需日期过滤。
    RPT_DMSK_FN_INCOME/BALANCE 等报表不支持 REPORT_DATE_NAME 过滤，
    因此按日期降序取前若干页，再本地去重保留每只股票最新一条。
    """
    all_items = []
    for page in range(1, max_pages + 1):
        try:
            items, total = _fetch_em_report_page_no_filter(
                report_name, page, sort_col="REPORT_DATE", sort_dir="-1")
            all_items.extend(items)
            if page <= 2 or page % 5 == 0:
                print(f"  [{label} 第{page}页] 获取 {len(items)} 条，累计 {len(all_items)} 条")
            if len(items) == 0 or len(all_items) >= total:
                break
        except Exception as e:
            print(f"  [{label} 第{page}页获取失败: {e}]")
            break

    if not all_items:
        return pd.DataFrame()

    df = pd.DataFrame(all_items)
    # 只保留A股
    if "SECURITY_CODE" in df.columns:
        df = df[df["SECURITY_CODE"].astype(str).str.match(r"^[036]\d{5}")].copy()
        df["SECURITY_CODE"] = df["SECURITY_CODE"].astype(str).str.zfill(6)
    # 按 REPORT_DATE 降序，保留每只股票最新一条
    if "REPORT_DATE" in df.columns:
        df["_rd"] = pd.to_datetime(df["REPORT_DATE"], errors="coerce")
        df = df.sort_values(["SECURITY_CODE", "_rd"], ascending=[True, False])
        df = df.drop_duplicates(subset="SECURITY_CODE", keep="first")
        df = df.drop(columns=["_rd"])
    return df


def _fetch_em_report_page_no_filter(report_name: str, page: int, page_size: int = 500,
                                     sort_col: str = "SECURITY_CODE",
                                     sort_dir: str = "1") -> tuple:
    """获取东方财富数据中心一页报表数据（无日期过滤，按指定字段排序）"""
    params = {
        "reportName": report_name,
        "columns": "ALL",
        "pageNumber": str(page),
        "pageSize": str(page_size),
        "sortTypes": sort_dir,
        "sortColumns": sort_col,
        "source": "WEB",
        "client": "WEB",
    }
    data = _http_get(EM_DATA_URL, params=params)
    result = data.get("result") or {}
    if not result:
        return [], 0
    items = result.get("data", [])
    total_count = result.get("count", 0)
    return items, total_count


def _get_ebitda_supplement_from_emdata() -> pd.DataFrame:
    """
    从东方财富利润表和资产负债表获取 EBITDA/EV 计算所需补充字段:
      - 营业利润（利润表）
      - 财务费用（利润表，近似利息支出）
      - 总负债（资产负债表）
      - 货币资金（资产负债表）
    """
    print("正在获取EBITDA/EV补充数据（利润表+资产负债表）...")
    start_time = time.time()

    # 利润表：获取营业利润和财务费用
    income_df = _fetch_report_latest_pages("RPT_DMSK_FN_INCOME", max_pages=20, label="利润表")

    # 资产负债表：获取总负债和货币资金
    balance_df = _fetch_report_latest_pages("RPT_DMSK_FN_BALANCE", max_pages=20, label="资产负债表")

    # 合并两张表
    if income_df.empty and balance_df.empty:
        print("[警告] 利润表和资产负债表均获取失败")
        return pd.DataFrame()

    if not income_df.empty and not balance_df.empty:
        income_df = income_df[["SECURITY_CODE", "OPERATE_PROFIT", "FINANCE_EXPENSE"]].copy()
        balance_df = balance_df[["SECURITY_CODE", "TOTAL_LIABILITIES", "MONETARYFUNDS"]].copy()
        df = income_df.merge(balance_df, on="SECURITY_CODE", how="outer")
    elif not income_df.empty:
        df = income_df[["SECURITY_CODE", "OPERATE_PROFIT", "FINANCE_EXPENSE"]].copy()
        df["TOTAL_LIABILITIES"] = np.nan
        df["MONETARYFUNDS"] = np.nan
    else:
        df = balance_df[["SECURITY_CODE", "TOTAL_LIABILITIES", "MONETARYFUNDS"]].copy()
        df["OPERATE_PROFIT"] = np.nan
        df["FINANCE_EXPENSE"] = np.nan

    # 字段映射
    df = df.rename(columns=EM_EBITDA_FIELD_MAP)
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)

    for col in EM_EBITDA_NUMERIC_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    elapsed = time.time() - start_time
    print(f"[完成] EBITDA/EV补充数据获取 {len(df)} 只股票（{elapsed:.1f}s）")
    return df


def _fetch_em_financial_page(page: int, page_size: int) -> List[Dict]:
    """获取东方财富数据中心一页年报财务数据"""
    params = {
        "reportName": "RPT_F10_FINANCE_MAINFINADATA",
        "columns": "ALL",
        "filter": '(REPORT_DATE_NAME="2024年报")',
        "pageNumber": str(page),
        "pageSize": str(page_size),
        "sortTypes": "1",
        "sortColumns": "SECURITY_CODE",
        "source": "WEB",
        "client": "WEB",
    }
    data = _http_get(EM_DATA_URL, params=params)
    result = data.get("result", {})
    if not result:
        return [], 0
    items = result.get("data", [])
    total_count = result.get("count", 0)
    return items, total_count


def _get_financial_from_emdata() -> pd.DataFrame:
    """
    从东方财富数据中心批量获取全A股年报财务指标
    一次请求约5000条，分页获取全量，~3秒完成
    """
    print("正在获取A股财务数据（东方财富数据中心）...")
    start_time = time.time()

    page_size = 5000
    all_items = []

    # 第一页
    items, total = _fetch_em_financial_page(1, page_size)
    all_items.extend(items)
    print(f"  [第1页] 获取 {len(items)} 条，共 {total} 条")

    # 后续分页
    page = 2
    while len(all_items) < total:
        try:
            items, _ = _fetch_em_financial_page(page, page_size)
            all_items.extend(items)
            print(f"  [第{page}页] 获取 {len(items)} 条，累计 {len(all_items)} 条")
            page += 1
        except Exception as e:
            print(f"  [警告] 第{page}页获取失败: {e}")
            break

    if not all_items:
        raise RuntimeError("东方财富数据中心返回空财务数据")

    df = pd.DataFrame(all_items)

    # 只保留A股（0/3/6开头，排除北交所8开头）
    sc_col = "SECURITY_CODE"
    if sc_col in df.columns:
        df = df[df[sc_col].astype(str).str.match(r"^[036]\d{5}")].copy()

    # 字段映射
    df = df.rename(columns=EM_FIN_FIELD_MAP)

    # 标准化代码
    if "code" in df.columns:
        df["code"] = df["code"].astype(str).str.zfill(6)

    # 数值化
    for col in EM_FIN_NUMERIC_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 每只股票只保留最新一年年报（REPORT_DATE最新的）
    if "报告期" in df.columns:
        df["报告期"] = pd.to_datetime(df["报告期"], errors="coerce")
        df = df.sort_values(["code", "报告期"], ascending=[True, False])
        df = df.drop_duplicates(subset="code", keep="first")

    elapsed = time.time() - start_time
    print(f"[完成] 东方财富数据中心获取 {len(df)} 只股票财务数据（{elapsed:.1f}s）")
    return df


def get_financial_data(codes: List[str], force_refresh: bool = False,
                       progress_callback=None) -> Optional[pd.DataFrame]:
    """
    获取财务数据
    优先级：缓存 > 东方财富数据中心（批量，~3秒）
    """
    _ensure_cache_dir()
    cache_path = os.path.join(CACHE_DIR, "financial_data.csv")

    if not force_refresh:
        cached = _load_cache("financial_data.csv", dtype_cols={"code": str})
        if cached is not None and _is_cache_valid(cache_path, FINANCIAL_CACHE_HOURS):
            age = _cache_age_hours(cache_path)
            print(f"[缓存] 财务数据 {len(cached)} 只（{age:.1f}h前）")
            return cached

    # 东方财富数据中心（批量，免费无限制，~3秒）
    try:
        df = _get_financial_from_emdata()
        _save_cache(df, "financial_data.csv")
        return df
    except Exception as e:
        print(f"[警告] 东方财富数据中心失败({e})")

    # 使用旧缓存作为回退
    cached = _load_cache("financial_data.csv", dtype_cols={"code": str})
    if cached is not None:
        print(f"[回退] 使用旧缓存 {len(cached)} 条")
        return cached

    print("[错误] 财务数据获取失败")
    return None


# ============================================================
# 股票列表
# ============================================================

def get_stock_list(force_refresh: bool = False) -> pd.DataFrame:
    """获取全部A股股票列表（基于行情数据缓存）"""
    cache_path = os.path.join(CACHE_DIR, "stock_list.csv")
    if not force_refresh and os.path.exists(cache_path):
        df = pd.read_csv(cache_path, dtype=str)
        if len(df) > 100:
            print(f"[缓存] 股票列表 {len(df)} 只")
            return df

    # 从行情数据中提取代码列表
    market = get_market_data(force_refresh)
    df = pd.DataFrame({"code": market["代码"].astype(str).str.zfill(6), "name": market.get("名称", "")})
    _save_cache(df, "stock_list.csv")
    print(f"[完成] 股票列表 {len(df)} 只")
    return df


# ============================================================
# 股票过滤
# ============================================================

def filter_stocks(df: pd.DataFrame) -> pd.DataFrame:
    """过滤ST、*ST、退市、停牌等"""
    original = len(df)
    if "名称" not in df.columns:
        return df

    mask = pd.Series(True, index=df.index)
    for tag in EXCLUDE_TAGS:
        mask &= ~df["名称"].str.contains(tag, na=False, regex=False)
    if "最新价" in df.columns:
        mask &= df["最新价"].notna() & (df["最新价"] > 0)

    filtered = df[mask].copy().reset_index(drop=True)
    removed = original - len(filtered)
    if removed > 0:
        print(f"[过滤] 去除 {removed} 只（ST/停牌等），剩余 {len(filtered)} 只")
    return filtered


# ============================================================
# 数据准备（主入口）
# ============================================================

def prepare_data(force_refresh: bool = False, progress_callback=None) -> pd.DataFrame:
    """
    准备完整的选股数据集

    流程：
      1. 腾讯行情接口批量获取行情+动量（~2秒）→ 失败回退东方财富push2
      2. 过滤ST/停牌
      3. 东方财富数据中心批量获取财务数据（~24秒，免费无限制）
      4. 补充EBITDA/EV数据（利润表+资产负债表，~5秒）
      5. 合并
    """
    # 1. 行情数据（腾讯接口自带近3月/6月涨跌幅）
    market = get_market_data(force_refresh)
    market = filter_stocks(market)

    code_col = "代码" if "代码" in market.columns else "code"
    codes = market[code_col].dropna().astype(str).str.zfill(6).tolist()
    print(f"待补充数据: {len(codes)} 只股票")

    # 检查行情数据是否已含动量字段
    has_momentum = "近3月涨跌幅" in market.columns or "近6月涨跌幅" in market.columns
    if has_momentum:
        momentum_valid = market.get("近3月涨跌幅", pd.Series(dtype=float)).notna().sum()
        print(f"[信息] 行情数据已含动量字段，{momentum_valid} 只有效")

    # 2. 财务数据（东方财富数据中心批量获取）
    financial = get_financial_data(codes, force_refresh, progress_callback)

    if financial is not None and not financial.empty:
        df = market.merge(financial, left_on=code_col, right_on="code",
                          how="left", suffixes=("", "_fin"))
        dup_cols = [c for c in df.columns if c.endswith("_fin") and c[:-4] in df.columns]
        df = df.drop(columns=dup_cols)
        print(f"[合并] {len(df)} 只股票，{len(df.columns)} 个字段")
    else:
        df = market.copy()
        print(f"[警告] 无财务数据，仅使用行情字段（{len(df.columns)} 个）")

    # 3. EBITDA/EV 补充数据（利润表+资产负债表）
    _ensure_cache_dir()
    ebitda_cache = os.path.join(CACHE_DIR, "ebitda_supplement.csv")
    ebitda_df = None
    if not force_refresh:
        cached = _load_cache("ebitda_supplement.csv", dtype_cols={"code": str})
        if cached is not None and _is_cache_valid(ebitda_cache, FINANCIAL_CACHE_HOURS):
            ebitda_df = cached
            age = _cache_age_hours(ebitda_cache)
            print(f"[缓存] EBITDA补充数据 {len(cached)} 只（{age:.1f}h前）")
    if ebitda_df is None:
        try:
            ebitda_df = _get_ebitda_supplement_from_emdata()
            if not ebitda_df.empty:
                _save_cache(ebitda_df, "ebitda_supplement.csv")
        except Exception as e:
            print(f"[警告] EBITDA补充数据获取失败: {e}")

    if ebitda_df is not None and not ebitda_df.empty:
        merge_code = "代码" if "代码" in df.columns else "code"
        df = df.merge(ebitda_df, left_on=merge_code, right_on="code",
                       how="left", suffixes=("", "_ebitda"))
        dup_cols = [c for c in df.columns if c.endswith("_ebitda") and c[:-7] in df.columns]
        df = df.drop(columns=dup_cols)
        print(f"[合并] EBITDA补充数据已合并，{len(df.columns)} 个字段")

    # 4. 动量数据检查（腾讯接口已提供，此处仅做提示）
    if not has_momentum or df.get("近3月涨跌幅", pd.Series(dtype=float)).notna().sum() < 10:
        print("[信息] 动量数据不可用，依赖动量的策略（趋势+价值、价值+增长等）将无法运行")

    return df
