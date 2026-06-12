"""实时行情服务

数据源优先级:
1. Windows curl -> 腾讯行情接口 (qt.gtimg.cn)
2. 新浪行情 (hq.sinajs.cn) - WSL可能不通
3. DB daily_price 兜底 (收盘后/网络不通)
"""
import re, time, sqlite3, logging, subprocess, platform
from dataclasses import dataclass
from pathlib import Path

import requests

DB = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"
log = logging.getLogger(__name__)

# Windows curl 路径
_WIN_CURL = "/mnt/c/Windows/System32/curl.exe"
_IS_WSL = "microsoft" in platform.uname().release.lower()


@dataclass
class Quote:
    code: str
    name: str = ""
    open: float = 0
    pre_close: float = 0
    price: float = 0
    high: float = 0
    low: float = 0
    bid1: float = 0
    volume: int = 0
    amount: float = 0
    date: str = ""
    time_: str = ""
    source: str = "tencent"

    @property
    def pct(self):
        return (self.price - self.pre_close) / self.pre_close * 100 if self.pre_close else 0

    @property
    def short(self):
        return self.code[2:] if self.code.startswith(("sh", "sz", "bj")) else self.code


def _sina(code: str) -> str:
    code = code.strip()
    if code.startswith(("sh", "sz")):
        return code
    if code.startswith("399"):
        return f"sz{code}"
    if code in ("000001", "000300", "000905", "000016"):
        return f"sh{code}"
    return f"sh{code}" if code[0] in ("6", "9") else f"sz{code}"


def _tencent(code: str) -> str:
    code = code.strip()
    if code.startswith(("sh", "sz", "bj")):
        return code
    # 指数特殊: 000001上证/399001深证/399006创业板 都用 sh/sz 前缀
    if code.startswith("399"):
        return f"sz{code}"
    if code in ("000001", "000300", "000905", "000016", "000688"):
        return f"sh{code}"
    # 北交所指数(899xxx)用bj前缀
    if code.startswith("899"):
        return f"bj{code}"
    # 北交所个股(8/9开头6位)用bj前缀
    if len(code) == 6 and code[0] in ("8", "9"):
        return f"bj{code}"
    return f"sh{code}" if code[0] in ("6",) else f"sz{code}"


# ---- 腾讯行情 (通过 Windows curl) ----
def _fetch_tencent_win(codes: list[str]) -> dict[str, Quote]:
    """通过 Windows curl 调用腾讯接口"""
    if not _IS_WSL:
        return {}
    result = {}
    tencent_codes = ",".join(_tencent(c) for c in codes)
    try:
        proc = subprocess.run(
            [_WIN_CURL, "-s", f"http://qt.gtimg.cn/q={tencent_codes}"],
            capture_output=True, timeout=6,
        )
        raw = proc.stdout.decode("gbk", errors="replace")
    except Exception as e:
        log.debug(f"win curl fail: {e}")
        return {}

    for line in raw.strip().split(";"):
        line = line.strip()
        if not line or '=""' in line:
            continue
        try:
            q = _parse_tencent(line)
            if q and q.price > 0:
                result[q.short] = q
        except Exception:
            pass
    return result


def _parse_tencent(raw: str) -> Quote | None:
    """解析腾讯行情: v_sh000001="1~上证指数~000001~4180.09~..." """
    # 字段: 0=market, 1=name, 2=code, 3=price, 4=yesterday_close, 5=open,
    #        6=volume(手), ..., 33=high, 34=low, ...
    m = re.search(r'v_(\w+)="(.+)"', raw)
    if not m:
        return None
    full_code = m.group(1)
    fields = m.group(2).split("~")
    if len(fields) < 35:
        return None
    try:
        name = fields[1]
        price = float(fields[3])
        pre_close = float(fields[4])
        open_ = float(fields[5])
        high = float(fields[33]) if fields[33] else float(fields[3])
        low = float(fields[34]) if fields[34] else float(fields[3])
        vol = int(float(fields[6])) if fields[6] else 0
        amount = float(fields[37]) if len(fields) > 37 and fields[37] else 0
        datetime_str = fields[30] if len(fields) > 30 else ""
        date_ = datetime_str[:8] if datetime_str else ""
        time_ = datetime_str[8:] if datetime_str else ""
        if date_:
            date_ = f"{date_[:4]}-{date_[4:6]}-{date_[6:8]}"
        return Quote(
            code=full_code, name=name,
            price=price, pre_close=pre_close, open=open_,
            high=high, low=low, volume=vol, amount=amount,
            date=date_, time_=time_, source="tencent",
        )
    except Exception:
        return None


# ---- 新浪行情 (WSL可能不通) ----
_SINA_URL = "http://hq.sinajs.cn/list={codes}"
_SINA_HEADERS = {"Referer": "http://finance.sina.com.cn"}


def _fetch_sina(codes: list[str], timeout: int = 3) -> dict[str, Quote]:
    result = {}
    sc = [_sina(c) for c in codes]
    for i in range(0, len(sc), 30):
        batch = sc[i:i + 30]
        try:
            r = requests.get(
                _SINA_URL.format(codes=",".join(batch)),
                headers=_SINA_HEADERS, timeout=timeout,
            )
            for line in r.text.strip().split("\n"):
                q = _parse_sina(line)
                if q and q.price > 0:
                    result[q.short] = q
        except Exception:
            pass
        if i + 30 < len(sc):
            time.sleep(0.3)
    return result


def _parse_sina(raw: str) -> Quote | None:
    mc = re.search(r'_(\w+)=', raw)
    md = re.search(r'="(.+?)"', raw)
    if not mc or not md:
        return None
    f = md.group(1).split(",")
    if len(f) < 32:
        return None
    try:
        return Quote(
            code=mc.group(1), name=f[0], source="sina",
            open=float(f[1] or 0), pre_close=float(f[2] or 0),
            price=float(f[3] or 0), high=float(f[4] or 0), low=float(f[5] or 0),
            volume=int(float(f[8] or 0)), amount=float(f[9] or 0),
            date=f[30] if len(f) > 30 else "", time_=f[31] if len(f) > 31 else "",
        )
    except Exception:
        return None


# ---- DB兜底 ----
def _db_quote(code: str, conn=None) -> Quote | None:
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(str(DB))
    try:
        row = conn.execute(
            "SELECT trade_date,open,high,low,close,pre_close,amount "
            "FROM daily_price WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            (code,),
        ).fetchone()
        if not row:
            return None
        return Quote(
            code=_sina(code), name="", source="db",
            open=row[1] or 0, pre_close=row[5] or 0, price=row[4] or 0,
            high=row[2] or 0, low=row[3] or 0, amount=row[6] or 0,
            date=row[0],
        )
    finally:
        if own_conn:
            conn.close()


# ---- 公开接口 ----
def fetch(codes: list[str], no_cache: bool = False) -> dict[str, Quote]:
    """批量行情: 腾讯(win curl) -> 新浪 -> DB兜底
    
    no_cache=True 时跳过 Streamlit 缓存，交易时段实时追踪用
    """
    return _fetch_impl(codes)


def _fetch_impl(codes: list[str]) -> dict[str, Quote]:
    result = {}

    # 1. 腾讯 (Windows curl)
    if _IS_WSL:
        result = _fetch_tencent_win(codes)
        if len(result) == len(codes):
            return result

    # 2. 新浪
    if len(result) < len(codes):
        missing = [c for c in codes if c not in result]
        sina_result = _fetch_sina(missing, timeout=3)
        result.update(sina_result)

    # 3. DB兜底
    missing = [c for c in codes if c not in result]
    if missing:
        conn = sqlite3.connect(str(DB))
        for code in missing:
            q = _db_quote(code, conn)
            if q:
                result[code] = q
        conn.close()

    return result


def fetch_index() -> dict[str, Quote]:
    """大盘指数: 上证/深证/创业板/科创50/北证50"""
    codes = ["000001", "399001", "399006", "000688", "899050"]
    result = {}

    # 腾讯优先 (一次请求拿全部)
    if _IS_WSL:
        result = _fetch_tencent_win(codes)
        if len(result) >= 5:
            return result

    # 新浪
    if len(result) < 5:
        missing = [c for c in codes if c not in result]
        result.update(_fetch_sina(missing, timeout=3))

    # DB兜底
    missing = [c for c in codes if c not in result]
    if missing:
        conn = sqlite3.connect(str(DB))
        for code in missing:
            q = _db_quote(code, conn)
            if q:
                result[code] = q
        conn.close()

    return result


def fetch_stocks(codes: list[str]) -> dict[str, Quote]:
    """个股行情 (fetch 的别名，语义更清晰)"""
    return fetch(codes)
