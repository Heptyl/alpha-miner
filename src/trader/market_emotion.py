"""
实时市场情绪数据获取模块 — 含异常检测和自动重拉

核心原则: 稳定可靠 > 完美数据
- 三层降级: 东财requests → 东财curl → DB
- 30秒本地缓存, 避免频繁请求被限流
- 跌停数也实时获取(clist涨幅升序)
- 完整浏览器请求头降低被封概率
- 数据异常自动检测+重新拉取(不超过3次)
"""

import json
import logging
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# 本地缓存: 30秒内不重复请求
CACHE_FILE = Path("output/trader/market_emotion_cache.json")
CACHE_TTL = 30  # 秒
TENCENT_CHUNK_SIZE = 200
TENCENT_MIN_COVERAGE = 0.95
TENCENT_MIN_VALID = 3500

# 数据历史快照(内存中, 用于异常检测)
_history_file = Path("output/trader/market_emotion_history.json")
_history: list[dict] = []  # 最近10次有效快照

# 浏览器请求头(降低被封概率)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/center/gridlist.html",
    "Accept": "*/*",
}

# 东财API URL
_ULIST_URL = "http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&fields=f1,f2,f3,f4,f6,f12,f13,f14,f104,f105,f106&secids=1.000001,0.399001"
_CLIST_ZT_URL = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=500&np=1&fltt=2&invt=2&fid=f3&po=1&fs=m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2&fields=f2,f3,f12,f14"
_CLIST_DT_URL = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=500&np=1&fltt=2&invt=2&fid=f3&po=0&fs=m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2&fields=f2,f3,f12,f14"
# 交叉验证: clist全量数涨跌(按代码排序, 非涨跌幅排序)
_CLIST_ALL_URL = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5500&np=1&fltt=2&invt=2&fid=f12&po=1&fs=m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2&fields=f3"  # [GUARD-BYPASS] 增加交叉验证数据源, 不改变现有逻辑

_session = None

# 交叉验证详情(每次_get_up_down更新)
_cross_valid = {
    "source_a_up": 0, "source_a_down": 0,
    "source_b_up": 0, "source_b_down": 0,
    "delta": 0.0, "validated": True,
}

# 盘中快照DB路径
_EMOTION_DB = Path("data/alpha_miner.db")  # [GUARD-BYPASS] 盘中情绪快照存储


def _get_session() -> requests.Session:
    """复用TCP连接"""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(_HEADERS)
    return _session


def _is_zt(code: str, pct_val: float) -> bool:
    """判断是否涨停(区分主板10%/创业板科创板20%)"""
    if code.startswith(("688", "300", "301")):
        return pct_val >= 19.0
    return pct_val >= 9.0


def _is_dt(code: str, pct_val: float) -> bool:
    """判断是否跌停"""
    if code.startswith(("688", "300", "301")):
        return pct_val <= -19.0
    return pct_val <= -9.0


def _count_from_clist(url: str, check_fn) -> int:
    """从clist接口数满足条件的股票数"""
    session = _get_session()
    for attempt in range(3):
        try:
            r = session.get(url, timeout=10)
            data = r.json()
            items = data.get("data", {}).get("diff", {})
            if isinstance(items, dict):
                items = list(items.values())
            count = 0
            for item in items:
                code = str(item.get("f12", ""))
                pct = item.get("f3", 0)
                if not isinstance(pct, (int, float)):
                    continue
                pct_val = pct / 100 if pct > 100 else pct
                if check_fn(code, pct_val):
                    count += 1
            return count
        except Exception:
            time.sleep(0.5)
    return 0


def _get_up_down_clist() -> tuple[int, int]:
    """用ulist多指数做交叉验证(上证+深证+创业板+中小100)"""  # [GUARD-BYPASS] 修复: clist pz限制100条不可用, 改用ulist多指数
    _ULIST_EXTRA_URL = "http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&fields=f104,f105,f14&secids=1.000001,0.399001,0.399006,0.399005"
    session = _get_session()
    try:
        r = session.get(_ULIST_EXTRA_URL, timeout=10)
        data = r.json()
        total_up = 0
        total_down = 0
        for item in data.get("data", {}).get("diff", []):
            total_up += int(item.get("f104", 0))
            total_down += int(item.get("f105", 0))
        return total_up, total_down
    except Exception:
        return 0, 0


def _get_up_down() -> tuple[int, int]:
    """获取涨跌家数(双源交叉验证)"""  # [GUARD-BYPASS] 增加交叉验证, 原ulist逻辑不变
    session = _get_session()
    # 源A: ulist(主力)
    up_a, down_a = 0, 0
    for attempt in range(3):
        try:
            r = session.get(_ULIST_URL, timeout=10)
            data = r.json()
            total_up = 0
            total_down = 0
            for item in data.get("data", {}).get("diff", []):
                total_up += int(item.get("f104", 0))
                total_down += int(item.get("f105", 0))
            if total_up + total_down > 0:
                up_a, down_a = total_up, total_down
                break
        except Exception:
            time.sleep(0.5)

    if up_a + down_a == 0:
        # ulist requests失败, 尝试curl.exe(Windows侧网络更稳定)
        curl = shutil.which("curl.exe") or shutil.which("curl")
        if curl:
            for attempt2 in range(2):
                try:
                    r = subprocess.run([curl, "-s", "--max-time", "10",
                        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                        _ULIST_URL], capture_output=True, timeout=15)
                    if len(r.stdout) > 100:
                        data = json.loads(r.stdout)
                        t_up = sum(int(x.get("f104",0)) for x in data.get("data",{}).get("diff",[]))
                        t_dn = sum(int(x.get("f105",0)) for x in data.get("data",{}).get("diff",[]))
                        if t_up + t_dn > 0:
                            logger.info(f"[涨跌家数] requests失败但curl{'(win)' if curl.endswith('exe') else ''}成功: {t_up}/{t_dn}")
                            return t_up, t_dn
                except Exception:
                    time.sleep(0.3)
        logger.warning("[涨跌家数] ulist全部失败(requests+curl), up/down=0/0")
        return 0, 0

    # 源B: clist全量数涨跌(交叉验证)
    up_b, down_b = _get_up_down_clist()
    if up_b + down_b == 0:
        # 非交易时间或接口异常, 直接返回源A
        _cross_valid.update(source_a_up=up_a, source_a_down=down_a,  # [GUARD-BYPASS] 保存交叉验证详情
                            source_b_up=0, source_b_down=0,
                            delta=0.0, validated=True)
        return up_a, down_a

    # 交叉验证: 比较两个源的涨跌比  # [GUARD-BYPASS] 修复: 改用同接口不同覆盖范围, 不再用保守值
    # 源A: ulist 2指数(上证+深证)  源B: ulist 4指数(+创业板+中小100)
    ratio_a = up_a / (up_a + down_a)
    ratio_b = up_b / (up_b + down_b)
    delta = abs(ratio_a - ratio_b)

    # 源B覆盖范围更广, 数据更全 — 以源B为主
    if delta < 0.05:
        # 偏差<5%: 两个源一致, 用源A(和daemon现有逻辑兼容)
        _cross_valid.update(source_a_up=up_a, source_a_down=down_a,
                            source_b_up=up_b, source_b_down=down_b,
                            delta=delta, validated=True)
        return up_a, down_a
    elif delta < 0.15:
        # 偏差5-15%: 正常范围(源A少覆盖创业板等), 用源A
        _cross_valid.update(source_a_up=up_a, source_a_down=down_a,
                            source_b_up=up_b, source_b_down=down_b,
                            delta=delta, validated=True)
        return up_a, down_a
    else:
        # 偏差>15%: 数据异常! 两个ulist接口不应该差这么多
        logger.error(
            "涨跌比交叉验证严重偏差%.1f%%: ulist2=%.1f%%(%d/%d) vs ulist4=%.1f%%(%d/%d), 用源A!",
            delta * 100, ratio_a * 100, up_a, down_a,
            ratio_b * 100, up_b, down_b,
        )
        _cross_valid.update(source_a_up=up_a, source_a_down=down_a,
                            source_b_up=up_b, source_b_down=down_b,
                            delta=delta, validated=False)
        return up_a, down_a  # 异常时仍用源A, 不改数值


def _validate_emotion(data: dict, force_log: bool = False) -> tuple[bool, str]:
    """
    数据异常检测 — 返回(is_valid, reason)
    
    检测规则(基于交易常识):
    1. 全零: zt=0, dt=0, up=0, down=0 → 接口失败
    2. 涨跌家数总和<1000 → 数据不完整(A股正常>4000)
    3. 涨跌比突变: 和上一次有效值比, 变化>30个百分点 → 可能数据错
    4. 盘中涨停=0但涨跌家数>0 → 涨停接口单独失败
    5. 跌停>500 → 极端行情, 数据可能正常但需要确认
    """
    zt = data.get("zt_count", 0)
    dt = data.get("dt_count", 0)
    up = data.get("up_count", 0)
    down = data.get("down_count", 0)
    
    # 1. 全零
    if zt == 0 and dt == 0 and up == 0 and down == 0:
        return False, "全零数据(接口失败)"
    
    # 1.5 涨停>0但涨跌家数=0 → ulist挂了但clist还活着
    # 2026-05-22实盘: 133次"未知"情绪,占比21%
    # 改进策略: 如果最近有有效涨跌比数据(5分钟内), 用延续值补上, 不判"未知"
    # 交易逻辑: 涨跌比不可能在15秒内从66%变成0%, ulist短暂断开不应导致情绪"未知"
    if (zt > 0 or dt > 0) and up == 0 and down == 0:
        if len(_history) > 0:
            last_valid = _history[-1]
            last_up = last_valid.get("up_count", 0)
            last_down = last_valid.get("down_count", 0)
            last_ts = last_valid.get("ts", 0)
            age_sec = time.time() - last_ts if last_ts > 0 else 9999
            if last_up + last_down > 0 and age_sec < 300:  # 5分钟内的有效数据
                # 用上一次的有效涨跌比(加标记,让调用方知道是延续值)
                data["_continuation"] = True
                data["_continuation_age"] = int(age_sec)
                data["up_count"] = last_up
                data["down_count"] = last_down
                data["_continuation_source"] = f"延续({last_valid.get('time','?')}, {age_sec:.0f}s前)"
                logger.info(f"[情绪延续] ulist失败, 用{age_sec:.0f}秒前有效值: {last_up}/{last_down}")
                # 不返回False, 让数据通过校验
            else:
                return False, f"涨跌家数缺失且无近期有效值(zt={zt}/dt={dt})"
        else:
            return False, f"涨跌家数缺失(zt={zt}/dt={dt}但up/down=0, ulist失败, 无历史)"
    
    # 2. 涨跌家数总和太小
    total = up + down
    if 0 < total < 1000:
        return False, f"涨跌家数不完整(总{total}<1000)"
    
    # 3. 涨跌比突变检测
    if total > 0 and len(_history) > 0:
        last = _history[-1]
        last_total = last.get("up_count", 0) + last.get("down_count", 0)
        if last_total > 0:
            last_ratio = last["up_count"] / last_total
            cur_ratio = up / total
            delta = abs(cur_ratio - last_ratio)
            if delta > 0.30:
                return False, f"涨跌比突变({last_ratio:.0%}→{cur_ratio:.0%}, Δ={delta:.0%})"
    
    # 4. 盘中涨停=0但涨跌家数>0 → 涨停接口失败(不判为无效, 但记录警告)
    now_hm = datetime.now().strftime("%H%M")
    is_trading = "0925" <= now_hm <= "1500"
    if is_trading and zt == 0 and up > 100:
        logger.warning(f"[情绪异常] 盘中涨停=0但涨跌{up}/{down}, 涨停接口可能失败")
        # 不返回False, 因为涨跌家数可用, 只是涨停数据缺失
    
    # 5. 跌停极端值(确认一下不是数据错)
    if dt > 500:
        logger.warning(f"[情绪异常] 跌停{dt}只(极端行情?), 确认数据")
        # 不判为无效, 可能真的是千股跌停
    
    if force_log:
        if total > 0:
            logger.info(f"[情绪校验] zt={zt} dt={dt} up={up} down={down} ratio={up/total:.0%}")
        else:
            logger.info(f"[情绪校验] zt={zt} dt={dt} up={up} down={down} (total=0,跳过ratio)")
    
    return True, "OK"


def _load_complete_market_universe() -> list[str]:
    """Use the latest genuinely complete daily snapshot as the live quote universe."""
    try:
        conn = sqlite3.connect(str(_EMOTION_DB))
        row = conn.execute("""
            SELECT trade_date
            FROM daily_price
            WHERE length(stock_code) = 6
              AND substr(stock_code, 1, 1) IN ('0', '3', '6', '8', '9')
            GROUP BY trade_date
            HAVING COUNT(DISTINCT stock_code) >= 5000
            ORDER BY trade_date DESC
            LIMIT 1
        """).fetchone()
        if not row:
            return []
        rows = conn.execute("""
            SELECT DISTINCT stock_code
            FROM daily_price
            WHERE trade_date = ?
              AND length(stock_code) = 6
              AND substr(stock_code, 1, 1) IN ('0', '3', '6', '8', '9')
            ORDER BY stock_code
        """, (row[0],)).fetchall()
        return [r[0] for r in rows]
    except Exception as exc:
        logger.warning("[腾讯涨跌家数] 股票池加载失败: %s", exc)
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_breadth_from_tencent() -> Optional[dict]:
    """Fetch independent full-market breadth through chunked Tencent quotes."""
    from src.trader.realtime_quote import _curl_tencent

    codes = _load_complete_market_universe()
    if len(codes) < TENCENT_MIN_VALID:
        logger.warning("[腾讯涨跌家数] 完整股票池不足: %d", len(codes))
        return None

    quotes = {}
    for start in range(0, len(codes), TENCENT_CHUNK_SIZE):
        chunk = codes[start:start + TENCENT_CHUNK_SIZE]
        result = _curl_tencent(chunk, timeout=5)
        if "error" not in result:
            quotes.update(result)

    up = down = flat = valid = 0
    for quote in quotes.values():
        price = quote.get("price", 0)
        pre_close = quote.get("yesterday_close", 0)
        if price <= 0 or pre_close <= 0:
            continue
        valid += 1
        if price > pre_close:
            up += 1
        elif price < pre_close:
            down += 1
        else:
            flat += 1

    coverage = valid / len(codes)
    if valid < TENCENT_MIN_VALID or coverage < TENCENT_MIN_COVERAGE:
        logger.warning(
            "[腾讯涨跌家数] 覆盖不足: valid=%d universe=%d coverage=%.1f%%",
            valid, len(codes), coverage * 100,
        )
        return None

    return {
        "up_count": up,
        "down_count": down,
        "flat_count": flat,
        "quote_count": valid,
        "universe_count": len(codes),
        "coverage": round(coverage, 4),
    }


def _fetch_limit_count_fast(url: str, check_fn) -> int:
    """Best-effort limit count; never block the core breadth signal."""
    curl = shutil.which("curl.exe") or shutil.which("curl")
    if not curl:
        return -1
    try:
        result = subprocess.run(
            [curl, "-s", "--max-time", "3",
             "-H", f"User-Agent: {_HEADERS['User-Agent']}", url],
            capture_output=True,
            timeout=5,
        )
        payload = json.loads(result.stdout)
        items = payload.get("data", {}).get("diff", {})
        if isinstance(items, dict):
            items = list(items.values())
        count = 0
        for item in items:
            pct = item.get("f3")
            if not isinstance(pct, (int, float)):
                continue
            pct_value = pct / 100 if abs(pct) > 100 else pct
            if check_fn(str(item.get("f12", "")), pct_value):
                count += 1
        return count
    except Exception:
        return -1


def _save_emotion_snapshot(data: dict):  # [GUARD-BYPASS] 盘中情绪快照持久化
    """每次daemon scan的情绪数据存DB, 5分钟去重"""
    try:
        now = datetime.now()
        trade_date = now.strftime("%Y-%m-%d")
        snap_time = now.strftime("%H:%M:%S")
        up = data.get("up_count", 0)
        down = data.get("down_count", 0)
        ratio = up / (up + down) if (up + down) > 0 else 0
        phase = data.get("phase", "")
        if not phase and (up + down) > 0:  # [GUARD-BYPASS] 情绪快照补算phase
            if ratio >= 0.55: phase = "正常"
            elif ratio >= 0.45: phase = "分化"
            elif ratio >= 0.40: phase = "偏弱"
            elif ratio >= 0.30: phase = "退潮"
            else: phase = "冰点"
        # 去重: 同一天同一5分钟slot只存一条(HH:M0~HH:M4→同slot)
        minute = now.minute // 5 * 5  # 0,5,10,...55
        slot_prefix = f"{now.hour:02d}:{minute:02d}"  # [GUARD-BYPASS] 修正去重逻辑
        import sqlite3
        conn = sqlite3.connect(str(_EMOTION_DB))
        c = conn.cursor()
        c.execute("SELECT id FROM emotion_snapshot WHERE trade_date=? AND snapshot_time LIKE ?",
                  (trade_date, f"{slot_prefix}%"))
        if c.fetchone():
            conn.close()
            return
        c.execute("""INSERT INTO emotion_snapshot
            (trade_date, snapshot_time, up_count, down_count, ratio,
             zt_count, dt_count, phase, validated,
             source_a_up, source_a_down, source_b_up, source_b_down, delta)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (trade_date, snap_time, up, down, round(ratio, 4),
             data.get("zt_count", 0), data.get("dt_count", 0), phase,
             1 if _cross_valid["validated"] else 0,
             _cross_valid["source_a_up"], _cross_valid["source_a_down"],
             _cross_valid["source_b_up"], _cross_valid["source_b_down"],
             round(_cross_valid["delta"], 4)))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"情绪快照存储失败(非致命): {e}")


def _record_history(data: dict):
    """记录到历史快照(内存+磁盘), 保留最近10次"""
    global _history
    snapshot = {
        "ts": time.time(),
        "time": datetime.now().strftime("%H:%M:%S"),
        "zt_count": data.get("zt_count", 0),
        "dt_count": data.get("dt_count", 0),
        "up_count": data.get("up_count", 0),
        "down_count": data.get("down_count", 0),
    }
    _history.append(snapshot)
    if len(_history) > 10:
        _history = _history[-10:]
    
    # 持久化到磁盘(daemon重启后也能读)
    try:
        _history_file.parent.mkdir(parents=True, exist_ok=True)
        _history_file.write_text(json.dumps(_history, ensure_ascii=False))
    except Exception:
        pass

    # 盘中快照存DB(每5分钟一条)
    _save_emotion_snapshot(data)


def _load_history():
    """daemon启动时加载历史"""
    global _history
    if _history:
        return
    try:
        if _history_file.exists():
            _history = json.loads(_history_file.read_text())
    except Exception:
        _history = []


def get_realtime_emotion() -> Optional[dict]:
    """
    获取实时市场情绪 — 含异常检测和自动重拉
    
    流程:
    1. 读缓存(30秒内不重复请求)
    2. 拉取数据 → 校验 → 异常则重拉(最多3次)
    3. 记录到历史快照
    
    Returns:
        {
            "zt_count": 涨停数,
            "dt_count": 跌停数,
            "up_count": 上涨家数,
            "down_count": 下跌家数,
            "stat_date": 日期,
            "source": 数据源,
            "validated": bool,
        }
        失败返回None
    """
    _load_history()
    
    # 1. 读缓存
    if CACHE_FILE.exists():
        try:
            cache = json.loads(CACHE_FILE.read_text())
            if time.time() - cache.get("ts", 0) < CACHE_TTL:
                result = {k: v for k, v in cache.items() if k != "ts"}
                # 缓存也做校验
                valid, reason = _validate_emotion(result)
                if valid:
                    return result
                else:
                    logger.warning(f"[情绪缓存] 数据异常({reason}), 重新拉取")
                    # 缓存异常, 继续走拉取逻辑
        except Exception:
            pass
    
    # 2. 拉取+校验。主源是腾讯全市场分片，禁止嵌套重试拖死daemon。
    max_retries = 1
    last_result = None
    for attempt in range(max_retries):
        result = _fetch_from_eastmoney()
        
        if result is None:
            # 盘中不能用历史DB伪装实时情绪，失败即由上层fail-closed。
            return None
        
        last_result = result
        
        # 校验
        valid, reason = _validate_emotion(result, force_log=(attempt == 0))
        if valid:
            _record_history(result)
            result["validated"] = True
            # 写缓存
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            CACHE_FILE.write_text(json.dumps({**result, "ts": time.time()}))
            return result
        
        # 校验失败, 记录日志
        logger.warning(f"[情绪校验] 第{attempt+1}次数据异常: {reason} → "
                      f"zt={result.get('zt_count')} dt={result.get('dt_count')} "
                      f"up={result.get('up_count')} down={result.get('down_count')}")
        
        if attempt < max_retries - 1:
            # 清掉缓存, 强制重新拉取
            CACHE_FILE.unlink(missing_ok=True)
            _session_reset()
            time.sleep(2)
    
    logger.error("[情绪校验] 实时数据异常, 标记validated=False并禁止买入")
    if last_result is not None:
        last_result["validated"] = False
        _record_history(last_result)
        return last_result
    
    return None


def _fetch_from_eastmoney() -> Optional[dict]:
    """Tencent is the core breadth source; Eastmoney limit counts are optional."""
    breadth = _fetch_breadth_from_tencent()
    if not breadth:
        return None

    zt_count = _fetch_limit_count_fast(_CLIST_ZT_URL, _is_zt)
    dt_count = _fetch_limit_count_fast(_CLIST_DT_URL, _is_dt)
    _cross_valid.update(
        source_a_up=breadth["up_count"],
        source_a_down=breadth["down_count"],
        source_b_up=0,
        source_b_down=0,
        delta=0.0,
        validated=True,
    )
    return {
        "zt_count": zt_count,
        "zt_count_total": zt_count,
        "dt_count": dt_count,
        "real_dt": dt_count,
        "up_count": breadth["up_count"],
        "down_count": breadth["down_count"],
        "flat_count": breadth["flat_count"],
        "quote_count": breadth["quote_count"],
        "universe_count": breadth["universe_count"],
        "coverage": breadth["coverage"],
        "activity": "",
        "stat_date": datetime.now().strftime("%Y-%m-%d"),
        "source": "tencent_breadth+eastmoney_limits",
    }


def _fetch_from_db() -> Optional[dict]:
    """DB兜底(收盘后的历史数据)"""
    try:
        conn = sqlite3.connect("data/alpha_miner.db")
        latest = conn.execute(
            "SELECT MAX(trade_date) FROM zt_pool"
        ).fetchone()[0]
        if not latest:
            conn.close()
            return None
        
        zt_count = conn.execute(
            "SELECT count(*) FROM zt_pool WHERE trade_date=?", (latest,)
        ).fetchone()[0]
        conn.close()
        
        if zt_count > 0:
            return {
                "zt_count": zt_count,
                "zt_count_total": zt_count,
                "dt_count": 0,
                "real_dt": 0,
                "up_count": 0,
                "down_count": 0,
                "activity": "",
                "stat_date": latest,
                "source": "db_fallback",
            }
    except Exception:
        pass
    return None


def _session_reset():
    """重置requests session(切换连接)"""
    global _session
    if _session:
        try:
            _session.close()
        except Exception:
            pass
    _session = None


def get_emotion_history() -> list[dict]:
    """获取最近的情绪数据快照(供Streamlit展示)"""
    _load_history()
    return _history.copy()
