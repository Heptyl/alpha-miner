"""
多模型交叉审核 — 智谱GLM-4 + 通义千问
"""
import json
import urllib.request
import os

REVIEW_PROMPT = """你是一位资深量化策略研究员，请审核以下A股交易策略的回测结果和逻辑合理性。

## 策略描述
策略名称：涨停次日低吸（策略B改版）
策略逻辑：
1. 选股：从涨停池中选昨日涨停的股票
2. 买入条件：次日开盘价相对昨日涨停价低开≥2%时，以开盘价买入
3. 卖出条件：当日收盘卖出（日内策略），盘中跌破-5%止损
4. 过滤：排除创业板(300xxx)、科创板(688xxx)、B股、成交额<1000万的小盘股

## 回测结果
- 回测期间：218个交易日
- 有效交易：859笔（日均3.9笔）
- 平均每笔收益：+1.06%（已扣除双向交易成本0.5%）
- 胜率：55.1%
- 盈亏比(PF)：1.86
- 夏普比率：3.78

## 交叉验证（按时间4段）
- 前1/4：154笔，+1.04%/笔，胜率52.6%
- 中前1/4：243笔，+0.50%/笔，胜率49.4%
- 中后1/4：268笔，+1.43%/笔，胜率58.2%
- 后1/4：194笔，+1.26%/笔，胜率59.8%

## 低开阈值敏感性
- 低开>1%：1455笔，+0.42%/笔，胜率48.9%
- 低开>2%：859笔，+1.06%/笔，胜率55.1%
- 低开>3%：491笔，+1.69%/笔，胜率58.7%
- 低开>5%：131笔，+3.81%/笔，胜率76.3%

## 反向验证
- 高开>2%买入：1657笔，-2.52%/笔，胜率26.5%
- 高开>5%买入：856笔，-6.38%/笔，胜率6.3%

## 外部验证
- 雪球实盘(237笔)：胜率57%，均赚1.2%
- 学术论文：涨停次日低开买入超额收益1-2%
- 华泰证券研报：年化15-20%

## 请回答
1. 策略逻辑是否合理？有没有漏洞？
2. 回测数据是否可信？有没有遗漏的偏差？
3. 理论基础是什么？行为金融学如何解释？
4. 最大风险和实盘问题？
5. 低开阈值-2%是否合适？
6. 能否在实盘中持续盈利？
7. 你有没有见过类似策略的验证？"""


def call_zhipu(prompt, model="glm-4-flash"):
    """调用智谱AI API"""
    api_key = os.environ.get("ZHIPU_API_KEY") or os.environ.get("ZAI_API_KEY")
    if not api_key:
        return "ERROR: ZHIPU_API_KEY or ZAI_API_KEY is not configured"
    
    url = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
    data = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content": "你是一位资深量化策略研究员，擅长A股短线策略分析、行为金融学和风险评估。请用中文回答，回答要专业严谨，不要泛泛而谈，要给出具体的数据分析和风险点。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 4000
    }).encode('utf-8')
    
    req = urllib.request.Request(url, data=data, headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    })
    
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            return result['choices'][0]['message']['content']
    except Exception as e:
        return f"ERROR: {e}"


def call_qwen(prompt):
    """调用通义千问 API"""
    # 从环境变量获取
    api_key = os.environ.get('QWEN_PORTAL_API_KEY', '')
    if not api_key:
        return "ERROR: No Qwen API key in env"
    
    url = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
    data = json.dumps({
        "model": "qwen-plus",
        "messages": [
            {"role": "system", "content": "你是一位资深量化策略研究员，擅长A股短线策略分析、行为金融学和风险评估。请用中文回答，回答要专业严谨，不要泛泛而谈，要给出具体的数据分析和风险点。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 4000
    }).encode('utf-8')
    
    req = urllib.request.Request(url, data=data, headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    })
    
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            return result['choices'][0]['message']['content']
    except Exception as e:
        return f"ERROR: {e}"


if __name__ == '__main__':
    results = {}
    
    # 1. 智谱 GLM-4-Flash
    print("=" * 70)
    print("【模型1】智谱 GLM-4-Flash")
    print("=" * 70)
    r1 = call_zhipu(REVIEW_PROMPT, "glm-4-flash")
    print(r1[:3000])
    results['glm_4_flash'] = r1
    
    # 2. 智谱 GLM-4-Plus (更强的模型)
    print("\n" + "=" * 70)
    print("【模型2】智谱 GLM-4-Plus")
    print("=" * 70)
    r2 = call_zhipu(REVIEW_PROMPT, "glm-4-plus")
    print(r2[:3000])
    results['glm_4_plus'] = r2
    
    # 3. 通义千问
    print("\n" + "=" * 70)
    print("【模型3】通义千问 Qwen-Plus")
    print("=" * 70)
    r3 = call_qwen(REVIEW_PROMPT)
    print(r3[:3000])
    results['qwen_plus'] = r3
    
    # 保存
    with open('output/backtest/ai_review_results.json', 'w') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n完整结果已保存到 output/backtest/ai_review_results.json")
