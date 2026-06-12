"""
多模型审核 v2 — 用更专业的prompt，逐个调用避免限频
"""
import json
import os
import urllib.request
import time

URL = "https://open.bigmodel.cn/api/paas/v4/chat/completions"

EXPERT_PROMPT = """你现在是3位专家的合体，请分别从3个角度严格审核这个A股量化策略。不要客气，直接挑毛病。

## 策略概述
"涨停次日低吸": 昨日涨停的股票，如果次日开盘相对涨停价低开≥2%，以开盘价买入，收盘卖出。

## 数据(218天/859笔，已扣0.5%交易成本)
- 均收益+1.06%/笔, 胜率55.1%, PF=1.86, 夏普3.78
- 4段交叉验证全正(+1.04%/+0.50%/+1.43%/+1.26%)
- 反向验证: 高开>2%买入亏-2.52%, >5%亏-6.38%
- 外部验证: 雪球57%/1.2%, 学术1-2%, 华泰年化15-20%

## 专家1: 行为金融学教授
请从学术角度分析：
1. 这个超额收益的理论解释是什么？过度反应？注意力效应？还是别的？
2. 什么条件下这个效应会消失？（市场微观结构变化？参与者结构变化？）
3. 这个策略容量有多大？如果很多人同时做会怎样？
4. 你知道哪些学术论文直接验证了这个效应？

## 专家2: 私募量化基金经理
请从实盘角度挑刺：
1. 回测中可能遗漏的偏差有哪些？特别注意：
   - 开盘价能否实际成交？（低开2%的票开盘后可能继续下跌）
   - 收盘卖出有没有流动性问题？
   - 涨停原因不同的票混在一起是否合理？
2. 如果用5万块实盘，预期月收益和最大回撤是多少？
3. 最可能导致策略实盘失效的3个原因是什么？
4. 你会投自己的钱做这个策略吗？为什么？

## 专家3: 风控总监
请从风控角度审核：
1. 最大连续亏损可能持续多久？（中前段胜率49.4%）
2. 极端情况下的单笔最大亏损是多少？（涨停股次日可能跌停-10%）
3. 止损-5%够不够？有没有更好的风控方案？
4. 这个策略和策略C(反弹低吸,245笔均赚+0.94%)同时运行，相关性高不高？分散效果如何？

请逐个专家回答，每个专家至少给出3个具体的、可操作的建议。"""


def call_glm(prompt, model, label):
    api_key = os.environ.get("ZHIPU_API_KEY") or os.environ.get("ZAI_API_KEY")
    if not api_key:
        return f"ERROR({label}): ZHIPU_API_KEY or ZAI_API_KEY is not configured"

    data = json.dumps({
        "model": model,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 4000
    }).encode('utf-8')
    
    req = urllib.request.Request(URL, data=data, headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    })
    
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            content = result['choices'][0]['message']['content']
            print(f"\n{'='*70}")
            print(f"【{label}】")
            print(f"{'='*70}")
            print(content)
            return content
    except Exception as e:
        error_msg = f"ERROR({label}): {e}"
        print(error_msg)
        return error_msg


if __name__ == '__main__':
    results = {}
    
    # GLM-4-Flash (快速版)
    r1 = call_glm(EXPERT_PROMPT, "glm-4-flash", "GLM-4-Flash (智谱)")
    results['glm_4_flash'] = r1
    time.sleep(2)
    
    # GLM-4-Plus (深度版)
    r2 = call_glm(EXPERT_PROMPT, "glm-4-plus", "GLM-4-Plus (智谱旗舰)")
    results['glm_4_plus'] = r2
    
    with open('output/backtest/ai_expert_review.json', 'w') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存到 output/backtest/ai_expert_review.json")
