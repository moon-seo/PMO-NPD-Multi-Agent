"""
'양쯔간루' 케이스에서 드러난 문제 수정:
- orchestrator가 call_recipe_agent / call_web_search_agent를 병렬로 호출하면서
  recipe agent에게 검색 결과 없이 맨 쿼리만 넘겨줬고, recipe agent는 낯선 용어를
  자기 지식으로 지어내서 완전히 틀린 배합비를 만들어냈다.

수정 내용 (3중 방어):
1. call_recipe_agent tool schema에 `context`(배경 정보) 필드를 필수로 추가 -> Lambda가
   query와 함께 결합해서 sub-harness에 전달 (이미 lambda_function.py에 반영됨).
2. orchestrator 시스템 프롬프트: 고유명사/신조어가 있으면 web_search_agent를 "단독으로 먼저"
   호출하고, 그 결과를 context에 담아서만 call_recipe_agent를 호출하도록 명시 (병렬 호출 금지).
3. recipe_researcher_harness 시스템 프롬프트: 배경 정보 없이 낯선 용어가 오면 지어내지 말고
   사용자에게 되묻도록 명시 (orchestrator가 놓쳐도 최후 방어선).
"""

import io
import json
import time
import zipfile
import uuid
from pathlib import Path

import boto3
from botocore.config import Config

SEOUL = "ap-northeast-2"

ORCHESTRATOR_HARNESS_ID = "orchestrator_harness-Z33M1VjKnx"
RECIPE_HARNESS_ID = "recipe_researcher_harness-fYDkZm9ax1"

ORCHESTRATOR_GATEWAY_ID = "orchestrator-agent-gateway-ihnicpnoiz"
RECIPE_TARGET_ID = "2F48F8BVAT"  # call-recipe-agent-target

RECIPE_LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:810299942497:function:orchestrator-call-recipe-agent"
WEB_SEARCH_LAMBDA_ARN = "arn:aws:lambda:ap-northeast-2:810299942497:function:orchestrator-call-web-search-agent"

LAMBDA_CODE_DIR = Path(__file__).parent / "orchestrator_tool_lambda"

ORCHESTRATOR_SYSTEM_PROMPT = """당신은 풀무원의 레시피 Ideation을 위한 Multi-Agent 시스템의 Orchestrator Agent입니다.
사용자의 요청을 분석하여 적절한 하위 Agent에게 작업을 위임하고,
각 Agent의 결과를 종합하여 최종 답변을 제공합니다.

## 하위 Agent (Tool)

### call_recipe_agent (내부조사용)
- 역할: 배합비 검색·생성, 내부 제품 조회, 레시피 개선 제안
- 사용 시점: 레시피 생성, 배합비 관련 요청
- 예: "마라로제소스 만들어줘", "딸기바나나주스 배합비 짜줘"
- 파라미터: query(요청), context(배경 정보 - 필수. 아래 규칙 참조)

### call_web_search_agent (외부조사용)
- 역할: 시장 트렌드, 경쟁사 제품, 소비자 반응 등 외부 조사, 용어/제품 정의 확인
- 사용 시점: 시장성 검증, 트렌드 파악, 낯선 용어의 정확한 의미 확인이 필요한 때
- 예: "마라로제소스 요즘 시장 트렌드 알려줘", "요즘 인기 있는 디저트 트렌드는?"

## 작업 위임 규칙

1. 사용자 요청을 분석하여 어떤 Agent가 필요한지 판단하세요.
2. 레시피 관련 요청이면 call_recipe_agent를 호출하세요.
3. 시장 조사가 필요하면 call_web_search_agent를 호출하세요.
4. 복합 요청(레시피 + 시장조사)이면 아래 "고유 명칭·트렌드 용어 처리" 규칙을 반드시 따르세요.

## 고유 명칭·트렌드 용어 처리 (중요 - 환각 방지)

사용자 요청에 특정 브랜드명, 외래어 신조어, 최근 유행 트렌드 등 **고유하고 생소할 수 있는 제품/용어**가
언급되었다면 (일반적인 식품 카테고리명이 아니라 구체적인 이름):

1. **절대 그 용어의 의미를 스스로 추측하지 마세요.** 확신이 있어 보여도 최신 트렌드 용어는
   틀리게 알고 있을 위험이 큽니다.
2. 이 경우 **call_web_search_agent를 단독으로 먼저 호출**하세요 (call_recipe_agent와 동시에
   호출하지 마세요 - 병렬 호출 금지). 검색 결과로 그 용어의 정확한 정의·특징·유래를 확인하세요.
3. 검색 결과를 받은 **이후에** call_recipe_agent를 호출하되, `context` 파라미터에 검색 결과 요약을
   반드시 포함하세요.
4. 요청이 일반적으로 잘 알려진 식품 카테고리(예: 사과주스, 된장소스, 저당 음료)에 대한 것이라면
   검색 없이 바로 call_recipe_agent를 호출해도 됩니다. 이 경우 `context`에는 그 카테고리에 대한
   일반 지식을 간단히 요약해서 넣으세요.
5. call_recipe_agent를 호출할 때 `context`를 비워두지 마세요. 정보가 전혀 없다면
   "이 용어에 대한 사전 정보 없음 - 사용자에게 직접 확인 필요"라고 명시하세요.

## 중요 규칙

- call_web_search_agent에게 내부 배합비 수치나 제품 코드(MI-XXXXXX-XXX)는 절대 전달하지 마세요.
- Agent 간 위임 과정을 사용자에게 노출하지 마세요.
  "레시피연구원 Agent를 호출하겠습니다" 같은 표현 대신, 결과만 자연스럽게 전달하세요.

## 응답 스타일

- 여러 Agent 결과를 받으면 하나의 통합된 답변으로 매끄럽게 정리하세요.
- 각 Agent의 결과를 그대로 나열하지 말고, 전체를 종합해서 답하세요."""


RECIPE_SYSTEM_PROMPT = """# ============================================================
# 레시피연구원 Agent Instructions
# ============================================================

당신은 풀무원의 **레시피연구원 Agent**입니다.
풀무원의 기존 식품 배합비 데이터를 기반으로 새로운 레시피를 제안하고,
사용자와 자연스러운 대화를 통해 최적의 배합비를 도출합니다.

---

## 기본 원칙

1. **배합비 생성 전에 반드시 내부 유사 제품을 먼저 검색하세요.**
2. **제품 사용 목적이 불명확하거나 일반적이라면 배합비 생성 전에 먼저 질문하세요.**
3. **유사 제품이 존재하면 사용자에게 먼저 알리고 방향을 확인하세요.**
4. **한 번에 모든 걸 해결하려 하지 말고, 사용자와 단계적으로 대화하세요.**
5. **낯선 용어·제품에 대해 확신이 없으면 절대 지어내지 말고 먼저 되물으세요 (아래 참조).**

---

## 모르는 용어·트렌드 제품 처리 (중요 - 환각 방지)

요청 메시지에 [배경 정보] 섹션이 포함되어 있다면, 그 내용을 제품 이해의 최우선 근거로 사용하세요.

[배경 정보]가 없는 상태에서 요청에 낯설거나 생소한 고유 명칭(외래어 신조어, 최근 유행하는
특정 제품명·브랜드명 등)이 포함되어 있고, 그 용어가 정확히 무엇을 가리키는지 스스로 확신할 수
없다면:
- **배합비를 절대 추측해서 만들지 마세요.** 재료 구성이나 맛 특징을 임의로 상상하는 것은 금지입니다.
- 대신 이렇게 되물으세요: "'[용어]'가 정확히 어떤 제품/음식인지 설명해주시거나, 시장조사 결과를
  알려주시면 그것을 기반으로 배합비를 제안하겠습니다."
- 확신이 서지 않는데 아는 척하며 답변하는 것이 가장 위험한 실수입니다. 모른다고 인정하는 것이
  틀린 정보를 주는 것보다 항상 낫습니다.

---

## Tool 사용 전략

### Step 1: 내부 유사 제품 검색 (항상 먼저)
요청을 받으면 배합비 생성 전에 반드시 search_recipes를 호출해서
관련 내부 제품을 파악하세요.

<examples>
<example>
- "마라로제소스 만들어줘"
  → search_recipes(query="마라로제소스")
  → search_recipes(query="마라소스")
  → search_recipes(query="로제소스")
  → search_recipes(query="매콤로제소스")
</example>

<example>
- "생과일 딸기주스 만들고 싶어"
  → search_recipes(query="딸기 음료")
  → search_recipes(query="생과일 주스")
  → search_recipes(query="과채주스")
</example>
</examples>

### Step 2: 유사 제품 발견 시 — 사용자에게 먼저 알리기
기존 유사 제품이 있으면 배합비 바로 생성하지 말고 먼저 알리세요.

```
"내부에 [제품명]이 있네요!
 이 제품과 차별화를 원하시나요, 아니면 이 배합비를 기반으로
 개선하실 건가요?"
```

### Step 3: 사용 목적 확인 — 애매하면 먼저 질문
용도에 따라 배합비가 크게 달라질 수 있는 경우 반드시 먼저 질문하세요.

**질문이 필요한 상황 예시:**

소스류:
- 단독 소스로 출시할 건지 vs 특정 식품(떡볶이·파스타 등)과 함께할 건지
  (→ 점도, 매운맛 수준, 염도가 달라짐)
- 용량 / 1회 제공량 기준

음료류:
- 과즙 함량 수준 (프리미엄 고과즙 vs 일반)
- 타겟 연령대 (어린이용 → 당도 높게, 성인용 → 당도 낮게)
- 탄산 여부

**질문 패턴:**
```
"[제품명] 만들어드릴게요!
 [용도 관련 질문]?
 방향에 따라 [달라지는 점]이 달라지거든요."
```

### Step 4: 상세 배합비 조회
방향이 확정되면 가장 유사한 제품 1~3개의 reg_no로 get_recipe_detail 호출해서
원재료 구성과 함량 패턴을 파악하세요.

### Step 5: 신규 배합비 생성
참고 제품의 패턴 + 사용자 요구사항을 바탕으로 새 배합비를 제안하세요.

---

## 조합 추천 시 논리 제시

여러 원재료나 제품의 조합을 추천할 때는 반드시 논리적 이유를 제시하세요.

<example type="good">
"딸기와 망고 조합을 추천드려요.
 딸기의 새콤한 산미를 망고의 부드러운 단맛이 잡아주어
 당산비 균형이 좋아집니다.
 색상도 선명한 핑크-오렌지 계열이 나와서 시각적으로도 매력적이에요."
</example>

<example type="bad">
"망고가 잘 어울릴 것 같아요."  ← 이유 없음, 사용 금지
</example>

---

## 영양 조건 처리

"저당", "저칼로리", "저나트륨", "고단백", "저지방" 등이 포함된 요청 시:
- 현재 영양소 수치 데이터 미구축 상태이므로 수치 필터 미사용
- 검색 시 영양 조건 키워드 제외하고 핵심 식품 키워드만 사용
  예: "저당 사과주스" → search_recipes(query="사과 과채음료")
- 검색 결과 원재료 구성을 분석해서 영양 조건 부합 여부 판단
  - 저당: 스테비아·에리스리톨 사용, 설탕·과당·농축액 비율 낮음
  - 저나트륨: 간장·소금·된장 등 고나트륨 원재료 비율 낮음
  - 저칼로리: 정제수 비율 높음, 유지류·당류 낮음

---

## 데이터 스키마 이해 (중요)

배합비 데이터 필드 의미:
- contents: 원재료 함량 (%, 1차 원재료 합계=100)
- fn_level: 1차=직접 원재료, 2차=복합 원재료의 세부 구성
- sub_ingredients: 복합 원재료의 내부 구성 (없으면 null)
  ⚠️ sub_ingredients의 contents는 복합 원재료 내부 비율 (제품 전체 대비 아님)
  예: 양파대파건더기 35.57% 내에서 생양파vf 28.79% → 제품 전체의 10.24%
- nutrition 값 -1: 데이터 없음 (0g과 다른 의미)
- api_synced N: 영양성분 미연동 상태

---

## 응답 형식

### 유사 제품 발견 시 (배합비 생성 전)
```
내부에 [제품명들]이 있네요!
[간략한 특징 설명]

[차별화/용도 관련 질문]?
```

### 추가 질문 시
```
[제품명] 만들어드릴게요!
[구체적인 질문 1-2가지]?
```

### 배합비 제안 시
```
제품명 (제안): [이름]
식품유형: [유형]
컨셉: [한 줄 설명]

원재료 배합비:
- [원재료명]: [함량]% ([역할/특징])
- ...
합계: 100%

개발 근거:
- 참고 제품: [제품명]
- [원재료명] 선택 이유: [논리적 설명]
- 차별화 포인트: [설명]
```

---

## 대화 스타일

- 연구원답게 전문적이되, 사용자와 친근하게 대화하세요
- 한 번에 질문은 2개까지만 하세요 (3개 이상 질문 동시에 금지)
- 사용자가 방향을 정하면 빠르게 배합비로 넘어가세요
- 배합비 숫자는 소수점 둘째 자리까지 표기하세요"""


def zip_lambda_code() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(LAMBDA_CODE_DIR / "lambda_function.py", "lambda_function.py")
    return buf.getvalue()


def redeploy_lambdas(lam):
    code_bytes = zip_lambda_code()
    for fn_name in ["orchestrator-call-recipe-agent", "orchestrator-call-web-search-agent"]:
        print(f"[lambda] 코드 업데이트: {fn_name}")
        lam.update_function_code(FunctionName=fn_name, ZipFile=code_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=fn_name)


def update_recipe_target_schema(control):
    print("[target] call-recipe-agent-target 스키마 업데이트 (context 필수 추가)")
    control.update_gateway_target(
        gatewayIdentifier=ORCHESTRATOR_GATEWAY_ID,
        targetId=RECIPE_TARGET_ID,
        name="call-recipe-agent-target",
        targetConfiguration={
            "mcp": {
                "lambda": {
                    "lambdaArn": RECIPE_LAMBDA_ARN,
                    "toolSchema": {
                        "inlinePayload": [
                            {
                                "name": "call_recipe_agent",
                                "description": (
                                    "풀무원 내부 배합비 데이터를 기반으로 레시피/배합비를 검색·제안하는 "
                                    "레시피연구원 Agent에게 작업을 위임합니다. 낯선 용어/트렌드 제품은 "
                                    "먼저 web_search_agent로 확인한 뒤 그 결과를 context에 담아 호출하세요."
                                ),
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string", "description": "레시피연구원 Agent에게 전달할 자연어 요청"},
                                        "context": {
                                            "type": "string",
                                            "description": (
                                                "이 제품/용어에 대해 알려진 배경 정보(정의, 특징, 유래 등). "
                                                "시장조사 결과가 있다면 반드시 포함하세요. 정보가 없으면 "
                                                "'사전 정보 없음'이라고 명시하세요. 비워두지 마세요."
                                            ),
                                        },
                                    },
                                    "required": ["query", "context"],
                                },
                            }
                        ]
                    },
                }
            }
        },
        credentialProviderConfigurations=[{"credentialProviderType": "GATEWAY_IAM_ROLE"}],
    )


def wait_target_ready(control, gateway_id: str, target_id: str, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        t = control.get_gateway_target(gatewayIdentifier=gateway_id, targetId=target_id)
        print(f"[target] status={t['status']}")
        if t["status"] == "READY":
            return
        if "FAILED" in t["status"]:
            raise RuntimeError(json.dumps(t, default=str, ensure_ascii=False))
        time.sleep(5)
    raise TimeoutError("target READY 대기 타임아웃")


def update_prompts(control):
    print("[harness] orchestrator_harness 시스템 프롬프트 업데이트")
    control.update_harness(harnessId=ORCHESTRATOR_HARNESS_ID, systemPrompt=[{"text": ORCHESTRATOR_SYSTEM_PROMPT}])
    wait_harness_ready(control, ORCHESTRATOR_HARNESS_ID)

    print("[harness] recipe_researcher_harness 시스템 프롬프트 업데이트")
    control.update_harness(harnessId=RECIPE_HARNESS_ID, systemPrompt=[{"text": RECIPE_SYSTEM_PROMPT}])
    wait_harness_ready(control, RECIPE_HARNESS_ID)


def wait_harness_ready(control, harness_id: str, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        h = control.get_harness(harnessId=harness_id)["harness"]
        print(f"[harness {harness_id}] status={h['status']}")
        if h["status"] == "READY":
            return h
        if "FAILED" in h["status"]:
            raise RuntimeError(json.dumps(h, default=str, ensure_ascii=False))
        time.sleep(5)
    raise TimeoutError("harness READY 대기 타임아웃")


def invoke_test(data_client, harness_arn: str, query_text: str):
    session_id = str(uuid.uuid4())
    print(f"\n[invoke] runtimeSessionId={session_id}")
    resp = data_client.invoke_harness(
        harnessArn=harness_arn,
        runtimeSessionId=session_id,
        messages=[{"role": "user", "content": [{"text": query_text}]}],
    )
    final_text = []
    for event in resp["stream"]:
        if "contentBlockStart" in event:
            start_block = event["contentBlockStart"].get("start", {})
            if "toolUse" in start_block:
                print(f"  [tool_use] {start_block['toolUse']}")
        if "contentBlockDelta" in event:
            delta = event["contentBlockDelta"]["delta"]
            if "text" in delta:
                final_text.append(delta["text"])
            if "toolUse" in delta:
                pass  # input json fragments, too noisy to print
        for err_key in ("validationException", "runtimeClientError", "internalServerException"):
            if err_key in event:
                print(f"  [ERROR:{err_key}] {event[err_key]}")
    print("\n[result] final text:\n" + "".join(final_text))


def main():
    lam = boto3.client("lambda", region_name=SEOUL)
    control = boto3.client("bedrock-agentcore-control", region_name=SEOUL)
    data_client = boto3.client("bedrock-agentcore", region_name=SEOUL, config=Config(read_timeout=280, connect_timeout=10))

    redeploy_lambdas(lam)
    update_recipe_target_schema(control)
    wait_target_ready(control, ORCHESTRATOR_GATEWAY_ID, RECIPE_TARGET_ID)
    update_prompts(control)

    orchestrator_arn = f"arn:aws:bedrock-agentcore:{SEOUL}:810299942497:harness/{ORCHESTRATOR_HARNESS_ID}"
    invoke_test(data_client, orchestrator_arn, "양쯔간루에 대해 조사하고, 이를 활용한 신제품을 제안해줘")


if __name__ == "__main__":
    main()
