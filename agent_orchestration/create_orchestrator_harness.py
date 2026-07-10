"""
orchestrator_harness 생성 스크립트 (inline_function + 클라이언트 사이드 tool-calling 루프)

시도했던 접근(Gateway의 http.agentcoreRuntime target으로 다른 harness를 노출)은
"HTTP target configuration is not supported for gateways with MCP protocol type" 에러로
막혀 있었다 (문서에는 있지만 이 계정/리전에서는 아직 미지원). mcp.mcpServer는 Harness가 아니라
진짜 MCP 서버(FastMCP 등으로 직접 구현한 Runtime)를 위한 target이라 Harness에는 맞지 않는다.

그래서 orchestrator harness에는 Gateway 없이 inline_function tool 2개만 정의하고,
harness가 toolUse를 emit하면 이 스크립트가 그걸 가로채서 recipe_researcher_harness /
web_search_harness_tavily_kr의 InvokeHarness를 직접 호출한 뒤 toolResult로 돌려주는
표준 tool-calling 루프를 구현한다 (Bedrock Converse API의 tool use 패턴과 동일).
"""

import json
import time
import uuid

import boto3
from botocore.exceptions import ClientError

ACCOUNT_ID = "810299942497"
SEOUL = "ap-northeast-2"

RECIPE_HARNESS_ARN = "arn:aws:bedrock-agentcore:ap-northeast-2:810299942497:harness/recipe_researcher_harness-fYDkZm9ax1"
WEB_SEARCH_HARNESS_ARN = "arn:aws:bedrock-agentcore:ap-northeast-2:810299942497:harness/web_search_harness_tavily_kr-hfzc6SS4Gb"

HARNESS_NAME = "orchestrator_harness"
HARNESS_ROLE_NAME = "AgentCoreHarnessOrchestrator-ExecutionRole"

SYSTEM_PROMPT = """당신은 풀무원의 레시피 Ideation을 위한 Multi-Agent 시스템의 Orchestrator Agent입니다.
사용자의 요청을 분석하여 적절한 하위 Agent에게 작업을 위임하고,
각 Agent의 결과를 종합하여 최종 답변을 제공합니다.

## 하위 Agent (Tool)

### call_recipe_agent (내부조사용)
- 역할: 배합비 검색·생성, 내부 제품 조회, 레시피 개선 제안
- 사용 시점: 레시피 생성, 배합비 관련 요청
- 예: "마라로제소스 만들어줘", "딸기바나나주스 배합비 짜줘"

### call_web_search_agent (외부조사용)
- 역할: 시장 트렌드, 경쟁사 제품, 소비자 반응 등 외부 조사
- 사용 시점: 시장성 검증, 트렌드 파악이 필요한 때
- 예: "마라로제소스 요즘 시장 트렌드 알려줘", "요즘 인기 있는 디저트 트렌드는?"

## 작업 위임 규칙

1. 사용자 요청을 분석하여 어떤 Agent가 필요한지 판단하세요.
2. 레시피 관련 요청이면 call_recipe_agent를 호출하세요.
3. 시장 조사가 필요하면 call_web_search_agent를 호출하세요.
4. 복합 요청이면 두 tool을 모두 호출하고 결과를 종합하세요.
   예: "마라로제소스 만들고 시장성도 알려줘"
   → call_recipe_agent로 배합비 생성
   → call_web_search_agent로 시장 트렌드 조사
   → 두 결과를 자연스럽게 종합

## 중요 규칙

- call_web_search_agent에게 내부 배합비 수치나 제품 코드(MI-XXXXXX-XXX)는 절대 전달하지 마세요.
- Agent 간 위임 과정을 사용자에게 노출하지 마세요.
  "레시피연구원 Agent를 호출하겠습니다" 같은 표현 대신, 결과만 자연스럽게 전달하세요.

## 응답 스타일

- 여러 Agent 결과를 받으면 하나의 통합된 답변으로 매끄럽게 정리하세요.
- 각 Agent의 결과를 그대로 나열하지 말고, 전체를 종합해서 답하세요."""

TOOLS = [
    {
        "type": "inline_function",
        "name": "call_recipe_agent",
        "config": {
            "inlineFunction": {
                "description": "풀무원 내부 배합비 데이터를 기반으로 레시피/배합비를 검색·제안하는 레시피연구원 Agent에게 작업을 위임합니다.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"query": {"type": "string", "description": "레시피연구원 Agent에게 전달할 자연어 요청"}},
                    "required": ["query"],
                },
            }
        },
    },
    {
        "type": "inline_function",
        "name": "call_web_search_agent",
        "config": {
            "inlineFunction": {
                "description": "시장 트렌드, 경쟁사 제품, 소비자 반응 등을 웹에서 조사하는 외부조사 Agent에게 작업을 위임합니다.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"query": {"type": "string", "description": "외부조사 Agent에게 전달할 자연어 요청"}},
                    "required": ["query"],
                },
            }
        },
    },
]


def ensure_harness_role(iam) -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {"aws:SourceAccount": ACCOUNT_ID},
                "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock-agentcore:{SEOUL}:{ACCOUNT_ID}:*"},
            },
        }],
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "BedrockModelInvocation",
                "Effect": "Allow",
                "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                "Resource": [
                    "arn:aws:bedrock:*::foundation-model/anthropic.claude-sonnet-4-6",
                    "arn:aws:bedrock:*:*:inference-profile/global.anthropic.claude-sonnet-4-6",
                ],
            },
            {
                "Sid": "AgentCoreWorkloadIdentity",
                "Effect": "Allow",
                "Action": ["bedrock-agentcore:GetWorkloadAccessToken", "bedrock-agentcore:GetWorkloadAccessTokenForJWT"],
                "Resource": [
                    f"arn:aws:bedrock-agentcore:{SEOUL}:{ACCOUNT_ID}:workload-identity-directory/default",
                    f"arn:aws:bedrock-agentcore:{SEOUL}:{ACCOUNT_ID}:workload-identity-directory/default/workload-identity/harness_{HARNESS_NAME}-*",
                ],
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"],
                "Resource": [
                    f"arn:aws:logs:{SEOUL}:{ACCOUNT_ID}:log-group:/aws/bedrock-agentcore/runtimes/*",
                    f"arn:aws:logs:{SEOUL}:{ACCOUNT_ID}:log-group:*",
                ],
            },
            {
                "Sid": "XRayAndMetrics",
                "Effect": "Allow",
                "Action": ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets", "cloudwatch:PutMetricData"],
                "Resource": "*",
            },
        ],
    }
    try:
        role = iam.get_role(RoleName=HARNESS_ROLE_NAME)
        print(f"[role] 기존 재사용: {HARNESS_ROLE_NAME}")
        iam.update_assume_role_policy(RoleName=HARNESS_ROLE_NAME, PolicyDocument=json.dumps(trust))
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        print(f"[role] 새로 생성: {HARNESS_ROLE_NAME}")
        role = iam.create_role(RoleName=HARNESS_ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust))
        time.sleep(10)
    iam.put_role_policy(RoleName=HARNESS_ROLE_NAME, PolicyName="execution-policy", PolicyDocument=json.dumps(policy))
    return role["Role"]["Arn"]


def ensure_harness(control, execution_role_arn: str) -> dict:
    existing = control.list_harnesses()
    for h in existing.get("harnesses", []):
        if h["harnessName"] == HARNESS_NAME:
            print(f"[harness] 기존 재사용: {h['harnessId']}")
            return control.get_harness(harnessId=h["harnessId"])["harness"]

    print(f"[harness] 새로 생성: {HARNESS_NAME}")
    resp = control.create_harness(
        harnessName=HARNESS_NAME,
        executionRoleArn=execution_role_arn,
        model={"bedrockModelConfig": {"modelId": "global.anthropic.claude-sonnet-4-6", "apiFormat": "converse_stream"}},
        systemPrompt=[{"text": SYSTEM_PROMPT}],
        tools=TOOLS,
        allowedTools=["*"],
        environment={"agentCoreRuntimeEnvironment": {"networkConfiguration": {"networkMode": "PUBLIC"}}},
        memory={"disabled": {}},
    )
    return resp["harness"]


def wait_harness_ready(control, harness_id: str, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        h = control.get_harness(harnessId=harness_id)["harness"]
        print(f"[harness] status={h['status']}")
        if h["status"] == "READY":
            return h
        if "FAILED" in h["status"]:
            raise RuntimeError(json.dumps(h, default=str, ensure_ascii=False))
        time.sleep(5)
    raise TimeoutError("harness READY 대기 타임아웃")


def run_harness_turn(data_client, harness_arn: str, session_id: str, messages: list) -> tuple:
    """한 번의 InvokeHarness 호출을 실행하고 (assistant_message, tool_uses) 를 반환한다."""
    resp = data_client.invoke_harness(harnessArn=harness_arn, runtimeSessionId=session_id, messages=messages)

    role = "assistant"
    blocks = {}  # index -> block dict
    tool_input_buffers = {}  # index -> str buffer

    for event in resp["stream"]:
        if "messageStart" in event:
            role = event["messageStart"]["role"]
            blocks = {}
            tool_input_buffers = {}
        elif "contentBlockStart" in event:
            idx = event["contentBlockStart"]["contentBlockIndex"]
            start = event["contentBlockStart"].get("start", {})
            if "toolUse" in start:
                blocks[idx] = {"toolUse": {"toolUseId": start["toolUse"]["toolUseId"], "name": start["toolUse"]["name"], "input": {}}}
                tool_input_buffers[idx] = ""
            else:
                blocks[idx] = {"text": ""}
        elif "contentBlockDelta" in event:
            idx = event["contentBlockDelta"]["contentBlockIndex"]
            delta = event["contentBlockDelta"]["delta"]
            if "text" in delta:
                blocks.setdefault(idx, {"text": ""})
                blocks[idx]["text"] = blocks[idx].get("text", "") + delta["text"]
            elif "toolUse" in delta:
                tool_input_buffers[idx] = tool_input_buffers.get(idx, "") + delta["toolUse"]["input"]
        elif "contentBlockStop" in event:
            idx = event["contentBlockStop"]["contentBlockIndex"]
            if idx in tool_input_buffers and idx in blocks and "toolUse" in blocks[idx]:
                raw = tool_input_buffers[idx]
                blocks[idx]["toolUse"]["input"] = json.loads(raw) if raw else {}
        elif "validationException" in event:
            raise RuntimeError(f"validationException: {event['validationException']}")
        elif "runtimeClientError" in event:
            raise RuntimeError(f"runtimeClientError: {event['runtimeClientError']}")
        elif "internalServerException" in event:
            raise RuntimeError(f"internalServerException: {event['internalServerException']}")

    content = [blocks[i] for i in sorted(blocks.keys())]
    assistant_message = {"role": role, "content": content}
    tool_uses = [b["toolUse"] for b in content if "toolUse" in b]
    return assistant_message, tool_uses


def call_sub_agent(data_client, harness_arn: str, query: str) -> str:
    """
    sub-agent(recipe/web_search harness)는 자체 Gateway tool을 서버 사이드에서 처리하는데,
    이 과정이 내부적으로 messageStart/Stop을 여러 번 거칠 수 있어 content block index가
    구간마다 리셋된다. 여기서는 구조화된 메시지 재구성 없이 text delta만 순서대로 이어붙인다
    (orchestrator쪽 run_harness_turn과 달리 이 응답을 다시 보낼 필요가 없어 단순 처리로 충분).
    """
    session_id = str(uuid.uuid4())
    messages = [{"role": "user", "content": [{"text": query}]}]
    resp = data_client.invoke_harness(harnessArn=harness_arn, runtimeSessionId=session_id, messages=messages)

    final_text = []
    for event in resp["stream"]:
        if "contentBlockDelta" in event:
            delta = event["contentBlockDelta"]["delta"]
            if "text" in delta:
                final_text.append(delta["text"])
        elif "validationException" in event:
            raise RuntimeError(f"validationException: {event['validationException']}")
        elif "runtimeClientError" in event:
            raise RuntimeError(f"runtimeClientError: {event['runtimeClientError']}")
        elif "internalServerException" in event:
            raise RuntimeError(f"internalServerException: {event['internalServerException']}")

    return "".join(final_text)


def orchestrate(data_client, orchestrator_arn: str, user_query: str, max_turns=6) -> str:
    session_id = str(uuid.uuid4())
    messages = [{"role": "user", "content": [{"text": user_query}]}]

    for turn in range(max_turns):
        print(f"\n[orchestrator turn {turn}] invoke_harness 호출")
        assistant_message, tool_uses = run_harness_turn(data_client, orchestrator_arn, session_id, messages)
        messages.append(assistant_message)

        text = "".join(b.get("text", "") for b in assistant_message["content"])
        if text:
            print(f"  [assistant text] {text[:200]}")

        if not tool_uses:
            return text

        tool_result_blocks = []
        for tu in tool_uses:
            query = tu["input"].get("query", "")
            print(f"  [tool_use] {tu['name']}(query={query!r})")
            if tu["name"] == "call_recipe_agent":
                answer = call_sub_agent(data_client, RECIPE_HARNESS_ARN, query)
            elif tu["name"] == "call_web_search_agent":
                answer = call_sub_agent(data_client, WEB_SEARCH_HARNESS_ARN, query)
            else:
                answer = f"알 수 없는 tool: {tu['name']}"
            print(f"  [sub-agent answer] {answer[:200]}")
            tool_result_blocks.append({
                "toolResult": {"toolUseId": tu["toolUseId"], "content": [{"text": answer}], "status": "success"}
            })
        messages.append({"role": "user", "content": tool_result_blocks})

    raise RuntimeError("max_turns 초과")


def main():
    iam = boto3.client("iam")
    control = boto3.client("bedrock-agentcore-control", region_name=SEOUL)
    data_client = boto3.client("bedrock-agentcore", region_name=SEOUL)

    harness_role_arn = ensure_harness_role(iam)
    harness = ensure_harness(control, harness_role_arn)
    harness = wait_harness_ready(control, harness["harnessId"])
    print(f"[harness] arn={harness['arn']}")

    final_answer = orchestrate(
        data_client,
        harness["arn"],
        "마라로제소스 배합비도 짜주고, 요즘 마라로제 시장 트렌드도 같이 알려줘",
    )
    print("\n" + "=" * 60)
    print("[최종 답변]")
    print(final_answer)


if __name__ == "__main__":
    main()
