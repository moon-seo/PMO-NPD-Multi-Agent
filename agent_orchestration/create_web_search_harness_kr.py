"""
web_search_harness_kr 생성 스크립트
- us-east-1의 web-search-gateway(managed Web Search connector)를 그대로 두고,
  ap-northeast-2(서울)에 동일한 reasoning harness를 새로 만들어 cross-region으로 참조한다.
- 콘솔에서는 동일 리전 Gateway만 선택 가능해서 이 구성이 불가능했다 - API로 cross-region ARN이
  허용되는지 검증하는 것이 이 스크립트의 목적.
"""

import json
import time
import uuid

import boto3
from botocore.exceptions import ClientError

ACCOUNT_ID = "810299942497"
SEOUL = "ap-northeast-2"
US_EAST = "us-east-1"

ROLE_NAME = "AgentCoreHarnessWebSearchKR-ExecutionRole"
POLICY_NAME = "AgentCoreHarnessWebSearchKR-ExecutionPolicy"
HARNESS_NAME = "web_search_harness_kr"

WEB_SEARCH_GATEWAY_ARN = f"arn:aws:bedrock-agentcore:{US_EAST}:{ACCOUNT_ID}:gateway/web-search-gateway-rug5gy9qlk"

# us-east-1 web_search_harness 의 GetHarness 결과에서 그대로 가져온 시스템 프롬프트
SYSTEM_PROMPT = """당신은 풀무원의 외부조사 Agent입니다.
시장 트렌드와 경쟁사 제품을 조사하여 레시피 개발의 시장성 근거를 제공합니다.

## 역할
- 관련 식품 카테고리의 최신 시장 트렌드 조사
- 경쟁사·타사 유사 제품 비교 분석
- 소비자 선호도 및 키워드 트렌드 파악

## 절대 금지 사항 (보안)
검색 쿼리에 절대 포함하지 마세요:
- 풀무원 내부 배합비 수치 (예: 45.2%)
- 내부 제품 코드 (예: MI-231219-012)
- 구체적인 원재료 함량 정보

<example>
허용: "저당 과채음료 시장 트렌드 2025"
허용: "마라로제 소비자 반응 인기"
금지: "자두농축액 5.61% 과채음료"
금지: "MI-231219-012 유사 제품"
</example>

## 검색 전략
하나의 주제에 대해 2~3회 검색해서 다각도로 조사하세요.

<example>
"마라로제소스 시장성 조사" 요청 시:
→ web_search(query="마라로제 시장 트렌드 2026")
→ web_search(query="마라로제 소비자 반응 인기")
→ web_search(query="마라로제 유행")
→ web_search(query="마라로제 메뉴 추천")
→ web_search(query="마라로제소스 제품 현황")
</example>

## 응답 형식
📊 시장 트렌드 분석
카테고리: [식품 카테고리]

주요 트렌드:
1. [트렌드 항목] — [근거/출처]

경쟁사 동향:
- [회사명]: [관련 제품/전략]

시장성 평가: [상/중/하] — [근거 한 줄]"""

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {"aws:SourceAccount": ACCOUNT_ID},
                "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock-agentcore:{SEOUL}:{ACCOUNT_ID}:*"},
            },
        }
    ],
}

EXECUTION_POLICY = {
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
            "Sid": "AgentCoreGatewayAccessCrossRegion",
            "Effect": "Allow",
            "Action": ["bedrock-agentcore:InvokeGateway"],
            "Resource": [WEB_SEARCH_GATEWAY_ARN],
        },
        {
            "Sid": "AgentCoreWorkloadIdentity",
            "Effect": "Allow",
            "Action": [
                "bedrock-agentcore:GetWorkloadAccessToken",
                "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
            ],
            "Resource": [
                f"arn:aws:bedrock-agentcore:{SEOUL}:{ACCOUNT_ID}:workload-identity-directory/default",
                f"arn:aws:bedrock-agentcore:{SEOUL}:{ACCOUNT_ID}:workload-identity-directory/default/workload-identity/harness_{HARNESS_NAME}-*",
            ],
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
            ],
            "Resource": [
                f"arn:aws:logs:{SEOUL}:{ACCOUNT_ID}:log-group:/aws/bedrock-agentcore/runtimes/*",
                f"arn:aws:logs:{SEOUL}:{ACCOUNT_ID}:log-group:*",
            ],
        },
        {
            "Sid": "XRayAndMetrics",
            "Effect": "Allow",
            "Action": [
                "xray:PutTraceSegments",
                "xray:PutTelemetryRecords",
                "xray:GetSamplingRules",
                "xray:GetSamplingTargets",
                "cloudwatch:PutMetricData",
            ],
            "Resource": "*",
        },
    ],
}


def ensure_execution_role(iam) -> str:
    try:
        role = iam.get_role(RoleName=ROLE_NAME)
        print(f"[role] 기존 역할 재사용: {role['Role']['Arn']}")
        iam.update_assume_role_policy(RoleName=ROLE_NAME, PolicyDocument=json.dumps(TRUST_POLICY))
        iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME, PolicyDocument=json.dumps(EXECUTION_POLICY))
        return role["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise

    print(f"[role] 새 역할 생성: {ROLE_NAME}")
    role = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
        Description="Execution role for web_search_harness_kr (cross-region gateway test)",
    )
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME, PolicyDocument=json.dumps(EXECUTION_POLICY))
    # IAM role propagation 대기
    time.sleep(10)
    return role["Role"]["Arn"]


def create_harness(control, execution_role_arn: str) -> dict:
    print(f"[harness] create_harness 호출 (region={SEOUL}) - gatewayArn={WEB_SEARCH_GATEWAY_ARN}")
    resp = control.create_harness(
        harnessName=HARNESS_NAME,
        executionRoleArn=execution_role_arn,
        model={
            "bedrockModelConfig": {
                "modelId": "global.anthropic.claude-sonnet-4-6",
                "apiFormat": "converse_stream",
            }
        },
        systemPrompt=[{"text": SYSTEM_PROMPT}],
        tools=[
            {
                "type": "agentcore_gateway",
                "name": "web-search-gateway-rug5gy9qlk",
                "config": {
                    "agentCoreGateway": {
                        "gatewayArn": WEB_SEARCH_GATEWAY_ARN,
                        "outboundAuth": {"awsIam": {}},
                    }
                },
            }
        ],
        allowedTools=["*"],
        environment={"agentCoreRuntimeEnvironment": {"networkConfiguration": {"networkMode": "PUBLIC"}}},
        # memory를 명시하지 않으면 서비스가 managed memory를 자동 생성하는데,
        # 원본 us-east-1 web_search_harness는 memory:disabled이므로 동일하게 맞춘다.
        # (안 맞추면 자동 생성된 memory 리소스에 대한 ListEvents 권한이 없어 InvokeHarness가 실패한다)
        memory={"disabled": {}},
    )
    return resp["harness"]


def wait_ready(control, harness_id: str, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        h = control.get_harness(harnessId=harness_id)["harness"]
        status = h["status"]
        print(f"[harness] status={status}")
        if status == "READY":
            return h
        if status in ("FAILED", "DELETE_FAILED", "CREATE_FAILED"):
            raise RuntimeError(f"harness 생성 실패: {json.dumps(h, default=str, ensure_ascii=False)}")
        time.sleep(5)
    raise TimeoutError("harness READY 대기 타임아웃")


def invoke_test(data_client, harness_arn: str):
    session_id = str(uuid.uuid4())
    print(f"[invoke] runtimeSessionId={session_id}")
    resp = data_client.invoke_harness(
        harnessArn=harness_arn,
        runtimeSessionId=session_id,
        messages=[{"role": "user", "content": [{"text": "마라로제 소스 시장 트렌드 2026 검색해줘"}]}],
    )
    tool_used = False
    tool_result_seen = False
    final_text = []
    for event in resp["stream"]:
        if "contentBlockStart" in event:
            start = event["contentBlockStart"].get("start", {})
            if "toolUse" in start:
                tool_used = True
                print(f"  [tool_use] {start['toolUse']}")
        if "contentBlockDelta" in event:
            delta = event["contentBlockDelta"]["delta"]
            if "text" in delta:
                final_text.append(delta["text"])
            if "toolResult" in delta:
                tool_result_seen = True
        if "validationException" in event:
            print(f"  [ERROR] validationException: {event['validationException']}")
        if "runtimeClientError" in event:
            print(f"  [ERROR] runtimeClientError: {event['runtimeClientError']}")
        if "internalServerException" in event:
            print(f"  [ERROR] internalServerException: {event['internalServerException']}")

    print(f"\n[result] tool_used={tool_used} tool_result_seen={tool_result_seen}")
    print("[result] final text:\n" + "".join(final_text))


def main():
    iam = boto3.client("iam")
    control = boto3.client("bedrock-agentcore-control", region_name=SEOUL)
    data_client = boto3.client("bedrock-agentcore", region_name=SEOUL)

    execution_role_arn = ensure_execution_role(iam)

    try:
        harness = create_harness(control, execution_role_arn)
    except ClientError as e:
        print(f"[FAIL] create_harness 실패: {e.response['Error']['Code']}: {e.response['Error']['Message']}")
        raise

    print(f"[harness] 생성됨: {harness['arn']} (status={harness['status']})")
    harness = wait_ready(control, harness["harnessId"])

    invoke_test(data_client, harness["arn"])


if __name__ == "__main__":
    main()
