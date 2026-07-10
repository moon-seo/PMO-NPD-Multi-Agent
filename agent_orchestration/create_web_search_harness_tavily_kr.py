"""
web_search_harness_tavily_kr 생성 스크립트
- Tavily Search API를 Lambda로 감싸서 새 Gateway target으로 노출하고,
  recipe_researcher_harness와 동일한 패턴(Harness + Gateway + Lambda target)으로
  ap-northeast-2에 새 harness를 만든다.
- 기존 web_search_harness_kr(us-east-1 managed Web Search connector 사용)은 건드리지 않는다 -
  두 harness를 나중에 같은 질의로 비교하기 위함.
"""

import io
import json
import time
import uuid
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ACCOUNT_ID = "810299942497"
SEOUL = "ap-northeast-2"

SECRET_ARN = "arn:aws:secretsmanager:ap-northeast-2:810299942497:secret:tavily-api-key-web-search-kr-7OFZZS"

LAMBDA_NAME = "tavily-search-target"
LAMBDA_ROLE_NAME = "tavily-lambda-role"
GATEWAY_NAME = "tavily-search-gateway"
GATEWAY_ROLE_NAME = "tavily-gateway-role"
TARGET_NAME = "tavily-search-target"
HARNESS_NAME = "web_search_harness_tavily_kr"
HARNESS_ROLE_NAME = "AgentCoreHarnessWebSearchTavilyKR-ExecutionRole"

LAMBDA_CODE_DIR = Path(__file__).parent / "tavily_lambda"

# web_search_harness(us-east-1)와 동일한 프롬프트를 재사용 (tool 예시만 tavily 기준으로 일반화된 표현 유지)
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
하나의 주제에 대해 2~3회 검색해서 다각도로 조사하세요. 한국 고유의 신조어·트렌드 키워드는
한국어 그대로 검색하세요.

<example>
"마라로제소스 시장성 조사" 요청 시:
→ tavily_web_search(query="마라로제 시장 트렌드 2026")
→ tavily_web_search(query="마라로제 소비자 반응 인기")
→ tavily_web_search(query="마라로제 유행")
</example>

## 응답 형식
📊 시장 트렌드 분석
카테고리: [식품 카테고리]

주요 트렌드:
1. [트렌드 항목] — [근거/출처]

경쟁사 동향:
- [회사명]: [관련 제품/전략]

시장성 평가: [상/중/하] — [근거 한 줄]"""


def zip_lambda_code() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(LAMBDA_CODE_DIR / "lambda_function.py", "lambda_function.py")
    return buf.getvalue()


def ensure_lambda_role(iam) -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "SecretsRead", "Effect": "Allow", "Action": ["secretsmanager:GetSecretValue"], "Resource": [SECRET_ARN]},
        ],
    }
    try:
        role = iam.get_role(RoleName=LAMBDA_ROLE_NAME)
        print(f"[role] 기존 재사용: {LAMBDA_ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        print(f"[role] 새로 생성: {LAMBDA_ROLE_NAME}")
        role = iam.create_role(RoleName=LAMBDA_ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust))
        iam.attach_role_policy(
            RoleName=LAMBDA_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        )
        time.sleep(10)
    iam.put_role_policy(RoleName=LAMBDA_ROLE_NAME, PolicyName="tavily-secrets-access", PolicyDocument=json.dumps(policy))
    return role["Role"]["Arn"]


def ensure_lambda(lam, role_arn: str) -> str:
    code_bytes = zip_lambda_code()
    try:
        fn = lam.get_function(FunctionName=LAMBDA_NAME)
        print(f"[lambda] 기존 재사용, 코드 업데이트: {LAMBDA_NAME}")
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=code_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Environment={"Variables": {"TAVILY_SECRET_ARN": SECRET_ARN}},
        )
        lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
        return fn["Configuration"]["FunctionArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"[lambda] 새로 생성: {LAMBDA_NAME}")
    resp = lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": code_bytes},
        Timeout=30,
        MemorySize=256,
        Environment={"Variables": {"TAVILY_SECRET_ARN": SECRET_ARN}},
    )
    return resp["FunctionArn"]


def ensure_gateway_role(iam, lambda_arn: str) -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "bedrock-agentcore.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": [lambda_arn]}],
    }
    try:
        role = iam.get_role(RoleName=GATEWAY_ROLE_NAME)
        print(f"[role] 기존 재사용: {GATEWAY_ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        print(f"[role] 새로 생성: {GATEWAY_ROLE_NAME}")
        role = iam.create_role(RoleName=GATEWAY_ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust))
        time.sleep(10)
    iam.put_role_policy(RoleName=GATEWAY_ROLE_NAME, PolicyName="lambda", PolicyDocument=json.dumps(policy))
    print("[role] IAM 전파 대기 (15s)")
    time.sleep(15)
    return role["Role"]["Arn"]


def ensure_gateway(control, role_arn: str) -> dict:
    """GetGateway 응답(dict)을 그대로 반환한다 - wrapper 키 없이 gatewayArn 등이 top-level에 있다."""
    existing = control.list_gateways()
    for gw in existing.get("items", []):
        if gw["name"] == GATEWAY_NAME:
            print(f"[gateway] 기존 재사용: {gw['gatewayId']}")
            return control.get_gateway(gatewayIdentifier=gw["gatewayId"])

    print(f"[gateway] 새로 생성: {GATEWAY_NAME}")
    resp = control.create_gateway(
        name=GATEWAY_NAME,
        roleArn=role_arn,
        protocolType="MCP",
        authorizerType="AWS_IAM",
    )
    gw_id = resp["gatewayId"]
    _wait_gateway_ready(control, gw_id)
    return control.get_gateway(gatewayIdentifier=gw_id)


def _wait_gateway_ready(control, gateway_id: str, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        gw = control.get_gateway(gatewayIdentifier=gateway_id)
        print(f"[gateway] status={gw['status']}")
        if gw["status"] == "READY":
            return
        time.sleep(5)
    raise TimeoutError("gateway READY 대기 타임아웃")


def ensure_gateway_target(control, gateway_id: str, lambda_arn: str):
    existing = control.list_gateway_targets(gatewayIdentifier=gateway_id)
    for t in existing.get("items", []):
        if t["name"] == TARGET_NAME:
            print(f"[target] 기존 재사용: {t['targetId']}")
            return

    print(f"[target] 새로 생성: {TARGET_NAME}")
    last_err = None
    for attempt in range(6):
        try:
            _create_gateway_target(control, gateway_id, lambda_arn)
            return
        except ClientError as e:
            if e.response["Error"]["Code"] != "ValidationException" or "lacks permission" not in str(e):
                raise
            last_err = e
            print(f"[target] IAM 전파 대기 중... 재시도 {attempt + 1}/6")
            time.sleep(10)
    raise last_err


def _create_gateway_target(control, gateway_id: str, lambda_arn: str):
    control.create_gateway_target(
        gatewayIdentifier=gateway_id,
        name=TARGET_NAME,
        targetConfiguration={
            "mcp": {
                "lambda": {
                    "lambdaArn": lambda_arn,
                    "toolSchema": {
                        "inlinePayload": [
                            {
                                "name": "tavily_web_search",
                                "description": "Tavily Search API를 이용한 웹 검색. 한국어 신조어·최신 트렌드 검색에 유리.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string", "description": "검색어"},
                                        "max_results": {"type": "integer", "description": "반환할 결과 수 (기본값 5)"},
                                    },
                                    "required": ["query"],
                                },
                            }
                        ]
                    },
                }
            }
        },
        credentialProviderConfigurations=[{"credentialProviderType": "GATEWAY_IAM_ROLE"}],
    )


def ensure_harness_role(iam, gateway_arn: str) -> str:
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
                "Sid": "AgentCoreGatewayAccess",
                "Effect": "Allow",
                "Action": ["bedrock-agentcore:InvokeGateway"],
                "Resource": [gateway_arn],
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


def ensure_harness(control, execution_role_arn: str, gateway_arn: str, gateway_name: str) -> dict:
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
        tools=[{
            "type": "agentcore_gateway",
            "name": gateway_name,
            "config": {"agentCoreGateway": {"gatewayArn": gateway_arn, "outboundAuth": {"awsIam": {}}}},
        }],
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


def invoke_test(data_client, harness_arn: str, query_text: str):
    session_id = str(uuid.uuid4())
    print(f"\n[invoke] runtimeSessionId={session_id}")
    resp = data_client.invoke_harness(
        harnessArn=harness_arn,
        runtimeSessionId=session_id,
        messages=[{"role": "user", "content": [{"text": query_text}]}],
    )
    tool_used = False
    final_text = []
    for event in resp["stream"]:
        if "contentBlockStart" in event:
            start_block = event["contentBlockStart"].get("start", {})
            if "toolUse" in start_block:
                tool_used = True
                print(f"  [tool_use] {start_block['toolUse']}")
        if "contentBlockDelta" in event:
            delta = event["contentBlockDelta"]["delta"]
            if "text" in delta:
                final_text.append(delta["text"])
            if "toolResult" in delta:
                for item in delta["toolResult"]:
                    text = item.get("text", "")
                    print(f"  [tool_result] {text[:800]}")
        for err_key in ("validationException", "runtimeClientError", "internalServerException"):
            if err_key in event:
                print(f"  [ERROR:{err_key}] {event[err_key]}")

    print(f"\n[result] tool_used={tool_used}")
    print("[result] final text:\n" + "".join(final_text))


def main():
    iam = boto3.client("iam")
    lam = boto3.client("lambda", region_name=SEOUL)
    control = boto3.client("bedrock-agentcore-control", region_name=SEOUL)
    data_client = boto3.client("bedrock-agentcore", region_name=SEOUL)

    lambda_role_arn = ensure_lambda_role(iam)
    lambda_arn = ensure_lambda(lam, lambda_role_arn)
    print(f"[lambda] arn={lambda_arn}")

    gateway_role_arn = ensure_gateway_role(iam, lambda_arn)
    gw_resp = ensure_gateway(control, gateway_role_arn)

    gateway_id = gw_resp["gatewayId"]
    gateway_arn = gw_resp["gatewayArn"]
    print(f"[gateway] id={gateway_id} arn={gateway_arn}")

    ensure_gateway_target(control, gateway_id, lambda_arn)

    harness_role_arn = ensure_harness_role(iam, gateway_arn)
    harness = ensure_harness(control, harness_role_arn, gateway_arn, gateway_id)
    harness = wait_harness_ready(control, harness["harnessId"])
    print(f"[harness] arn={harness['arn']}")

    invoke_test(
        data_client,
        harness["arn"],
        "양쯔간루가 뭐야? 유래와 소개, 해외권 인기를 조사해줘.",
    )


if __name__ == "__main__":
    main()
