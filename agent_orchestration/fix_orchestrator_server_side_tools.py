"""
orchestrator_harness의 tool을 inline_function -> agentcore_gateway(Lambda target)로 교체.

inline_function은 harness가 스스로 실행 못 해서 콘솔 harness playground 같은 범용
클라이언트에서는 동작하지 않는다 (toolUse가 허공에 뜸). 이번에는 recipe-agent-gateway /
tavily-search-gateway와 동일한 Lambda target 패턴으로, "sub-harness를 호출해주는 Lambda"를
Gateway target으로 노출해서 harness 서버 사이드에서 자동 실행되게 만든다.
"""

import io
import json
import time
import zipfile
import uuid
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ACCOUNT_ID = "810299942497"
SEOUL = "ap-northeast-2"

RECIPE_HARNESS_ARN = "arn:aws:bedrock-agentcore:ap-northeast-2:810299942497:harness/recipe_researcher_harness-fYDkZm9ax1"
WEB_SEARCH_HARNESS_ARN = "arn:aws:bedrock-agentcore:ap-northeast-2:810299942497:harness/web_search_harness_tavily_kr-hfzc6SS4Gb"
ORCHESTRATOR_HARNESS_ID = "orchestrator_harness-Z33M1VjKnx"
ORCHESTRATOR_HARNESS_ROLE_NAME = "AgentCoreHarnessOrchestrator-ExecutionRole"

LAMBDA_CODE_DIR = Path(__file__).parent / "orchestrator_tool_lambda"
LAMBDA_ROLE_NAME = "orchestrator-tool-lambda-role"

LAMBDAS = {
    "orchestrator-call-recipe-agent": RECIPE_HARNESS_ARN,
    "orchestrator-call-web-search-agent": WEB_SEARCH_HARNESS_ARN,
}

GATEWAY_NAME = "orchestrator-agent-gateway"
GATEWAY_ROLE_NAME = "orchestrator-gateway-role"

TARGETS = {
    "call-recipe-agent-target": {
        "lambda_name": "orchestrator-call-recipe-agent",
        "tool_name": "call_recipe_agent",
        "description": "풀무원 내부 배합비 데이터를 기반으로 레시피/배합비를 검색·제안하는 레시피연구원 Agent에게 작업을 위임합니다.",
    },
    "call-web-search-agent-target": {
        "lambda_name": "orchestrator-call-web-search-agent",
        "tool_name": "call_web_search_agent",
        "description": "시장 트렌드, 경쟁사 제품, 소비자 반응 등을 웹에서 조사하는 외부조사 Agent에게 작업을 위임합니다.",
    },
}


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
            {
                "Sid": "InvokeSubHarnesses",
                "Effect": "Allow",
                "Action": ["bedrock-agentcore:InvokeHarness", "bedrock-agentcore:InvokeAgentRuntime"],
                "Resource": [RECIPE_HARNESS_ARN, WEB_SEARCH_HARNESS_ARN],
            }
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
        iam.attach_role_policy(RoleName=LAMBDA_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
        time.sleep(10)
    iam.put_role_policy(RoleName=LAMBDA_ROLE_NAME, PolicyName="invoke-sub-harnesses", PolicyDocument=json.dumps(policy))
    return role["Role"]["Arn"]


def ensure_lambda(lam, function_name: str, role_arn: str, target_harness_arn: str) -> str:
    code_bytes = zip_lambda_code()
    env = {"Variables": {"TARGET_HARNESS_ARN": target_harness_arn}}
    try:
        fn = lam.get_function(FunctionName=function_name)
        print(f"[lambda] 기존 재사용, 코드 업데이트: {function_name}")
        lam.update_function_code(FunctionName=function_name, ZipFile=code_bytes)
        lam.get_waiter("function_updated").wait(FunctionName=function_name)
        lam.update_function_configuration(FunctionName=function_name, Environment=env, Timeout=120)
        lam.get_waiter("function_updated").wait(FunctionName=function_name)
        return fn["Configuration"]["FunctionArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"[lambda] 새로 생성: {function_name}")
    resp = lam.create_function(
        FunctionName=function_name,
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": code_bytes},
        Timeout=120,
        MemorySize=256,
        Environment=env,
    )
    return resp["FunctionArn"]


def ensure_gateway_role(iam, lambda_arns: list) -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "bedrock-agentcore.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }
    policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": lambda_arns}],
    }
    try:
        role = iam.get_role(RoleName=GATEWAY_ROLE_NAME)
        print(f"[role] 기존 재사용: {GATEWAY_ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        print(f"[role] 새로 생성: {GATEWAY_ROLE_NAME}")
        role = iam.create_role(RoleName=GATEWAY_ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust))
    iam.put_role_policy(RoleName=GATEWAY_ROLE_NAME, PolicyName="lambda", PolicyDocument=json.dumps(policy))
    print("[role] IAM 전파 대기 (15s)")
    time.sleep(15)
    return role["Role"]["Arn"]


def ensure_gateway(control, role_arn: str) -> dict:
    existing = control.list_gateways()
    for gw in existing.get("items", []):
        if gw["name"] == GATEWAY_NAME:
            print(f"[gateway] 기존 재사용: {gw['gatewayId']}")
            return control.get_gateway(gatewayIdentifier=gw["gatewayId"])

    print(f"[gateway] 새로 생성: {GATEWAY_NAME}")
    resp = control.create_gateway(name=GATEWAY_NAME, roleArn=role_arn, protocolType="MCP", authorizerType="AWS_IAM")
    gw_id = resp["gatewayId"]
    start = time.time()
    while time.time() - start < 180:
        gw = control.get_gateway(gatewayIdentifier=gw_id)
        print(f"[gateway] status={gw['status']}")
        if gw["status"] == "READY":
            break
        time.sleep(5)
    return control.get_gateway(gatewayIdentifier=gw_id)


def ensure_lambda_target(control, gateway_id: str, target_name: str, lambda_arn: str, tool_name: str, description: str):
    existing = control.list_gateway_targets(gatewayIdentifier=gateway_id)
    for t in existing.get("items", []):
        if t["name"] == target_name:
            print(f"[target] 기존 재사용: {target_name}")
            return

    print(f"[target] 새로 생성: {target_name}")
    last_err = None
    for attempt in range(6):
        try:
            control.create_gateway_target(
                gatewayIdentifier=gateway_id,
                name=target_name,
                targetConfiguration={
                    "mcp": {
                        "lambda": {
                            "lambdaArn": lambda_arn,
                            "toolSchema": {
                                "inlinePayload": [
                                    {
                                        "name": tool_name,
                                        "description": description,
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {"query": {"type": "string", "description": "sub-agent에게 전달할 자연어 요청"}},
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
            return
        except ClientError as e:
            if e.response["Error"]["Code"] != "ValidationException" or "permission" not in str(e).lower():
                raise
            last_err = e
            print(f"[target] IAM 전파 대기 중... 재시도 {attempt + 1}/6")
            time.sleep(10)
    raise last_err


def update_orchestrator_harness(iam, control, gateway_arn: str, gateway_id: str):
    # 1) 실행 role에 새 gateway InvokeGateway 권한 추가
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
                    f"arn:aws:bedrock-agentcore:{SEOUL}:{ACCOUNT_ID}:workload-identity-directory/default/workload-identity/harness_orchestrator_harness-*",
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
    iam.put_role_policy(RoleName=ORCHESTRATOR_HARNESS_ROLE_NAME, PolicyName="execution-policy", PolicyDocument=json.dumps(policy))
    print("[role] orchestrator 실행 role에 gateway 권한 추가, 전파 대기 (15s)")
    time.sleep(15)

    # 2) harness의 tools를 agentcore_gateway로 교체
    print("[harness] tools를 agentcore_gateway로 교체")
    control.update_harness(
        harnessId=ORCHESTRATOR_HARNESS_ID,
        tools=[{
            "type": "agentcore_gateway",
            "name": gateway_id,
            "config": {"agentCoreGateway": {"gatewayArn": gateway_arn, "outboundAuth": {"awsIam": {}}}},
        }],
        allowedTools=["*"],
    )


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
        for err_key in ("validationException", "runtimeClientError", "internalServerException"):
            if err_key in event:
                print(f"  [ERROR:{err_key}] {event[err_key]}")
    print("\n[result] final text:\n" + "".join(final_text))


def main():
    iam = boto3.client("iam")
    lam = boto3.client("lambda", region_name=SEOUL)
    control = boto3.client("bedrock-agentcore-control", region_name=SEOUL)
    data_client = boto3.client(
        "bedrock-agentcore", region_name=SEOUL,
        config=Config(read_timeout=280, connect_timeout=10),
    )

    lambda_role_arn = ensure_lambda_role(iam)
    print("[role] IAM 전파 대기 (10s)")
    time.sleep(10)

    lambda_arns = {}
    for fn_name, target_harness_arn in LAMBDAS.items():
        arn = ensure_lambda(lam, fn_name, lambda_role_arn, target_harness_arn)
        lambda_arns[fn_name] = arn
        print(f"[lambda] {fn_name} -> {arn}")

    gateway_role_arn = ensure_gateway_role(iam, list(lambda_arns.values()))
    gw = ensure_gateway(control, gateway_role_arn)
    gateway_id = gw["gatewayId"]
    gateway_arn = gw["gatewayArn"]
    print(f"[gateway] id={gateway_id} arn={gateway_arn}")

    for target_name, cfg in TARGETS.items():
        ensure_lambda_target(
            control, gateway_id, target_name,
            lambda_arns[cfg["lambda_name"]], cfg["tool_name"], cfg["description"],
        )

    update_orchestrator_harness(iam, control, gateway_arn, gateway_id)
    harness = wait_harness_ready(control, ORCHESTRATOR_HARNESS_ID)
    print(f"[harness] arn={harness['arn']}")

    invoke_test(data_client, harness["arn"], "마라로제소스 배합비도 짜주고, 요즘 마라로제 시장 트렌드도 같이 알려줘")


if __name__ == "__main__":
    main()
