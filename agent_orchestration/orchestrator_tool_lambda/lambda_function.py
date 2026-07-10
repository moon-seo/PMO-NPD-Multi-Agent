"""
Tool: call_recipe_agent / call_web_search_agent (동일 코드, 환경변수로 대상만 다름)
orchestrator harness의 agentcore_gateway tool -> 이 Lambda -> sub-harness InvokeHarness

inline_function으로는 harness가 스스로 tool을 실행하지 못해 콘솔 harness playground 같은
범용 클라이언트에서는 toolUse가 허공에 뜬다. 이 Lambda가 서버 사이드에서 sub-harness를
대신 호출해줌으로써 orchestrator harness의 agentcore_gateway tool을 통해 자동 실행되게 한다.
"""

import json
import os
import uuid

import boto3

REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
TARGET_HARNESS_ARN = os.environ["TARGET_HARNESS_ARN"]

_client = boto3.client("bedrock-agentcore", region_name=REGION)


def lambda_handler(event, lambda_context):
    try:
        query = event.get("query", "")
        background = event.get("context", "")
        if not query:
            return {"statusCode": 400, "body": json.dumps({"error": "query 파라미터가 필요합니다"}, ensure_ascii=False)}

        if background:
            message_text = f"[배경 정보]\n{background}\n\n[요청]\n{query}"
        else:
            message_text = query

        session_id = str(uuid.uuid4())
        resp = _client.invoke_harness(
            harnessArn=TARGET_HARNESS_ARN,
            runtimeSessionId=session_id,
            messages=[{"role": "user", "content": [{"text": message_text}]}],
        )

        final_text = []
        for evt in resp["stream"]:
            if "contentBlockDelta" in evt:
                delta = evt["contentBlockDelta"]["delta"]
                if "text" in delta:
                    final_text.append(delta["text"])
            for err_key in ("validationException", "runtimeClientError", "internalServerException"):
                if err_key in evt:
                    return {"statusCode": 500, "body": json.dumps({"error": f"{err_key}: {evt[err_key]}"}, ensure_ascii=False)}

        return {"statusCode": 200, "body": json.dumps({"answer": "".join(final_text)}, ensure_ascii=False)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)}, ensure_ascii=False)}
