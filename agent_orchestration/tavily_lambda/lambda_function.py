"""
Tool: tavily_web_search
AgentCore Gateway -> Lambda ARN Target 방식 (recipe-search-recipes와 동일 패턴)
Tavily Search API를 감싸서 MCP tool로 노출한다.
"""

import json
import os
import urllib.request
import urllib.error

import boto3

REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
SECRET_ARN = os.environ["TAVILY_SECRET_ARN"]
TAVILY_URL = "https://api.tavily.com/search"
DEFAULT_MAX_RESULTS = 5

_secrets = boto3.client("secretsmanager", region_name=REGION)
_api_key_cache = None


def _get_api_key() -> str:
    global _api_key_cache
    if _api_key_cache is None:
        resp = _secrets.get_secret_value(SecretId=SECRET_ARN)
        _api_key_cache = json.loads(resp["SecretString"])["apiKey"]
    return _api_key_cache


def tavily_search(query: str, max_results: int = DEFAULT_MAX_RESULTS) -> dict:
    body = json.dumps({
        "api_key": _get_api_key(),
        "query": query,
        "search_depth": "advanced",
        "max_results": max_results,
        "include_answer": False,
    }).encode("utf-8")

    req = urllib.request.Request(
        TAVILY_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Tavily API error {e.code}: {e.read().decode('utf-8')}")

    results = [
        {
            "title": r.get("title", ""),
            "url": r.get("url", ""),
            "text": r.get("content", ""),
            "score": r.get("score"),
            "publishedDate": r.get("published_date"),
        }
        for r in data.get("results", [])
    ]
    return {"query": query, "count": len(results), "results": results}


def lambda_handler(event, context):
    """
    event: tool 파라미터 딕셔너리 (query, max_results)
    """
    try:
        query = event.get("query", "")
        max_results = int(event.get("max_results", DEFAULT_MAX_RESULTS))

        if not query:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "query 파라미터가 필요합니다"}, ensure_ascii=False),
            }

        result = tavily_search(query, max_results)
        return {"statusCode": 200, "body": json.dumps(result, ensure_ascii=False)}

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)}, ensure_ascii=False)}
