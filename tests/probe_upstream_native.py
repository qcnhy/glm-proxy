#!/usr/bin/env python3
"""探针：绕过网关清洗，直连 GLM 官方 /responses 验证三项兼容性。
1. encrypted_content：带上一轮返回的 encrypted_content 块直接发
2. exec 裸描述：不注入 _EXEC_FILE_EDIT_RULES 发 exec 调用
3. item_reference：带 rs_* 纯引用直接发
全部直连，不走 glm-proxy。"""
import json, urllib.request, sys

CFG = json.load(open("config.json"))
UP = next(u for u in CFG["upstreams"] if u["name"] == "glm-native")
URL, KEY = UP["responses_url"], UP["key"]

def post(body):
    req = urllib.request.Request(URL, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {KEY}"})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            return r.status, r.read().decode()[:500]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")[:500]
    except Exception as e:
        return -1, str(e)

def get_encrypted():
    """先发一轮拿真实 encrypted_content 块"""
    st, body = post({"model": "glm-5.3", "input": "say ok", "max_output_tokens": 256})
    if st != 200:
        return None
    r = json.loads(body) if len(body) < 5000 else None
    return r

print("=== 1. encrypted_content（需真实块；用占位块测协议层是否接受该 type） ===")
st, out = post({"model": "glm-5.3", "max_output_tokens": 256,
    "input": [{"type": "message", "role": "user", "content": [
        {"type": "encrypted_content", "encrypted_content": "gAAAAA-placeholder"}]}]})
print(f"HTTP {st}: {out[:300]}")
print("  -> 接受该 type = 协议层认（占位内容若解密失败属另一层）")

print("=== 2. exec 裸描述（不注入沙箱规则） ===")
st, out = post({"model": "glm-5.3", "max_output_tokens": 512,
    "input": [{"type": "message", "role": "user", "content": [
        {"type": "input_text", "text": "List files in the current directory using the exec tool."}]}],
    "tools": [{"type": "function", "name": "exec",
        "description": "Run JavaScript in a V8 sandbox.",
        "parameters": {"type": "object", "properties": {"code": {"type": "string"}},
                       "required": ["code"]}}]})
print(f"HTTP {st}")
try:
    o = json.loads(out) if st == 200 else {}
    for it in o.get("output", []):
        if it.get("type") == "function_call":
            print(f"  call: {it.get('name')} args={str(it.get('arguments'))[:200]}")
        elif it.get("type") == "message":
            print(f"  text: {str(it.get('content'))[:200]}")
except Exception: pass
if st != 200: print(" ", out[:200])

print("=== 3. item_reference（rs_* 纯引用，GLM 无状态） ===")
st, out = post({"model": "glm-5.3", "max_output_tokens": 256,
    "input": [{"type": "item_reference", "id": "rs_00000000deadbeef"}]})
print(f"HTTP {st}: {out[:300]}")
