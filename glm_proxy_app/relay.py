"""v5.0.0: codex-relay 已退役。

GLM 内部端点（/ai/zhipu/responses）与官方端点（/api/v1/responses）均已原生支持
Responses 协议；messages 路径直连 Anthropic 端点；chat/completions 直连。
本模块保留为空操作兼容层（main.py 仍 import 这三个函数）。
"""

def start_interceptors():
    return

def start_relays():
    return

def stop_relays():
    return
