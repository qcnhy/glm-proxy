"""应用启动与优雅退出。"""
import signal
import sys
import json
import ssl
import threading
import time
from urllib.request import Request, urlopen

from .common import LISTEN, UPSTREAMS, ThreadedHTTPServer, log
from .server import Handler

VERSION = "5.4.0"

_PROBE_INTERVAL = 300  # venusgroup 内网探针周期（秒），兼作保活


def _probe_venus_loop():
    """v4.9.1: venusgroup 内网渠道周期探针。

    v5.4.0: 探针改走 responses_url（Responses 端点），摆脱对 completion_url 的依赖。
    每 5 分钟对 responses_url 指向 venusgroup 的启用渠道发一条 1-token 请求：
    - 记录可用性日志（仅观测，不影响路由——失败渠道由现有回退链处理）
    - 顺带保活内网网关会话/连接
    """
    targets = [up for up in UPSTREAMS
               if "venusgroup" in up.get("responses_url", "") and not up.get("disabled")]
    if not targets:
        return
    ctx = ssl.create_default_context()
    while True:
        time.sleep(_PROBE_INTERVAL)
        for up in targets:
            model = up.get("model", "")
            body = json.dumps({"model": model,
                               "input": "ping",
                               "max_output_tokens": 1,
                               "stream": False}).encode()
            req = Request(up["responses_url"], data=body,
                          headers={"Content-Type": "application/json",
                                   "Authorization": "Bearer " + up.get("key", "")},
                          method="POST")
            try:
                resp = urlopen(req, timeout=30, context=ctx)
                resp.read()
                log.info("[probe] %s OK (model=%s)", up["name"], model)
            except Exception as e:
                log.warning("[probe] %s FAIL: %s", up["name"], e)


def _log_upstreams():
    for up in UPSTREAMS:
        ctx = f"{up['max_context_tokens'] // 1000}K" if up.get("max_context_tokens") else "?"
        if up.get("responses_url"):
            log.info("  %s: responses → %s | model=%s ctx=%s",
                     up["name"], up["responses_url"], up["model"], ctx)
        else:
            log.info("  %s: messages → %s | model=%s ctx=%s",
                     up["name"], up.get("anthropic_url", "?"), up["model"], ctx)


def main():
    log.info("GLM Proxy v%s :%d", VERSION, LISTEN[1])
    _log_upstreams()
    threading.Thread(target=_probe_venus_loop, daemon=True, name="venus-probe").start()

    def shutdown(_sig=None, _frame=None):
        log.info("Shutting down...")
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        ThreadedHTTPServer(LISTEN, Handler).serve_forever()
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
