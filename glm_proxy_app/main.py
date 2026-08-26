"""应用启动与优雅退出。"""
import signal
import sys
import time

from .common import LISTEN, UPSTREAMS, ThreadedHTTPServer, log
from .relay import start_interceptors, start_relays, stop_relays
from .server import Handler

VERSION = "4.6.0"


def _log_upstreams():
    for up in UPSTREAMS:
        ctx = f"{up['max_context_tokens'] // 1000}K" if up.get("max_context_tokens") else "?"
        if "relay_port" in up:
            log.info("  %s: relay :%d → interceptor :%d → %s | model=%s ctx=%s",
                     up["name"], up["relay_port"], up["interceptor_port"],
                     up["openai_url"], up["model"], ctx)
        else:
            log.info("  %s: messages → %s | model=%s ctx=%s",
                     up["name"], up.get("anthropic_url", "?"), up["model"], ctx)


def main():
    log.info("GLM Proxy v%s :%d", VERSION, LISTEN[1])
    _log_upstreams()
    start_interceptors()
    start_relays()
    time.sleep(1)

    def shutdown(_sig=None, _frame=None):
        log.info("Shutting down...")
        stop_relays()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        ThreadedHTTPServer(LISTEN, Handler).serve_forever()
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
