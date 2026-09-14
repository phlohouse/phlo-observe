"""``observe`` diagnostic CLI: emit a test event or inspect configuration."""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``observe`` command."""
    parser = argparse.ArgumentParser(prog="observe", description="observe-core diagnostics")
    sub = parser.add_subparsers(dest="command", required=True)

    emit_p = sub.add_parser("emit-test", help="emit a test event through the configured pipeline")
    emit_p.add_argument("--event", default="application.start", help="event name")
    emit_p.add_argument("--wait", type=float, default=1.0, help="flush wait seconds")

    sub.add_parser("config", help="print resolved configuration as JSON")
    sub.add_parser("stats", help="print internal counters after a flush")

    replay_p = sub.add_parser(
        "replay-spool", help="replay a local critical-event spool into the configured drains"
    )
    replay_p.add_argument("--dir", required=True, help="spool directory path")
    replay_p.add_argument("--max-events", type=int, default=None)

    args = parser.parse_args(argv)

    if args.command == "config":
        from observe_core.config import ObserveSettings

        settings = ObserveSettings()
        print(json.dumps(settings.model_dump(mode="json"), indent=2, default=str))
        return 0

    if args.command == "emit-test":
        from observe_core import (
            configure,
            event,
            flush,
            shutdown,
        )

        configure()
        event(args.event, attributes={"cli": "emit-test"})
        flush(timeout=args.wait)
        shutdown(timeout=2.0)
        return 0

    if args.command == "stats":
        from observe_core import get_stats

        print(json.dumps(get_stats(), indent=2, default=str))
        return 0

    if args.command == "replay-spool":
        from pathlib import Path

        from observe_core.config import ObserveSettings
        from observe_core.runtime import Runtime
        from observe_core.spool import Spool

        settings = ObserveSettings()
        drains = [Runtime._build_drain(cfg) for cfg in settings.drains]
        if not drains:
            print("no drains configured", file=sys.stderr)
            return 1
        spool = Spool(Path(args.dir))
        replayed = spool.replay(drains[0], max_events=args.max_events)
        for drain in drains:
            drain.close()
        print(json.dumps({"replayed": replayed}))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
