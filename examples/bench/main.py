"""CLI entry point for examples/bench: builds a registered topology and runs it.

Topologies live one-per-file under examples/bench/topologies/ (see topology.py's module
docstring) -- this script never hardcodes one. `--topology reference` (the default) is
the full Lakebase/Zerobus demo (see topologies/reference.py); `--topology quickstart`
needs no cloud credentials at all (see topologies/quickstart.py).

Run with:
    uv run --group testing python examples/bench/main.py
    uv run --group testing python examples/bench/main.py --topology quickstart --duration 2
    uv run --group testing python examples/bench/main.py --connection-config conn.yaml
    uv run --group testing python examples/bench/main.py --list-topologies
"""

from __future__ import annotations

import argparse

from bench.topology import MissingConfig, discover, get_topology, list_topologies
from transport.connection import ConnectionConfig, MissingSetting


def main(argv: list[str] | None = None) -> None:
    discover()  # import every examples/bench/topologies/*.py, running its registration

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--topology", default="reference", choices=list_topologies())
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Overall wall-clock bound; each Ecu also stops on its own should_stop()/KeyboardInterrupt.",
    )
    parser.add_argument(
        "--connection-config",
        default=None,
        metavar="PATH",
        help=(
            "YAML file of Lakebase/Zerobus connection settings (see "
            "transport/connection.py::ConnectionConfig.from_yaml). Falls back to "
            "LAKEBASE_*/ZEROBUS_* environment variables (see ConnectionConfig.from_environ) "
            "when omitted. Only the settings the chosen topology's buses actually need have "
            "to be there; a fully offline topology (e.g. quickstart) needs none."
        ),
    )
    parser.add_argument("--list-topologies", action="store_true", help="Print registered topology names and exit.")
    args = parser.parse_args(argv)

    if args.list_topologies:
        for name in list_topologies():
            print(name)
        return

    build = get_topology(args.topology)

    if args.connection_config:
        config = ConnectionConfig.from_yaml(args.connection_config)
    else:
        config = ConnectionConfig.from_environ()

    # Neither source insists on being complete: which settings a run needs is decided by
    # the buses the topology builds, so an incomplete one surfaces here as MissingSetting.
    try:
        bench = build(config=config, run_id=args.run_id)
    except (MissingConfig, MissingSetting) as exc:
        parser.error(f"--topology {args.topology}: {exc}")
    bench.run(duration=args.duration)


if __name__ == "__main__":
    main()
