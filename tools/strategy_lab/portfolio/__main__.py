"""Prepare, execute, report and regenerate frozen carried portfolios."""
import argparse
from contextlib import redirect_stdout
import json
import sys

from .plan import prepare
from .report import report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prep = sub.add_parser('prepare'); prep.add_argument('request'); prep.add_argument('--output-dir', required=True)
    for name in ('run', 'report', 'detail'):
        p = sub.add_parser(name)
        p.add_argument('--plan' if name == 'run' else '--result', required=True)
        p.add_argument('--output-dir', required=True)
        if name != 'report':
            p.add_argument('--data-root', required=True); p.add_argument('--filter-roots')
        if name == 'detail':
            p.add_argument('--variant', required=True)
    args = parser.parse_args(argv)
    try:
        with redirect_stdout(sys.stderr):
            if args.command == 'prepare':
                result = prepare(args.request, args.output_dir)
            elif args.command == 'report':
                result = report(args.result, args.output_dir)
            else:
                from .run import run, detail
                from ..filters import read_overrides
                options = dict(filter_root_overrides=read_overrides(args.filter_roots) if args.filter_roots else None)
                result = run(args.plan, args.data_root, args.output_dir, **options) if args.command == 'run' else detail(
                    args.result, args.variant, args.data_root, args.output_dir, **options)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(f'Strategy Lab portfolio: {exc}', file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
