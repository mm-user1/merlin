"""Agent-facing frozen validation. Does nothing until explicitly invoked.

First run the tracked context study and analysis using the ordinary CLI.
This example freezes their whole family for explicit dates and evaluates it;
the supplied pack must already cover the complete evaluation and warmup.
Actual operational reserve use requires a separate user instruction.
"""
import argparse
import json
from pathlib import Path
from tools.pattern_lab import analysis
from tools.pattern_lab.candidate import freeze_candidate, run_validation
from tools.pattern_lab.__main__ import _extension_roots


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("study-root", "analysis-root", "candidate-output", "data-root", "output-root"):
        parser.add_argument("--"+name, type=Path, required=True)
    for name in ("start", "end", "warmup-start"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--extension-root", action="append", default=[], metavar="MODULE=LOCAL_DIRECTORY",
                        help="Repeat for relocated extension roots; preserve exact module/helper bytes and use a fresh process.")
    args = parser.parse_args()
    frozen = freeze_candidate(study_root=args.study_root, analysis_root=args.analysis_root,
        start=args.start, end=args.end, warmup_start=args.warmup_start, output=args.candidate_output)
    receipt = run_validation(candidate=frozen, data_root=args.data_root,
                             output_root=args.output_root, workers=args.workers,
                             extension_roots=_extension_roots(args.extension_root))
    result = analysis.load_analysis(args.output_root/"analysis")
    print(json.dumps(receipt, indent=2))
    print(result.comparisons().to_string(index=False))


if __name__ == "__main__":
    main()
