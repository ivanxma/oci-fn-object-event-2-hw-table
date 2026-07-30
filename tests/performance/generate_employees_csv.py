#!/usr/bin/env python3
"""Generate a deterministic employees CSV for bounded loader throughput tests."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path


HEADER = [
    "EMPLOYEE_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "EMAIL",
    "PHONE_NUMBER",
    "HIRE_DATE",
    "JOB_ID",
    "SALARY",
    "COMMISSION_PCT",
    "MANAGER_ID",
    "DEPARTMENT_ID",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.rows <= 5_000_000:
        raise ValueError("--rows must be from 1 to 5,000,000.")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target, lineterminator="\n")
        writer.writerow(HEADER)
        for employee_id in range(1, args.rows + 1):
            writer.writerow(
                [
                    employee_id,
                    f"First{employee_id}",
                    f"Last{employee_id}",
                    f"employee{employee_id}@example.test",
                    f"44.20.{employee_id % 10_000:04d}",
                    "2026-01-01",
                    f"JOB{employee_id % 100:02d}",
                    f"{30000 + employee_id % 120000}.00",
                    "-" if employee_id % 3 else "0.10",
                    "" if employee_id == 1 else max(1, employee_id // 10),
                    10 + employee_id % 20,
                ]
            )
    print(f"rows={args.rows} path={args.output} bytes={args.output.stat().st_size}")


if __name__ == "__main__":
    main()
