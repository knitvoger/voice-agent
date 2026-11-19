"""Export exact-ID KQL for an authorized service operator; never query telemetry automatically."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def build_query(report: dict[str, Any]) -> str:
    expected = []
    seen = set()
    for customer in report["results"]:
        if not customer.get("success") or len(customer["turns"]) != 2:
            raise ValueError("Cohort contains incomplete/failed customers; retain and investigate, do not drop them")
        for turn in customer["turns"]:
            ids = [customer["session_id"], turn.get("response_id"), turn.get("input_item_id")]
            if not all(isinstance(value, str) and value for value in ids):
                raise ValueError("Missing exact session/response/input-item correlation ID")
            key = tuple(ids[:2])
            if key in seen:
                raise ValueError("Duplicate expected response ID")
            seen.add(key)
            expected.append(", ".join([*(json.dumps(value) for value in ids), str(turn["turn"])]))
    if not expected:
        raise ValueError("No measured turns")
    start = datetime.fromtimestamp(min(row["started_at"] for row in report["results"]), UTC) - timedelta(minutes=2)
    return (
        "// Run only with authorized access to the target Voice Live service's native telemetry.\n"
        "// Adapt the table/time-column names if your telemetry schema differs.\n"
        "let expected = datatable(session_id:string,response_id:string,input_item_id:string,turn:long)[\n"
        + ",\n".join(expected)
        + "\n];\n"
        "let metrics = va_turn_metrics\n"
        f"| where Time >= datetime({start.isoformat()})\n"
        "| where session_id in (expected | project session_id)\n"
        "| summarize metric_rows=count(),metric_input_id=take_any(input_item_id),"
        "native_ttfa=take_any(tolong(ttfa)) by session_id,response_id;\n"
        "let joined = expected | join kind=leftouter metrics on session_id,response_id\n"
        "| extend valid=coalesce(metric_rows == 1 and input_item_id == metric_input_id "
        "and isnotnull(native_ttfa) and native_ttfa >= 0,false);\n"
        "let stats = joined | where valid | summarize p50_ms=percentile(native_ttfa,50),"
        "p90_ms=percentile(native_ttfa,90) by turn;\n"
        "joined | summarize expected_samples=count(),valid_samples=countif(valid),"
        "missing_samples=countif(isnull(metric_rows)),duplicate_samples=countif(metric_rows > 1),"
        "invalid_samples=countif(metric_rows == 1 and not(valid)) by turn\n"
        "| join kind=leftouter stats on turn\n"
        "| project turn,expected_samples,valid_samples,missing_samples,"
        "duplicate_samples,invalid_samples,p50_ms,p90_ms\n"
        "| order by turn asc\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    query = build_query(json.loads(args.result.read_text()))
    with args.output.open("x") as output:
        output.write(query)
    print(
        "Query exported. Require valid_samples == expected_samples "
        "and zero missing/duplicates/invalid before reporting."
    )


if __name__ == "__main__":
    main()
