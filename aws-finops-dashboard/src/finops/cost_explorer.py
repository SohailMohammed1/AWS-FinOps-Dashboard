import csv
import json
from datetime import date, timedelta
from pathlib import Path

import boto3


def get_date_range(days: int = 30) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=days)
    return start.isoformat(), end.isoformat()


def fetch_cost_by_service(days: int = 30) -> list[dict]:
    start, end = get_date_range(days)

    ce = boto3.client("ce")  

    response = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )

    groups = response["ResultsByTime"][0]["Groups"]

    results = []
    for g in groups:
        service = g["Keys"][0]
        amount = float(g["Metrics"]["UnblendedCost"]["Amount"])
        unit = g["Metrics"]["UnblendedCost"]["Unit"]
        results.append({"service": service, "amount": amount, "unit": unit})

    # Sort by highest spend
    results.sort(key=lambda x: x["amount"], reverse=True)
    return results


def write_reports(results: list[dict]) -> None:
    with open("aws-finops-dashboard/src/reports/cost_report.csv", "w", newline="", encoding="utf-8") as f:
    # Anchor outputs to src/ (so it works no matter where you run the script from)
        SRC_DIR = Path(__file__).resolve().parent.parent  # .../src
        REPORTS_DIR = SRC_DIR / "reports"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # CSV
    csv_path = REPORTS_DIR / "cost_report.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["service", "amount", "unit"])
        writer.writeheader()
        writer.writerows(results)

    # JSON
    json_path = REPORTS_DIR / "cost_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)



if __name__ == "__main__":
    data = fetch_cost_by_service(days=30)
    write_reports(data)

    print("\nTop 10 services by cost (last 30 days):")
    for row in data[:10]:
        print(f"- {row['service']}: {row['amount']:.2f} {row['unit']}")

    print("\nSaved:")
    print("- src/reports/cost_report.csv")
    print("- src/reports/cost_report.json")