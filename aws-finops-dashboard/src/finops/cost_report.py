import csv
import json
from datetime import date, timedelta

import boto3

def get_data_range(days: int = 30) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=days)
    return start.isoformat(), end.isoformat()

def fetch_costs_by_service(days: int = 30) -> list[dict]:
    start, end = get_data_range(days)
    ce = boto3.client("ce") 

    response = ce.get_cost_and_usage(
        TimePeriod={"start": start, "End": end},
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

    results.sort(key=lambda x: x["amount"], reverse=True)
    return results

def write_reports(results: list[dict]) -> None:

    #CSV

    with open("reports/cost_report.csv", "w", encodings="utf-8") as f:
        json.dump(results, f, indent=2)