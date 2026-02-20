import json
import boto3
from botocore.exceptions import ClientError
from datetime import date, timedelta
from decimal import Decimal

def d(x: str) -> Decimal:
    """Safe decimal conversion for currency amounts."""
    return Decimal(x)

def main():
    session = boto3.Session()
    ce = session.client("ce", region_name="us-east-1")

    end = date.today()
    start = end - timedelta(days=30)

    # We'll paginate in case CE returns NextPageToken
    next_token = None
    service_totals = {}  # service -> Decimal amount
    grand_total = Decimal("0")
    currency = None

    try:
        while True:
            kwargs = dict(
                TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
                Granularity="DAILY",
                Metrics=["UnblendedCost"],
                GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
            )
            if next_token:
                kwargs["NextPageToken"] = next_token

            resp = ce.get_cost_and_usage(**kwargs)

            # Parse daily results
            for day in resp.get("ResultsByTime", []):
                for group in day.get("Groups", []):
                    service_name = group.get("Keys", ["Unknown"])[0]
                    metric = group.get("Metrics", {}).get("UnblendedCost", {})
                    amount = metric.get("Amount", "0")
                    unit = metric.get("Unit")

                    if unit and currency is None:
                        currency = unit

                    amt = d(amount)
                    grand_total += amt
                    service_totals[service_name] = service_totals.get(service_name, Decimal("0")) + amt

            next_token = resp.get("NextPageToken")
            if not next_token:
                break

        # Convert Decimals to strings for JSON
        service_breakdown = {
            k: format(v.quantize(Decimal("0.01")), "f")
            for k, v in sorted(service_totals.items(), key=lambda kv: kv[1], reverse=True)
        }

        output = {
            "timePeriod": {"start": start.isoformat(), "end": end.isoformat()},
            "totalCost": format(grand_total.quantize(Decimal("0.01")), "f"),
            "currency": currency or "UNKNOWN",
            "serviceBreakdown": service_breakdown,
        }

        print(json.dumps(output, indent=2))

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = e.response.get("Error", {}).get("Message", str(e))
        print(f"[FAIL] Cost Explorer call failed: {code} - {msg}")
        raise

if __name__ == "__main__":
    main()
