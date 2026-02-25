import boto3
from botocore.exceptions import ClientError
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

def money(x: Decimal) -> str:
    """Format Decimal to 2dp string."""
    return str(x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def main():
    session = boto3.Session()  # relies on AWS_PROFILE/env/role
    ce = session.client("ce", region_name="us-east-1")

    end = date.today()
    start = end - timedelta(days=30)

    service_totals: dict[str, Decimal] = {}
    currency = None
    next_token = None

    try:
        while True:
            kwargs = {
                "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
                "Granularity": "DAILY",
                "Metrics": ["UnblendedCost"],
                "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
            }
            if next_token:
                kwargs["NextPageToken"] = next_token

            resp = ce.get_cost_and_usage(**kwargs)

            for day in resp.get("ResultsByTime", []):
                for group in day.get("Groups", []):
                    # 1) Extract service name
                    service = group.get("Keys", ["Unknown Service"])[0]

                    # 2) Extract UnblendedCost
                    metric = group.get("Metrics", {}).get("UnblendedCost", {})
                    amount_str = metric.get("Amount", "0")
                    unit = metric.get("Unit")

                    if unit and currency is None:
                        currency = unit

                    amount = Decimal(amount_str)

                    # 3) Aggregate per service
                    service_totals[service] = service_totals.get(service, Decimal("0")) + amount

            next_token = resp.get("NextPageToken")
            if not next_token:
                break

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = e.response.get("Error", {}).get("Message", str(e))
        print(f"[FAIL] Cost Explorer call failed: {code} - {msg}")
        raise

    # 4) Print structured output (sorted)
    print(f"TimePeriod: {start.isoformat()} → {end.isoformat()}")
    print(f"Currency: {currency or 'UNKNOWN'}\n")

    if not service_totals:
        print("(No services returned)")
        return

    sorted_services = sorted(service_totals.items(), key=lambda kv: kv[1], reverse=True)

    # Nice aligned output
    max_len = max(len(name) for name, _ in sorted_services)
    for name, total in sorted_services:
        print(f"{name.ljust(max_len)}  {money(total)}")

if __name__ == "__main__":
    main()