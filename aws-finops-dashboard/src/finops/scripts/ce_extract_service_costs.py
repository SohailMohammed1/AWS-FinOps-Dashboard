import os
import sys
import json
import boto3
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from botocore.exceptions import (
    NoCredentialsError,
    PartialCredentialsError,
    ClientError,
    ProfileNotFound,
)

def money(x: Decimal) -> str:
    return str(x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def get_session():
    """
    Prefer AWS_PROFILE if set; otherwise fall back to default boto3 credential resolution.
    This keeps it Lambda-friendly (Lambda won't use profiles).
    """
    profile = os.getenv("AWS_PROFILE")
    if profile:
        return boto3.Session(profile_name=profile)
    return boto3.Session()

def query_last30_grouped_by_service(ce_client):
    end = date.today()
    start = end - timedelta(days=30)

    service_totals: dict[str, Decimal] = {}
    currency = None
    next_token = None

    while True:
        kwargs = {
            "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
            "Granularity": "DAILY",
            "Metrics": ["UnblendedCost"],
            "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
        }
        if next_token:
            kwargs["NextPageToken"] = next_token

        resp = ce_client.get_cost_and_usage(**kwargs)

        for day in resp.get("ResultsByTime", []):
            for group in day.get("Groups", []):
                service = group.get("Keys", ["Unknown Service"])[0]
                metric = group.get("Metrics", {}).get("UnblendedCost", {})
                amount_str = metric.get("Amount", "0")
                unit = metric.get("Unit")

                if unit and currency is None:
                    currency = unit

                amount = Decimal(amount_str)
                service_totals[service] = service_totals.get(service, Decimal("0")) + amount

        next_token = resp.get("NextPageToken")
        if not next_token:
            break

    return {
        "timePeriod": {"start": start.isoformat(), "end": end.isoformat()},
        "currency": currency or "UNKNOWN",
        "serviceTotals": {k: money(v) for k, v in sorted(service_totals.items(), key=lambda kv: kv[1], reverse=True)},
    }

def print_clean_service_table(payload: dict):
    print(f"TimePeriod: {payload['timePeriod']['start']} → {payload['timePeriod']['end']}")
    print(f"Currency: {payload['currency']}\n")

    service_totals = payload["serviceTotals"]
    if not service_totals:
        print("(No services returned)")
        return

    max_len = max(len(name) for name in service_totals.keys())
    for name, amt in service_totals.items():
        print(f"{name.ljust(max_len)}  {amt}")

def main() -> int:
    try:
        session = get_session()

        # Optional: prove credentials early (better error message than failing inside CE)
        sts = session.client("sts")
        ident = sts.get_caller_identity()
        print(f"[OK] Authenticated. Account: {ident['Account']}")
        print(f"[OK] Caller ARN: {ident['Arn']}")

        ce = session.client("ce", region_name="us-east-1")
        payload = query_last30_grouped_by_service(ce)

        # Structured output
        print("\n[OK] Cost Explorer response parsed.\n")
        print_clean_service_table(payload)

        # Also emit JSON for logs/automation (optional but useful)
        # print("\nJSON:\n" + json.dumps(payload, indent=2))

        return 0

    except ProfileNotFound:
        print("[ERROR] AWS profile not found.")
        print("Fix: set AWS_PROFILE to a valid profile name or configure it in ~/.aws/config.")
        return 2

    except (NoCredentialsError, PartialCredentialsError):
        print("[ERROR] Unable to locate AWS credentials.")
        print("Fix options:")
        print("- If using a profile: export AWS_PROFILE=finops (or your profile) then rerun")
        print("- Verify with: aws sts get-caller-identity --profile finops")
        return 2

    except ClientError as e:
        err = e.response.get("Error", {})
        code = err.get("Code", "Unknown")
        msg = err.get("Message", str(e))
        print(f"[ERROR] AWS API error calling Cost Explorer: {code} - {msg}")

        if code == "AccessDeniedException":
            print("Likely fix: add IAM permission ce:GetCostAndUsage to this principal/role.")
        elif code == "DataUnavailableException":
            print("Likely fix: try a wider time window (e.g., 60–90 days) or wait for billing data to populate.")
        elif code == "ValidationException":
            print("Likely fix: check Start/End date logic and request params.")
        return 3

    except Exception as e:
        # Catch-all so it *never* crashes with a traceback in unattended runs
        print(f"[ERROR] Unexpected failure: {type(e).__name__}: {e}")
        return 99

if __name__ == "__main__":
    sys.exit(main())