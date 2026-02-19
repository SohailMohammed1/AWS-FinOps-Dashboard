import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from datetime import date, timedelta

def main():
    # 1) Create a session 
    session = boto3.Session()

    # 2) Validate credentials using STS
    sts = session.client("sts")
    try:
        ident = sts.get_caller_identity()
        account_id = ident["Account"]
        arn = ident["Arn"]
        print(f"[OK] Credentials valid. Account: {account_id}")
        print(f"[OK] Caller ARN: {arn}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("[FAIL] AWS credentials not found or incomplete.")
        raise
    except ClientError as e:
        print("[FAIL] STS call failed (credentials/permissions/endpoint).")
        raise

    # 3) Create Cost Explorer client
    ce = session.client("ce", region_name="us-east-1")

    # 4) Confirm API connectivity (lightweight Cost Explorer call)
    # Use a small date range; CE needs Start < End.
    end = date.today()
    start = end - timedelta(days=2)

    try:
        resp = ce.get_cost_and_usage(
            TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
        )
        results = resp.get("ResultsByTime", [])
        print(f"[OK] Cost Explorer reachable. Returned {len(results)} day(s).")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        msg = e.response.get("Error", {}).get("Message", str(e))

        print(f"[FAIL] Cost Explorer call failed: {code} - {msg}")
        raise

if __name__ == "__main__":
    main()
