#!/usr/bin/env python3
"""Test DCE daily quotes API with different parameter combinations."""
import json
import requests

DCE_BASE_URL = "http://www.dce.com.cn"
API_KEY = "ofxc69rpmd59"
API_SECRET = "2UdFW^2G4!4^7#@URqWx"
TRADE_DATE = "20260427"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
})

# Step 1: Get token
print("=== Step 1: Get token ===")
r = session.post(
    f"{DCE_BASE_URL}/dceapi/cms/auth/accessToken",
    json={"secret": API_SECRET},
    headers={"apikey": API_KEY},
    timeout=15,
)
data = r.json()
print(f"  Status: {r.status_code}, success: {data.get('success')}")
if data.get("success"):
    token = data["data"]["token"]
    print(f"  Token obtained: {token[:20]}...")
else:
    print(f"  Failed: {data.get('msg')}")
    exit(1)

# Step 2: Test dayQuotes with various payloads
print("\n=== Step 2: Test dayQuotes ===")
url = f"{DCE_BASE_URL}/dceapi/forward/publicweb/dailystat/dayQuotes"
auth_headers = {
    "apikey": API_KEY,
    "Authorization": f"Bearer {token}",
}

payloads = [
    # From official doc
    {"varietyId": "all", "tradeDate": TRADE_DATE, "tradeType": "2",
     "lang": "zh", "statisticsType": 2},
    # Futures + contract level (from earlier successful test)
    {"varietyId": "all", "tradeDate": TRADE_DATE, "tradeType": "1",
     "lang": "zh", "statisticsType": 0},
    # Futures + no statisticsType
    {"varietyId": "all", "tradeDate": TRADE_DATE, "tradeType": "1",
     "lang": "zh"},
    # Try int types instead of string
    {"varietyId": "all", "tradeDate": TRADE_DATE, "tradeType": 1,
     "lang": "zh", "statisticsType": 0},
    # Try with POST data format (not json)
]

for i, payload in enumerate(payloads):
    print(f"\n  Payload {i}: {json.dumps(payload, ensure_ascii=False)}")
    r = session.post(url, json=payload, headers=auth_headers, timeout=15)
    try:
        data = r.json()
        records = data.get("data", []) or []
        has_id = sum(1 for rec in records if rec.get("contractId"))
        print(f"    Status: {r.status_code}, success: {data.get('success')}, "
              f"msg: {data.get('msg')}")
        if data.get("success") and has_id:
            print(f"    Records: {len(records)}, with contractId: {has_id}")
            sample = next((rec for rec in records if rec.get("contractId")), {})
            if sample:
                print(f"    Sample: {sample['contractId']}: "
                      f"open={sample.get('open')}, high={sample.get('high')}, "
                      f"low={sample.get('low')}, close={sample.get('close')}, "
                      f"vol={sample.get('volumn')}, "
                      f"settle={sample.get('clearPrice')}")
    except Exception as e:
        print(f"    Error: {e}")
        print(f"    Response: {r.text[:200]}")

# Step 3: Also test the settlement API to confirm token works
print("\n=== Step 3: Verify settlement API still works ===")
settle_url = f"{DCE_BASE_URL}/dceapi/forward/publicweb/tradepara/futAndOptSettle"
r = session.post(settle_url, json={
    "varietyId": "all", "tradeDate": TRADE_DATE, "tradeType": "1", "lang": "zh"
}, headers=auth_headers, timeout=15)
data = r.json()
records = data.get("data", []) or []
print(f"  Success: {data.get('success')}, records: {len(records)}")

print("\nDone.")
