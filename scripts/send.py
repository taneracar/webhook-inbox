"""Send a signed sample webhook to a running inbox.

python -m scripts.send                  # new event
python -m scripts.send --id evt_42        # run twice: second is a duplicate
python -m scripts.send --bad-signature    # see the 401
"""

import argparse
import json
import os
import time
import uuid

import httpx

from app.signing import sign

p = argparse.ArgumentParser()
p.add_argument("--url", default="http://127.0.0.1:8000/webhooks/ats")
p.add_argument("--secret", default=os.environ.get("INBOX_SECRET", "dev-secret"))
p.add_argument("--id", default=f"evt_{uuid.uuid4().hex[:10]}")
p.add_argument("--type", default="candidate.placed")
p.add_argument("--bad-signature", action="store_true")
a = p.parse_args()

body = json.dumps(
    {
        "id": a.id,
        "type": a.type,
        "data": {"candidate_id": "cand_7", "job_order_id": "jo_3", "start_date": "2026-12-01"},
    }
).encode()
secret = "wrong" if a.bad_signature else a.secret
r = httpx.post(
    a.url,
    content=body,
    headers={"content-type": "application/json", "X-Signature": sign(secret, int(time.time()), body)},
)
print(r.status_code, r.text)
