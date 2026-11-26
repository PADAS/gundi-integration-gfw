"""
Move Global Nature Watch (type `gfw`) connections to the shared-account auth settings (GUNDI-5968).

Run it right after the new action runner is deployed and re-registered, so the portal
form matches how each connection signs in:

- Saved login is the support account -> auth data becomes {"use_own_credentials": false}
  (email and password removed; the runner signs in with GNW_DEFAULT_EMAIL/PASSWORD).
- Saved login is any other account   -> "use_own_credentials": true is added, and the
  email and password are kept, so the portal switch shows On.
- Settings that already have "use_own_credentials", or have no email, are left alone.

Dry run by default. Never prints passwords. With --apply it writes each change, reads it
back, and saves the list of changed connection ids (no credentials) next to --report.

Usage:
  python local/helpers/gnw_auth_cleanup.py --env dev --support-email integrations@earthranger.com
  python local/helpers/gnw_auth_cleanup.py --env dev --support-email integrations@earthranger.com --apply
  python local/helpers/gnw_auth_cleanup.py --env prod --support-email ... --apply --yes-prod

The portal token is read from ~/.config/gundi/portal-token-<env> (or --token-file).
"""
import argparse
import json
import os
import sys
import urllib.request

API_URLS = {
    "dev": "https://api.dev.gundiservice.org",
    "stage": "https://api.stage.gundiservice.org",
    "prod": "https://api.gundiservice.org",
}

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--env", choices=API_URLS, required=True)
parser.add_argument("--support-email", required=True, help="Login of the support account the shared account replaces.")
parser.add_argument("--token-file", help="Portal token file (default ~/.config/gundi/portal-token-<env>).")
parser.add_argument("--apply", action="store_true", help="Write the changes. Without it, only show the plan.")
parser.add_argument("--yes-prod", action="store_true", help="Required together with --apply on prod.")
parser.add_argument("--report", default="gnw_auth_cleanup_report.json", help="Where --apply saves the changed ids.")
args = parser.parse_args()

if args.env == "prod" and args.apply and not args.yes_prod:
    sys.exit("Refusing to write to prod without --yes-prod.")

base = API_URLS[args.env]
token_path = os.path.expanduser(args.token_file or f"~/.config/gundi/portal-token-{args.env}")
try:
    token = open(token_path).read().strip()
except FileNotFoundError:
    sys.exit(f"No portal token at {token_path}. Save one there or pass --token-file.")
support_email = args.support_email.strip().lower()


def call(method, path_or_url, body=None):
    url = path_or_url if path_or_url.startswith("http") else base + path_or_url
    request = urllib.request.Request(
        url, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request) as response:
        body = response.read().decode()
        if "json" not in response.headers.get("Content-Type", "") and body:
            # An expired portal token gets redirected to the sign-in page.
            sys.exit(f"The {args.env} portal token didn't work (got a web page back). "
                     f"Save a fresh one at {token_path}.")
    return json.loads(body or "{}", strict=False)


def gfw_type_id():
    url = "/v2/integrations/types/"
    while url:
        page = call("GET", url)
        for integration_type in page["results"]:
            if integration_type["value"] == "gfw":
                return integration_type["id"]
        url = page.get("next")
    sys.exit("No gfw integration type found.")


plan = {"to_shared": [], "keep_own": [], "skipped": []}
url = f"/v2/integrations/?type={gfw_type_id()}&page_size=100"
while url:
    page = call("GET", url)
    for integration in page["results"]:
        auth = next((c for c in integration.get("configurations", []) if c["action"]["value"] == "auth"), None)
        data = (auth or {}).get("data") or {}
        row = {"integration_id": integration["id"], "name": integration["name"], "config_id": (auth or {}).get("id")}
        if not auth or "use_own_credentials" in data or not data.get("email"):
            plan["skipped"].append(row)
        elif data["email"].strip().lower() == support_email:
            plan["to_shared"].append({**row, "new_data": {"use_own_credentials": False}})
        else:
            plan["keep_own"].append({**row, "new_data": {**data, "use_own_credentials": True}})
    url = page.get("next")

print(f"{args.env}: {len(plan['to_shared'])} to the shared account, "
      f"{len(plan['keep_own'])} keep their own account, {len(plan['skipped'])} left alone")
for group, rows in plan.items():
    for row in rows:
        print(f"  {group:9}  {row['integration_id']}  {row['name']}")

if not args.apply:
    print("\nDry run: nothing changed. Re-run with --apply to update.")
    sys.exit(0)

changed, failed = [], []
for row in plan["to_shared"] + plan["keep_own"]:
    try:
        call("PATCH", f"/v2/integrations/{row['integration_id']}/",
             {"configurations": [{"id": row["config_id"], "data": row["new_data"]}]})
        after = call("GET", f"/v2/integrations/{row['integration_id']}/")
        saved = next(c["data"] for c in after["configurations"] if c["id"] == row["config_id"])
        ok = sorted(saved) == sorted(row["new_data"]) and saved["use_own_credentials"] == row["new_data"]["use_own_credentials"]
    except Exception as e:  # keep going; report at the end
        ok, saved = False, {"error": type(e).__name__}
    (changed if ok else failed).append({
        "integration_id": row["integration_id"], "name": row["name"],
        "use_own_credentials": row["new_data"]["use_own_credentials"],
    })
    print(f"{'OK  ' if ok else 'FAIL'} {row['integration_id']}  {row['name']}  keys={sorted(saved)}")

with open(args.report, "w") as report:
    json.dump({"env": args.env, "changed": changed, "failed": failed}, report, indent=2)
print(f"\n{len(changed)} changed, {len(failed)} failed. Report: {args.report}")
sys.exit(1 if failed else 0)
