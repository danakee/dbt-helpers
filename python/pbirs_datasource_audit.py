#!/usr/bin/env python
"""
pbirs_datasource_audit.py

Enumerate Power BI Report Server / SSRS catalog items via REST
and audit their data sources for potential credential / connection
string risks.

Tested conceptually against the v2.0 REST pattern used by the
ReportingServicesTools PowerShell module.

Usage examples:

    python pbirs_datasource_audit.py \
        --portal-url http://yourserver/reports \
        --root-folder / \
        --output pbirs_datasource_audit.csv \
        --username DOMAIN\\user --password "Secret"

Notes:
- Assumes PBIRS/SSRS v2017+ (REST v2.0).
- Uses basic/NTLM-style auth via requests; adapt to your environment.
"""

import argparse
import csv
import getpass
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from urllib.parse import quote

# Optional NTLM support (pip install requests-ntlm)
try:
    from requests_ntlm import HttpNtlmAuth  # type: ignore
except ImportError:  # pragma: no cover
    HttpNtlmAuth = None  # type: ignore


# ---------- HTTP helpers ----------

def build_session(
    username: Optional[str],
    password: Optional[str],
    verify_ssl: bool = True,
) -> requests.Session:
    """
    Build a requests Session. If username/password are provided and
    requests-ntlm is installed, use NTLM auth. Otherwise, fall back
    to basic auth. If no credentials, anonymous.
    """
    sess = requests.Session()
    sess.verify = verify_ssl

    if username and password:
        if "\\" in username and HttpNtlmAuth is not None:
            # DOMAIN\\user → NTLM
            sess.auth = HttpNtlmAuth(username, password)
        else:
            # Fall back to basic auth (assumes HTTPS in prod)
            sess.auth = (username, password)

    # PBIRS REST endpoints return JSON by default when Accept is JSON.
    sess.headers.update({"Accept": "application/json"})
    return sess


def api_get(session: requests.Session, url: str) -> Dict:
    resp = session.get(url)
    resp.raise_for_status()
    return resp.json()


# ---------- PBIRS REST helpers ----------

def encode_path(path: str) -> str:
    """
    Encode path for use inside Path='...' in the URL.
    Keep slash characters, encode spaces and specials.
    """
    return quote(path, safe="/")


def get_folder_items(
    session: requests.Session,
    portal_url: str,
    path: str,
) -> List[Dict]:
    """
    Call: {portal_url}/api/v2.0/Folders(Path='{path}')/CatalogItems?$expand=Properties
    Returns list of catalog item dicts.
    """
    base = portal_url.rstrip("/")
    encoded = encode_path(path)
    url = f"{base}/api/v2.0/Folders(Path='{encoded}')/CatalogItems?$expand=Properties"
    data = api_get(session, url)
    return data.get("value", [])


def get_item_datasources(
    session: requests.Session,
    portal_url: str,
    path: str,
) -> List[Dict]:
    """
    Mimic Get-RsRestItemDataSource:

    1. GET CatalogItems(Path='{path}') → get Type (e.g. 'Report','PowerBIReport').
    2. GET {Type}s(Path='{path}')?$expand=DataSources → return .DataSources.
    """
    base = portal_url.rstrip("/")
    encoded = encode_path(path)

    # Step 1: get item to learn its Type
    cat_url = f"{base}/api/v2.0/CatalogItems(Path='{encoded}')"
    cat = api_get(session, cat_url)
    item_type = cat.get("Type")
    if not item_type:
        return []

    # Step 2: get its data sources
    ds_url = f"{base}/api/v2.0/{item_type}s(Path='{encoded}')?$expand=DataSources"
    data = api_get(session, ds_url)
    return data.get("DataSources", [])


def walk_catalog(
    session: requests.Session,
    portal_url: str,
    root_folder: str,
    recurse: bool = True,
) -> Iterable[Dict]:
    """
    Depth-first walk of folders starting at root_folder.
    Yields catalog item dictionaries.
    """
    stack = [root_folder]

    while stack:
        folder = stack.pop()
        items = get_folder_items(session, portal_url, folder)

        for item in items:
            yield item

            if recurse and item.get("Type") == "Folder":
                # The REST API gives us a full Path already
                sub_path = item.get("Path") or f"{folder.rstrip('/')}/{item.get('Name')}"
                stack.append(sub_path)


# ---------- Risk heuristics ----------

SUSPICIOUS_USERS = {"sa", "root", "admin", "administrator"}


def extract_username(ds: Dict) -> Optional[str]:
    """
    Try to get a username from both paginated and Power BI style sources.
    Paginated: ds['UserName']
    Power BI:  ds['DataModelDataSource']['Username']
    """
    if "UserName" in ds and ds["UserName"]:
        return ds["UserName"]

    dmd = ds.get("DataModelDataSource") or {}
    if "Username" in dmd and dmd["Username"]:
        return dmd["Username"]

    return None


def extract_connection_string(ds: Dict) -> Optional[str]:
    """
    Try to get a connection string-like value.
    Paginated: ds['ConnectionString']
    Some Power BI sources may expose similar details under DataModelDataSource.
    """
    if "ConnectionString" in ds and ds["ConnectionString"]:
        return ds["ConnectionString"]

    dmd = ds.get("DataModelDataSource") or {}
    if "ConnectionString" in dmd and dmd["ConnectionString"]:
        return dmd["ConnectionString"]

    return None


def audit_datasource(
    item: Dict,
    ds: Dict,
) -> Dict:
    """
    Build a flattened row with basic risk flags.
    """
    item_path = item.get("Path", "")
    item_type = item.get("Type", "")
    item_name = item.get("Name", "")

    ds_name = ds.get("Name", "")
    ds_ext = ds.get("Extension", "")
    cred_mode = ds.get("CredentialRetrieval") or ds.get("DataModelDataSource", {}).get("AuthType")
    username = extract_username(ds)
    conn_str = extract_connection_string(ds) or ""

    # Heuristics
    is_sql_auth = "user id=" in conn_str.lower() or "uid=" in conn_str.lower()
    encrypt_disabled = "encrypt=false" in conn_str.replace(" ", "").lower()
    trust_server_cert = "trustservercertificate=true" in conn_str.replace(" ", "").lower()
    suspicious_user = bool(username and username.split("\\")[-1].lower() in SUSPICIOUS_USERS)
    is_stored_creds = str(cred_mode).lower() in {"store", "usernamepassword"}

    return {
        "ItemPath": item_path,
        "ItemName": item_name,
        "ItemType": item_type,
        "DataSourceName": ds_name,
        "Extension": ds_ext,
        "CredentialMode": cred_mode,
        "UserName": username or "",
        "ConnectionString": conn_str,
        "IsSqlAuth": is_sql_auth,
        "IsStoredCreds": is_stored_creds,
        "EncryptDisabled": encrypt_disabled,
        "TrustServerCert": trust_server_cert,
        "SuspiciousUser": suspicious_user,
    }


# ---------- CLI + main ----------

REPORT_LIKE_TYPES = {
    "Report",
    "PaginatedReport",
    "PowerBIReport",
    "DataSet",
}


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Audit PBIRS/SSRS data sources via REST API."
    )
    p.add_argument(
        "--portal-url",
        required=True,
        help="Report Portal URL, e.g. http://server/reports",
    )
    p.add_argument(
        "--root-folder",
        default="/",
        help="Root folder path to start from (default: /).",
    )
    p.add_argument(
        "--output",
        "-o",
        default="pbirs_datasource_audit.csv",
        help="Output CSV path (default: pbirs_datasource_audit.csv).",
    )
    p.add_argument(
        "--no-recurse",
        action="store_true",
        help="Do not recurse into subfolders.",
    )
    p.add_argument(
        "--username",
        help="Username for auth (DOMAIN\\user or user).",
    )
    p.add_argument(
        "--password",
        help="Password for auth. If omitted but username provided, you will be prompted.",
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS verification (NOT recommended in production).",
    )
    return p.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    password = args.password
    if args.username and not password:
        password = getpass.getpass(f"Password for {args.username}: ")

    session = build_session(
        username=args.username,
        password=password,
        verify_ssl=not args.insecure,
    )

    recurse = not args.no_recurse

    print(f"Connecting to {args.portal_url} ...")
    print(f"Walking catalog from folder: {args.root_folder!r} (recurse={recurse})")

    rows: List[Dict] = []

    for item in walk_catalog(session, args.portal_url, args.root_folder, recurse=recurse):
        item_type = item.get("Type")
        if item_type not in REPORT_LIKE_TYPES:
            continue

        path = item.get("Path")
        if not path:
            continue

        try:
            ds_list = get_item_datasources(session, args.portal_url, path)
        except requests.HTTPError as ex:
            print(f"[WARN] Failed to get data sources for {path}: {ex}", file=sys.stderr)
            continue

        if not ds_list:
            continue

        for ds in ds_list:
            row = audit_datasource(item, ds)
            rows.append(row)

    if not rows:
        print("No data sources found under the specified root/folder.")
        return 0

    out_path = Path(args.output).resolve()
    fieldnames = list(rows[0].keys())

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_path}")
    print("Tip: filter on IsSqlAuth/IsStoredCreds/EncryptDisabled/TrustServerCert/SuspiciousUser for quick wins.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
