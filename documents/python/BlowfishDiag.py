"""
BlowfishDiag.py
---------------
Diagnostic script for troubleshooting Blowfish decryption failures.
Inspects the raw key and content bytes and tries every combination
of mode, padding, and encoding before giving up.

Usage:
    python BlowfishDiag.py <document_id>
"""

import os
import sys
import base64
import binascii
from dotenv import load_dotenv
import pyodbc
from Crypto.Cipher import Blowfish
from Crypto.Util.Padding import unpad

load_dotenv()

# ── Known file magic bytes ───────────────────────────────────────
MAGIC = {
    bytes.fromhex("D0CF11E0"): "OLE2 compound doc (.doc .xls .ppt)",
    bytes.fromhex("504B0304"): "ZIP / Office Open XML (.docx .xlsx .pptx)",
    bytes.fromhex("25504446"): "PDF (%PDF)",
    bytes.fromhex("FFD8FFE0"): "JPEG image",
    bytes.fromhex("FFD8FFE1"): "JPEG image (EXIF)",
    bytes.fromhex("89504E47"): "PNG image",
    bytes.fromhex("47494638"): "GIF image",
    bytes.fromhex("3C68746D"): "HTML (<htm)",
    bytes.fromhex("3C48544D"): "HTML (<HTM)",
    bytes.fromhex("EFBBBF3C"): "HTML with BOM",
}

def identify_magic(data: bytes) -> str:
    if len(data) < 4:
        return "too short to identify"
    head = data[:4]
    for magic, desc in MAGIC.items():
        if head == magic:
            return desc
    return f"unknown ({head.hex().upper()})"

# ── Database connection ──────────────────────────────────────────
def get_connection():
    driver   = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    server   = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    uid      = os.getenv("DB_UID")
    pwd      = os.getenv("DB_PWD")
    if uid and pwd:
        cs = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"
    else:
        cs = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    return pyodbc.connect(cs)

# ── Fetch raw values ─────────────────────────────────────────────
def fetch_raw(document_id: str) -> dict:
    sql = f"""
        SELECT TOP 1
            dl.DOCUMENT_KEY,
            dl.DOCUMENT_KEY_TYPE,
            dl.DOCUMENT_CONTENT,
            dl.UDOCUMENT_CONTENT,
            DATALENGTH(dl.DOCUMENT_CONTENT)  AS content_len,
            DATALENGTH(dl.DOCUMENT_KEY)      AS key_len
        FROM [dbo].[DOCUMENT_LOCATORS] dl
        WHERE dl.DOCUMENT_ID = {document_id}
          AND dl.DOCUMENT_CONTENT IS NOT NULL
          AND dl.DOCUMENT_KEY     IS NOT NULL
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            return {}
        return {
            "key_raw":       row.DOCUMENT_KEY,
            "key_type":      row.DOCUMENT_KEY_TYPE,
            "content_raw":   bytes(row.DOCUMENT_CONTENT) if row.DOCUMENT_CONTENT else None,
            "ucontent_raw":  bytes(row.UDOCUMENT_CONTENT) if row.UDOCUMENT_CONTENT else None,
            "content_len":   row.content_len,
            "key_len":       row.key_len,
        }

# ── Try one decryption attempt ───────────────────────────────────
def try_decrypt(label: str, data: bytes, key: bytes) -> bytes | None:
    # ECB
    try:
        c = Blowfish.new(key, Blowfish.MODE_ECB)
        result = unpad(c.decrypt(data), Blowfish.block_size)
        print(f"  [PASS] {label} ECB  →  {len(result)} bytes  {identify_magic(result)}")
        return result
    except Exception:
        pass

    # CBC — first 8 bytes as IV
    try:
        iv = data[:8]
        c  = Blowfish.new(key, Blowfish.MODE_CBC, iv)
        result = unpad(c.decrypt(data[8:]), Blowfish.block_size)
        print(f"  [PASS] {label} CBC  →  {len(result)} bytes  {identify_magic(result)}")
        return result
    except Exception:
        pass

    # CBC — zero IV
    try:
        iv = b'\x00' * 8
        c  = Blowfish.new(key, Blowfish.MODE_CBC, iv)
        result = unpad(c.decrypt(data), Blowfish.block_size)
        print(f"  [PASS] {label} CBC(zero IV)  →  {len(result)} bytes  {identify_magic(result)}")
        return result
    except Exception:
        pass

    print(f"  [FAIL] {label}")
    return None

# ── Main diagnostic ──────────────────────────────────────────────
def run(document_id: str):
    print(f"\n{'='*60}")
    print(f"Blowfish Diagnostic  —  DOCUMENT_ID {document_id}")
    print(f"{'='*60}\n")

    raw = fetch_raw(document_id)
    if not raw:
        print("ERROR: No DOCUMENT_CONTENT + DOCUMENT_KEY row found for this ID.")
        return

    key_raw     = raw["key_raw"]
    content_raw = raw["content_raw"]

    # ── Key analysis ─────────────────────────────────────────────
    print("── Key ─────────────────────────────────────────────────")
    print(f"  Raw repr     : {repr(key_raw)}")
    print(f"  Length       : {raw['key_len']} chars")

    key_stripped = key_raw.strip() if isinstance(key_raw, str) else key_raw
    print(f"  Stripped     : {repr(key_stripped)}")
    print(f"  Stripped len : {len(key_stripped)}")

    key_as_bytes = key_stripped.encode("utf-8")
    print(f"  UTF-8 bytes  : {key_as_bytes.hex()}")

    key_b64_decoded = None
    try:
        key_b64_decoded = base64.b64decode(key_stripped)
        print(f"  Base64 decode: {len(key_b64_decoded)} bytes  hex={key_b64_decoded.hex()}")
        print(f"  Valid BF len : {4 <= len(key_b64_decoded) <= 56}")
    except Exception as e:
        print(f"  Base64 decode: NOT Base64 ({e})")

    # ── Content analysis ─────────────────────────────────────────
    print(f"\n── Content ─────────────────────────────────────────────")
    print(f"  Length       : {raw['content_len']} bytes")
    print(f"  Length % 8   : {raw['content_len'] % 8}  (must be 0 for Blowfish)")
    print(f"  First 16 hex : {content_raw[:16].hex().upper()}")
    print(f"  First 16 txt : {repr(content_raw[:16])}")

    # Check if content itself is Base64
    content_b64_decoded = None
    try:
        content_b64_decoded = base64.b64decode(content_raw)
        print(f"  Base64 decode: {len(content_b64_decoded)} bytes  "
              f"first4={content_b64_decoded[:4].hex().upper()}")
        print(f"  If B64 decoded, length % 8: {len(content_b64_decoded) % 8}")
    except Exception:
        print(f"  Base64 decode: content is NOT Base64")

    if raw["ucontent_raw"]:
        uc = raw["ucontent_raw"]
        print(f"\n  UDOCUMENT_CONTENT: {len(uc)} bytes  "
              f"first4={uc[:4].hex().upper()}  {identify_magic(uc)}")

    # ── Decryption attempts ───────────────────────────────────────
    print(f"\n── Decryption attempts ─────────────────────────────────")

    keys_to_try = [("stripped UTF-8", key_as_bytes)]
    if key_b64_decoded and 4 <= len(key_b64_decoded) <= 56:
        keys_to_try.append(("Base64-decoded key", key_b64_decoded))

    contents_to_try = [("raw content", content_raw)]
    if content_b64_decoded and len(content_b64_decoded) % 8 == 0:
        contents_to_try.append(("Base64-decoded content", content_b64_decoded))

    winner = None
    for key_label, key_bytes in keys_to_try:
        for content_label, content_bytes in contents_to_try:
            label = f"{key_label} + {content_label}"
            result = try_decrypt(label, content_bytes, key_bytes)
            if result and winner is None:
                winner = (label, result)

    print()
    if winner:
        label, result = winner
        print(f"SUCCESS  —  {label}")
        print(f"Decrypted {len(result)} bytes  →  {identify_magic(result)}")
    else:
        print("FAILED — no combination worked.")
        print()
        print("Next steps to investigate:")
        print("  1. Check if the app applies a hash/transform to the key before use")
        print("  2. Check if content needs trimming (e.g. leading header bytes)")
        print("  3. Check if a different cipher mode is used (OFB, CFB, CTR)")
        print("  4. Inspect the Prism application source or config for cipher details")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python BlowfishDiag.py <document_id>")
        sys.exit(1)
    run(sys.argv[1])
