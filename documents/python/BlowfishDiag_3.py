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

    # ── Build key variants ───────────────────────────────────────
    import hashlib
    keys_to_try = [("stripped UTF-8", key_as_bytes)]

    if key_b64_decoded and 4 <= len(key_b64_decoded) <= 56:
        keys_to_try.append(("Base64-decoded key", key_b64_decoded))

    # MD5 hash of the stripped key (common in older Java/.NET Blowfish impls)
    md5_key = hashlib.md5(key_as_bytes).digest()
    keys_to_try.append(("MD5(stripped key)", md5_key))

    # MD5 of the raw key including whitespace
    md5_raw_key = hashlib.md5(key_raw.encode("utf-8") if isinstance(key_raw, str) else key_raw).digest()
    if md5_raw_key != md5_key:
        keys_to_try.append(("MD5(raw key)", md5_raw_key))

    # SHA1 of the stripped key (less common but worth trying)
    sha1_key = hashlib.sha1(key_as_bytes).digest()[:56]
    keys_to_try.append(("SHA1(stripped key)[:56]", sha1_key))

    # ── Build content variants ────────────────────────────────────
    contents_to_try = [("raw content", content_raw)]
    if content_b64_decoded and len(content_b64_decoded) % 8 == 0:
        contents_to_try.append(("Base64-decoded content", content_b64_decoded))

    # ── Attempt standard padding modes ───────────────────────────
    winner = None
    for key_label, key_bytes_val in keys_to_try:
        for content_label, content_bytes in contents_to_try:
            label = f"{key_label} + {content_label}"
            result = try_decrypt(label, content_bytes, key_bytes_val)
            if result and winner is None:
                winner = (label, result)

    # ── Attempt no-padding / zero-padding (if still failing) ─────
    if not winner:
        print()
        print("  Trying no-padding / zero-padding variants...")
        for key_label, key_bytes_val in keys_to_try:
            for content_label, content_bytes in contents_to_try:
                # ECB no padding
                try:
                    c = Blowfish.new(key_bytes_val, Blowfish.MODE_ECB)
                    result = c.decrypt(content_bytes)
                    # Strip trailing null bytes (zero padding)
                    result_stripped = result.rstrip(b"\x00")
                    magic = identify_magic(result_stripped)
                    if "unknown" not in magic:
                        label = f"ECB no-pad zero-strip + {key_label} + {content_label}"
                        print(f"  [PASS] {label}  →  {len(result_stripped)} bytes  {magic}")
                        if winner is None:
                            winner = (label, result_stripped)
                    else:
                        print(f"  [FAIL] ECB no-pad + {key_label} + {content_label}"
                              f"  (first4={result[:4].hex().upper()})")
                except Exception as e:
                    print(f"  [ERR ] ECB no-pad + {key_label}: {e}")

                # CBC no padding (first 8 bytes as IV)
                try:
                    iv = content_bytes[:8]
                    c  = Blowfish.new(key_bytes_val, Blowfish.MODE_CBC, iv)
                    result = c.decrypt(content_bytes[8:])
                    result_stripped = result.rstrip(b"\x00")
                    magic = identify_magic(result_stripped)
                    if "unknown" not in magic:
                        label = f"CBC no-pad zero-strip + {key_label} + {content_label}"
                        print(f"  [PASS] {label}  →  {len(result_stripped)} bytes  {magic}")
                        if winner is None:
                            winner = (label, result_stripped)
                    else:
                        print(f"  [FAIL] CBC no-pad + {key_label} + {content_label}"
                              f"  (first4={result[:4].hex().upper()})")
                except Exception as e:
                    print(f"  [ERR ] CBC no-pad + {key_label}: {e}")

    # ── Attempt OFB and CFB modes ────────────────────────────────
    if not winner:
        print()
        print("  Trying OFB and CFB modes...")
        for key_label, key_bytes_val in keys_to_try:
            for content_label, content_bytes in contents_to_try:
                for mode_name, mode_const in [("OFB", Blowfish.MODE_OFB),
                                              ("CFB", Blowfish.MODE_CFB)]:
                    try:
                        iv = content_bytes[:8]
                        c  = Blowfish.new(key_bytes_val, mode_const, iv)
                        result = c.decrypt(content_bytes[8:])
                        magic = identify_magic(result)
                        status = "[PASS]" if "unknown" not in magic else "[FAIL]"
                        print(f"  {status} {mode_name} + {key_label} + {content_label}"
                              f"  (first4={result[:4].hex().upper()})  {magic if status=='[PASS]' else ''}")
                        if "unknown" not in magic and winner is None:
                            winner = (f"{mode_name} + {key_label}", result)
                    except Exception as e:
                        print(f"  [ERR ] {mode_name} + {key_label}: {e}")

    # ── Attempt content with leading byte offsets stripped ───────
    if not winner:
        print()
        print("  Trying leading-byte offset variants (8, 16, 24 bytes stripped)...")
        for key_label, key_bytes_val in keys_to_try[:2]:  # only top 2 keys
            for offset in [8, 16, 24]:
                trimmed = content_raw[offset:]
                if len(trimmed) % 8 != 0:
                    continue
                for mode_name, mode_const, use_iv in [
                    ("ECB", Blowfish.MODE_ECB, False),
                    ("CBC", Blowfish.MODE_CBC, True),
                ]:
                    try:
                        if use_iv:
                            iv = trimmed[:8]
                            c  = Blowfish.new(key_bytes_val, mode_const, iv)
                            result = c.decrypt(trimmed[8:])
                        else:
                            c  = Blowfish.new(key_bytes_val, mode_const)
                            result = c.decrypt(trimmed)
                        try:
                            result_unpadded = unpad(result, Blowfish.block_size)
                        except Exception:
                            result_unpadded = result.rstrip(b"\x00")
                        magic = identify_magic(result_unpadded)
                        status = "[PASS]" if "unknown" not in magic else "[FAIL]"
                        print(f"  {status} offset={offset} {mode_name} + {key_label}"
                              f"  (first4={result_unpadded[:4].hex().upper()})  "
                              f"{magic if status=='[PASS]' else ''}")
                        if "unknown" not in magic and winner is None:
                            winner = (f"offset={offset} {mode_name} + {key_label}",
                                      result_unpadded)
                    except Exception as e:
                        print(f"  [ERR ] offset={offset} {mode_name} + {key_label}: {e}")

    # ── Show all first4 results in a summary table ────────────────
    print()
    print("  First-4-byte summary of all no-pad ECB attempts:")
    print("  (Looking for D0CF=OLE2/xls, 504B=zip/xlsx, 2550=PDF)")
    for key_label, key_bytes_val in keys_to_try:
        try:
            c = Blowfish.new(key_bytes_val, Blowfish.MODE_ECB)
            result = c.decrypt(content_raw)
            print(f"    {key_label[:35]:<35} → {result[:4].hex().upper()}  {identify_magic(result)}")
        except Exception as e:
            print(f"    {key_label[:35]:<35} → ERR: {e}")

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
        print()
        print("  Key diagnostic — run this in SQL Server to see if other documents")
        print("  with the same DOCUMENT_KEY_TYPE decrypt correctly:")
        print("  SELECT TOP 5 DOCUMENT_ID, DATALENGTH(DOCUMENT_KEY),")
        print("    LEFT(DOCUMENT_KEY, 10), DATALENGTH(DOCUMENT_CONTENT)")
        print("  FROM [dbo].[DOCUMENT_LOCATORS]")
        print("  WHERE DOCUMENT_KEY_TYPE = 'BLOWFISH'")
        print("    AND DOCUMENT_CONTENT IS NOT NULL")
        print("  ORDER BY NEWID()")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python BlowfishDiag.py <document_id>")
        sys.exit(1)
    run(sys.argv[1])
