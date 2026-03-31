"""
document_materializer.py
------------------------
Materializes documents from PrismFlightSafety_SQL to the local file system.
Handles three storage patterns:
  1. External file on disk (encrypted with Blowfish)  -- ~358K rows
  2. Encrypted blob in DOCUMENT_CONTENT              -- ~60K rows
  3. Raw blob in DOCUMENT_FILE (legacy, no encrypt)  -- ~2.5K rows
  4. Linked document (DOCUMENT_LINK) -- skipped, nothing to write

Usage:
    result = materialize_document(document_id=12345, destination_path="C:/output")
    print(result)
"""

import os
import base64
import logging
import pyodbc

from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from Crypto.Cipher import Blowfish
from Crypto.Util.Padding import unpad

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Database connection
# ─────────────────────────────────────────────

def get_connection() -> pyodbc.Connection:
    """
    Returns a pyodbc connection using credentials from .env.

    Required .env keys:
        DB_SERVER   e.g. 0002wp-dbms-17v
        DB_NAME     e.g. PrismFlightSafety_SQL
        DB_DRIVER   e.g. ODBC Driver 17 for SQL Server
        DB_UID      (optional — omit for Windows auth)
        DB_PWD      (optional — omit for Windows auth)
    """
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    uid = os.getenv("DB_UID")
    pwd = os.getenv("DB_PWD")

    if uid and pwd:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={uid};"
            f"PWD={pwd};"
        )
    else:
        # Windows integrated authentication
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )

    return pyodbc.connect(conn_str)


# ─────────────────────────────────────────────
# Sub-function 1: Query all locator info
# ─────────────────────────────────────────────

DOCUMENT_QUERY = """
SELECT
    dh.DOCUMENT_ID,
    dh.DOCUMENT_NAME,
    dh.APPROVED_VERSION_ID,
    dh.ELIMINATED,
    dh.SECURITY_KEY_ENABLED,

    dv.VERSION,
    dv.APPROVAL_STATUS,
    dv.ARCHIVE_FLAG,
    dv.REMOVED_FLAG,
    dv.VERSION_SIZE,
    dv.EFFECTIVE_DATE,
    dv.AUTHOR_NAME,

    dl.DOCUMENT_NUMBER,
    dl.LOCATION_TYPE,
    dl.STORAGE_TYPE,
    dl.PATH_OR_TABLE,
    dl.PHYSICAL_LOCATION,
    dl.FILE_OR_COLUMN,
    dl.DOCUMENT_LINK,
    dl.DOCUMENT_FILE,
    dl.DOCUMENT_CONTENT,
    dl.DOCUMENT_KEY,
    dl.DOCUMENT_KEY_TYPE,
    dl.FILE_EXTENSION,
    dl.FILE_NAME_WITHOUT_EXTENSION,
    dl.Mime_Type,
    dl.Enc_File_Flag,

    st.DESCRIPTION  AS STORAGE_TYPE_DESC,
    ut.DESCRIPTION  AS USAGE_TYPE_DESC

FROM 
    [dbo].[DOCUMENT_HEADERS]       AS dh
    INNER JOIN [dbo].[DOCUMENT_VERSIONS]     AS dv
        ON dh.DOCUMENT_ID   = dv.DOCUMENT_ID
    INNER JOIN [dbo].[DOCUMENT_LOCATORS]     AS dl
        ON  dv.DOCUMENT_ID   = dl.DOCUMENT_ID
        AND dv.VERSION        = dl.VERSION
    LEFT JOIN [dbo].[DOCUMENT_STORAGE_TYPES_EN] AS st
        ON dl.STORAGE_TYPE  = st.STORAGE_TYPE
    LEFT JOIN [dbo].[DOCUMENT_USAGE_TYPES]  AS ut
        ON dh.USAGE_TYPE_ID = ut.USAGE_TYPE_ID
WHERE
    dh.DOCUMENT_ID = ?
ORDER BY
    dv.VERSION,
    dl.DOCUMENT_NUMBER
"""


def get_document_info(document_id: int) -> list[dict]:
    """
    Queries all locator rows for a given DOCUMENT_ID.
    Returns a list of dicts — one per locator row.
    An empty list means the document_id was not found.
    """
    rows = []
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(DOCUMENT_QUERY, document_id)
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                # Classify storage pattern for use downstream
                record["storage_pattern"] = _classify_storage(record)
                rows.append(record)
        log.info(
            "DOCUMENT_ID %s — %d locator row(s) found", document_id, len(rows)
        )
    except pyodbc.Error as exc:
        log.error("DB error fetching document %s: %s", document_id, exc)
        raise
    return rows


def _classify_storage(record: dict) -> str:
    """
    Returns a short label describing how this locator row stores its content.
    Used for routing in get_file_bytes() and for result reporting.
    """
    if record.get("DOCUMENT_LINK"):
        return "link"
    if record.get("DOCUMENT_FILE") is not None:
        if record.get("DOCUMENT_CONTENT") is not None:
            return "file_and_content"   # overlap rows — write DOCUMENT_FILE
        return "db_file"                # legacy unencrypted blob
    if record.get("DOCUMENT_CONTENT") is not None:
        return "db_content_blowfish"    # encrypted blob in DB
    if record.get("DOCUMENT_KEY"):
        return "disk_blowfish"          # encrypted file on disk
    return "unknown"


# ─────────────────────────────────────────────
# Version strategy constants
# ─────────────────────────────────────────────

VERSION_APPROVED_ONLY = "approved_only"
"""
Only materialize the version pointed to by APPROVED_VERSION_ID.
All other versions are skipped.
Output: <base_path>/<filename>.<ext>
Good for: "give me the current live document"
"""

VERSION_ALL_SUFFIX = "all_suffix"
"""
Materialize every version; append _v<N> to the filename stem.
Output: <base_path>/<filename>_v1.<ext>
         <base_path>/<filename>_v2.<ext>
Good for: flat archive where all versions live side by side
"""

VERSION_ALL_SUBFOLDERS = "all_subfolders"
"""
Materialize every version; place each in its own v<N> subfolder.
Output: <base_path>/v1/<filename>.<ext>
         <base_path>/v2/<filename>.<ext>
Good for: keeping version context without cluttering filenames
"""


# ─────────────────────────────────────────────
# Sub-function 2: Resolve destination path
# ─────────────────────────────────────────────

def resolve_destination_path(
    record: dict,
    base_path: str,
    version_strategy: str = VERSION_APPROVED_ONLY,
) -> Path:
    """
    Builds the full destination file path based on the version strategy.

    Strategies:
        approved_only    → <base_path>/<filename>.<ext>
        all_suffix       → <base_path>/<filename>_v<N>.<ext>
        all_subfolders   → <base_path>/v<N>/<filename>.<ext>

    Falls back gracefully if filename or extension are missing.
    """
    doc_id  = record.get("DOCUMENT_ID", "unknown")
    version = record.get("VERSION", 0)
    stem    = (
        record.get("FILE_NAME_WITHOUT_EXTENSION")
        or record.get("DOCUMENT_NAME")
        or f"doc_{doc_id}"
    )
    ext = record.get("FILE_EXTENSION", "").strip().lstrip(".")

    # Sanitize stem — strip characters illegal in Windows/POSIX filenames
    stem = "".join(c if c not in r'\/:*?"<>|' else "_" for c in str(stem))
    stem = stem[:150]  # cap length to avoid OS path limit issues

    if version_strategy == VERSION_ALL_SUFFIX:
        versioned_stem = f"{stem}_v{version}"
        filename = f"{versioned_stem}.{ext}" if ext else versioned_stem
        return Path(base_path) / filename

    elif version_strategy == VERSION_ALL_SUBFOLDERS:
        filename = f"{stem}.{ext}" if ext else stem
        return Path(base_path) / f"v{version}" / filename

    else:
        # approved_only — no version discriminator in path
        filename = f"{stem}.{ext}" if ext else stem
        return Path(base_path) / filename


# ─────────────────────────────────────────────
# Sub-function 3: Get raw (possibly encrypted) bytes
# ─────────────────────────────────────────────


def _decode_column_content(raw, column_name: str) -> bytes:
    """
    Handles the two ways pyodbc may return DB column content:

      str  → SQL text column (DOCUMENT_FILE): always Base64-encoded
               because text columns cannot store raw binary.
               Decode Base64 to get the real file bytes.

      bytes → SQL image column (DOCUMENT_CONTENT): may be raw bytes
               from older Prism versions, or Base64 string stored as
               image in newer versions. Try Base64 first; fall back
               to raw bytes if decode fails or result looks wrong.

    Logs the magic bytes so we can verify the file type is correct.
    """
    if isinstance(raw, str):
        try:
            decoded = base64.b64decode(raw)
            log.debug(
                "  %s Base64 decoded: %d chars -> %d bytes  magic=%s",
                column_name, len(raw), len(decoded), decoded[:4].hex(),
            )
            return decoded
        except Exception as exc:
            log.warning(
                "  %s Base64 decode failed (%s) — falling back to latin-1 bytes",
                column_name, exc,
            )
            return raw.encode("latin-1")

    # bytes path (image column)
    try:
        decoded = base64.b64decode(raw)
        # Sanity check: decoded result should be shorter and look like a real file.
        # Real files start with known magic bytes; Base64 content does not.
        magic = decoded[:4]
        known_magic = [
            b"\xd0\xcf\x11\xe0",  # OLE2  .doc .xls .ppt
            b"PK\x03\x04",          # ZIP   .docx .xlsx .pptx
            b"%PDF",                  # PDF
            b"\xff\xd8\xff",       # JPEG
            b"\x89PNG",              # PNG
            b"<htm",                  # HTML
            b"<HTM",
        ]
        if any(magic.startswith(m) for m in known_magic):
            log.debug(
                "  %s image col Base64 decoded: %d -> %d bytes  magic=%s",
                column_name, len(raw), len(decoded), magic.hex(),
            )
            return decoded
    except Exception:
        pass

    # Raw bytes — log magic for diagnostics
    log.debug(
        "  %s image col raw bytes: %d bytes  magic=%s",
        column_name, len(raw), raw[:4].hex() if len(raw) >= 4 else raw.hex(),
    )
    return bytes(raw)


def get_file_bytes(record: dict) -> Optional[bytes]:
    """
    Routes to the correct byte source based on storage_pattern.
    Returns raw bytes (may still be encrypted) or None for links.

    Patterns handled:
        link                → None  (nothing to write)
        db_file             → bytes from DOCUMENT_FILE column
        db_content_blowfish → bytes from DOCUMENT_CONTENT column
        file_and_content    → bytes from DOCUMENT_FILE (preferred for overlap rows)
        disk_blowfish       → bytes read from PATH_OR_TABLE on disk
        unknown             → None  (log warning, skip)
    """
    pattern = record.get("storage_pattern", "unknown")

    if pattern == "link":
        log.info(
            "  DOCUMENT_NUMBER %s is a linked document — skipping (%s)",
            record.get("DOCUMENT_NUMBER"),
            record.get("DOCUMENT_LINK"),
        )
        return None

    if pattern in ("db_file", "file_and_content"):
        raw = record.get("DOCUMENT_FILE")
        if raw is None:
            log.warning("  DOCUMENT_FILE is None for pattern '%s'", pattern)
            return None
        # DOCUMENT_FILE is a SQL text column — pyodbc returns it as a string.
        # Content is Base64-encoded before storage (text column cannot hold raw binary).
        return _decode_column_content(raw, "DOCUMENT_FILE")

    if pattern == "db_content_blowfish":
        raw = record.get("DOCUMENT_CONTENT")
        if raw is None:
            log.warning("  DOCUMENT_CONTENT is None")
            return None
        # DOCUMENT_CONTENT is a SQL image column — may be raw bytes or Base64 string
        # depending on how the Prism app version stored it.
        return _decode_column_content(raw, "DOCUMENT_CONTENT")

    if pattern == "disk_blowfish":
        file_path = record.get("PATH_OR_TABLE") or record.get("PHYSICAL_LOCATION")
        if not file_path:
            log.warning("  No file path found for disk_blowfish record")
            return None
        try:
            with open(file_path, "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            log.error("  File not found on disk: %s", file_path)
            return None
        except PermissionError:
            log.error("  Permission denied reading: %s", file_path)
            return None

    log.warning(
        "  Unknown storage pattern '%s' for DOCUMENT_NUMBER %s",
        pattern,
        record.get("DOCUMENT_NUMBER"),
    )
    return None


# ─────────────────────────────────────────────
# Sub-function 4: Blowfish decryption
# ─────────────────────────────────────────────

def decrypt_bytes(raw: bytes, key: str) -> bytes:
    """
    Decrypts Blowfish-encrypted bytes.

    Blowfish in ECB mode with PKCS7 padding is the most common
    Prism implementation — we try that first, then fall back to
    CBC (which requires the first 8 bytes to be the IV).

    Args:
        raw:  Raw encrypted bytes from DB column or disk file
        key:  The DOCUMENT_KEY string from DOCUMENT_LOCATORS

    Returns:
        Decrypted bytes ready to write to disk.

    Raises:
        ValueError if decryption fails with both modes.
    """
    key_bytes = key.encode("utf-8") if isinstance(key, str) else key

    # Blowfish key must be 4–56 bytes
    if not (4 <= len(key_bytes) <= 56):
        raise ValueError(
            f"Blowfish key length {len(key_bytes)} is out of range (4–56 bytes)"
        )

    # ── Attempt 1: ECB mode ──
    try:
        cipher = Blowfish.new(key_bytes, Blowfish.MODE_ECB)
        decrypted = unpad(cipher.decrypt(raw), Blowfish.block_size)
        log.debug("  Decrypted via Blowfish ECB (%d → %d bytes)", len(raw), len(decrypted))
        return decrypted
    except (ValueError, KeyError):
        pass

    # ── Attempt 2: CBC mode (first 8 bytes = IV) ──
    try:
        iv = raw[:8]
        cipher = Blowfish.new(key_bytes, Blowfish.MODE_CBC, iv)
        decrypted = unpad(cipher.decrypt(raw[8:]), Blowfish.block_size)
        log.debug("  Decrypted via Blowfish CBC (%d → %d bytes)", len(raw), len(decrypted))
        return decrypted
    except (ValueError, KeyError) as exc:
        raise ValueError(
            f"Blowfish decryption failed in both ECB and CBC modes: {exc}"
        ) from exc


# ─────────────────────────────────────────────
# Sub-function 5: Write file to disk
# ─────────────────────────────────────────────

def write_file(file_bytes: bytes, destination: Path) -> bool:
    """
    Writes bytes to destination path.
    Creates parent directories if they don't exist.
    Returns True on success, False on failure.
    """
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with open(destination, "wb") as fh:
            fh.write(file_bytes)
        log.info("  Written: %s (%d bytes)", destination, len(file_bytes))
        return True
    except OSError as exc:
        log.error("  Failed to write %s: %s", destination, exc)
        return False


# ─────────────────────────────────────────────
# Sub-function 6: Build result summary
# ─────────────────────────────────────────────

def build_result(
    record: dict,
    destination: Optional[Path],
    success: bool,
    bytes_written: int = 0,
    error: Optional[str] = None,
) -> dict:
    """
    Assembles a result dict for logging and reporting.
    """
    return {
        "document_id":       record.get("DOCUMENT_ID"),
        "version":           record.get("VERSION"),
        "document_number":   record.get("DOCUMENT_NUMBER"),
        "document_name":     record.get("DOCUMENT_NAME"),
        "filename":          destination.name if destination else None,
        "destination":       str(destination) if destination else None,
        "storage_pattern":   record.get("storage_pattern"),
        "encrypted":         record.get("DOCUMENT_KEY_TYPE") == "BLOWFISH",
        "storage_type_desc": record.get("STORAGE_TYPE_DESC"),
        "approval_status":   record.get("APPROVAL_STATUS"),
        "bytes_written":     bytes_written,
        "success":           success,
        "error":             error,
    }


# ─────────────────────────────────────────────
# Top-level orchestrator
# ─────────────────────────────────────────────

def materialize_document(
    document_id: int,
    destination_path: str,
    version_strategy: str = VERSION_APPROVED_ONLY,
) -> list[dict]:
    """
    Top-level entry point.

    Fetches all locator rows for document_id, filters by version
    strategy, then for each qualifying row:
      1. Resolves the destination file path
      2. Gets the raw bytes (DB blob or disk file)
      3. Decrypts if BLOWFISH key present
      4. Writes to destination_path

    Args:
        document_id:       DOCUMENT_HEADERS.DOCUMENT_ID
        destination_path:  Base folder to write files into
        version_strategy:  One of:
                             VERSION_APPROVED_ONLY  (default)
                             VERSION_ALL_SUFFIX
                             VERSION_ALL_SUBFOLDERS

    Returns:
        List of result dicts — one per locator row processed.
    """
    log.info(
        "── Materializing DOCUMENT_ID %s  [strategy: %s] ──",
        document_id, version_strategy,
    )
    results = []

    records = get_document_info(document_id)
    if not records:
        log.warning("No records found for DOCUMENT_ID %s", document_id)
        return results

    # ── Apply version filter for approved_only strategy ──
    if version_strategy == VERSION_APPROVED_ONLY:
        approved_version_id = records[0].get("APPROVED_VERSION_ID")
        if approved_version_id is None:
            log.warning(
                "  APPROVED_VERSION_ID is NULL for DOCUMENT_ID %s "
                "— falling back to latest version",
                document_id,
            )
            # Fall back to the highest version number available
            latest = max(r["VERSION"] for r in records)
            records = [r for r in records if r["VERSION"] == latest]
        else:
            records = [r for r in records if r["VERSION"] == approved_version_id]
            if not records:
                log.warning(
                    "  No locator rows match APPROVED_VERSION_ID %s "
                    "for DOCUMENT_ID %s",
                    approved_version_id, document_id,
                )
                return results

        log.info(
            "  approved_only: materializing version %s (%d locator row(s))",
            records[0]["VERSION"] if records else "?",
            len(records),
        )

    else:
        log.info(
            "  %s: materializing %d locator row(s) across all versions",
            version_strategy, len(records),
        )

    for record in records:
        pattern = record.get("storage_pattern")
        dest    = resolve_destination_path(record, destination_path, version_strategy)

        log.info(
            "  Version %s | Pattern: %-22s | File: %s",
            record.get("VERSION"),
            pattern,
            dest.name,
        )

        # ── Skip linked documents ──
        if pattern == "link":
            results.append(build_result(record, dest, success=False,
                                        error="linked document — no bytes to write"))
            continue

        # ── Get raw bytes ──
        raw = get_file_bytes(record)
        if raw is None:
            results.append(build_result(record, dest, success=False,
                                        error="could not retrieve bytes"))
            continue

        # ── Decrypt if needed ──
        file_bytes = raw
        if record.get("DOCUMENT_KEY_TYPE") == "BLOWFISH":
            key = record.get("DOCUMENT_KEY")
            if not key:
                results.append(build_result(record, dest, success=False,
                                            error="BLOWFISH key type set but DOCUMENT_KEY is null"))
                continue
            try:
                file_bytes = decrypt_bytes(raw, key)
            except ValueError as exc:
                results.append(build_result(record, dest, success=False,
                                            error=f"decryption failed: {exc}"))
                continue

        # ── Write to disk ──
        success = write_file(file_bytes, dest)
        results.append(
            build_result(record, dest, success=success,
                         bytes_written=len(file_bytes) if success else 0,
                         error=None if success else "write failed — see log")
        )

    # ── Summary ──
    total   = len(results)
    written = sum(1 for r in results if r["success"])
    log.info(
        "── Done: %d/%d locator(s) written for DOCUMENT_ID %s ──",
        written, total, document_id,
    )
    return results


# ─────────────────────────────────────────────
# Quick POC runner
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 3:
        print("Usage: python document_materializer.py <document_id> <destination_path> [strategy]")
        print("  strategy options: approved_only (default)  |  all_suffix  |  all_subfolders")
        print("  Example: python document_materializer.py 12345 C:/output approved_only")
        sys.exit(1)

    doc_id   = int(sys.argv[1])
    dest     = sys.argv[2]
    strategy = sys.argv[3] if len(sys.argv) > 3 else VERSION_APPROVED_ONLY

    valid = {VERSION_APPROVED_ONLY, VERSION_ALL_SUFFIX, VERSION_ALL_SUBFOLDERS}
    if strategy not in valid:
        print(f"Unknown strategy '{strategy}'. Choose from: {', '.join(valid)}")
        sys.exit(1)

    results = materialize_document(doc_id, dest, version_strategy=strategy)
    print("\n── Results ──")
    print(json.dumps(results, indent=2, default=str))
