# Run this on your machine to inspect the raw values
# before any decryption attempt

import os, base64
from dotenv import load_dotenv
import pyodbc

load_dotenv()

conn = pyodbc.connect(
    f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"Trusted_Connection=yes;"
)

cursor = conn.cursor()
cursor.execute("""
    SELECT
        dl.DOCUMENT_KEY,
        dl.DOCUMENT_KEY_TYPE,
        dl.Enc_File_Flag,
        DATALENGTH(dl.DOCUMENT_CONTENT) AS content_length_bytes,
        DATALENGTH(dl.DOCUMENT_KEY)     AS key_length_chars,
        LEFT(CAST(dl.DOCUMENT_CONTENT AS VARCHAR(MAX)), 100) AS content_preview
    FROM [dbo].[DOCUMENT_LOCATORS] dl
    WHERE dl.DOCUMENT_ID = '100001426414828928869433444387258660160'
""")

row = cursor.fetchone()
print(f"DOCUMENT_KEY       : {row.DOCUMENT_KEY}")
print(f"DOCUMENT_KEY_TYPE  : {row.DOCUMENT_KEY_TYPE}")
print(f"Enc_File_Flag      : {row.Enc_File_Flag}")
print(f"Key length (chars) : {row.key_length_chars}")
print(f"Content length     : {row.content_length_bytes} bytes")
print(f"Content preview    : {row.content_preview}")
print()

# Check if key is Base64
key = row.DOCUMENT_KEY
print(f"Key raw            : {repr(key)}")
try:
    decoded_key = base64.b64decode(key)
    print(f"Key Base64 decoded : {len(decoded_key)} bytes  hex={decoded_key.hex()}")
    print(f"Valid Blowfish len : {4 <= len(decoded_key) <= 56}")
except Exception as e:
    print(f"Key NOT Base64     : {e}")

# Check content modulo block size (must be multiple of 8 for Blowfish)
content_len = row.content_length_bytes
print(f"\nContent % 8        : {content_len % 8}  (must be 0 for Blowfish)")

conn.close()
