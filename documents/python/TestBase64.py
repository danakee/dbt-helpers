import base64

# Paste the DOCUMENT_KEY value from your DB record here
document_key = "@_...CZG#LL!<..."   # replace with actual value

# Check if it's Base64
try:
    decoded = base64.b64decode(document_key)
    print(f"Key decoded OK — {len(decoded)} bytes: {decoded.hex()}")
    print(f"Valid Blowfish key length: {4 <= len(decoded) <= 56}")
except Exception as e:
    print(f"Key is NOT Base64: {e}")

# Also check the raw file bytes from DOCUMENT_FILE
# Open the .doc you just wrote and check the first bytes
with open(r"C:\output\testJagan_2.doc", "rb") as f:
    first_bytes = f.read(64)
print(f"\nFirst 64 bytes hex: {first_bytes.hex()}")
print(f"First 64 bytes txt: {first_bytes}")

# Check if the file content is Base64
try:
    decoded_file = base64.b64decode(first_bytes)
    print(f"File content IS Base64 — decoded starts with: {decoded_file[:8].hex()}")
    # A real .doc file starts with D0CF11E0A1B11AE1 (OLE2 compound doc magic bytes)
    # A real .docx starts with 504B0304 (ZIP magic bytes)
except Exception as e:
    print(f"File content is NOT Base64: {e}")