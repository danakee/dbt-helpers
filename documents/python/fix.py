import base64

def decrypt_bytes(raw: bytes, key: str) -> bytes:
    """
    Decrypts Blowfish-encrypted bytes.
    Handles Base64-encoded content and/or keys.
    """
    key_bytes = key.encode("utf-8") if isinstance(key, str) else key

    # ── Step 1: Base64-decode the key if needed ──
    try:
        decoded_key = base64.b64decode(key_bytes)
        if 4 <= len(decoded_key) <= 56:
            key_bytes = decoded_key
            log.debug("  Key was Base64-encoded, decoded to %d bytes", len(key_bytes))
    except Exception:
        pass  # key was not Base64, use as-is

    # ── Step 2: Base64-decode the content if needed ──
    # The raw bytes may themselves be Base64 before encryption
    try:
        decoded_raw = base64.b64decode(raw)
        raw_to_try = decoded_raw
        log.debug("  Content appears Base64-encoded, decoded to %d bytes", len(raw_to_try))
    except Exception:
        raw_to_try = raw

    # ── Step 3: Try all combinations ──
    attempts = [
        (raw_to_try, key_bytes, "ECB + decoded content"),
        (raw,        key_bytes, "ECB + raw content"),
        (raw_to_try, key_bytes, "CBC + decoded content"),
        (raw,        key_bytes, "CBC + raw content"),
    ]

    for i, (content, kbytes, label) in enumerate(attempts):
        try:
            if i < 2:  # ECB attempts
                cipher    = Blowfish.new(kbytes, Blowfish.MODE_ECB)
                decrypted = unpad(cipher.decrypt(content), Blowfish.block_size)
            else:       # CBC attempts
                iv        = content[:8]
                cipher    = Blowfish.new(kbytes, Blowfish.MODE_CBC, iv)
                decrypted = unpad(cipher.decrypt(content[8:]), Blowfish.block_size)

            log.debug("  Decrypted via %s (%d → %d bytes)", label, len(content), len(decrypted))
            return decrypted
        except (ValueError, KeyError):
            continue

    raise ValueError("Blowfish decryption failed across all mode/encoding combinations")