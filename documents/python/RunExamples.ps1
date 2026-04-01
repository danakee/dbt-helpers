# Approved version only (default)
python document_materializer.py 138 C:\DocOutput

# All versions, version number in filename
python document_materializer.py 138 C:\DocOutput --strategy all_suffix

# All versions, each in its own subfolder
python document_materializer.py 138 C:\DocOutput --strategy all_subfolders

# Specific version — ignores strategy, always uses all_suffix for clarity
python document_materializer.py 138 C:\DocOutput --version 2

# Works with large decimal IDs too
python document_materializer.py 100001426414828928869433444387258660160 C:\DocOutput --version 1