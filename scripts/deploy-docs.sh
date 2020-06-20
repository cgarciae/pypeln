cp README.md docs/index.md
python scripts/generate_docs.py
mkdocs build
mkdocs gh-deploy