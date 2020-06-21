set -e

coverage run --concurrency=multiprocessing -m pytest
coverage combine
coverage html --omit '.venv/*' --omit '*_test.py'
