set -e

coverage run --concurrency=multiprocessing -m pytest --hypothesis-profile=pypeln
coverage combine
coverage html --omit '.venv/*' --omit '*_test.py'
rm .coverage
rm .coverage.*