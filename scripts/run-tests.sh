sudo chown -R $USER:$USER .
find . -name "*.pyc" -delete

docker-compose build

echo "PYTHON 2.7"
docker-compose run --rm python27 bash -c 'pip install -e . && py.test -k "not io"'


sudo chown -R $USER:$USER .
find . -name "*.pyc" -delete
