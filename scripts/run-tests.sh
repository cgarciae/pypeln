bash scripts/clean.sh

# docker-compose build

echo "PYTHON 2.7"
P27_FILES=($(find tests/* | grep -v io | grep -v __pycache__ | grep '\.py'))
COMMAND="pip install -q -e . && py.test ${P27_FILES[@]}"
docker-compose run --rm python27 bash -c "$COMMAND"


bash scripts/clean.sh


echo -e "\n\nPYTHON 3.6"
docker-compose run --rm python36 bash -c 'pip install -q -e . && py.test'

bash scripts/clean.sh
