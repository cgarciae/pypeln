# You can update the PY_VERSION to pick a python version
ARG PY_VERSION=3.8
FROM docker.io/python:${PY_VERSION}

WORKDIR /usr/src/app
COPY pyproject.toml poetry.lock /usr/src/app/
ENV PIP_DISABLE_PIP_VERSION_CHECK=on

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction

CMD ["pytest"]