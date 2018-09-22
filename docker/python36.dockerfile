FROM python:3.6

RUN pip install pytest
RUN pip install hypothesis
RUN pip install cytoolz
RUN pip install pytest-sugar
RUN pip install aiohttp
RUN pip install python_path