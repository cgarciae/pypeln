FROM python:2.7

RUN pip install pytest
RUN pip install hypothesis
RUN pip install cytoolz
RUN pip install pytest-sugar
RUN pip install python_path
RUN pip install wrapt