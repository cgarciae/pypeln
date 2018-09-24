import os
from setuptools import setup, find_packages

install_requires = [

]

setup(
    name = "pypeln",
    version = "0.1.0",
    author = "Cristian Garcia",
    author_email = "cgarcia.e88@gmail.com",
    description = (""),
    license = "MIT",
    keywords = [],
    url = "https://github.com/cgarciae/pypeln",
   	packages = find_packages(),
    package_data={
        '': ['LICENCE', 'requirements.txt', 'README.md', 'CHANGELOG.md'],
    },
    include_package_data = True,
    install_requires = install_requires,
)
