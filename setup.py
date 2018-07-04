import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="amfi_get_data",
    version="0.0.1",
    author="milan.m.shenai",
    author_email="shenaimm@gmail.com",
    description="Data scraper to get indian mutual fund data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shenaimm/mutual_fund_data_api",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
from data_fetch_functions import *