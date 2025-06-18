#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-bigquery",
    version="0.1.0",
    description="Singer.io tap for extracting data from BigQuery tables",
    author="FIXD Automotive, Inc",
    url="https://github.com/fixdauto/tap-bigquery",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_bigquery"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.25.1",
        "google-cloud-bigquery==2.34.4",
        "pandas==1.3.5",
        "psutil==7.0.0",
        "pyarrow==17.0.0"
    ],
    entry_points="""
    [console_scripts]
    tap-bigquery=tap_bigquery:main
    """,
    packages=["tap_bigquery"],
    include_package_data=True,
)
