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
        "singer-python>=5.0.12",
        "requests",
        "google-cloud-bigquery>=1.26.1"
    ],
    entry_points="""
    [console_scripts]
    tap-bigquery=tap_bigquery:main
    """,
    packages=["tap_bigquery"],
    include_package_data=True,
)
