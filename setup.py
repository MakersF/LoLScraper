#!/usr/bin/env python

import sys

from setuptools import setup, find_packages


install_requires = [
    "cassiopeia"
]

setup(
    name="lol_scraper",
    version="0.0.01",
    author="Francesco Zoffoli",
    author_email="makers.f.dev@gmail.com",
    url="https://github.com/MakersF/LoLScraper",
    description="A python script and library to download and store League of Legends matches with Riot API",
    long_description=open("README.md").read(),
    keywords=["LoL", "League of Legends", "Riot Games", "API", "REST"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Environment :: Web Environment",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Games/Entertainment",
        "Topic :: Games/Entertainment :: Real Time Strategy",
        "Topic :: Games/Entertainment :: Role-Playing",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    license="MIT",
    packages=find_packages(),
    zip_safe=True,
    install_requires=install_requires
)