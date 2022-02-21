"""
Package setup.
"""

import os
import re
import site
import sys

from setuptools import find_packages, setup
import toml

site.ENABLE_USER_SITE = "--user" in sys.argv[1:]

VERSION = os.environ.get("VERSION", "1.0.0")
HERE = os.path.abspath(os.path.dirname(__file__))

config = toml.load("pyproject.toml")
INSTALL_REQUIRES = []
_re = re.compile(r"^[0-9]")
for p, v in config["tool"]["poetry"]["dependencies"].items():
    p = p.strip("'")
    if p == "python":
        continue
    v = v.strip("'")
    if _re.match(v):
        INSTALL_REQUIRES.append(f"{p}=={v}")
    else:
        INSTALL_REQUIRES.append(p + v)


def long_description() -> str:
    """
    Get the long description from the readme.
    """
    try:
        with open("README.md", encoding="utf-8") as readme_file:
            return readme_file.read()
    except FileNotFoundError:
        return ""


setup(
    name="shifter-pandas",
    version=VERSION,
    description="Convert some data into Panda DataFrames",
    long_description=long_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Typing :: Typed",
    ],
    keywords="shifter pandas",
    author="Stéphane Brunner",
    author_email="stephane.brunner@gmail.com",
    url="https://github.com/sbrunner/shifter-pandas",
    packages=find_packages(exclude=["tests", "docs"]),
    install_requires=INSTALL_REQUIRES,
    package_data={"shifter_pandas": ["py.typed", "*.json"]},
)
