#!/usr/bin/env python

# Copyright 2022 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

"""The setup script."""

from setuptools import find_namespace_packages, find_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

requirements = [
    "networkx>=2.6,<3.0",
    "openpyxl>=3.0.3,<4.0",
    "pandas>=1.3.4,<2.0",
    "sqlalchemy>=1.4,<2.0",
    "toml>=0.10,<0.11",
    "taipy-config>=2.0,<2.1",
]

test_requirements = ["pytest>=3.8"]

extras_require = {
    "mssql": ["pyodbc>=4,<4.1"],
}

setup(
    author="Avaiga",
    author_email="dev@taipy.io",
    python_requires=">=3.8",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    description="A Python library to build powerful and customized data-driven back-end applications.",
    install_requires=requirements,
    long_description=readme,
    long_description_content_type="text/markdown",
    license="Apache License 2.0",
    keywords="taipy-core",
    name="taipy-core",
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src") + find_packages(include=["taipy", "taipy.core", "taipy.core.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/avaiga/taipy-core",
    version="2.0.4",
    zip_safe=False,
    extras_require=extras_require,
)
