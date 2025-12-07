import os
from pathlib import Path

from setuptools import find_packages, setup


def read_long_description():
    readme_path = Path(__file__).parent / "README.md"
    with readme_path.open(encoding="utf-8") as fh:
        return fh.read()


setup(
    name="daplug-sql",
    version=os.getenv("CIRCLE_TAG", "0.1.0"),
    url="https://github.com/dual/daplug-sql",
    author="Paul Cruse III",
    author_email="paulcruse3@gmail.com",
    description="Shared schema, merge, and SNS helpers powering daplug adapters.",
    long_description=read_long_description(),
    long_description_content_type="text/markdown",
    packages=find_packages(include=["daplug_sql", "daplug_sql.*"]),
    python_requires=">=3.10",
    install_requires=[
        "boto3>=1.42.4,<2; python_version >= '3.9'",
        "daplug-core>=1.0.0b5,<2; python_version >= '3.9'",
        "psycopg2-binary>=2.9.11,<3; python_version >= '3.9'",
        "mysql-connector-python>=9.5.0,<10; python_version >= '3.10'",
    ],
    keywords=[
        "daplug",
        "schema",
        "sns",
        "event-driven",
        "database",
        "adapter",
        "python-library",
    ],
    project_urls={
        "Homepage": "https://github.com/dual/daplug-sql",
        "Documentation": "https://github.com/dual/daplug-sql#readme",
        "Source Code": "https://github.com/dual/daplug-sql",
        "Bug Reports": "https://github.com/dual/daplug-sql/issues",
        "CI/CD": "https://circleci.com/gh/dual/daplug-sql",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
    ],
    license="Apache License 2.0",
    platforms=["any"],
    zip_safe=False,
)
