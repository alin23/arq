import types
from importlib.machinery import SourceFileLoader
from pathlib import Path

from setuptools import setup


readme = Path(__file__).parent.joinpath("README.rst")
if readme.exists():
    with readme.open() as f:
        long_description = f.read()
else:
    long_description = "-"
# avoid loading the package before requirements are installed:
loader = SourceFileLoader("version", "arq/version.py")
version = types.ModuleType(loader.name)
loader.exec_module(version)


setup(
    name="arq",
    version=str(version.VERSION),  # pylint: disable=no-member
    description="Job queues in python with asyncio, redis and msgpack.",
    long_description=long_description,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Clustering",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Systems Administration",
    ],
    python_requires=">=3.6",
    author="Samuel Colvin",
    author_email="s@muelcolvin.com",
    url="https://github.com/samuelcolvin/arq",
    license="MIT",
    packages=["arq"],
    zip_safe=True,
    entry_points="""
        [console_scripts]
        arq=arq.cli:cli
    """,
    install_requires=[
        "async-timeout>=1.2.1",
        "aioredis>=1.0",
        "click>=6.7",
        "msgpack-python>=0.4.8",
    ],
    extras_require={"testing": ["pytest>=3.1.0"]},
)
