from setuptools import setup, find_packages

setup(
    name="flow",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click>=8.0.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "flow=flow.cli:cli",
        ],
    },
    author="Flow Team",
    description="simple task orchestrator for running commands with dependencies",
    python_requires=">=3.10",
)