from setuptools import setup, find_packages

setup(
    name="taskflow",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "click>=8.0.0",
        "pyyaml>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "taskflow=taskflow.cli:cli",
        ],
    },
    author="TaskFlow Team",
    description="Мини-оркестратор задач — упрощённый аналог Airflow",
    python_requires=">=3.10",
)