from setuptools import setup, find_packages

setup(
    name="manacore",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "duckdb>=1.3.2"
    ],
    python_requires=">=3.9.12",
    description="ManaCore data pipeline package",
    author="Dr. Guy Schnidrig",
    author_email="schnidrig.guy@gmail.com",
)
