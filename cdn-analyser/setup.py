# setup.py
from setuptools import setup, find_packages

setup(
    name="cloudflare-analytics",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pandas>=2.0.0',
        'numpy>=1.24.0',
        'requests>=2.28.0',
        'matplotlib>=3.7.0',
        'seaborn>=0.12.0',
        'python-dotenv>=1.0.0',
        'typing-extensions>=4.5.0',
    ],
    entry_points={
        'console_scripts': [
            'cloudflare-analytics=src.main:main',
        ],
    },
)

# src/__init__.py
# Empty file to mark the directory as a Python package

# pyproject.toml
[build-system]
requires = ["setuptools>=45", "wheel"]
build-backend = "setuptools.build_meta"

[tool.black]
line-length = 100
target-version = ['py38']
include = '\.pyi?$'

[tool.isort]
profile = "black"
multi_line_output = 3
