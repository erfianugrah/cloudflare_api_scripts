from setuptools import setup, find_packages

setup(
    name="cloudflare-analytics",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[
        'pandas>=2.0.0',
        'numpy>=1.24.0',
        'requests>=2.28.0',
        'matplotlib>=3.7.0',
        'seaborn>=0.12.0',
        'python-dotenv>=1.0.0',
        'typing-extensions>=4.5.0',
        'plotly>=5.13.0',
        'scipy>=1.10.0',
        'prettytable>=3.0.0',  # For formatted table output
        'concurrent-log-handler>=0.9.20',  # For better logging with concurrency
    ],
    entry_points={
        'console_scripts': [
            'cloudflare-analytics=src.main:main',
        ],
    },
    python_requires='>=3.8',
    author="Erfi Anugrah",
    author_email="",
    description="Advanced analytics tool for analyzing Cloudflare zone performance metrics",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/erfianugrah/cloudflare_api_scripts",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: System :: Monitoring",
    ],
    project_urls={
        "Bug Reports": "https://github.com/erfianugrah/cloudflare_api_scripts/issues",
        "Source": "https://github.com/erfianugrah/cloudflare_api_scripts",
    },
)
