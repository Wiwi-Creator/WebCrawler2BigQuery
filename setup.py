from setuptools import setup, find_packages

setup(
    name='bqcrawler',
    version='1.0.0',
    author='WiwiKuo',
    entry_points={
        'console_scripts': ['bqcrawler=bqcrawler.cli.main:bqcrawler'],
    },
    install_requires=[
        'setuptools',
        'google-cloud-bigquery[bqstorage,pandas]',
        'pytz == 2021.3'
    ],
    packages=find_packages(),
    python_requires='>=3.6, <3.9'
)
