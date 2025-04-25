from setuptools import setup, find_packages

setup(
    name="youtube-crawl",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "google-api-python-client==2.108.0",
        "pymongo==4.6.1",
        "requests==2.31.0",
        "pandas==2.1.4",
        "python-dotenv==1.0.0"
    ],
) 