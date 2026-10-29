from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="pubsubx",
    version="0.2.0",
    author="xxparthparekhxx",
    author_email="xx.parthparekh.xx@gmail.com",
    description="A publish-subscribe messaging system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/xxparthparekhxx/pubsubx",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "pubsubx=pubsubx.__main__:main",
        ],
    },
)
