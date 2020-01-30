import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dynampy",
    version="0.0.0",
    author="Harry Wei",
    author_email="pepsimixt@gmail.com",
    description="Dynamically organized Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/haochuanwei/dynampy",
    packages=setuptools.find_packages(),
    install_requires=[
        'deco>=0.5.2',
        'wasabi>=0.4.2',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
