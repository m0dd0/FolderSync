import setuptools

setuptools.setup(
    name="folder_sync",
    version="0.0.1",
    author="Moritz Hesche",
    author_email="mo.hesche@gmail.com",
    # description="",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    # url="",
    classifiers=["Programming Language :: Python :: 3"],
    packages=setuptools.find_packages(),
    python_requires="~=3.9",
    # not specifying versions might result in pip downloading multiple versions
    # of a package in order to solve dependencies
    # therfore it might be useful to fix the versions someday
    install_requires=[],
    extras_require={"dev": ["black", "pylint", "jupyter"]},
    include_package_data=True,
    use_scm_version=True,
)
