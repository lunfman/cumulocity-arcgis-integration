from setuptools import find_packages, setup

setup(
    name="cumo",
    packages=find_packages(exclude=["cumo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
