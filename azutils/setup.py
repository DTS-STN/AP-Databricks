from setuptools import find_packages, setup
import io

setup(
    name = "azutils",
    version = "0.1.0",
    author="Arpit Dhindsa",
    author_email="arpit.dhindsa@hrsdc-rhdcc.gc.ca",
    description="Azure Utilities that grant access to Azure Cloud Services",
    long_description=io.open("README.md", encoding="UTF-8").read(),
    url="https://github.com/DTS-STN/AP-Databricks/tree/add_python_library/azutils",
    packages = find_packages(),
    setup_requires = ["wheel"],
)
