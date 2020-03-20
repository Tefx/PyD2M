from setuptools import setup, find_packages

setup(
    name="pyd2m",
    version=1.0,
    author="Tefx",
    author_email="zhaomeng.zhu@gmail.com",
    url="https://github.com/Tefx/PyD2M",
    project_urls={
        "Source Code": "https://github.com/Tefx/PyD2M",
        },
    packages=find_packages(),
    install_requires=["pandas>=1", "PyYAML"],
    extras_require={
        "all": ["numpy", "msgpack>=1", "pyarrow"]
        }
    )
