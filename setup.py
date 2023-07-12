from setuptools import setup

# run:
#   setup.py install
# or (if you'll be modifying the package):
#   setup.py develop
# To use a consistent encoding
# To upload to PyPI:
# twine upload dist/*
#
# Tag the release in github!
#

classifiers = [
    "Environment :: Console",
    "Intended Audience :: Information Technology",
    "Intended Audience :: Science/Research",
    "License :: OSI Approved :: Apache Software License",
    "Natural Language :: English",
    "Operating System :: POSIX :: Linux",
    "Programming Language :: Python :: 3",
    "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
    "Topic :: Scientific/Engineering",
]

#
# The specific versions given below are not desirable,
# but there are needed given aiobotocore and botocore
# requirements (e.g. see: https://github.com/boto/botocore/issues/2926#issuecomment-1535456104)
# as well as matching with packages present
# in the Docker build base image
#
install_requires = [
    "aiohttp==3.8.4",
    "aiobotocore==2.5.0",
    "aiohttp_cors",
    "aiofiles",
    "botocore",
    "cryptography",
    "numcodecs",
    "numpy",
    "psutil",
    "pyjwt",
    "pytz",
    "pyyaml",
    "requests-unixsocket",
    "simplejson",
]


setup(
    name="hsds",
    version="0.8.0",
    description="HDF REST API",
    url="http://github.com/HDFGroup/hsds",
    author="John Readey",
    author_email="jreadey@hdfgrouup.org",
    license="Apache",
    packages=["hsds", "hsds.util", "admin"],
    install_requires=install_requires,
    setup_requires=["setuptools"],
    extras_require={"azure": ["azure-storage-blob"]},
    zip_safe=False,
    classifiers=classifiers,
    include_package_data=True,
    data_files=[
        (
            "admin",
            [
                "admin/config/config.yml",
            ],
        )
    ],
    entry_points={
        "console_scripts": [
            "hsds = hsds.app:main",
            "hsds-datanode = hsds.datanode:main",
            "hsds-servicenode = hsds.servicenode:main",
            "hsds-headnode = hsds.headnode:main",
            "hsds-rangeget = hsds.rangeget_proxy:main",
            "hsds-node = hsds.node_runner:main",
            "hsds-chunklocator = hsds.chunklocator:main"
        ]
    },
)
