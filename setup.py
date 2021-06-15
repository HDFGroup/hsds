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
    'Environment :: Console',
    'Intended Audience :: Information Technology',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: Apache Software License',
    'Natural Language :: English',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python :: 3',
    'Topic :: Internet :: WWW/HTTP :: HTTP Servers',
    'Topic :: Scientific/Engineering',
    ]


install_requires = [
    # 'urllib3 <= 1.25.11',  # required by botocore
    'aiohttp <= 3.7.5',
    'aiobotocore',
    'aiohttp_cors',
    'aiofiles',
    'chardet <= 3.0.4',
    'cryptography',
    'kubernetes',
    'numba',
    'numpy',
    'psutil',
    'pyjwt',
    'pytz',
    'requests-unixsocket'
    ]


setup(name='hsds',
      version='0.7.0',
      description='HDF REST API',
      url='http://github.com/HDFGroup/hsds',
      author='John Readey',
      author_email='jreadey@hdfgrouup.org',
      license='Apache',
      packages=['hsds', 'hsds.util'],
      install_requires=install_requires,
      setup_requires=['setuptools'],
      extras_require={'azure': ['azure', 'azure-storage-blob']},
      zip_safe=False,
      classifiers=classifiers,
      data_files = [
        ('./config', ['admin/config/config.yml',]),
      ],
      entry_points={'console_scripts': [
          'hsds = hsds.app:main',
          'hsds-datanode = hsds.datanode:main',
          'hsds-servicenode = hsds.servicenode:main',
          'hsds-headnode = hsds.headnode:main',
          'hsds-rangeget = hsds.rangeget_proxy:main', 
          ]}
)
