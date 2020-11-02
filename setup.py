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
    'aiobotocore<1',
    'aiohttp<= 3.6.2',
    'aiohttp_cors',
    'aiofiles',
     #'botocore',
    'cryptography',
    'kubernetes',
    'numba',
    'numpy >= 1.10.4',
    'psutil',
    'pyjwt',
    'pytz',
    'requests',
    ]


setup(name='hsds',
      version='0.6.2',
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
      entry_points={'console_scripts': [
          'hsds = hsds.app:main',
          'hsds-datanode = hsds.datanode:main',
          'hsds-servicenode = hsds.servicenode:main',
          'hsds-headnode = hsds.headnode:main',
          ]}
)
