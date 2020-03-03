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


install_requires = [
    'aiobotocore',
    'aiohttp',
    'botocore',
    'kubernetes',
    'numba',
    'numpy >= 1.10.4',
    'psutil',
    'pytz',
    'requests', # for tests and examples
    ]


setup(name='hsds',
      version='0.0.1',
      description='HDF REST API',
      url='http://github.com/HDFGroup/hsds',
      author='John Readey',
      author_email='jreadey@hdfgrouup.org',
      license='Apache',
      packages=['hsds', 'hsds.util'],
      install_requires=install_requires,
      setup_requires=['setuptools'],
      extras_require={'azure': ['azure']},
      zip_safe=False,
      # not compatible
      # entry_points={'console_scripts':
      #     ['datanode = hsds.datanote:main',
      #      'servicenode = hsds.servicenode:main']
      # }
)
