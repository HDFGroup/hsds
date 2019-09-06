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
from codecs import open
from os import path

setup(name='hsds',
      version='0.0.1',
      description='HDF REST API',
      url='http://github.com/HDFGroup/h5pyd',
      author='John Readey',
      author_email='jreadey@hdfgrouup.org',
      license='BSD',
      packages=['hsds', 'hsds.util'],
      # requires=['h5py (>=2.5.0)', 'h5json>=1.0.2'],
      install_requires=['numpy >= 1.10.4', 'requests', 'six', 'pytz'],
      setup_requires=['pkgconfig', 'six'],
      zip_safe=False,
      # not compatible
      # entry_points={'console_scripts':
      #     ['datanode = hsds.datanote:main',
      #      'servicenode = hsds.servicenode:main']
      # }
)
