import os
from distutils.core import setup
from setuptools import find_packages
from slingdata.io import cli

version = cli('--version', return_output=True).strip().replace('Version: ', '')

setup(
  name='slingdata',
  version=version,
  description='Slings data from a data source to a data target',
  author='Fritz Larco',
  author_email='flarco@live.com',
  url='https://github.com/flarco/sling',
  download_url='https://github.com/flarco/sling/archive/master.zip',
  keywords=['sling', 'etl', 'elt', 'extract', 'load', 'slingdata'],
  packages=find_packages(exclude=['tests']),
  long_description=open(os.path.join(os.path.dirname(__file__),
                                     'README.rst')).read(),
  include_package_data=True,
  install_requires=[],
  extras_require={},
  entry_points={
    'console_scripts': ['sling=slingdata.io:cli',],
  },
  classifiers=[
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: MacOS', 'Operating System :: Unix',
    'Topic :: Utilities'
  ])
