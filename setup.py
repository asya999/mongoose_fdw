###
### Author: David Wallin
### Time-stamp: <2015-03-04 23:04:21 dwa>

from setuptools import setup

if __name__ == '__main__':
    setup(name='mongoose_fdw',
          author='David Wallin',
          author_email='dwa@havanaclub.org',
          description='An experimental Postgres fdw for MongoDB',
          url='http://github.com/dwa/mongoose_fdw',
          version='0.0.1',
          install_requires=['pymongo>=2.8,<3.0',
                            'python-dateutil'],
          packages=['mongoose_fdw'],
          classifier=['Private :: Do Not Upload'])

## Local Variables: ***
## mode:python ***
## coding: utf-8 ***
## End: ***
