from setuptools import setup, find_packages

__version__ = '6.2.10'


setup(name='seafobj',
      version=__version__,
      license='BSD',
      description='python library for accessing seafile data model',
      author='Shuai Lin',
      author_email='linshuai2012@gmail.com',
      url='http://seafile.com',
      platforms=['Any'],
      packages=find_packages(exclude=["test.*", "test"]),
      classifiers=['Development Status :: 4 - Beta',
                   'License :: OSI Approved :: BSD License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python'])
