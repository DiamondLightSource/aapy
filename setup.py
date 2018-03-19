from setuptools import setup


setup(
      name='aapy',
      version='0.2',
      description='Python client for the EPICS Archiver Appliance',
      license='Apache License 2.0',
      packages=['aa'],
      install_requires=[
          'numpy',
          'protobuf',
          'pytz',
          'requests'
      ]
     )
