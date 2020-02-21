from setuptools import setup


setup(
      name='aapy',
      version='0.8',
      description='Python client for the EPICS Archiver Appliance',
      license='Apache License 2.0',
      packages=['aa'],
      install_requires=[
          'numpy<=1.16',
          'protobuf',
          'pytz',
          'requests',
          'tzlocal',
      ]
     )
