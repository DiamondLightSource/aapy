from setuptools import setup


setup(
      name='aapy',
      version='0.0.1',
      description='Python client for the EPICS Archiver Appliance',
      license='Apache License 2.0',
      packages=['aa'],
      install_requires=[
          'pytz',
          'numpy',
          'protobuf'
      ]
     )
