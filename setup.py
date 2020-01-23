from setuptools import setup

setup(name='penquins', version='1.0.3', py_modules=['penquins'],
      install_requires=['pymongo>=3.4.0',
                        'pytest>=3.3.0',
                        'requests>=2.18.4']
      )

