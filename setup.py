from setuptools import setup

setup(name='penquins', version='1.0.0', py_modules=['penquins'],
      install_requires=['numpy>=1.13.1',
                        'pymongo>=3.4.0',
                        'requests>=2.18.4']
      )

