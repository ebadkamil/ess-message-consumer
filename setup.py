import os.path as osp
import re
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


def find_version():
    with open(osp.join('analysis', '__init__.py'), 'r') as f:
        match = re.search(r'^__version__ = "(\d+\.\d+\.\d+)"', f.read(), re.M)
        if match is not None:
            return match.group(1)
        raise RuntimeError("Unable to find version string.")


setup(name="ess-message-consumer",
      version=find_version(),
      author="Ebad Kamil",
      author_email="ebad.kamil@ess.eu",
      maintainer="Ebad Kamil",
      packages=find_packages(),
      entry_points={
          "console_scripts": [
              "start_consumer = ess_message_consumer.consumer:start_consumer",
        ],
      },
      install_requires=[
           'confluent_kafka >= 1.7.0',
      ],
      extras_require={
        'test': [
          'pytest',
        ]
      },
      python_requires='>=3.6',
)