import os.path as osp
import re
import sys

from setuptools import find_packages, setup


def find_version():
    with open(osp.join("ess_message_consumer", "__init__.py"), "r") as f:
        match = re.search(r'^__version__ = "(\d+\.\d+\.\d+)"', f.read(), re.M)
        if match is not None:
            return match.group(1)
        raise RuntimeError("Unable to find version string.")


def get_readme_content():
    basedir = osp.dirname(osp.realpath(sys.argv[0]))
    with open(osp.join(basedir, "README.md"), "r") as f:
        content = f.read()
    return content


setup(
    name="ess-message-consumer",
    version=find_version(),
    author="Ebad Kamil",
    author_email="ebad.kamil@ess.eu",
    maintainer="Ebad Kamil",
    description="Kafka consumer to handle ESS flatbuffer messages and provide a rich display on console.",
    long_description=get_readme_content(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "start_consumer = ess_message_consumer.application:start_application",
            "start_producer = ess_message_consumer.producer:start_producer",
            "list_available_topics = ess_message_consumer.application:list_available_topics",
        ],
    },
    install_requires=[
        "confluent_kafka >= 1.7.0",
        "ess-streaming_data_types>=0.10.0",
        "rich>=10.6.0",
        "black==20.8b1",
        "flake8==3.8.4",
        "isort==5.7.0",
    ],
    extras_require={
        "test": [
            "pytest",
        ]
    },
    python_requires=">=3.6",
)
