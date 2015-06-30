from setuptools import setup

setup(
    name="mastermind",
    version="2.25",
    author="Andrey Vasilenkov",
    author_email="indigo@yandex-team.ru",
    url="https://github.com/yandex/mastermind",
    description="Common components and a client library for Mastermind",
    long_description="",
    license="LGPLv3+",
    packages=[
        "mastermind",
        "mastermind.query",
    ],
    package_dir={'mastermind': 'src/python-mastermind/src/mastermind',
                 'mastermind.query': 'src/python-mastermind/src/mastermind/query'},
    install_requires=["tornado >= 3.0", "cocaine < 0.12"],
)
