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
        "mastermind.utils",
    ],
    package_dir={'mastermind': 'src/python-mastermind/src/mastermind',
                 'mastermind.query': 'src/python-mastermind/src/mastermind/query',
                 'mastermind.utils': 'src/python-mastermind/src/mastermind/utils',
                 },
    install_requires=["tornado >= 3.0", "cocaine < 0.12"],
)
