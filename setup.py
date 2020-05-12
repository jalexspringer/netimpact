from setuptools import setup, find_packages

setup(
    name='netimpact',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'Click',
        'requests',
        'toml'
    ],
    entry_points='''
        [console_scripts]
        netimpact=netimpact.netimpact:cli
    ''',
)