from setuptools import setup, find_packages

setup(
    name='netimpact',
    version='0.2.2',
    packages=find_packages(),
    install_requires=[
        'Click',
        'requests',
        'toml',
        'boto3',
        's3fs',
        'pandas',
        'pyarrow'
    ],
    entry_points='''
        [console_scripts]
        netimpact=netimpact.netimpact:cli
    ''',
    description='Cross affiliate network data and partner tools',
    url='http://github.com/jalexspringer/netimpact',
    author='Alex Springer',
    author_email='alexspringer@pm.me',
    license='MIT',
)