from setuptools import setup

setup(
    name='nflx-genie-client',
    version='0.1.7',
    author='Netflix Inc.',
    author_email='BigDataPlatform@netflix.com',
    packages=['genie2', 'genie2.client', 'genie2.exception', 'genie2.model'],
    scripts=[],
    url='http://netflix.github.io/genie/',
    license='LICENSE.txt',
    description='Genie Python client api.',
    long_description=open('README.txt').read(),
    install_requires=[
        "python-dateutil >= 2.2",
    ]
)
