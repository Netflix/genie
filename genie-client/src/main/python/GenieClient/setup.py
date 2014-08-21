from setuptools import setup

setup(
    name='GenieClient',
    version='0.1.1',
    author='Neflix Inc.',
    author_email='bigdataplatform@netflix.com',
    packages=['genie_client', 'genie_client.test','genie_client.apis','genie_client.models','genie_client.exception'],
    url='http://pypi.python.org/pypi/TowelStuff/',
    license='LICENSE.txt',
    description='Genie Python client api.',
    long_description=open('README.txt').read(),
    install_requires=[
        "python-dateutil >= 2.2",
    ],
)
