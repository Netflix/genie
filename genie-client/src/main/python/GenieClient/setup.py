from distutils.core import setup

setup(
    name='GenieClient',
    version='0.1.0',
    author='Netflix Inc.',
    author_email='bigdataplatform@netflix.com',
    packages=['genie_client', 'genie_client.test', 'genie_client.apis', 'genie_client.models', 'genie_client.exception'],
    scripts=['bin/foo.py','bin/bar.py'],
    url='http://pypi.python.org/pypi/TowelStuff/',
    license='LICENSE.txt',
    description='Genie Python client api.',
    long_description=open('README.txt').read(),
    install_requires=[
        "python-dateutil >= 2.2",
    ],
)
