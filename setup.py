import setuptools
import os, stat

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='hiraid',
    version='1.0.05',
    description='Hitachi storage communication toolkit',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Darren Chambers',
    author_email='darren.chambers@hitachivantara.com',
    url='https://github.com/hv-ps/hiraid',
    packages=setuptools.find_packages(),
    install_requires=[ ],

    scripts = [
	'scripts/raid-get-ports.py',
	'scripts/raid-get-ports-cmrest.py',
	'scripts/raid-get-ports-cmrest-hidepwd.py',
	]
)
