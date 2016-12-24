import os
import sys

from setuptools import setup
from setuptools import find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

install_requires = [
    'pandas>=0.18',
    'google-api-python-client>=1.5',
    'google-cloud-datastore>=0.22',
    'httplib2>=0.9.1',
    'oauth2client>=2.0.1',
    'ipyparallel>=5.1',
]

setup(
    name="gcp-dsclient",
    version="0.0.1",
    description="Simple Python client library for interactive data science with Google Cloud Platform",
    long_description=README,
    author="Yoichi NAGAI",
    url="http://github.com/orfeon/gcp-python-dsclient",
    install_requires=install_requires,
    packages=find_packages(),
    package_data={},
    license="Apache 2.0",
    keywords=["google api client","data science","pandas"],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Framework :: IPython',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
    ],
)
