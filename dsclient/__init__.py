from pkg_resources import get_distribution
from . client import Client
from . schema import Schema

__version__ = get_distribution('gcp-dsclient').version
