"""easyaccess version"""

import json
import urllib2

def last_pip_version():
    """
    Return last available version of easyaccess from pypi
    """
    url = "https://pypi.python.org/pypi/%s/json" % ('easyaccess',)
    data = json.load(urllib2.urlopen(urllib2.Request(url)))
    versions = data["releases"].keys()
    versions.sort()
    return versions[-1]

version_tag = (1, 2, 1, 'rc4')
__version__ = '.'.join(map(str, version_tag[:3]))

if len(version_tag) > 3:
    __version__ = '%s-%s' % (__version__, version_tag[3])


