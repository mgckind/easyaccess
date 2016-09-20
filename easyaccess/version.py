"""easyaccess version"""

import logging
import warnings
warnings.filterwarnings("ignore")

#def last_pip_version():
#    import requests
#    logging.getLogger("requests").setLevel(logging.WARNING)
#    """
#    Return last available version of easyaccess from pypi
#    """
#    url = "https://pypi.python.org/pypi/%s/json" % ('easyaccess',)
#    #data = json.load(urllib2.urlopen(urllib2.Request(url)))
#    data = requests.get(url).json()
#    versions = list(data["releases"].keys())
#    versions.sort()
#    return versions[-1]

version_tag = (1, 3, 2, 'dev-d72c700')
__version__ = '.'.join(map(str, version_tag[:3]))

if len(version_tag) > 3:
    __version__ = '%s-%s' % (__version__, version_tag[3])


