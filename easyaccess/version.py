"""easyaccess version"""

import logging
import warnings
from datetime import datetime
warnings.filterwarnings("ignore")

def last_pip_version():
    import requests
    logging.getLogger("requests").setLevel(logging.WARNING)
    """
    Return last available version of easyaccess from pypi
    """
    url = "https://pypi.python.org/pypi/%s/json" % ('easyaccess',)
    #data = json.load(urllib2.urlopen(urllib2.Request(url)))
    data = requests.get(url, verify=False).json()
    uploads = []
    for k in data['releases'].keys():
        up_time = data['releases'][k][0]['upload_time']
        uploads.append([k,datetime.strptime(up_time,'%Y-%m-%dT%H:%M:%S')])
    return sorted(uploads,key=lambda x: x[1])[-1][0]

version_tag = (1, 3, 2, 'dev-fd41ecc')
__version__ = '.'.join(map(str, version_tag[:3]))

if len(version_tag) > 3:
    __version__ = '%s-%s' % (__version__, version_tag[3])


