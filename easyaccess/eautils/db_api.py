from __future__ import print_function
"""
api client to access cutout services in DES for coadded images and Single Exposures
"""
__author__ = 'Matias Carrasco Kind'
import easyaccess.config_ea as config_mod
import os
import stat
import getpass
import requests
import time
#import easyaccess
from easyaccess.eautils.python_api import connect 

DESFILE = os.getenv("DES_SERVICES")
if not DESFILE:
    DESFILE = os.path.join(os.getenv("HOME"), ".desservices.ini")
if os.path.exists(DESFILE):
    AMODE = stat.S_IMODE(os.stat(DESFILE).st_mode)
    if AMODE != 2 ** 8 + 2 ** 7:
        print('Changing permissions to des_service file to read/write only by user')
        os.chmod(DESFILE, 2 ** 8 + 2 ** 7)


class Token(object):
    """
    Token object that keeps track of tockes created to make requests
    """

    def __init__(self, token, url):
        self.value = token
        self.url = url
        self._active = True

    def __repr__(self):
        return 'Token(token=%s, url=%s)' % (self.value, self.url)

    def __str__(self):
        return self.value

    def ttl(self):
        """
        Time-To-Live (ttl) prints out the time in seconds for the current token
        """
        req = self.url + '/api/token/?token=%s' % self.value
        temp = requests.get(req)
        if temp.json()['status'] == 'ok':
            self._active = True
        else:
            self._active = False
        print(temp.json()['message'])

    @property
    def active(self):
        """
        Checks whether the token is still active and valid
        """
        req = self.url + '/api/token/?token=%s' % self.value
        temp = requests.get(req)
        if temp.json()['status'] == 'ok':
            self._active = True
        else:
            self._active = False
        return self._active


class Job(object):
    def __init__(self, jobid, user, token, url):
        self._jobid = jobid
        self._user = user
        self._token = token
        self._url = url

    def __repr__(self):
        return 'Job(jobid=%s, user=%s, token=%s, url=%s)' % (self._jobid,
                                                             self._user, self._token, self._url)

    def __str__(self):
        return self._jobid

    @property
    def status(self):
        req = self._url + '/api/jobs/?token=%s&jobid=%s' % (self._token, self._jobid)
        temp = requests.get(req)
        self.req_status = temp.json()['status']
        self.message = temp.json()['message']
        if temp.json()['status'] == 'ok':
            self._status = temp.json()['job_status']
            if temp.json()['job_status'] == 'SUCCESS':
                self.links = temp.json()['links']
        else:
            self._status = 'Error'
        return self._status

    def __delete__(self):
        req = self._url + '/api/jobs/?token=%s&jobid=%s' % (self._token, self._jobid)
        temp = requests.delete(req)
        if temp.json()['status'] == 'ok':
            print('Job %s was deleted from the DB' % self._jobid)
        else:
            print(temp.text)

    def delete(self):
        return self.__delete__()


class MyJobs(object):

    def __init__(self, user=None, token=None, root_url=None, db='desoper', verbose=False):
        passwd = None
        self.desconf = config_mod.get_desconfig(DESFILE, db)
        self._db = db
        self.verbose = verbose
        self.jobid = None
        self.token = None
        self.submit = None
        self._status = None
        self.job = None
        self.links = None
        self.files = []
        if user is not None:
            if self.verbose:
                print('Bypassing .desservices file with user : %s' % user)
            if passwd is None:
                passwd = getpass.getpass(prompt='Enter password : ')
            self.desconf.set('db-' + self._db, 'user', user)
            self.desconf.set('db-' + self._db, 'passwd', passwd)
        self.user = self.desconf.get('db-' + self._db, 'user')
        self._passwd = self.desconf.get('db-' + self._db, 'passwd')
        self.root_url = root_url.strip('/')
        self.get_token()
        self.jobs_info = requests.get(
            self.root_url + '/api/jobs/?token=%s&list_jobs' % self.token).json()
        self._jobs = [Job(j, self.user, self.token, self.root_url)
                      for j in self.jobs_info['list_jobs']]
        self.creation_times = self.jobs_info['creation_time']
        self.job_types = self.jobs_info['job_type']

    def get_token(self):
        """Generate a new token using user and password in the API."""
        ext = '/api/token/'
        req = self.root_url + ext
        res = requests.post(req, data={'username': self.user, 'password': self._passwd})
        status = res.json()['status']
        if status == 'ok':
            self.token = Token(res.json()['token'], self.root_url)
        else:
            self.token = None

    def __len__(self):
        return len(self._jobs)

    def __repr__(self):
        return 'My Jobs (%d in total)' % len(self._jobs)

    def __getitem__(self, index):
        return self._jobs[index]

    def __delitem__(self, index):
        del self._jobs[index]
        return

    @property
    def list(self):
        return self._jobs


class DesCoaddCuts(object):
    """
    This Class handles the object for the cutouts

    Parameters:
    -----------

    user (optional)     : DB username
    passwd (optional)   : DB password
    root_url (optional) : The url for the cutouts API
    db (optional)       : DB to be used (default: desoper)
    verbose (optional)  : print extra information
    """

    def __init__(self, user=None, root_url=None, db='desoper', verbose=True):
        passwd = None
        self.desconf = config_mod.get_desconfig(DESFILE, db)
        self._db = db
        self.verbose = verbose
        self.jobid = None
        self.token = None
        self.submit = None
        self._status = None
        self.job = None
        self.links = None
        self.files = []
        if user is not None:
            if self.verbose:
                print('Bypassing .desservices file with user : %s' % user)
            if passwd is None:
                passwd = getpass.getpass(prompt='Enter password : ')
            self.desconf.set('db-' + self._db, 'user', user)
            self.desconf.set('db-' + self._db, 'passwd', passwd)
        self.user = self.desconf.get('db-' + self._db, 'user')
        self._passwd = self.desconf.get('db-' + self._db, 'passwd')
        self.root_url = root_url.strip('/')

    def get_token(self):
        """Generate a new token using user and password in the API."""
        ext = '/api/token/'
        req = self.root_url + ext
        res = requests.post(req, data={'username': self.user, 'password': self._passwd})
        status = res.json()['status']
        if status == 'ok':
            self.token = Token(res.json()['token'], self.root_url)
        else:
            self.token = None
        if self.verbose:
            print(res.json()['message'])
            return self.token.value

    def make_cuts(self, ra=None, dec=None, csvfile=None, xsize=None,
                  ysize=None, email=None, list_only=False, wait=False, timeout=3600):
        """
        Submit a job to generate the cuts on the server side, if wait keyword id
        True the functions waits until the job is completed
        """
        req = self.root_url + '/api/jobs/'
        self.body = {'token': self.token.value, 'list_only': 'false', 'job_type': 'coadd'}
        if ra is not None:
            try:
                self.body['ra'] = str(list(ra))
                self.body['dec'] = str(list(dec))
            except:
                self.body['ra'] = str(ra)
                self.body['dec'] = str(dec)
        if xsize is not None:
            try:
                self.body['xsize'] = str(list(xsize))
            except:
                self.body['xsize'] = str(xsize)
        if ysize is not None:
            try:
                self.body['ysize'] = str(list(ysize))
            except:
                self.body['ysize'] = str(ysize)
        if email is not None:
            self.body['email'] = email
        if list_only:
            self.body['list_only'] = 'true'
        if csvfile is not None:
            self.body['ra'] = '0,0'
            self.body['dec'] = '0,0'
            self.body_files = {'csvfile': open(csvfile, 'rb')}
            self.submit = requests.post(req, data=self.body, files=self.body_files)
        else:
            self.submit = requests.post(req, data=self.body)
        self._status = 'Submitted'
        if self.verbose:
            print(self.submit.json()['message'])
        if self.submit.json()['status'] == 'ok':
            self.job = Job(self.submit.json()['job'], self.user, self.token, self.root_url)
        elif self.submit.json()['status'] == 'error':
            self.job = None
            if not self.verbose:
                print(self.submit.json()['message'])
        else:
            assert False, self.submit.text
        if wait:
            t_init = time.time()
            if self.job is not None:
                for _ in range(100000):
                    if self.job.status == 'SUCCESS':
                        requests.get(self.root_url +
                                     '/api/refresh/?user={}&jid={}'.format(self.user, self.jobid))
                        self._status = self.job.req_status
                        break
                    if time.time() - t_init > timeout:
                        break
                    time.sleep(0.5)
                if self._status != 'ok':
                    print('Job is taking longer than expected, '
                          'will continue running but check status later')

    @property
    def status(self):
        """Return the status of the submited job (if any)."""
        if self.job is None:
            return 'No jobs has been submitted'
        else:
            try:
                status = self.job.status
                if status == 'SUCCESS':
                    self._status = 'ok'
                    requests.get(self.root_url +
                                 '/api/refresh/?user={}&jid={}'.format(self.user, self.jobid))
                return status
            except:
                self._status = 'Error!'
                return self.job.text

    def get_files(self, folder=None, print_only=False, force=True):
        """Copy all files generated to local folder."""
        if self.job.status == 'SUCCESS':
            self._status = 'ok'
        else:
            print('Job is not completed yet or there was an error, check job status')
            return
        if self._status == 'ok':
            self.files = []
            self.links = self.job.links
            if folder is not None:
                if not os.path.exists(folder):
                    os.mkdir(folder)
            else:
                folder = ''
            k = 0
            for link in self.links:
                if link.endswith('png') or link.endswith('fits'):
                    temp_file = os.path.join(folder, os.path.basename(link))
                    self.files.append(temp_file)
                    if print_only:
                        print(temp_file)
                    else:
                        if not force:
                            if os.path.exists(temp_file):
                                continue
                        req = requests.get(link, stream=True)
                        if req.status_code == 200:
                            with open(temp_file, 'wb') as temp_file:
                                for chunk in req:
                                    temp_file.write(chunk)
                            k += 1
            if self.verbose:
                print('%d files copied to local server' % k)
        else:
            print('Something went wrong with the job')

    def show_pngs(self, folder=None, limit=100):
        """Display all pngs generated after copying files in local directory."""
        from IPython.display import Image, display
        if folder is None:
            folder = ''
        displayed = 0
        for file_png in self.files:
            if file_png.endswith('.png'):
                if displayed == limit:
                    break
                temp_display = Image(filename=os.path.join(folder, file_png))
                display(temp_display)
                displayed += 1


class DesSingleCuts(DesCoaddCuts):
    # def __init__(self):
    #    super(DesSingleCuts, self).__init__()

    def make_cuts(self, ra=None, dec=None, csvfile=None, band=None, blacklist=True,
                  xsize=None, ysize=None, email=None, list_only=False, wait=False, timeout=3600):
        """
        Submit a job to generate the cuts on the server side, if wait keyword id
        True the functions waits until the job is completed
        """
        req = self.root_url + '/api/jobs/'
        self.body = {'token': self.token.value, 'list_only': 'false', 'job_type': 'single'}
        if ra is not None:
            try:
                self.body['ra'] = str(list(ra))
                self.body['dec'] = str(list(dec))
            except:
                self.body['ra'] = str(ra)
                self.body['dec'] = str(dec)
        if xsize is not None:
            try:
                self.body['xsize'] = str(list(xsize))
            except:
                self.body['xsize'] = str(xsize)
        if ysize is not None:
            try:
                self.body['ysize'] = str(list(ysize))
            except:
                self.body['ysize'] = str(ysize)
        if email is not None:
            self.body['email'] = email
        if list_only:
            self.body['list_only'] = 'true'
        if not blacklist:
            self.body['no_blacklist'] = 'true'
        if band is not None:
            self.body['band'] = str(list(band))
        if csvfile is not None:
            self.body['ra'] = '0,0'
            self.body['dec'] = '0,0'
            self.body_files = {'csvfile': open(csvfile, 'rb')}
            self.submit = requests.post(req, data=self.body, files=self.body_files)
        else:
            self.submit = requests.post(req, data=self.body)
        self._status = 'Submitted'
        if self.verbose:
            print(self.submit.json()['message'])
        if self.submit.json()['status'] == 'ok':
            self.job = Job(self.submit.json()['job'], self.user, self.token, self.root_url)
        elif self.submit.json()['status'] == 'error':
            self.job = None
            if not self.verbose:
                print(self.submit.json()['message'])
        else:
            assert False, self.submit.text
        if wait:
            t_init = time.time()
            if self.job is not None:
                for _ in range(100000):
                    if self.job.status == 'SUCCESS':
                        requests.get(self.root_url +
                                     '/api/refresh/?user={}&jid={}'.format(self.user, self.jobid))
                        self._status = self.job.req_status
                        break
                    if time.time() - t_init > timeout:
                        break
                    time.sleep(0.5)
                if self._status != 'ok':
                    print('Job is taking longer than expected,'
                          'will continue running but check status later')


class DesSingleExposure(object):
    """
    This Class handles the object for Single Exposures and individual CCDs

    Parameters:
    -----------

    user (optional)     : DB username
    passwd (optional)   : DB password
    root_url (optional) : The url for the cutouts API
    db (optional)       : DB to be used (default: desoper)
    verbose (optional)  : print extra information
    """

    def __init__(self, user=None,
                 root_url='https://desar2.cosmology.illinois.edu/DESFiles/desarchive/',
                 db='desoper', verbose=True):
        passwd = None
        self.desconf = config_mod.get_desconfig(DESFILE, db)
        self._db = db
        self.verbose = verbose
        self.links = []
        self.files = []
        if user is not None:
            if self.verbose:
                print('Bypassing .desservices file with user : %s' % user)
            if passwd is None:
                passwd = getpass.getpass(prompt='Enter password : ')
            self.desconf.set('db-' + self._db, 'user', user)
            self.desconf.set('db-' + self._db, 'passwd', passwd)
        self.user = self.desconf.get('db-' + self._db, 'user')
        self._passwd = self.desconf.get('db-' + self._db, 'passwd')
        self.root_url = root_url
        self.base_query = """
        SELECT
        file_archive_info.PATH || '/' || file_archive_info.FILENAME || file_archive_info.COMPRESSION
         as path,
        image.PFW_ATTEMPT_ID,
        image.BAND,
        image.CCDNUM,
        image.NITE,
        image.EXPNUM
        FROM
        ops_proctag, image, file_archive_info
        WHERE
        file_archive_info.FILENAME = image.FILENAME AND
        image.PFW_ATTEMPT_ID = ops_proctag.PFW_ATTEMPT_ID AND
        image.FILETYPE = 'red_immask' AND
        ops_proctag.TAG = '{tag}' AND
        image.EXPNUM = {expnum} AND image.CCDNUM in ({ccd});
        """

    def get_paths(self, expnum, ccd, tag='Y3A1_FINALCUT'):

        try:
            ccd = ','.join(map(str, ccd))
        except:
            pass
        inputs = dict(expnum=expnum, ccd=ccd, tag=tag)
        self.base_query = self.base_query.format(**inputs)
        print(self.base_query)
        con = connect(self._db, user=self.user, passwd=self._passwd)
        self.data = con.query_to_pandas(self.base_query)
        print(self.data)
        for j in range(len(self.data)):
            self.links.append(self.root_url + self.data.PATH.ix[j])

    def get_files(self, folder=None, print_only=False, force=True):
        """Copy all files to local folder."""

        if folder is not None:
            if not os.path.exists(folder):
                os.mkdir(folder)
        else:
            folder = ''
        k = 0
        for link in self.links:
            temp_file = os.path.join(folder, os.path.basename(link))
            self.files.append(temp_file)
            if print_only:
                print(temp_file)
            else:
                if not force:
                    if os.path.exists(temp_file):
                        continue
                req = requests.get(link, stream=True, auth=(self.user, self._passwd))
                if req.status_code == 200:
                    with open(temp_file, 'wb') as temp_file:
                        for chunk in req:
                            temp_file.write(chunk)
                    k += 1
        if self.verbose:
            print('%d files copied to local server' % k)
