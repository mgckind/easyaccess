#Config file
import ConfigParser

configcomment="""#
# Easyaccess default parameters
# 
# database  : Default is dessci, change to desoper, destest and others
#             Make sure the db-"database" section is in the .desservices.ini
# editor    : Deafault editor to open from inside easyaccess if $EDITOR is not set
# prefetch  : Prefetch number of rows to get from oracle (not the number of total rows)
#           : This determine the number of trips to the DB to get all results from query
# histcache : The number of line in the history cache (when possible)
# timeout   : The time in seconds before closing a connection for a query to print on screen
#             If the results are redirected to a file there is not a timeout
"""

def get_config(configfile):
    """
    Loads config file or create one if not
    Returns a configParser object
    """
    config = ConfigParser.ConfigParser()
    configwrite = False
    check=config.read(configfile)
    if check == []:
        configwrite= True
        print '\nError in config file, creating a new one...\n'

    if not config.has_section('easyaccess'):
        configwrite = True
        config.add_section('easyaccess')
    if not config.has_option('easyaccess','database'): configwrite = True ;config.set('easyaccess','database','dessci')
    if not config.has_option('easyaccess','editor'): configwrite = True ;config.set('easyaccess','editor','nano')
    if not config.has_option('easyaccess','prefetch'): configwrite = True ;config.set('easyaccess','prefetch',10000)
    if not config.has_option('easyaccess','histcache'): configwrite = True ;config.set('easyaccess','histcache',5000)
    if not config.has_option('easyaccess','timeout'): configwrite = True ;config.set('easyaccess','timeout',900)
    
    if not config.has_section('display'):
        configwrite=True
        config.add_section('display')
    if not config.has_option('display','max_rows'): configwrite = True ;config.set('display','max_rows',1500)
    if not config.has_option('display','width'): configwrite = True ;config.set('display','width',1000)
    if not config.has_option('display','max_columns'): configwrite = True ;config.set('display','max_columns',50)
    
    check = True
    if configwrite == True:
        check=write_config(configfile, config)
        config.read(configfile)
    if check:
        print config.getint('display','max_rows')
        return config


def write_config(configfile, config_ob):
    """
    Writes configuration file
    """
    try:
        F=open(configfile,'w')
        F.write(configcomment+'\n')
        config_ob.write(F)
        F.flush()
        F.close()
        return True
    except:
        print "Problems writing the configuration  file %s" % configfile
        return False


