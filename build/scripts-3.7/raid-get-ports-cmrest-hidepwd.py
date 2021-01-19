#!python
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.00
# -----------------------------------------------------------------------------------------------------------------------------------
#
# License Terms
# -------------
# Unless stated otherwise, Hitachi Vantara Limited and/or its group companies is/are the owner or the licensee
# of all intellectual property rights in this script. This work is protected by copyright laws and treaties around
# the world. This script is solely for use by Hitachi Vantara Limited and/or its group companies in the provision
# of services to you by Hitachi Vantara Limited and/or its group companies and, as a condition of your receiving
# such services, you expressly agree not to use, reproduce, duplicate, copy, sell, resell or exploit for any purposes,
# commercial or otherwise, this script or any portion of this script. All of Hitachi Vantara Limited and/or its
# group companies rights are reserved.
#
# -----------------------------------------------------------------------------------------------------------------------------------
# Changes:
#
# 22/12/2020    v1.1.00     raid-get-ports-cmrest-hidepwd.py example script
#
# -----------------------------------------------------------------------------------------------------------------------------------
# About:
#   This script demonstrates a method for utilizing an encrypted password stored using pwdstore.
#
# Directions:
#   1/ Install pwdstore either by downloading:
#   https://github.com/hv-ps/Pwdstore/raw/main/dist/pwdstore-1.0.1.tar.gz
#   And installing pip3 install pwdstore-1.0.1.tar.gz
#   OR
#   By installing directly from github:
#   pip3 install git+https://github.com/hv-ps/Pwdstore.git
#
#   2/ Configure your connection.
#   Using category 'cmrest' and the serial number of the storage array as the connection name can work well but there are
#   a few ways to craft this.
#   # pwdstore add -n 350147 -c cmrest -i scinf-cmrest.spsc.hds.com -r 23450 -t http -u cmrest
#
# -----------------------------------------------------------------------------------------------------------------------------------
import os
import socket
import argparse, sys
import json
import time
import collections
import re
from getpass import getpass
from datetime import datetime
from pprint import pprint
from hiraid.raidlib import Storage as storage
from hiraid import hvutil
from hiraid.storageexception import StorageException
from hiraid.scriptconf import Scriptconf as scriptconf

def main():

    ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
    scriptname = os.path.basename(__file__)

    parser = hvutil.NewArgParser(description='hiraid example script')
    parser.add_argument("-s", "--serial", help="Storage serial number", required=True)
    parser.add_argument("-d", "--storagedeviceid", help="storagedeviceid e.g. 800000050147", required=True)
    args = parser.parse_args()

    undofile = '{}.{}'.format(scriptname,ts)
    logname = scriptname+"_"+ts
    log = hvutil.configlog(scriptname,scriptconf.logdir,logname)

    log.info('-- Start --')
    log.info('Hostname: {}'.format(socket.gethostname()))
    log.info("Serial: "+args.serial)
    log.info("StorageDeviceid: "+args.storagedeviceid)

    try:
        import pwdstore
    except Exception as e:
        log.error("Unable to import pwdstore: "+str(e))
        sys.exit(1)
    webserviceconnections = pwdstore.Connections()
    connection = webserviceconnections.getconnection(category='cmrest',name=args.serial)
    # Establish storage object
    cmraid = storage(args.serial,log)
    # Set cmrest as storage api
    cmraid.cmrest('http',connection['ipAddress'],23450,args.storagedeviceid,userid=connection['userID'],password=connection['password'])
    # Print getport to the log
    log.info(json.dumps(cmraid.getport(),indent=4))
    
if __name__ == "__main__":
    main()


