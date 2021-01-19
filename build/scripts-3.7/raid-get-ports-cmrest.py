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
# 22/12/2020    v1.1.00     raid-get-ports-cmrest.py example script
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

    parser = hvutil.NewArgParser(description='Index storage array')

    parser.add_argument("-s", "--serial", help="Storage serial number", required=True)
    parser.add_argument("-c", "--cmresthost", help="cmrest host", required=True)
    parser.add_argument("-d", "--storagedeviceid", help="storagedeviceid e.g. 800000050147", required=True)
    parser.add_argument("-u", "--userid", help="cmrest userID", required=True)
    parser.add_argument("-p", "--password", help="Better to omit password and enter at prompt", required=False, default=False)

    args = parser.parse_args()

    undofile = '{}.{}'.format(scriptname,ts)
    logname = scriptname+"_"+ts
    log = hvutil.configlog(scriptname,scriptconf.logdir,logname)


    log.info('-- Start --')
    log.info('Hostname: {}'.format(socket.gethostname()))
    log.info("Serial: "+args.serial)
    log.info("CMRest host: "+args.cmresthost)
    log.info("StorageDeviceid: "+args.storagedeviceid)


    #import pwdstore
    #connections = pwdstore.Connections()
    #print(json.dumps(connections.getconnection(category='cmrest',name='scinf-cmrest'),indent=4))

    password = (getpass(),args.password)[args.password]

    # Establish storage object
    cmraid = storage(args.serial,log)

    # Set cmrest as storage api
    cmraid.cmrest('http',args.cmresthost,23450,args.storagedeviceid,userid=args.userid,password=password)
    log.info(json.dumps(cmraid.getport(),indent=4))

if __name__ == "__main__":
    main()


