#!/usr/bin/python3.6
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
# 10/12/2020    v1.1.00     Initial Release - DEC
#
# -----------------------------------------------------------------------------------------------------------------------------------

import collections
import importlib
import inspect
import json
import re
import os
from datetime import datetime

class MultipathLib():

    def __init__(self,log,host,useapi='redhat'):
        self.log = log
        self.host = host
        self.multipath = useapi
        self.views = {}

    def redhat_multipathd(self,ssh_username,ssh_password):
        from .sshhost import RedhatMultipathd as RedhatMultipathd
        self.client = RedhatMultipathd(self,self.host,ssh_username,ssh_password)

    def exec_command(self,command='ls -ltr'):
        return self.client.exec_command(command)

    def getmultipathing(self):
        return self.client.getmultipathing()
    
    def multipathll(self):
        return self.client.multipathll()

    def devdiskbypath(self):
        return self.client.devdiskbypath()






