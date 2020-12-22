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
# 14/01/2020    v1.1.00     Initial Release
#
# -----------------------------------------------------------------------------------------------------------------------------------

from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint,pformat
import base64
from . import cmrestparser as cmrestparse
import json

class cmrest:
    def __init__(self,serial,log,protocol="",resthost="",port="",storagedeviceid="",username="CMREST",password="hds123"):
        auth = '{}:{}'.format(username,password)
        basicauth = base64.b64encode(auth.encode("ascii"))
        configuration = swagger_client.Configuration()
        configuration.host = protocol+"://"+resthost+":"+str(port)+"/ConfigurationManager"
        #pprint(vars(configuration))
        #configuration._Configuration__debug = True
        #configuration._Configuration__logger_file = "/tmp/swagger.log"
        #configuration.logger_file = "/tmp/swagger.log"
        #configuration.debug = True
        #pprint(vars(configuration))
        #pprint(vars(self.apiinstance))
        configuration.api_key['Authorization'] = "Basic "+basicauth.decode("ascii")
        self.apiinstance = swagger_client.ObjectsApi(swagger_client.ApiClient(configuration))
        self.serial = serial
        self.log = log
        self.parser = cmrestparse.cmrestparser(log)
        self.storagedeviceid = storagedeviceid

    
    def identify(self):
        storagedict = self.storages()['display']
        return storagedict[self.storagedeviceid]['model']

    def storages(self):
        apiresponse = self.apiinstance.v1_objects_storages_get().to_dict()
        apiresponse['display'] = self.parser.storages(apiresponse)
        return apiresponse

    def getldev(self,ldevid):
        apiresponse = self.apiinstance.v1_objects_storages_storage_device_id_ldevs_ldev_id_get(self.storagedeviceid,10000).to_dict()
        self.log.info(apiresponse)
        return apiresponse

    def getport(self):
        apiresponse = self.apiinstance.v1_objects_storages_storage_device_id_ldevs_ldev_id_get(self.storagedeviceid,10000).to_dict()
        self.log.info(apiresponse)
        return apiresponse 
 
