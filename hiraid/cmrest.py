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
import base64
from .cmrestparser import Cmrestparser
import json
import requests
from datetime import datetime
import os
import logging

class cmrest:
    def __init__(self,storage,serial,log,protocol="",address="",port="",user="cmrest",password="cmrest",base="/ConfigurationManager/"):

        self.rooturl = '{}://{}:{}'.format(protocol,address,port)
        self.baseurl = '{}{}'.format(self.rooturl,base)
        self.protocol = protocol
        self.port = port
        self.address = address
        self.user = user
        self.password = password
        self.log = log
        self.serial = serial
        
        self.sessionAlivetime = { "aliveTime": 300 }
        #self.parser = cmrestparse.cmrestparser(log)
        self.headers = {'Content-Type': 'application/json'}
        self.parser = Cmrestparser(storage)
        self.cmversion()
        self.getStorageDeviceId()
        self.getSession()

    def get(self,url):
      self.log.info(url)
      r = self.session.get(url)
      self.checkresponse(r)
      return r.json()

    def post(self,url,data=''):
      self.log.info(url)
      self.log.info(json.dumps(data,indent=4))
      r = self.session.post(url,data=json.dumps(data))
      self.checkresponse(r)
      return r.json()

    def put(self,url,data=''):
      self.log.info(url)
      self.log.info(json.dumps(data,indent=4))
      r = self.session.put(url,data=json.dumps(data))
      self.checkresponse(r)
      return r.json()
    
    def delete(self,url):
      self.log(url)
      r = self.session.delete(url)
      self.checkresponse(r)
      return r.json()

    def checkresponse(self,r):
        if not r.ok:
            err = "Request failed. url: {}, status_code: {}, reason: {}, text: {}, request-header: {}".format(r.url,r.status_code,r.reason,r.text,r.request.headers)
            self.log.info(err)
            raise Exception('Unable to complete request: {}'.format(err))
        self.log.info(r.json())

    def cmversion(self,api='configuration/version'):
        self.log.info("Obtain cmrest version")
        url = '{}{}'.format(self.baseurl,api)
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        r = self.session.get(url)
        self.checkresponse(r)
        return r.json()

    def register(self,api='v1/objects/storages/'):
        url = '{}{}'.format(self.baseurl,api)
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        r = self.session.get(url)
        self.checkresponse(r)
        return r.json()
        
    def getStorageDeviceId(self):
        self.log.info(f"Get storageDeviceId from cmrest serial number {self.serial} ( {str(self.serial)[-5:]} )")
        registeredStorages = self.storages()
        for registeredStorage in registeredStorages['data']:
            if str(registeredStorage['serialNumber']) == str(self.serial) or str(registeredStorage['serialNumber']) == str(self.serial)[-5:]:
                self.storageDeviceId = registeredStorage['storageDeviceId']
                self.log.info(f"Located registered storageDeviceId '{self.storageDeviceId}'")
                return registeredStorage['storageDeviceId']
        err = f"Unable to locate registered storageDeviceId from serial {self.serial}"
        self.log.info(err)
        raise Exception(f'{err}')

    def storages(self,api='v1/objects/storages/'):
        url = '{}{}'.format(self.baseurl,api)
        r = self.session.get(url)
        self.checkresponse(r)
        return r.json()

    def identify(self,api='v1/objects/storages/'):
        url = '{}{}{}'.format(self.baseurl,api,self.storageDeviceId)
        data = self.get(url)
        return data
        
    def getSession(self,api='v1/objects/storages/'):
        url = '{}{}{}{}'.format(self.baseurl,api,self.storageDeviceId,'/sessions')
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        body = self.sessionAlivetime
        r = self.session.post(url,auth=(self.user,self.password))
        self.checkresponse(r)
        self.log.info(r.headers)
        self.session.headers.update({'Authorization': 'Session '+r.json()['token']})
        #if r.status_code != 200:
        #    self.log.info("Unable to establish session with cmrest host {}, http error \'{}\'".format(self.address,r.status_code))
            #sys.exit(r.status_code)
        #else:
        #    self.log.info("Successfully established session with Automator host {}".format(self.address))  
            #self.version = json.loads(r.text)
            #self.session.headers.update({'Authorization':r.headers['WWW-Authenticate']})

    def getport(self,api='v1/objects/storages/',optviews=[]):
        url = '{}{}{}{}'.format(self.baseurl,api,self.storageDeviceId,'/ports')
        data = self.get(url)
        data['views'] = self.parser.getport(data)
        return data

    def addldevauto(self,poolid,capacityblk,resource_id,start=0,end=65279,associatemap=[{}],api='v1/objects/storages/'):
        jobs = []
        url = '{}{}{}{}'.format(self.baseurl,api,self.storageDeviceId,'/ldevs')
        body = { "poolId":poolid,"startLdevId":start,"endLdevId":end,"blockCapacity":capacityblk,"isParallelExecutionEnabled":True }

        for mapper in maplist:
            data = self.post(url,body)
            data['_append'] = mapper
            jobs.append(data)

        for job in jobs:
            self.log.info("JOB: "+json.dumps(job))

def configlog(scriptname,logdir,logname,basedir=os.getcwd()):
    
    def createdir(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
    try:
        separator = ('/','\\')[os.name == 'nt']
        cwd = basedir
        createdir('{}{}{}'.format(cwd,separator,logdir))
        logfile = '{}{}{}{}{}'.format(cwd, separator, logdir, separator, logname)
        logger = logging.getLogger(scriptname)
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # Add handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)
    except Exception as e:
        raise Exception('Unable to configure logger > {}'.format(str(e)))
    
    return logger

if __name__ == '__main__':

    scriptname = os.path.basename(__file__)
    ts = datetime.now().strftime('%Y-%m-%d_%H.%M.%S')
    logname = scriptname+"_"+ts
    logdir = "/var/log/cmrest"
    log = configlog(scriptname,logdir,logname)

    a = cmrest(350147,log,"http","172.16.168.19",23450,800000050147)
    a.getPorts()
    a.addldevauto(0,2097152,0,10000,11000)
