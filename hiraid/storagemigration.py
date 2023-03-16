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
# 08/03/2021    v1.1.01     Modified to allow targeted_rollback.
#                           Reverse directory contains rollback scripts for each migrating object.
#                           To enable set scriptconf.py Scriptconf.targeted_rollback = True
#
# -----------------------------------------------------------------------------------------------------------------------------------

import json.encoder
import re
import collections
import sys
import inspect
from string import Template
import os
from datetime import datetime
import time
from .raidlib import Storage as Storage
from .storageexception import StorageException
import random
import uuid
from .messaging import Gadmessaging as messaging
import copy

class requiredcustomviews:
    wwn_view = 'wwns_bylcwwnportgid'

class textstyle:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class StorageMigration:

    def __init__(self, migrationtype: str, log: object, start: str=None, group: str=None, source: object=None, target: object=None, config: object=None, cache: bool=False, env: object=None, host: str=None, step: str=None, horcmdir: str="/etc/", targeted_rollback: bool=False):
        
        self.migrationtype = migrationtype
        self.source = source
        self.target = target
        self.log = log
        self.config = config
        self.migrationhosts = ''
        self.hostcount = 0
        self.basedir = os.getcwd()
        self.cache = cache
        self.sessioncache = {}
        self.migrationjson = {}
        self.env = env
        self.group = group
        # Soon to be removed self.host
        self.host = host
        self.separator = ('/','\\')[os.name == 'nt']
        self.migrationdir = '{}{}{}'.format(self.env.migrationoutputdir,self.separator,group)
        self.step = step
        self.scriptname = log.name
        self.start = start
        self.storages = Storage
        self.initmigration()
        self.warnings = 0
        self.warningmessages = []
        self.endmessage = "Ended Successfully"
        self.horcmdir = horcmdir
        self.actioninghostlist = {}
        self.copygrps = {}
        self.edgecopygrps = {}
        self.storage = {}
        self.edgestorage = False
        self.edgestoragearrays = {}
        self.storagearrays = {}
        self.targeted_rollback = targeted_rollback
        self.undocmds = {}
        self.undodir = '{}{}{}'.format(self.basedir,os.sep,'reverse')


    def initmigration(self):
        # Initialise migration json
        self.initmigrationjson()
        if self.group:
            self.createdir(self.migrationdir)

    def createdir(self,directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    def loadenv(self,env):
        # Unused, overly complicated and probably dangerous
        attrs = [attr for attr in dir(env) if not callable(getattr(env,attr)) and not attr.startswith("__")]
        for item in attrs:
            setattr(self,item,attrs[item])

    def die2(self):
        raise StorageException('dies',Storage,self.log)

    def now(self):
        return datetime.now().strftime('%d-%m-%Y_%H.%M.%S')

    def logtaskstart(self,taskname,group='',host='',step='',taskid='',storage: object=''):
        jsonin = self.migrationjson
        log = self.log.info
        migrationtype = self.migrationtype
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
        log('Starting task {}'.format(taskname))
        step = self.step
        group = self.group

        if not taskid:
            return

        def storetask(taskref):
            if storage:
                storage.storetaskref(taskref)

        if host:
            jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid] = { "taskname":taskname, "status":"started", "begin":ts, "messages":[] }
            taskref = jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]
            storetask(taskref)
        else:
            for host in jsonin[migrationtype]['migrationgroups'][group]:
                if not jsonin[migrationtype]['migrationgroups'][group][host]['omit']:
                    jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid] = { "taskname":taskname, "status":"started", "begin":ts, "messages":[] }
                    taskref = jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]
                    storetask(taskref)

    def logtaskcomplete(self,taskname):
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
        self.log.info('Task Succeeded {}'.format(taskname))

    def logendtask(self,host,taskid,status,extrafields={},storage: object=''):
        jsonin = self.migrationjson
        log = self.log.debug
        migrationtype = self.migrationtype
        group = self.group
        step = self.step
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')

        def includefields(appendto,extrafields):
            for key in extrafields:
                appendto[key] = extrafields[key]

        def removetask():
            if storage:
                storage.removetaskref()

        if host:
            jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]['status'] = status
            jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]['end'] = ts
            includefields(jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid],extrafields)
            removetask()

        else:
            for host in jsonin[migrationtype]['migrationgroups'][group]:
                if not jsonin[migrationtype]['migrationgroups'][group][host]['omit']:
                    jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]['status'] = status
                    jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]['end'] = ts
                    includefields(jsonin[migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid],extrafields)
                    removetask()

    def checkconfig(self,jsonin: dict,template: dict):
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        try:
            for key in template:
                log.debug('Checking key {}'.format(key))
                if key not in jsonin and template[key]['required']:
                    options = ''
                    if 'options' in template[key]:
                        options = ' - Possible options {}'.format(str([*template[key]['options']]))
                    msg = 'Required key field \'{}\' missing from input{}'.format(key,options)
                    log.debug(msg)
                    raise Exception(msg)

                if key not in jsonin and not template[key]['required']:
                    msg = 'Key not present {}'.format(key)
                    log.debug(msg)
                    continue

                if str(type(jsonin[key]).__name__) != template[key]['type']:
                    msg = 'Input key {} type, expected {}, received {}'.format(key,template[key]['type'],type(jsonin[key]).__name__)
                    log.debug(msg)
                    raise Exception(msg)

                if 'options' in template[key] and template[key]['type'] == 'str':
                    if jsonin[key] not in template[key]['options']:
                        msg = 'Input key \'{}\' possible options {}, given option \'{}\' not expected'.format(key,template[key]['options'],jsonin[key])
                        log.debug(msg)
                        raise Exception(msg)
                    else:
                        log.debug('key: {} val: {} opts: {}'.format(key,jsonin[key],template[key]['options']))
                        if 'mutualrequirements' in template[key]['options'][jsonin[key]]:
                            for mutualrequirement in template[key]['options'][jsonin[key]]['mutualrequirements']:
                                if mutualrequirement not in jsonin:
                                    msg = 'Field {} value {} depends on fields {}'.format(key,jsonin[key],template[key]['options'][jsonin[key]]['mutualrequirements'])
                                    log.debug(msg)
                                    raise Exception(msg)

                if 'options' in template[key] and template[key]['type'] == 'bool':
                    if jsonin[key]:
                        optkey = "true"
                    else:
                        optkey = "false"

                    if optkey not in template[key]['options']:
                        msg = 'Input key \'{}\' possible options are true or false, given option \'{}\' not expected'.format(key,jsonin[key])
                        log.debug(msg)
                        raise Exception(msg)



                if 'options' in template[key] and template[key]['type'] == 'list':
                    templateoptiontype = None

                    for templateoption in template[key]:
                        templateoptiontype = type(templateoption).__name__
                    log.info('Template options type: {}'.format(templateoptiontype))

                    for option in jsonin[key]:
                        inputoptiontype = type(option).__name__
                        if templateoptiontype != inputoptiontype:
                            msg = 'Input key \'{}\' option {} type \'{}\' does not match template option type \'{}\', please set input option type to \'{}\''.format(key,option,inputoptiontype,templateoptiontype,templateoptiontype)
                            log.debug(msg)
                            raise Exception(msg)

                        if option not in template[key]['options']:
                            msg = 'Input key \'{}\' possible options {}, given option \'{}\' not expected'.format(key,template[key]['options'],option)
                            log.debug(msg)
                            raise Exception(msg)
                        
                if template[key]['type'] == 'list' and 'minitems' in template[key]:
                    if len(jsonin[key]) < template[key]['minitems']:
                        raise Exception('Input key \'{}\' items {}, less than minimum \'{}\''.format(key,len(jsonin[key]),template[key]['minitems']))

                if 'minlength' in template[key]:
                    if template[key]['type'] == 'list':
                        for item in jsonin[key]:
                            if len(item) < template[key]['minlength']:
                                raise Exception('Input key \'{}\' value \'{}\' length less than minimum length \'{}\''.format(key,item,template[key]['minlength']))
                    else:
                        if len(jsonin[key]) < template[key]['minlength']:
                            raise Exception('Input key \'{}\' value \'{}\' length less than minimum length \'{}\''.format(key,jsonin[key],template[key]['minlength']))

                if 'maxlength' in template[key]:
                    if template[key]['type'] == 'list':
                        for item in jsonin[key]:
                            if len(item) > template[key]['maxlength']:
                                raise Exception('Input key \'{}\' value \'{}\' length greater than max length \'{}\''.format(key,item,template[key]['maxlength']))
                    else:
                        if len(jsonin[key]) > template[key]['maxlength']:
                            raise Exception('Input key \'{}\' value \'{}\' length greater than max length \'{}\''.format(key,jsonin[key],template[key]['maxlength']))

                if 'length' in template[key]:
                    if template[key]['type'] == 'list':
                        for item in jsonin[key]:
                            if len(item) != template[key]['length']:
                                raise Exception('Input key \'{}\' value \'{}\' length \'{}\' not equal to required length \'{}\''.format(key,item,len(item),template[key]['maxlength']))
                    else:
                        if len(jsonin[key]) != template[key]['length']:
                            raise Exception('Input key \'{}\' value \'{}\' length \'{}\' not equal to required length \'{}\''.format(key,jsonin[key],template[key]['maxlength']))

                if 'regex' in template[key] and template[key]['type'] == 'list':
                    pattern = re.compile(template[key]['regex'])
                    #pattern = re.compile("[a-zA-Z0-9]")
                    for item in jsonin[key]:
                        if not pattern.match(item):
                            raise Exception('Input key \'{}\' value \'{}\' failed expression validation'.format(key,item))
                log.debug('Key {} ok'.format(key))

        except Exception as e:
            raise StorageException('Config file failed validation {}'.format(str(e)),Storage,self.log,migration=self)
        
        self.logtaskcomplete(taskname)
        return jsonin

    def loadconfig(self,configfile: str,configtemplatefile: str=''):
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        try:
            config,template = self.openinputs(configfile,configtemplatefile)

            if configtemplatefile:
                self.checkconfig(config,template)
            self.config = config

        except Exception as e:
            raise StorageException('Unable to load config {}'.format(str(e)),Storage,self.log,migration=self)
        self.migrationjson[self.migrationtype]['conf'] = self.config
        self.logtaskcomplete(taskname)

    def loadhosts(self,hostconfigfile: str,configtemplatefile=''):
        log = self.log.info
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        try:
            config,template = self.openinputs(hostconfigfile,configtemplatefile)
            if configtemplatefile:
                for host in config:
                    self.checkconfig(config[host],template)
            self.migrationhosts = config
            self.nummigrationhosts = len(config)
            log('Number of migration hosts: {}'.format(self.nummigrationhosts))

        except Exception as e:
            raise StorageException('Unable to load host input file {} and/or template {}, error: {}'.format(hostconfigfile,configtemplatefile,str(e)),Storage,self.log,migration=self)
        self.logtaskcomplete(taskname)

    def loadjsonin(self,jsonfile: str):
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        log = self.log.debug
        try:
            jsonin,template = self.openinputs(jsonfile,self.env.jsonintemplate)
            self.checkjsonin(jsonin)
        except Exception as e:
            raise StorageException('Unable to validate json input {}, error \'{}\''.format(jsonfile,str(e)),Storage,self.log,migration=self)

        self.jsonin = jsonin
        self.migrationjson = self.jsonin
        self.config = jsonin[self.migrationtype]['conf']
        self.log.info('Source storage serial: {}'.format(self.config.get('source_serial')))
        self.log.info('Target storage serial: {}'.format(self.config.get('target_serial')))
        log('Successfully loaded jsonin')

    def loadmigrationhosts(self,step=None,includehosts=[],excludehosts=[]):

        omithosts = {}
        inchosts = {}

        for eh in excludehosts:
            omithosts[eh] = eh
        try:
            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                if len(includehosts):
                    if host not in includehosts:
                        self.log.info("Host: {} not present in include list".format(host))
                        omithosts[host] = host
                    else:
                        if host not in omithosts:
                            inchosts[host] = host
                else:
                    if host not in omithosts:
                        inchosts[host] = host


            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                copy_grp_name = None
                #if len(includehosts) and host not in includehosts:
                #    self.log.info("Host: {} not present in include list".format(host))
                #    omithosts[host] = host
                    #continue

                #if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('omit',False) and host not in omithosts:
                if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('omit',False):
                    if int(step) == 1:
                        self.log.info("Step {}, nothing to load for host".format(step))
                        continue
                    self.log.info('Loading all possible hosts ( except those with omit tag ) to obtain copy_grp data')
                    self.log.info('loading host {}'.format(host))
                    hostinputjsonfile = '{}{}{}.json'.format(self.migrationdir,self.separator,host)
                    try:
                        self.loadhostjson(hostinputjsonfile,host)
                    except Exception as l:
                        if host in inchosts:
                            raise Exception('Unable to load host {} json - error: {}'.format(host,str(l)))
                        else:
                            self.log.warn('Unable to load host {} data, host not included for this step'.format(host))

                    # In order to allow users to include and exclude hosts at any step, capture copy_grp names for ALL node regardless otherwise
                    # when horcm is overwritten problems will occur.
                    copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('copy_grp_name')
                    if copy_grp_name:
                        self.copygrps[copy_grp_name] = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('device_grp_name')
                else:
                    self.log.info('Host {} omitted'.format(host))
                    omithosts[host] = host

            for omitted in omithosts:
                self.log.info("Dropping host: {} from migration step, it was either omitted, excluded or an include list did not explicitly contain it".format(omitted))
                try:
                    del self.jsonin[self.migrationtype]['migrationgroups'][self.group][omitted]
                except KeyError:
                    self.log.warn("Omitted host {} not present in json file!".format(omitted))
        except Exception as e:
            #if int(step) == 1:
            #    self.log.info('No host files are expected for step 1 or other error {}'.format(str(e)))
            #else:
            raise StorageException('Unable to load migration hosts - error \'{}\''.format(e),Storage,self.log,migration=self)

        self.log.info("Number of hosts in migration group {}: {}".format(self.group,len(self.jsonin[self.migrationtype]['migrationgroups'][self.group])))
        if not len(self.jsonin[self.migrationtype]['migrationgroups'][self.group]):
            self.log.warn("There are no hosts to migrate, please check gadhosts.json and any include / exclude lists")
            self.exitroutine()
        else:
            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                self.log.info("---> This host is migrating: {}".format(host))
                self.actioninghostlist[host] = host

        self.log.info('Successfully loaded migrating host json from {}'.format(self.migrationdir))
        self.log.debug(json.dumps(self.jsonin, indent=4, sort_keys=True))



    def loadhostjson(self,hostinputjsonfile,host):
        log = self.log.debug
        self.log.info('Loading host {} data from migration directory'.format(host))
        hostjson = None
        hostjson,template = self.openinputs(hostinputjsonfile,'')
        try:
            if hostjson:
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host] = hostjson
        except Exception as e:
            raise Exception('Unable to load host json {}'.format(str(e)))

        return

    def checkjsonin(self,jsonin):

        log = self.log
        omitted = 0

        if self.group not in jsonin[self.migrationtype]['migrationgroups']:
            message = 'Group {} does not exist in json input file'.format(self.group)
            raise Exception(message)

        numhosts = len(jsonin[self.migrationtype]['migrationgroups'].get(self.group,[]))
        message = 'Number of hosts in group: {}'.format(numhosts)

        if len(jsonin[self.migrationtype]['migrationgroups'][self.group]) < 1:
            raise Exception(message)

        log.info(message)


        for host in jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('omit'):
                omitted += 1
        
        if numhosts == omitted:
            raise Exception('Appears that all {} host(s) are omitted, omitted host count {}'.format(numhosts,omitted))

        return

    def openinputs(self,configfile: dict,templatefile: dict=''):
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        template = None
        try:
            with open(configfile) as configfile:
                config = json.load(configfile)

        except Exception as e:
            raise Exception('Unable to open user config {} error: {}'.format(configfile, str(e)))

        try:
            if templatefile:
                with open(templatefile) as templatefile:
                    template = json.load(templatefile)
        
        except Exception as e:
            raise Exception('Unable to open script templatefile {} error: {}'.format(templatefile, str(e)))




        self.logtaskcomplete(taskname)
        return config,template

    def checkstep(self):
        errtracker = { 'error': 0 }
        previousstep = int(self.step) - 1
        log = self.log

        def process(host,group,previousstep,errtracker):
            log.info('Check step sequence for host {}, group {}, this step {}, previousstep {}'.format(host,group,self.step,previousstep))
            lasthoststep = 0
            jsonstepslist = sorted(list(self.jsonin[self.migrationtype]['migrationgroups'][group][host]['steps'].keys()))
            if len(jsonstepslist):
                lasthoststep = jsonstepslist[-1]
            log.info('Host {} last executed step {}'.format(host,lasthoststep))
            log.debug('Steps index {}'.format(jsonstepslist))
            if int(lasthoststep) >= int(self.step):
                errormessage = 'This step {}, however step {} has already executed, appear to be running out of sequence'.format(self.step,lasthoststep)
                log.error(errormessage)
                errtracker['error'] = 1
            if previousstep > 0:
                if str(previousstep) not in self.jsonin[self.migrationtype]['migrationgroups'][group][host]['steps']:
                    log.error('Step {} not in steps for host {} in group {}'.format(str(previousstep),host,group))
                    errtracker['error'] = 1
                if str(previousstep) not in self.jsonin[self.migrationtype]['migrationgroups'][group][host]['steps'] or not re.search('successful|completed|ended', self.jsonin[self.migrationtype]['migrationgroups'][group][host]['steps'][str(previousstep)]['status'],re.IGNORECASE):
                    errormessage = 'There is a problem with the previous step {} ( status {} ) for host {} in migration group {}'.format(previousstep,self.jsonin[self.migrationtype]['migrationgroups'][group][host]['steps'][str(previousstep)]['status'],host,group)
                    log.error(errormessage)
                    errtracker['error'] = 1


        for node in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            process(node,self.group,previousstep,errtracker)

        if errtracker['error']:
            message = 'Seem to be running out of step or previous step did not complete appropriately, see log for further details'
            log.error(message)
            raise Exception(message)

        log.info("Step sequence OK")

    def indexhbawwns(self,storage: object):
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        log = self.log.debug
        hostgroup_view = 'byportgid'
        wwn_view = 'wwns_bylcwwnportgid'

        def runindex():
            storage.getport()
            portlist = list(storage.views['_ports'].keys())
            storage.gethostgrps(portlist,optviews=[hostgroup_view])
            hostgrouplist = list(storage.views[hostgroup_view].keys())
            storage.gethbawwns(ports=hostgrouplist,optviews=[wwn_view])

        if self.cache:
            log('cache option {}. Loading views from cache if present')
            try:
                storage.readcache()
                log('Cached views: {}'.format(storage.showviews()))
            except:
                log('Unable to open cache, begin indexing operation')
                runindex()
        else:
            runindex()

        self.uniquewwns = len(storage.views[wwn_view])
        self.log.info('Number of unique host wwns {} on storage array {} '.format(self.uniquewwns,storage.serial))
        self.logtaskcomplete(taskname)
        
    def initmigrationjson(self):
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        now = self.now()
        self.migrationjsonfilename = '{}.{}.json'.format(self.migrationtype,now)
        defaults = { "meta": { "filename": self.migrationjsonfilename, "timestamp": now }, "conf": {}, "migrationgroups": {} ,"reports": { "capacity": {} } }
        self.migrationjson = { self.migrationtype: defaults }
        self.migrationjsonfile = '{}{}{}'.format(self.basedir,self.separator,self.migrationjsonfilename)
        self.logtaskcomplete(taskname)
    
    def writemigrationjsonfile(self):
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname)
        try:
            file = open(self.migrationjsonfile,"w")
            file.write(json.dumps(self.migrationjson, indent=4, sort_keys=True))
        except Exception as e:
            raise StorageException('Unable to write migration json file {}, error {}'.format(self.migrationjsonfile,str(e)),Storage,self.log,migration=self)
        self.logtaskcomplete(taskname)
        self.log.info('Output migration json file: {}'.format(self.migrationjsonfile))

    def gethostgroupresourcegroupids(self,storage: object, port: str, gid: str, popkeys: list=[], returnkeys: list=[], refresh: bool=False):
        data = None
        returndata = {}

        self.sessioncache[storage.serial] = self.sessioncache.get(storage.serial,{ 'hostgroups':{}, 'ldevs':{} })

        if ( self.cache and not refresh ) or ( self.sessioncache[storage.serial]['hostgroups'].get(port) and not refresh ):
            data = storage.views['_ports'][port]['_host_grps'].get(gid)

        if refresh or not data:
            storage.gethostgrprgid(port)
            self.sessioncache[storage.serial]['hostgroups'][port] = True
            data = storage.views['_ports'][port]['_host_grps'][gid]

        for key in data:
            if key in popkeys:
                continue
            if len(returnkeys):
                if key in returnkeys:
                    returndata[key] = data[key]
            else:
                returndata[key] = data[key]

        return returndata

    def getldev(self,storage: object, ldevid: str, popkeys: list=[], returnkeys: list=[], refresh: bool=False):
        
        data = None
        returndata = {}

        self.sessioncache[storage.serial] = self.sessioncache.get(storage.serial,{ 'hostgroups':{}, 'ldevs':{} })

        if ( self.cache and not refresh ) or ( self.sessioncache[storage.serial]['ldevs'].get(ldevid) and not refresh ):
            data = storage.views['_ldevs'].get(ldevid)

        if refresh or not data:
            storage.getldev(ldevid=ldevid)
            self.sessioncache[storage.serial]['ldevs'][ldevid] = True
            data = storage.views['_ldevs'][ldevid]

        for key in returnkeys:
            if key in popkeys:
                popkeys.remove(key)

        for key in data:
            if key in popkeys:
                continue
            if len(returnkeys):
                if key in returnkeys:
                    returndata[key] = data[key]
            else:
                returndata[key] = data[key]

        return returndata

    def returnportgid(self,portgid):
        port = str('-'.join(portgid.split('-')[:2]))
        gid = str(portgid.split('-')[-1])
        return port,gid     
    
    def populatemigrationjson(self, storage: object):
        conf = self.config
        startgroup = conf.get('migration_group_start')
        hostspergroup = conf.get('migration_objects_per_group')
        log = self.log.debug
        groupid = '{}.{}.{}'.format(conf['source_serial'],conf['target_serial'],startgroup)
        groups = { groupid: {}}
        
        hostpergroupcounter = 0
        ldevpopkeys = ['CL','EXP_SPACE','FLA(MB)','F_POOLID','LDEV','MP#','NUM_PORT','OPE_RATE','OPE_TYPE','PORTs','PWSV_S','RSV(MB)','SL','CMP']
        hostgroupreturnkeys = ['PORT','GID','GROUP_NAME','Serial#','HMD','HMO_BITs','RSGID']

        for host in self.migrationhosts:
            resourcekeys = {}
            wwns = self.migrationhosts[host]['wwns']
            wwnsdetected = 0
            for wwn in wwns:
                if wwn.lower() in storage.views[requiredcustomviews.wwn_view]:
                    wwnsdetected += 1
                    log('Located host {} wwn {} in storage array {}'.format(host,wwn,storage.serial))
                    groups[groupid][host] = groups[groupid].get(host, { "hostgroups": {}, "ldevs": {}, "resource": {}, "steps": {}, "edgesteps": {}, "omit": False })
                    for hostgroup in storage.views[requiredcustomviews.wwn_view][wwn]:
                        port,gid = self.returnportgid(hostgroup)
                        log("port {} gid {}".format(port,gid))
                        groups[groupid][host]['hostgroups'][hostgroup] = groups[groupid][host]['hostgroups'].get(hostgroup, self.gethostgroupresourcegroupids(storage,port,gid,returnkeys=hostgroupreturnkeys))
                        groups[groupid][host]['hostgroups'][hostgroup]['WWNS'] = groups[groupid][host]['hostgroups'][hostgroup].get('WWNS', {})
                        groups[groupid][host]['hostgroups'][hostgroup]['LUNS'] = groups[groupid][host]['hostgroups'][hostgroup].get('LUNS', {})
                        groups[groupid][host]['hostgroups'][hostgroup]['WWNS'][wwn] = { 'NICK_NAME': storage.views[requiredcustomviews.wwn_view][wwn][hostgroup]['NICK_NAME'] }
                        rsgid = groups[groupid][host]['hostgroups'][hostgroup]['RSGID']
                        groups[groupid][host]['resource']['resourceGroupId'] = rsgid
                        resourcekeys[rsgid] = rsgid
                        storage.getluns(ports=[hostgroup])
                        try:
                            hostgroupluns = storage.views['_ports'][port]['_host_grps'][gid]['_luns']
                        except:
                            hostgroupluns = []

                        if len(hostgroupluns):
                            for lun in hostgroupluns:
                                luns = groups[groupid][host]['hostgroups'][hostgroup]['LUNS']
                                self.log.info("luns: {}".format(luns))
                                ldevid = hostgroupluns[lun]['LDEV']
                                storage.log.debug('LDEV: {}'.format(ldevid))
                                luns[lun] = {}
                                luns[lun]['LDEV'] = ldevid
                                luns[lun]['CULDEV'] = storage.returnldevid(hostgroupluns[lun]['LDEV'])['culdev']
                                luns[lun]['CM'] = hostgroupluns[lun]['CM']
                                luns[lun]['OPKMA'] = hostgroupluns[lun]['OPKMA']
                                groups[groupid][host]['ldevs'][ldevid] = groups[groupid][host]['ldevs'].get(ldevid, self.getldev(storage,ldevid,popkeys=ldevpopkeys))

                else:
                    log('wwn: {} not found in storage array host groups'.format(wwn))
            if wwnsdetected:
                self.hostcount += 1
                hostpergroupcounter += 1
                if hostpergroupcounter == hostspergroup:
                    hostpergroupcounter = 0
                    startgroup += 1
                if len(groups[groupid][host]['ldevs']):
                    for ldev in groups[groupid][host]['ldevs']:
                        ldevrsgid = groups[groupid][host]['ldevs'][ldev]['RSGID']
                        groups[groupid][host]['resource']['resourceGroupId'] = rsgid
                        resourcekeys[rsgid] = rsgid       
            else:
                continue
            
            if len(resourcekeys) > 1:
                log('Unable to handle hosts which are associated with more than one resource group {}'.format(list[resourcekeys.keys()]))
                raise Exception('Unable to handle hosts which are associated with more than one resource group {}'.format(list[resourcekeys.keys()]))
        if self.hostcount < 1:
            self.log.info("None of the supplied wwns could be located, migrating host count: 0")
            sys.exit(0)

        self.migrationjson[self.migrationtype]['migrationgroups'] = groups
        self.migrationjson[self.migrationtype]['storage'] = {}

    def writestoragerole(self, storage: object, role):
        self.migrationjson[self.migrationtype]['storage'][storage.serial] = self.migrationjson[self.migrationtype]['storage'].get(storage.serial,{ 'role':[],"micro_ver":storage.micro_ver, "model":storage.model, "vtype":storage.vtype, "v_id":storage.v_id, "migration_path":storage.migrationpath})
        updatestorage = self.migrationjson[self.migrationtype]['storage'][storage.serial]
        updatestorage['role'].append(role)
        self.storage[role] = storage

    def raidscansourcehorcdevices(self,storage: object):

        log = self.log
        jsonin = self.migrationjson
        migrationgroups = jsonin[self.migrationtype]['migrationgroups']
        replicationhostgroups = {}

        for group in migrationgroups:
            for host in migrationgroups[group]:
                for hostgroup in migrationgroups[group][host]['hostgroups']:
                    for lun in migrationgroups[group][host]['hostgroups'][hostgroup]['LUNS']:
                        ldevid = migrationgroups[group][host]['hostgroups'][hostgroup]['LUNS'][lun]['LDEV']
                        if 'HORC' in migrationgroups[group][host]['ldevs'][ldevid]['VOL_ATTR']:
                            replicationhostgroups[hostgroup] = replicationhostgroups.get(hostgroup,[])
                            replicationhostgroups[hostgroup].append(ldevid)
                            self.edgestorage = True
                            log.warn

        replicationhostgrouplist = list(replicationhostgroups.keys())
        for hg in replicationhostgrouplist:
            log.warn('Hostgroup \'{}\' has ldevs which are remotely replciated {}'.format(hg,replicationhostgroups[hg]))
            storage.raidscan(port=hg)

        for group in migrationgroups:
            for host in migrationgroups[group]:
                for hostgroup in migrationgroups[group][host]['hostgroups']:
                    for lun in migrationgroups[group][host]['hostgroups'][hostgroup]['LUNS']:
                        ldevid = migrationgroups[group][host]['hostgroups'][hostgroup]['LUNS'][lun]['LDEV']
                        if 'HORC' in migrationgroups[group][host]['ldevs'][ldevid]['VOL_ATTR']:

                            # Loop raidscan and report un/used mu's
                            freemu = []
                            usedmu = []
                            for mu in storage.getview('_raidscanmu')[ldevid]:
                                _ = storage.getview('_raidscanmu')[ldevid][mu]
                                if _['Fence'] != '-':
                                    usedmu.append(mu)
                                    migrationgroups[group][host]['ldevs'][ldevid]['P/S'] = _['P/S']
                                    migrationgroups[group][host]['ldevs'][ldevid]['Status'] = _['Status']
                                    migrationgroups[group][host]['ldevs'][ldevid]['Fence'] = _['Fence']
                                    migrationgroups[group][host]['ldevs'][ldevid]['P-Seq#'] = _['P-Seq#']
                                    migrationgroups[group][host]['ldevs'][ldevid]['P-LDEV#'] = _['P-LDEV#']
                                    migrationgroups[group][host]['mu_used'] = migrationgroups[group][host].get('mu_used',{})
                                    migrationgroups[group][host]['mu_used'][mu] = 'undefinedcg.{}.{}'.format(storage.serial,_['P-Seq#'])
                                else:
                                    freemu.append(mu)
                                    migrationgroups[group][host]['mu_free'] = migrationgroups[group][host].get('mu_free',{})
                                    migrationgroups[group][host]['mu_free'][mu] = 'undefinedcg.{}.{}'.format(storage.serial,_['P-Seq#'])

                            migrationgroups[group][host]['ldevs'][ldevid]['mu_used'] = usedmu
                            migrationgroups[group][host]['ldevs'][ldevid]['mu_free'] = freemu

                            edgetarget = migrationgroups[group][host]['ldevs'][ldevid]['P-Seq#']

                            if migrationgroups[group][host]['ldevs'][ldevid]['Fence'] == 'ASYNC':
                                migrationgroups[group][host]['ldevs'][ldevid]['remote_replication'] = 'GAD+UR'
                                migrationgroups[group][host]['remote_replication'] = {'GAD+UR':{'targets':{edgetarget:{'ldevs':{},'hostgroups':{}, "resource":{} }}}}
                                
                            if migrationgroups[group][host]['ldevs'][ldevid]['Fence'] == 'NEVER':
                                migrationgroups[group][host]['ldevs'][ldevid]['remote_replication'] = 'GAD+TC'
                                migrationgroups[group][host]['remote_replication'] = {'GAD+TC':{'targets':{edgetarget:{'ldevs':{}, 'hostgroups':{}, "resource":{} }}}}

                            self.edgestoragearrays[migrationgroups[group][host]['ldevs'][ldevid]['P-Seq#']] = {}
                            
    def capacityreport(self,storage,skipifthisstepcomplete='',taskid=''):

        storage.getpool()
        storage.getpool(opts='-key opt')
        log = storage.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.migrationjson
        conf = self.config
        step = self.step
        node = self.host
    
        if node:
            log.info('taskname {} node specified {}'.format(taskname,node))
        
        overallcapacitybypool = {}
        capacitybygroup = {}
        report = {}
        fullallocationrequirementMB = 0
        usedallocationrequirementMB = 0
        compressedallocationrequirementMB = 0
        asrequestedallocationrequirementMB = 0
        compressionratio = 1
        ratio = conf['compressionratio']
        potentialcompressionratio = int(ratio.split(':')[0]) / int(ratio.split(':')[1])
        capacity_saving = conf['capacity_saving']
        if capacity_saving != "disable":
            compressionratio = int(ratio.split(':')[0]) / int(ratio.split(':')[1])
    
        log.info('Compression ratio {}'.format(compressionratio))
        log.info('Potential compression ratio {}'.format(potentialcompressionratio))
        
        jsonin[self.migrationtype]['reports']['capacity_requirements'] = { 'bypool':{}, 'bymigrationgroup':{} }
        for group in jsonin[self.migrationtype]['migrationgroups']:
            for host in jsonin[self.migrationtype]['migrationgroups'][group]:
                jsonstepslist = jsonin[self.migrationtype]['migrationgroups'][group][host]['steps'].keys()
                if skipifthisstepcomplete and skipifthisstepcomplete in jsonstepslist:
                    log.info('Skip this host {}, has already completed step {}'.format(host,skipifthisstepcomplete))
                    continue
                if 'omit' in jsonin[self.migrationtype]['migrationgroups'][group][host] and jsonin[self.migrationtype]['migrationgroups'][group][host]['omit']:
                    log.info('Skip this host {}, has omit flat set'.format(host))
                    continue
                if step:
                    if node:
                        if node and host == node:
                            log.debug('{}'.format(json.dumps(jsonin)))
                            log.debug('{} {} {} {} {}'.format(group,host,step,taskid,taskname))
                            self.logtaskstart(taskname,host=self.host,taskid=taskid)

                    else:
                        log.debug('{}'.format(json.dumps(jsonin)))
                        log.debug('{} {} {} {} {}'.format(group,host,step,taskid,taskname))
                        self.logtaskstart(taskname,host=self.host,taskid=taskid)
    
    
                hostcapacityreport = { 'bypool': {}, 'total': { 'total_fullallocationrequirementMB':0, 'total_usedallocationrequirementMB': 0 } }
                stepcapacitylog = { 'bypool': {} }
                
                numberofmigratingldevs = 0

                for ldevid in jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs']:
                    if jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid]['omit']:
                        log.info('Ldev {} omitted, capacity not included in report'.format(ldevid))
                        continue
                    numberofmigratingldevs += 1
                    targetpool = jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid]['target_poolid']
                    capacity = jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid]['VOL_Capacity(BLK)']
                    usedblock = jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid]['Used_Block(BLK)']
    
                    if targetpool not in hostcapacityreport['bypool']:
                        hostcapacityreport['bypool'][targetpool] = { 'fullallocationrequirementMB':0, 'usedallocationrequirementMB':0 }
    
                    if targetpool not in overallcapacitybypool:
                        overallcapacitybypool[targetpool] = { 'ldevs': {} }
    
                    overallcapacitybypool[targetpool]['ldevs'][ldevid] = { "capacity":capacity,"used":usedblock,"migrationgroup":group }
                    hostcapacityreport['bypool'][targetpool]['fullallocationrequirementMB'] += storage.blkstomb(capacity)['MB']
                    hostcapacityreport['bypool'][targetpool]['usedallocationrequirementMB'] += storage.blkstomb(usedblock)['MB']
                    hostcapacityreport['bypool'][targetpool]['potentialcompressedallocationrequirementMB'] = hostcapacityreport['bypool'][targetpool]['usedallocationrequirementMB'] / potentialcompressionratio
                    hostcapacityreport['bypool'][targetpool]['withcurrentcompressionallocationrequirementMB'] = hostcapacityreport['bypool'][targetpool]['usedallocationrequirementMB'] / compressionratio
                    hostcapacityreport['bypool'][targetpool]['availablepoolcapacity'] = int(storage.getview('_pools')[targetpool]['Available(MB)'])
    
                    hostcapacityreport['total']['total_fullallocationrequirementMB'] += storage.blkstomb(capacity)['MB']
                    hostcapacityreport['total']['total_usedallocationrequirementMB'] += storage.blkstomb(usedblock)['MB']
    
                    if targetpool not in stepcapacitylog['bypool']:
                        stepcapacitylog['bypool'][targetpool] = { 'availablepoolcapacity': int(storage.getview('_pools')[targetpool]['Available(MB)']) }

                # What if the host has no luns?
                if not len(jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'].keys()) or not numberofmigratingldevs:
                    log.info('host {} has zero luns'.format(host))
                    log.info(self.config)
                    targetpool = str(self.config['default_target_poolid'])
                    if targetpool not in hostcapacityreport['bypool']:
                        hostcapacityreport['bypool'][targetpool] = { 'fullallocationrequirementMB':0, 'usedallocationrequirementMB':0 }
                    if targetpool not in overallcapacitybypool:
                        overallcapacitybypool[targetpool] = { 'ldevs': {} }
                    hostcapacityreport['bypool'][targetpool]['fullallocationrequirementMB'] += 0
                    hostcapacityreport['bypool'][targetpool]['usedallocationrequirementMB'] += 0
                    hostcapacityreport['bypool'][targetpool]['potentialcompressedallocationrequirementMB'] = hostcapacityreport['bypool'][targetpool]['usedallocationrequirementMB'] / potentialcompressionratio
                    hostcapacityreport['bypool'][targetpool]['withcurrentcompressionallocationrequirementMB'] = hostcapacityreport['bypool'][targetpool]['usedallocationrequirementMB'] / compressionratio
                    hostcapacityreport['bypool'][targetpool]['availablepoolcapacity'] = int(storage.getview('_pools')[targetpool]['Available(MB)'])

                    hostcapacityreport['total']['total_fullallocationrequirementMB'] += 0
                    hostcapacityreport['total']['total_usedallocationrequirementMB'] += 0

                    if targetpool not in stepcapacitylog['bypool']:
                        stepcapacitylog['bypool'][targetpool] = { 'availablepoolcapacity': int(storage.getview('_pools')[targetpool]['Available(MB)']) }
    
                jsonin[self.migrationtype]['migrationgroups'][group][host]['capacityreport'] = hostcapacityreport
                #jsonin[self.migrationtype]['migrationgroups'][group][host]['capacityreport']['bypool'][targetpool]['availablepoolcapacity'] = int(storage.getview('_pools')[targetpool]['Available(MB)'])
                
                if step:
                    if node:
                        if node and host == node:
                            jsonin[self.migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]['bypool'] = stepcapacitylog['bypool']
                            self.logendtask(host=host,taskid=taskid,status='completed')
                    else:
                        jsonin[self.migrationtype]['migrationgroups'][group][host]['steps'][step]['tasks'][taskid]['bypool'] = stepcapacitylog['bypool']
                        self.logendtask(host=host,taskid=taskid,status='completed')
    
    
        for pool in overallcapacitybypool:
            fullallocationrequirementMB = 0
            usedallocationrequirementMB = 0
            availablepoolcapacityMB = int(storage.getview('_pools')[pool]['Available(MB)'])
            for ldevid in overallcapacitybypool[pool]['ldevs']:
                fullallocationrequirementMB += storage.blkstomb(overallcapacitybypool[pool]['ldevs'][ldevid]['capacity'])['MB']
                usedallocationrequirementMB += storage.blkstomb(overallcapacitybypool[pool]['ldevs'][ldevid]['used'])['MB']
            
            potentialcompressedallocationrequirementMB = usedallocationrequirementMB / potentialcompressionratio
            withcurrentcompressionallocationrequirementMB = usedallocationrequirementMB / compressionratio
            jsonin[self.migrationtype]['reports']['capacity_requirements']['bypool'][pool] = { 'fullallocationrequirementMB':fullallocationrequirementMB, 'usedallocationrequirementMB':usedallocationrequirementMB, 'availablepoolcapacity':availablepoolcapacityMB, 'FULL_ALLOCATION_CAPACITY_WARNING':int(availablepoolcapacityMB) <= int(fullallocationrequirementMB), 'potentialcompressedallocationrequirementMB':potentialcompressedallocationrequirementMB , 'withcurrentcompressionallocationrequirementMB':withcurrentcompressionallocationrequirementMB}
            jsonin[self.migrationtype]['reports']['capacity_requirements']['bypool'][pool]['USED_ALLOCATION_CAPACITY_WARNING'] = int(availablepoolcapacityMB) <= int(usedallocationrequirementMB)
            jsonin[self.migrationtype]['reports']['capacity_requirements']['bypool'][pool]['POTENTIAL_CAPACITY_SAVING_ALLOCATION_CAPACITY_WARNING'] = int(availablepoolcapacityMB) <= int(potentialcompressedallocationrequirementMB)
            jsonin[self.migrationtype]['reports']['capacity_requirements']['bypool'][pool]['CURRENT_SETTING_CAPACITY_SAVING_ALLOCATION_CAPACITY_WARNING'] = int(availablepoolcapacityMB) <= int(withcurrentcompressionallocationrequirementMB)
    
        self.log.info('overallcapacitybypool: {}'.format(overallcapacitybypool))

        for pool in overallcapacitybypool:
            availablepoolcapacityMB = int(storage.getview('_pools')[pool]['Available(MB)'])
            for ldevid in overallcapacitybypool[pool]['ldevs']:
                migrationgroup = overallcapacitybypool[pool]['ldevs'][ldevid]['migrationgroup']
    
                if migrationgroup not in capacitybygroup:
                    capacitybygroup[migrationgroup] = { pool: {'fullallocationrequirementMB':0,'usedallocationrequirementMB':0, 'availablepoolcapacity':availablepoolcapacityMB }}
                if pool not in capacitybygroup[migrationgroup]:
                    capacitybygroup[migrationgroup][pool] = {'fullallocationrequirementMB':0,'usedallocationrequirementMB':0, 'availablepoolcapacity':availablepoolcapacityMB }

                capacitybygroup[migrationgroup][pool]['fullallocationrequirementMB'] += storage.blkstomb(overallcapacitybypool[pool]['ldevs'][ldevid]['capacity'])['MB']
                capacitybygroup[migrationgroup][pool]['usedallocationrequirementMB'] += storage.blkstomb(overallcapacitybypool[pool]['ldevs'][ldevid]['used'])['MB']
                capacitybygroup[migrationgroup][pool]['potentialcompressedallocationrequirementMB'] = int(capacitybygroup[migrationgroup][pool]['usedallocationrequirementMB']) / potentialcompressionratio
                capacitybygroup[migrationgroup][pool]['withcurrentcompressionallocationrequirementMB'] = int(capacitybygroup[migrationgroup][pool]['usedallocationrequirementMB']) / compressionratio
                capacitybygroup[migrationgroup][pool]['FULL_ALLOCATION_CAPACITY_WARNING'] = int(availablepoolcapacityMB) <= int(capacitybygroup[migrationgroup][pool]['fullallocationrequirementMB'])
                capacitybygroup[migrationgroup][pool]['USED_ALLOCATION_CAPACITY_WARNING'] = int(availablepoolcapacityMB) <= int(capacitybygroup[migrationgroup][pool]['usedallocationrequirementMB'])
                capacitybygroup[migrationgroup][pool]['POTENTIAL_CAPACITY_SAVING_ALLOCATION_CAPACITY_WARNING'] = int(availablepoolcapacityMB) <= int(capacitybygroup[migrationgroup][pool]['potentialcompressedallocationrequirementMB'])
                capacitybygroup[migrationgroup][pool]['CURRENT_SETTING_CAPACITY_SAVING_ALLOCATION_CAPACITY_WARNING'] = int(availablepoolcapacityMB) <= int(capacitybygroup[migrationgroup][pool]['withcurrentcompressionallocationrequirementMB'])
    
        self.log.info('capacitybygroup: {}'.format(capacitybygroup))

        jsonin[self.migrationtype]['reports']['capacity_requirements']['bymigrationgroup'] = capacitybygroup
    
    def mapldevreserves(self,storage,ldevs,skipontrue='REMOTE_FLAG'):

        log = self.log.debug

        for ldevid in ldevs:
            sot = ldevs[ldevid].get(skipontrue,False)
            if ldevs[ldevid].get('omit',False):
                log('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            if sot:
                log('Ldevid {} was omitted because doontrue filter: {} = {}'.format(ldevid,skipontrue,sot))
                continue
            ldev = str(ldevid)
            targetldevid = ldevs[ldev]['target_ldevid']
            storage.log.debug('Storage array {} - Source ldevid {}, map reserve target ldev '.format(storage.serial,ldev,targetldevid))
            storage.mapldev(targetldevid,'reserve')

    def logstartstep(self):
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
        scriptname = self.scriptname

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]["steps"][self.step] = { "name":scriptname, "status": "started", "tasks": {}, "begin":ts }

    def XXXXXconnectstorage(self,storageserial,horcminst):

        log = self.log
        storageapi = self.config.get('api','raidcom')
        apiconfig = { 'serial': storageserial, 'horcminst': horcminst,  }
        log.info('Establish connectivity with storage {} @horcminst {}'.format(storageserial,horcminst))
        storage = Storage(storageserial,self.log)
        getattr(storage,storageapi)(apiconfig=apiconfig)
        storage.setundodir(self.group)
        undofile = undofile = '{}.{}'.format(self.scriptname,self.start)
        storage.setundofile(undofile)
        storage.jsonin = self.jsonin
        self.edgestoragearrays[str(storageserial)] = { 'storage':storage }

    def connectstorage(self,role: str=None,storageserial: str=None,horcminst: int=None,forceattr: bool=False):
        '''
        definedroles = 'source | target'
        '''
        log = self.log
        definedroles = ['source','target']
        storageapi = self.config.get('api','raidcom')
        resthost = None
        storageDeviceId = None

        if role in definedroles:
            storageserial = self.config['{}_serial'.format(role)]
            horcminst = self.config.get('{}_horcm_inst'.format(role),None)
            resthost = self.config.get('{}'.format('resthost'),None)
            storageDeviceId = self.config.get('{}'.format('storageDeviceId'),None)
        else:
            if not storageserial or not horcminst:
                raise StorageException('Unless storage role is defined in config, will require storageserial and horcminst')

        apiconfig = { 'serial': storageserial, 'horcminst': horcminst, 'resthost':resthost, 'storageDeviceId':storageDeviceId }

        log.info('Establish connectivity with {} storage {}'.format(role,storageserial))
        if str(storageserial) in self.storagearrays:
            if horcminst in self.storagearrays[str(storageserial)]:
                self.log.info('Storage {} is already defined on horcminst {}'.format(storageserial,horcminst))
                return self.storagearrays[str(storageserial)][horcminst]['storage']
        
        log.info('Establish connectivity with storage {} @horcminst {}'.format(storageserial,horcminst))
        storage = Storage(storageserial,self.log)
        getattr(storage,storageapi)(apiconfig=apiconfig)
        storage.setundodir(self.group)
        undofile = undofile = '{}.{}'.format(self.scriptname,self.start)
        storage.setundofile(undofile)
        storage.jsonin = self.jsonin

        # 06/03/2021
        if self.targeted_rollback:
            self.undocmds[storageserial] = self.undocmds.get(storageserial, { 'pre':[],'post':[], 'objects':{} })
            # We wish to produce backout commands for individual migrating objects.
            log.info("targeted_rollback: {}, tweak api to accept this mod".format(self.targeted_rollback))
            setattr(storage.apis[storageapi], 'undocmds',{})

        self.storagearrays[str(storageserial)] = { horcminst: { 'storage':storage } }

        if role:
            # Does the role exist as an attr?
            if hasattr(self,role):
                log.info("Attr already exists!")
                if forceattr: setattr(self,role, storage)
            log.info("Setting attr storage serial {} to storage role {}".format(storageserial,role))
            setattr(self,role, storage)
            self.storagearrays[str(storageserial)][horcminst]['role'] = role
        
        return self.storagearrays[str(storageserial)][horcminst]['storage']

    def Xconnectstorage(self,role: str=None):
        '''
        role = 'source | target'
        '''
        log = self.log
        storageapi = self.config.get('api','raidcom')
        possibleroles = ['source','target']
        if role not in possibleroles:
            raise StorageException('Storage definition {} not supported, must be one of {}'.format(role,possibleroles),Storage,log,migration=self)
        storageserial = self.config['{}_serial'.format(role)]
        horcminst = self.config.get('{}_horcm_inst'.format(role),None)
        resthost = self.config.get('{}'.format('resthost'),None)
        storageDeviceId = self.config.get('{}'.format('storageDeviceId'),None)

        apiconfig = { 'serial': storageserial, 'horcminst': horcminst, 'resthost':resthost, 'storageDeviceId':storageDeviceId }

        log.info('Establish connectivity with {} storage {}'.format(role,storageserial))
        setattr(self,role, Storage(storageserial,log,useapi=storageapi))
        storage = getattr(self,role)
        getattr(storage,storageapi)(apiconfig=apiconfig)
        getattr(storage,'setundodir')(self.group)
        undofile = '{}.{}'.format(self.scriptname,self.start)
        getattr(storage,'setundofile')(undofile)
        setattr(storage,'jsonin',self.jsonin)
        '''
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        storage.raidcom(jsonin['gad']['conf']['source_horcm_inst'])
        # Append undo dir to default /reverse
        source.setundodir(group)
        # Set undo file
        source.setundofile(undofile)

    
        '''


        

    def pairdisplay(self,storage,copy_grp_name,horcminst,opts=''):
        return storage.pairdisplay(horcminst,copy_grp_name,opts=opts)['stdout']

        #     def logtaskstart(self,taskname,group='',host='',step='',taskid='',storage: object=''):

    def pairsplit(self,storage,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        requests = []
        pairevtwaits = {}

        def processpairsplit(storage,host):
            target_inst = self.config['target_cci_horcm_inst']
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe').split('\n')))]
            for row in pairdisplayout:
                log.info(row)

            question = 'pairsplit gad group to SMPL {} - y/n ? : '.format(copy_grp_name)
            log.info('Input to question required: \'{}\''.format(question))

            print('\n')
            for row in pairdisplayout:
                print('{}'.format(row))

            # Create Pairs
            ans = ""
            while not re.search('^y$|^n$',ans):
                ans = input('\n{}'.format(question))
                log.info('User answered {}'.format(ans))
                if ans == 'y':
                    storage.pairsplit(inst=target_inst,group=copy_grp_name)
                    pairdisplayout = self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe')
                    print(pairdisplayout)
                    storage.pairsplit(inst=target_inst,group=copy_grp_name,opts="-S")
                    requests.append(host)
                    pairevtwaits[copy_grp_name] = { "host":host }
                elif ans == 'n':
                    self.logendtask(host=host,taskid=taskid,status="skipped")
                    log.info('Skipped pairsplit for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')

            self.logendtask(host=host,taskid=taskid,status="ended")


        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            processpairsplit(storage,host)

        return { "requests":requests, "pairevtwaits":pairevtwaits }


    def pairvolchk(self,role,expectedreturn,taskid):

        storage = getattr(self,role)
        horcm_inst = self.config['{}_cci_horcm_inst'.format(role)]
        taskname = inspect.currentframe().f_code.co_name

        def process(storage,horcm_inst,host,expectedreturn,taskid):
            try:
                devices = {}
                copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
                pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(storage,copy_grp_name,horcm_inst,opts='-fcxe').split('\n')))]
                pairdisplayout.pop(0)
                for row in pairdisplayout:
                    columns = row.split()
                    devices[columns[1]] = columns[1]
                for device in devices:
                    storage.pairvolchk(inst=horcm_inst,group=copy_grp_name,device=device,expectedreturn=expectedreturn)
            except Exception as e:
                message = 'Copygrp pairs {} not in expected state {}, error {}'.format(copy_grp_name,expectedreturn,str(e))
                self.log.error(message)
                self.logendtask(host=host,taskid=taskid,status="Failed")
                raise StorageException(message,Storage,self.log)

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            process(storage,horcm_inst,host,expectedreturn,taskid)
            self.logendtask(host=host,taskid=taskid,status="Successful")

    def pairsplitRS(self,storage,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        requests = []
        pairevtwaits = {}

        def processpairsplit(storage,host):
            target_inst = self.config['target_cci_horcm_inst']
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe').split('\n')))]
            for row in pairdisplayout:
                log.info(row)

            q = self.usermessage('prepairsplitRS',False,True)
            question = '{} {} - y/n ? : '.format(q,copy_grp_name)
            log.info('Input to question required: \'{}\''.format(question))

            print('\n')
            for row in pairdisplayout:
                print('{}'.format(row))

            # Create Pairs
            ans = ""
            while not re.search('^y$|^n$',ans):
                ans = input('\n{}'.format(question))
                log.info('User answered {}'.format(ans))
                if ans == 'y':
                    storage.pairsplit(inst=target_inst,group=copy_grp_name,opts="-RS")
                    pairdisplayout = self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe')
                    print(pairdisplayout)
                    requests.append(host)
                    pairevtwaits[copy_grp_name] = { "host":host }
                elif ans == 'n':
                    self.logendtask(host=host,taskid=taskid,status="skipped")
                    log.info('Skipped pairsplit for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')

            self.logendtask(host=host,taskid=taskid,status="ended")


        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            processpairsplit(storage,host)

        return { "requests":requests, "pairevtwaits":pairevtwaits }

    def pairsplitR(self,storage,taskid,opts=''):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        requests = []
        pairevtwaits = {}

        def processpairsplit(storage,host):
            target_inst = self.config['target_cci_horcm_inst']
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe').split('\n')))]
            for row in pairdisplayout:
                log.info(row)

            q = self.usermessage('prepairsplitR',False,True)
            question = '{} {} - y/n ? : '.format(q,copy_grp_name)
            log.info('Input to question required: \'{}\''.format(question))

            print('\n')
            for row in pairdisplayout:
                print('{}'.format(row))

            # Create Pairs
            ans = ""
            while not re.search('^y$|^n$',ans):
                ans = input('\n{}'.format(question))
                log.info('User answered {}'.format(ans))
                if ans == 'y':
                    storage.pairsplit(inst=target_inst,group=copy_grp_name,opts=opts)
                    pairdisplayout = self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe')
                    print(pairdisplayout)
                    requests.append(host)
                    pairevtwaits[copy_grp_name] = { "host":host }
                elif ans == 'n':
                    self.logendtask(host=host,taskid=taskid,status="skipped")
                    log.info('Skipped pairsplit for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')

            self.logendtask(host=host,taskid=taskid,status="ended")

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            processpairsplit(storage,host)

        return { "requests":requests, "pairevtwaits":pairevtwaits }

    def monitorpairevtwaits(self,storage,pairevtwaits,status,taskid,horcm_inst=None,pairevtwaitvolumerole='-s'):
        procs = {}
        log = self.log
        returncode = 0
        taskname = inspect.currentframe().f_code.co_name
        pairevtwaittimeout = self.config['pairevtwaittimeout']
        pairevtwaitpollseconds = self.config['pairevtwaitpollseconds']
        horcminst = self.config['source_cci_horcm_inst']
        if horcm_inst:
            horcminst = horcm_inst

        if len(pairevtwaits):
            for copy_grp_name in pairevtwaits:
                storagearray = pairevtwaits[copy_grp_name].get('storage', storage)
                pairevtwaits[copy_grp_name]['cmd'] = 'pairevtwait -g {} -I{} {} {} -t {}'.format(copy_grp_name,horcminst,pairevtwaitvolumerole,status,pairevtwaittimeout)
                self.logtaskstart(taskname,host=pairevtwaits[copy_grp_name]['host'],taskid=taskid)
                pairevtwaits[copy_grp_name]['proc'] = storagearray.pairevtwaitexec(pairevtwaits[copy_grp_name]['cmd'])
                procs[copy_grp_name] = 1

            time.sleep(pairevtwaitpollseconds)

            log.info('Number of pairevtwait processes to monitor: {}'.format(len(procs)))
            while len(procs):
                for copy_grp_name in pairevtwaits:
                    poll = pairevtwaits[copy_grp_name]['proc'].poll()
                    log.info('Polled pairevtwait for horcm group {} - result {}'.format(copy_grp_name,poll))
                    
    
                    if poll is not None:
                        log.info('pairevtwait completed for copy_grp \'{}\' return {}'.format(copy_grp_name,poll))
                        procs.pop(copy_grp_name, None)
                        log.info('Popped copy_grp_name \'{}\' from procs list'.format(copy_grp_name))
                        pairevtwaits[copy_grp_name]['returncode'] = poll
                        pairdisplay = storagearray.pairdisplay(inst=horcminst,group=copy_grp_name,opts='-fcxe')
                        print(pairdisplay['stdout'])
                        if not pairevtwaits[copy_grp_name]['returncode']:
                            status = 'completed'
                        else:
                            status = 'Error copy_grp_name \'{}\' pairevtwait returned: {}'.format(copy_grp_name,pairevtwaits[copy_grp_name]['returncode'])
                            log.warn(status)
                            returncode = 1
                            self.warnings = 1
                            self.warningmessages.append(status)
                            self.endmessage = "Warning!"

                        log.info('copy_grp \'{}\' status: {}'.format(copy_grp_name,status))

                        self.logendtask(host=pairevtwaits[copy_grp_name]['host'],taskid=taskid,status=status)
                    else:
                        print(storagearray.pairdisplay(inst=horcminst,group=copy_grp_name,opts='-fcxe')['stdout'])
                        log.info('Poll returned None, continuing to loop')

                if len(procs):
                    log.info('Number of pairevtwait processes left to monitor: {}'.format(len(procs)))
                    time.sleep(pairevtwaitpollseconds)

        log.info('returning from monitorpairevtwaits')

        return returncode

    def pairsplitS(self,storage,taskid,horcminst,copygrpkey,storageserialkey,messagekey):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        requests = []
        pairevtwaits = {}

        def processpairsplit(storage,host,copy_grp_name,horcminst):
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(storage,copy_grp_name,horcminst,opts='-fcxe').split('\n')))]
            for row in pairdisplayout:
                log.info(row)

            q = self.usermessage(messagekey,False,True)
            question = '{} {} - y/n ? : '.format(q,copy_grp_name)
            log.info('Input to question required: \'{}\''.format(question))

            print('\n')
            for row in pairdisplayout:
                print('{}'.format(row))

            # Create Pairs
            ans = ""
            while not re.search('^y$|^n$',ans):
                ans = input('\n{}'.format(question))
                log.info('User answered {}'.format(ans))
                if ans == 'y':
                    storage.pairsplit(inst=horcminst,group=copy_grp_name,opts="-S")
                    pairdisplayout = self.pairdisplay(storage,copy_grp_name,horcminst,opts='-fcxe')
                    print(pairdisplayout)
                    requests.append(host)
                    pairevtwaits[copy_grp_name] = { "host":host, "storage":storage }
                elif ans == 'n':
                    self.logendtask(host=host,taskid=taskid,status="skipped")
                    log.info('Skipped pairsplit for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')

            self.logendtask(host=host,taskid=taskid,status="ended")


        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            copygrpname = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][copygrpkey]
            storageserial = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][storageserialkey]
            storage = self.connectstorage(storageserial=storageserial,horcminst=horcminst)
            processpairsplit(storage,host,copygrpname,horcminst)

        return { "requests":requests, "pairevtwaits":pairevtwaits }
    
    def addsmile(self):
        smilies = { 1:' :)',2:' 8D',3:' ;)',4:' \\0/',5:' 8)',6:' ;D',7:' ;-)',8:' =)',9:' =D',10:' :-D',11:' 8-D',12:' :-)' }
        returnsmile = ''
        if self.env.smilies:
            returnsmile = smilies[random.randint(1,12)]
        return returnsmile

    def logendstep(self,status):
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]["steps"][self.step]['status'] = status
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]["steps"][self.step]['end'] = ts
        self.log.info('-- {} --'.format(status))

    def writehostmigrationfile(self):

        previousstep = int(self.step) - 1
        prepend = '{}__'.format(previousstep)
        log = self.log

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                hostmigrationfile = '{}{}{}.json'.format(self.migrationdir,self.separator,host)
                self.backupfile(hostmigrationfile,prepend)
                file = open(hostmigrationfile,"w")
                file.write(json.dumps(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host], indent=4, sort_keys=True))

    def backupfile(self,fqfile,prepend='',append=''):
        log = self.log
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
        separator = ('/','\\')[os.name=='nt']
        try:
            if os.path.isfile(fqfile):
                fqfilebackup = '{}{}{}{}{}.{}{}'.format(('/','')[os.name=='nt'],separator.join(fqfile.split(separator)[(1,0)[os.name=='nt']:-1]),separator,prepend,fqfile.split(separator)[-1],ts,append)
                log.debug('Backup file {} to {}'.format(fqfile,fqfilebackup))
                os.rename(fqfile,fqfilebackup)
            else:
                log.warn('File does not exist \'{}\''.format(fqfile))

        except Exception as e:
            raise Exception('Unable to backup files \'{}\''.format(e))

    def pairresyncswaps(self,storage,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        requests = []
        pairevtwaits = {}

        def processpairresync(storage,host):
            target_inst = self.config['target_cci_horcm_inst']
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe').split('\n')))]

            for row in pairdisplayout:
                log.info(row)
            question = 'pairresync -swaps group {} - y/n ? : '.format(copy_grp_name)
            log.info('Input to question required: \'{}\''.format(question))

            print('\n')
            for row in pairdisplayout:
                print('{}'.format(row))

            # Create Pairs
            ans = ''
            while not re.search('^y$|^n$',ans):
                ans = input('\n{}'.format(question))
                log.info('User answered {}'.format(ans))
                if ans == 'y':
                    storage.pairresyncswaps(inst=target_inst,group=copy_grp_name)
                    pairdisplayout = self.pairdisplay(storage,copy_grp_name,target_inst,opts='-fcxe')
                    print(pairdisplayout)
                    requests.append(host)
                    pairevtwaits[copy_grp_name] = { "host":host }
                elif ans == 'n':
                    self.logendtask(host=host,taskid=taskid,status="skipped")
                    log.info('Skipped pairresync -swaps for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')

            self.logendtask(host=host,taskid=taskid,status="ended")

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            processpairresync(storage,host)

        return { "requests":requests, "pairevtwaits":pairevtwaits }




    def backuphorcmfiles(self,filelist,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        status = 'completed'
        self.logtaskstart(taskname,host=self.host,taskid=taskid)
        errors = {}

        for horcmfile in filelist:
            try:
                backupfile(horcmfile,log)
            except Exception as e:
                status = 'Failed'
                if 'errmessages' not in errors:
                    errors['errmessages'] = []
                errors['errmessages'].append(str(e))
        
        self.logendtask(host=self.host,taskid=taskid,status=status,extrafields=errors)
        
        if 'status' == 'Failed':
            raise Exception('Unable to {}'.format(taskname))

    def createhorcmfiles(self,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        log = self.log

        source_horcm_udp_port = self.config['source_horcm_udp_port']
        target_horcm_udp_port = self.config['target_horcm_udp_port']
        source_cci_horcm_inst = self.config['source_cci_horcm_inst']
        target_cci_horcm_inst = self.config['target_cci_horcm_inst']
        horcmtemplatefile = self.env.horcmtemplatefile
        sourceccihorcmfile = '{}horcm{}.conf'.format(self.horcmdir,source_cci_horcm_inst)
        targetccihorcmfile = '{}horcm{}.conf'.format(self.horcmdir,target_cci_horcm_inst)
        source_horcm_ldevg = []
        target_horcm_ldevg = []
        source_horcm_inst = []
        target_horcm_inst = []
        status = 'completed'
        errors = {}

        self.logtaskstart(taskname,host=self.host,taskid=taskid)

        def processhorcmcontent(host):
            log.info('process horcm content for host {}'.format(host))
            copygrpname = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            if copygrpname not in self.source.views['_copygrps']:
                errmessage = 'Copygrp {} not present on storage array!'.format(copygrpname)
                log.error(errmessage)
                raise Exception(errmessage)
            devicegrpname = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['device_grp_name']
            source_horcm_ldevg.append('{}\t{}\t{}'.format(copygrpname,devicegrpname,self.source.serial))
            target_horcm_ldevg.append('{}\t{}\t{}'.format(copygrpname,devicegrpname,self.target.serial))
            source_horcm_inst.append('{}\tlocalhost\t{}'.format(copygrpname,target_horcm_udp_port))
            target_horcm_inst.append('{}\tlocalhost\t{}'.format(copygrpname,source_horcm_udp_port))

        try:

            for node in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['omit']:
                    if 'copy_grp_name' in self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]:
                        processhorcmcontent(node)
                    else:
                        thishoststeps = sorted(list(self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['steps'].keys()))
                        log.info('Copy group name cannot be found for host {}, perhaps this host has not run step 1'.format(node))
                else:
                    log.info('Host {} omitted, skip gneratehorcmfiles'.format(node))

            if self.config['cci_horcm_ipcmd']:
                srccmddevice = '\\\\.\IPCMD-{}-31001'.format(self.config['source_ipaddress'])
                trgcmddevice = '\\\\.\IPCMD-{}-31001'.format(self.config['target_ipaddress'])
            else:
                srccmddevice = '\\\\.\CMD-{}{}'.format(self.source.serial,(':/dev/sd','')[os.name=='nt'])
                trgcmddevice = '\\\\.\CMD-{}{}'.format(self.target.serial,(':/dev/sd','')[os.name=='nt'])

            horcmtemplate = open(horcmtemplatefile)
            horcm = Template(horcmtemplate.read())

            # Append copy_grps from different steps in the process.
            for cg in self.copygrps:
                additional_source_horcm_ldevg = '{}\t{}\t{}'.format(cg,self.copygrps[cg],self.source.serial)
                if additional_source_horcm_ldevg not in source_horcm_ldevg:
                    if cg not in self.source.views['_copygrps']:
                        log.info('Copygrp {} not present on storage, skipping'.format(cg))
                        continue
                    additional_target_horcm_ldevg = '{}\t{}\t{}'.format(cg,self.copygrps[cg],self.target.serial)
                    additional_source_horcm_inst = '{}\tlocalhost\t{}'.format(cg,target_horcm_udp_port)
                    additional_target_horcm_inst = '{}\tlocalhost\t{}'.format(cg,source_horcm_udp_port)
                    source_horcm_ldevg.append(additional_source_horcm_ldevg)
                    target_horcm_ldevg.append(additional_target_horcm_ldevg)
                    source_horcm_inst.append(additional_source_horcm_inst)
                    target_horcm_inst.append(additional_target_horcm_inst)

            sourcehorcm = { 'service':source_horcm_udp_port, 'serial':self.source.serial, 'cmddevice':srccmddevice, 'horcm_ldevg':'\n'.join(source_horcm_ldevg), 'horcm_inst':'\n'.join(source_horcm_inst) }
            targethorcm = { 'service':target_horcm_udp_port, 'serial':self.target.serial, 'cmddevice':trgcmddevice, 'horcm_ldevg':'\n'.join(target_horcm_ldevg), 'horcm_inst':'\n'.join(target_horcm_inst) }

            sourcehorcmcontent = horcm.substitute(sourcehorcm)
            targethorcmcontent = horcm.substitute(targethorcm)


            self.writehorcmfile(sourceccihorcmfile,sourcehorcmcontent)
            self.writehorcmfile(targetccihorcmfile,targethorcmcontent)

        except Exception as e:
            status = "Failed"
            if 'errmessages' not in errors:
                errors['errmessages'] = []
            errors['errmessages'].append(str(e))

        finally:
            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                    self.logendtask(host=host,taskid=taskid,status=status)

        if len(errors):
            for err in errors['errmessages']:
                log.error(err)
            raise Exception("Unable to create horcm files!")

        return { "sourcehorcmcontent":sourcehorcmcontent, "targethorcmcontent": targethorcmcontent}

    def writehorcmfile(self,horcmfile,content):
        try:
            self.log.info('Writing horcm file {}'.format(horcmfile))
            file = open(horcmfile,"w")
            file.write(content)
        except Exception as e:
            raise Exception('Unable to {}, error \'{}\''.format('writehorcmfile',str(e)))


    def restarthorcminsts(self,instlist,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        status = 'completed'
        storage = self.source

        self.logtaskstart(taskname,host=self.host,taskid=taskid)

        try:
            for horcminst in instlist:
                storage.restarthorcminst(horcminst)
        except Exception as e:
            status = 'Failed'
            message = 'Failed to restart horcm instances, error {}'.format(str(e))
            log.error(message)
            raise Exception(message)
        finally:
            self.logendtask(host=self.host,taskid=taskid,status=status)

    
    def producecapacityreport(self,taskid,askuser=True):

        taskname = inspect.currentframe().f_code.co_name
        taskstatus = { 'status':'','userans':'' }
        useranswer = False
        reference = ''
        jsonin = self.jsonin
        log = self.log
        status = 'completed'

        def processquestion(migratingnodes,singlenode=''):
            nodecounter = 0
            ans = ''
            header = 'Capacity Report > Source Serial {} > Target Serial {} > Migration Group {}'.format(self.config['source_serial'],self.config['target_serial'],self.group)
            headerunderline = ''.join(['-']*len(header)) 
            print('\n{}\n{}\n'.format(header,headerunderline))
            header = 'Migrating host(s):'
            headerunderline = ''.join(['-']*len(header))
            print('{}\n{}\n'.format(header,headerunderline))

            if singlenode:
                nodecounter += 1
                print('\t{}. {:<37}{:>15}  {:<4}'.format(nodecounter,singlenode,'target_ldev_policy:',self.jsonin[self.migrationtype]['migrationgroups'][self.group][singlenode]['target_ldev_policy']))
                print('\t{} {:<33}{:>15}  {:<4}'.format('','','total allocated capacity:',self.jsonin[self.migrationtype]['migrationgroups'][self.group][singlenode]['capacityreport']['total']['total_fullallocationrequirementMB']))
                print('\t{} {:<38}{:>15}  {:<4}'.format('','','total used capacity:',self.jsonin[self.migrationtype]['migrationgroups'][self.group][singlenode]['capacityreport']['total']['total_usedallocationrequirementMB']))
                print('\t{} {:<40}{:>15}  {:<4}\n'.format('','','target pool id(s):',', '.join(self.jsonin[self.migrationtype]['migrationgroups'][self.group][singlenode]['capacityreport']['bypool'].keys())))
                #print('\t{:>58}{:<4}'.format('target_ldev_policy',self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['target_ldev_policy']))
                reference = jsonin[self.migrationtype]['migrationgroups'][self.group][singlenode]['capacityreport']['bypool']
                #reference = jsonin[self.migrationtype]['reports']['capacity_requirements']['bymigrationgroup'][self.group]
            else:
                for node in migratingnodes:
                    nodecounter += 1
                    if not jsonin[self.migrationtype]['migrationgroups'][self.group][node]['omit']:
                        print('\t{}. {:<37}{:>15}  {:<4}'.format(nodecounter,node,'target_ldev_policy:',self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['target_ldev_policy']))
                        print('\t{} {:<33}{:>15}  {:<4}'.format('','','total allocated capacity:',self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['capacityreport']['total']['total_fullallocationrequirementMB']))
                        print('\t{} {:<38}{:>15}  {:<4}'.format('','','total used capacity:',self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['capacityreport']['total']['total_usedallocationrequirementMB']))
                        print('\t{} {:<40}{:>15}  {:<4}\n'.format('','','target pool id(s):',', '.join(self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['capacityreport']['bypool'].keys())))
                        #print('\t{:>58}{:<4}'.format('target_ldev_policy',self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['target_ldev_policy']))
                reference = jsonin[self.migrationtype]['reports']['capacity_requirements']['bymigrationgroup'][self.group]

            print('\t{} {:<37}{:>15}  {:<4}\n'.format('','','Total Hosts in Group:',nodecounter))
            #for poolid in jsonin['gad']['reports']['capacity_requirements']['bymigrationgroup'][group]:
            for poolid in reference:
                abv = reference[poolid]
                capacity_header = 'Capacity Settings'
                headerunderline = ''.join(['-']*len(capacity_header))
                print('\n{}\n{}\n'.format(capacity_header,headerunderline))
                print('\t{:<33}{:>25}'.format('Capacity Saving:',self.config['capacity_saving']))
                if 'compressionratio' in jsonin['gad']['conf']:
                    print('\t{:<33}{:>25}'.format('Compression Ratio:',self.config['compressionratio']))
                print('\nPool: {}'.format(poolid))
                print('\t{:<43}{:>15}'.format('Available Capacity (MB):',abv['availablepoolcapacity']))
                print('\n\t{:<43}{:>15}  {:<4}'.format('Requirement ldev full allocation (MB):',abv['fullallocationrequirementMB'],'OK' if abv['availablepoolcapacity'] >= abv['fullallocationrequirementMB'] else textstyle.FAIL+'FAIL'+textstyle.ENDC))
                print('\t{:<43}{:>15}  {:<4}'.format('Requirement ldev used allocation (MB):',abv['usedallocationrequirementMB'],'OK' if abv['availablepoolcapacity'] >= abv['usedallocationrequirementMB'] else textstyle.FAIL+'FAIL'+textstyle.ENDC))
                print('\t{:<43}{:>15}  {:<4}'.format('Requirement potential capacity saving (MB):',int(abv['potentialcompressedallocationrequirementMB']),'OK' if abv['availablepoolcapacity'] >= int(abv['potentialcompressedallocationrequirementMB']) else textstyle.FAIL+'FAIL'+textstyle.ENDC))
                print('\t{:<43}{:>15}  {:<4}'.format('Requirement current capacity saving (MB):',int(abv['withcurrentcompressionallocationrequirementMB']),'OK' if abv['availablepoolcapacity'] >= int(abv['withcurrentcompressionallocationrequirementMB']) else textstyle.FAIL+'FAIL'+textstyle.ENDC))
            #print('{}'.format(json.dumps(jsonin['gad']['reports']['capacity_requirements']['bymigrationgroup'][group])))
            question = '\tDo you wish to continue (y/n) ?: '
            if askuser:
                while not re.search('^y$|^n$',ans):
                    ans = input('\n{}'.format(question))
                    log.info('User answered {}'.format(ans))
                    if ans == 'y':
                        return { "continue":True }
                    elif ans == 'n':
                        return { "continue":False }
                    else:
                        log.info('Invalid answer')
            else:
                return { "continue":True }
        


        for node in jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if 'omit' in jsonin[self.migrationtype]['migrationgroups'][self.group][node] and jsonin[self.migrationtype]['migrationgroups'][self.group][node]['omit']:
                log.info('Produce capacity report: Skip this host {}, has omit flat set'.format(node))
                continue
            self.logtaskstart(taskname,host=node,taskid=taskid)
        try:
            useranswer = processquestion(self.jsonin[self.migrationtype]['migrationgroups'][self.group])['continue']
        except Exception as e:
            raise Exception('Unable to produce capacity report, error {}'.format(str(e)))
            status = 'Failed'
        else:
            if useranswer:
                taskstatus = { 'status':'completed', 'userans':useranswer }
            else:
                taskstatus = { 'status':'skipped', 'userans':useranswer }
        finally:
            for node in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                if 'omit' in jsonin[self.migrationtype]['migrationgroups'][self.group][node] and jsonin[self.migrationtype]['migrationgroups'][self.group][node]['omit']:
                    continue
                self.logendtask(host=node,taskid=taskid,status=status,extrafields=taskstatus)

        return useranswer

    def exitroutine(self):
        self.log.info('Dumping json to log..')
        self.log.debug(json.dumps(self.jsonin,indent=4,sort_keys=True))
        for instance in self.storages.lockedstorage:
            self.log.info('Unlock storage {}'.format(instance.serial))
            instance.unlockresource()
            self.writeundofile(instance)

            #instance.writeundofile()
        if self.warnings:
            print('\n{}\n\n{} ({})\n'.format('\n'.join(self.warningmessages),self.endmessage,self.warnings))
        print("Goodbye")
        sys.exit((0,self.warnings)[self.warnings > 0])

    def createpairs(self,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        pairrequests = []
        status = 'completed'

        def processpairdisplay(host):
            source_inst = self.config['source_cci_horcm_inst']
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(self.source,copy_grp_name,source_inst,opts='-fcxe').split('\n')))]
            for row in pairdisplayout:
                log.info(row)
            paircreatequestion = 'Paircreate group {} - y/n ? : '.format(copy_grp_name)
            log.info('Input to question required: \'{}\''.format(paircreatequestion))

            print('\n')
            for row in pairdisplayout:
                print('{}'.format(row))
            # Create Pairs
            paircreateans = ""
            while not re.search('^y$|^n$',paircreateans):
                paircreateans = input('\n{}'.format(paircreatequestion))
                log.info('User answered {}'.format(paircreateans))
                if paircreateans == 'y':
                    self.paircreate(self.source,copy_grp_name,source_inst,self.config['quorum'])
                    pairdisplayout = self.pairdisplay(self.source,copy_grp_name,source_inst,opts='-fcxe')
                    print(pairdisplayout)
                    pairrequests.append(host)
                elif paircreateans == 'n':
                    self.logendtask(host=self.host,taskid=taskid,status='skipped')
                    log.info('Skipped paircreate for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')


        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:

            if self.targeted_rollback:
                self.undocmds[self.source.serial]['objects'][host] = self.undocmds[self.source.serial]['objects'].get(host,[])
                setattr(self.source.apis[self.source.useapi], 'undocmds', self.undocmds[self.source.serial]['objects'][host])
                self.log.info("targeted_rollback: {}, tweak api undocmd destination to writeback to storage class".format(self.targeted_rollback))
            
            self.logtaskstart(taskname,host=host,taskid=taskid)
            try:
                processpairdisplay(host)
            except Exception as e:
                status = 'Failed'
                message = 'Unable to create pair, host {}, error \'{}\''.format(host,str(e))
                self.log.error(message)
                raise Exception(message)
            finally:
                self.logendtask(host=host,taskid=taskid,status=status)

        return { "pairrequests":pairrequests }

    def createdrgadpairs(self,taskid):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        pairrequests = []
        status = 'completed'

        def processpairdisplay(host):
            source_inst = self.config['source_cci_horcm_inst']
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(self.source,copy_grp_name,source_inst,opts='-fcxe').split('\n')))]
            for row in pairdisplayout:
                log.info(row)
            paircreatequestion = 'Paircreate group {} - y/n ? : '.format(copy_grp_name)
            log.info('Input to question required: \'{}\''.format(paircreatequestion))

            print('\n')
            for row in pairdisplayout:
                print('{}'.format(row))
            # Create Pairs
            paircreateans = ""
            while not re.search('^y$|^n$',paircreateans):
                paircreateans = input('\n{}'.format(paircreatequestion))
                log.info('User answered {}'.format(paircreateans))
                if paircreateans == 'y':
                    self.paircreate(self.source,copy_grp_name,source_inst,self.config['quorum'])
                    pairdisplayout = self.pairdisplay(self.source,copy_grp_name,source_inst,opts='-fcxe')
                    print(pairdisplayout)
                    pairrequests.append(host)
                elif paircreateans == 'n':
                    self.logendtask(host=self.host,taskid=taskid,status='skipped')
                    log.info('Skipped paircreate for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')


        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            try:
                processpairdisplay(host)
            except Exception as e:
                status = 'Failed'
                message = 'Unable to create pair, host {}, error \'{}\''.format(host,str(e))
                self.log.error(message)
                raise Exception(message)
            finally:
                self.logendtask(host=host,taskid=taskid,status=status)

        return { "pairrequests":pairrequests }


    def monitorpairs(self,pairrequests,taskid):
        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        pairevtwaits = {}

        def processmonitor(host):
            monitorans = ""
            horcmgrp = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name']
            while not re.search('^y$|^n$',monitorans):
                monitorans = input('Monitor horcmgrp paircreate {} - y/n ? : '.format(horcmgrp))
            if monitorans == 'y':
                pairevtwaits[horcmgrp] = { "host":host }
                
            else:
                log.info('Monitoring not required for group {}'.format(horcmgrp))
                self.logtaskstart('monitorpairevtwaits',host=host,taskid=taskid)
                self.logendtask(host=host,taskid=taskid,status='skipped')

        try:
            for host in pairrequests:
                processmonitor(host)
            pairevtwaitmonitor = self.monitorpairevtwaits(self.source,pairevtwaits,'pair',taskid)

        except Exception as e:
            message = 'Unable to monitor pairs, error \'{}\''.format(str(e))
            raise Exception(message)



    # Task functions





    def swaps(self):
        self.log.info("Begin SWAPS process")
        try:
            storage = self.target
            pairevtwaits = self.pairresyncswaps(storage,taskid=1)['pairevtwaits']
            self.monitorpairevtwaits(storage,pairevtwaits,'pair',2)
        except Exception as e:
            raise StorageException('Failed to SWAPS volumes {}'.format(str(e)),Storage,self.log)

    def smpl(self):
        self.log.info("Begin SMPL process")
        try:
            storage = self.target
            pairevtwaits = self.pairsplitR(storage,taskid=1,opts='-R')['pairevtwaits']
            self.monitorpairevtwaits(storage,pairevtwaits,'smpl',2)
        except Exception as e:
            raise StorageException('Failed to SMPL volumes {}'.format(str(e)),Storage,self.log)

    def smplR(self,storagerole,taskid):
        self.log.info("Begin SMPL process")
        storage = getattr(self,storagerole)
        try:
            pairevtwaits = self.pairsplitR(storage,taskid=taskid,opts='-R')['pairevtwaits']
        except Exception as e:
            raise StorageException('Failed to SMPL volumes {}'.format(str(e)),Storage,self.log)
        self.log.info('Successfully executed pairsplit -R')
        return pairevtwaits

    
    def splitRS(self,storagerole,taskid):
        self.log.info("Begin pairsplit -RS (Svol_SSWS)")
        storage = getattr(self,storagerole)
        try:
            pairevtwaits = self.pairsplitRS(storage,taskid=taskid)['pairevtwaits']
        except Exception as e:
            raise StorageException('Failed to pairsplitRS volumes {}'.format(str(e)),Storage,self.log)
        self.log.info('Successfully executed pairsplit -RS')
        return pairevtwaits





    def locatehostgroupgids(self,storage,host,taskid):
        # gadmigration.locatehostgroupgids(target,host,jsonin['gad']['migrationgroups'][group])
        taskname = inspect.currentframe().f_code.co_name
        log = self.log
        uniqueports = {}
        createhostgroups = {}
        lunhashgroups = {}
        groupbyname = {}
        gidgroups = { '0': { 'group':{}}}
        gidgroupcounter = 0
        start = self.now()

        self.logtaskstart(taskname,host=host,taskid=taskid)

        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        for hostgroup in group[host]['hostgroups']:

            if group[host]['hostgroups'][hostgroup]['_create']:
                groupname = group[host]['hostgroups'][hostgroup]['GROUP_NAME']
                targetport = group[host]['hostgroups'][hostgroup]['targetport']
                gidgroups['0']['group'][hostgroup] = targetport
                if targetport not in uniqueports:
                    uniqueports[targetport] = {}
                uniqueports[targetport][hostgroup] = groupname
                createhostgroups[hostgroup] = {}
                if groupname not in groupbyname:
                    groupbyname[groupname] = {}
                groupbyname[groupname][hostgroup] = targetport

        log.debug('groupbyname dict {}'.format(groupbyname))

        if len(uniqueports) == len(createhostgroups):
            log.info('Number of unique target ports {} matches number of required host groups {}'.format(len(uniqueports),len(createhostgroups)))
            log.info('All host groups to have mathcing gid numbers')
            log.debug('gidgroups {}'.format(gidgroups))

        elif len(uniqueports) < len(createhostgroups):
            gidgroups = {}
            log.info('Appears that some host groups reside on the same port, attempt to match up the host groups')
            gidcounter = 0
            gidgroups[gidcounter] = { 'group': {} }
            for port in uniqueports:
                log.info('Number of host groups {} to target port {}, {}'.format(len(uniqueports[port]),port,uniqueports[port]))
            for port in uniqueports:
                if len(uniqueports[port]) == 1:
                    for hg in uniqueports[port]:
                        gidgroups[gidcounter]['group'][hg] = port
                    for hg in uniqueports[port]:
                        groupbyname.pop(uniqueports[port][hg])
                        log.debug(groupbyname)

            for remport in gidgroups[gidcounter]['group']:
                uniqueports.pop(gidgroups[gidcounter]['group'][remport])
            gidcounter += 1

            garbagelist = []
            for uniquename in groupbyname:
                if len(groupbyname[uniquename]) > 1:
                    if gidcounter not in gidgroups:
                        gidgroups[gidcounter] = {'group':{}}
                    for hg in groupbyname[uniquename]:
                        gidgroups[gidcounter]['group'][hg] = groupbyname[uniquename][hg]
                        #gidgroups[gidcounter].append(groupbyname[uniquename][hg])
                    garbagelist.append(uniquename)
                    gidcounter += 1

            for name in garbagelist:
                groupbyname.pop(name)

        log.debug('Ungrouped destination ports: {}'.format(groupbyname))
        log.debug(groupbyname)
        log.debug(gidgroups)

        log.debug('Number of remainder groupbyname {}, groupbyname {}'.format(len(groupbyname),groupbyname))
        for remainderhostgroups in groupbyname:
            for hg in groupbyname[remainderhostgroups]:
                group[host]['hostgroups'][hg]['targetgid'] = "-"

        self.returngids(storage,gidgroups)
        self.populatejsoningids(gidgroups,host)
        log.debug('gidgroups {}'.format(gidgroups))

        self.logendtask(host=host,taskid=taskid,status='completed')

    def returngids(self,storage,gidgroups):
        tracker = {}
        for gidgroup in gidgroups:
            if 'gid' in gidgroups[gidgroup]: continue
            gid = None
            gidmap = [False] * 255
            for hostgroup in gidgroups[gidgroup]['group']:
                port = gidgroups[gidgroup]['group'][hostgroup]
                storage.gethostgrpkeyhostgrprgid(port,0)
                storage.gethostgrp(port)
                #storage.log.info("gethostgrpkeyhostgrprgid {}".format(storage.views['_portskeyhostgrp']))

                if port not in tracker:
                    tracker[port] = [False] * 255
                else:
                    for index, value in enumerate(tracker[port]):
                        if value is True:
                            gidmap[index] = value

                for hsd in storage.views['_portskeyhostgrp'][port]['_host_grps']:
                    if storage.views['_portskeyhostgrp'][port]['_host_grps'][hsd]['GROUP_NAME'] != "-" and storage.views['_portskeyhostgrp'][port]['_host_grps'][hsd]['RSGID'] != '0':
                        gidmap[int(hsd)] = True
                        tracker[port][int(hsd)] = True
                for hsd in storage.views['_ports'][port]['_host_grps']:
                    gidmap[int(hsd)] = True
                    tracker[port][int(hsd)] = True


            for index, value in enumerate(gidmap):
                if value is False:
                    gid = index
                    gidgroups[gidgroup]['gid'] = gid
                    storage.log.debug('Located my gid: {}'.format(gid))
                    break

            for hostgroup in gidgroups[gidgroup]['group']:
                port = gidgroups[gidgroup]['group'][hostgroup]
                tracker[port][gid] = True

        return

    def populatejsoningids(self,gidgroups,host):
        for gidgroup in gidgroups:
            for sourcehostgroup in gidgroups[gidgroup]['group']:
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['hostgroups'][sourcehostgroup]['targetgid'] = gidgroups[gidgroup]['gid']

    def hostgroupexists(self,storage):
        targetportlist = {}
        output = { 'messages':[], 'error': 0}
        jsonin = self.jsonin

        def processhost(storage,host):

            jsonin = self.jsonin
            group = self.group
            hostgroups = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['hostgroups']

            for hostgroup in hostgroups:
                targetportlist[hostgroups[hostgroup]['targetport']] = {}

            targetporthostgroups = storage.gethostgrps(targetportlist,optviews=['hostgroupsbyportname'])['hostgroupsbyportname']
            #targetporthostgroups = storage.gethostgrpsrgid(targetportlist,optviews=['hostgroupsbyportname'])['hostgroupsbyportname']

            for hostgroup in hostgroups:
                port,gid = storage.returnportandgid(hostgroup)
                targetname = hostgroups[hostgroup]['target_group_name']
                targethmobits = hostgroups[hostgroup]['target_hmo_bits']
                self.log.debug('targethmobits: {}'.format(targethmobits))
                targethmd = hostgroups[hostgroup]['target_hmd']
                targetport = hostgroups[hostgroup]['targetport']
                if targetname in targetporthostgroups[targetport]:
                    self.log.warn('Hostgroup name {} already exists on target port {}'.format(targetname,targetport))
                    hostgroups[hostgroup]['_create'] = 0
                    hostgroups[hostgroup]['_exists'] = 1
                    hostgroups[hostgroup]['target_gid'] = targetporthostgroups[port][targetname]['GID']
                    existingtargetgid = targetporthostgroups[port][targetname]['GID']

                    existinghmobits = targetporthostgroups[port][targetname]['HMO_BITs']
                    storage.log.info('Existing hmobits type {}'.format(type(existinghmobits)))
                    existinghmd = targetporthostgroups[port][targetname]['HMD']
                    if collections.Counter(targethmobits) != collections.Counter(existinghmobits):
                        message = 'Target host group HMO_BITs \'{}\' differ from requested \'{}\''.format(existinghmobits,targethmobits)
                        self.log.error(message)
                        output['messages'].append(message)
                        output['error'] = 1
                        hostgroups[hostgroup]['_hmobitserr'] = message
                    if targethmd != existinghmd:
                        message = 'Target HMD \'{}\' differs from requested HMD \'{}\''.format(existinghmd,targethmd)
                        self.log.error(message)
                        output['messages'].append(message)
                        output['error'] = 1
                        hostgroups[hostgroup]['_hmderr'] = message

                    luncount = storage.getlun(port=targetport,gid=existingtargetgid)['metaview']['stats']['luncount']
                    if luncount:
                        message = 'Target host group {}-{} exists and contains {} lun(s)'.format(targetport,existingtargetgid,luncount)
                        output['messages'].append(message)
                        output['error'] = 1

                else:
                    hostgroups[hostgroup]['_create'] = 1


        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                processhost(storage,host)

        if output['error']:
            raise StorageException(str(output['messages']),Storage,self.log)
        self.log.info("Existing host check ended")

    def configurehostgrps(self, storage: object, host: str, taskid: int):

        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.jsonin

        self.logtaskstart(taskname,host=self.host,taskid=taskid)
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]

        for hostgroup in group[host]['hostgroups']:
            targetport = group[host]['hostgroups'][hostgroup]['targetport']
            targetgid = group[host]['hostgroups'][hostgroup]['targetgid']
            targethostgroupname = group[host]['hostgroups'][hostgroup]['target_group_name']
            if targetgid == '-':
                continue
            outcome = storage.addhostgroup(targetport,targethostgroupname,targetgid)
        for hostgroup in group[host]['hostgroups']:
            targetport = group[host]['hostgroups'][hostgroup]['targetport']
            targetgid = group[host]['hostgroups'][hostgroup]['targetgid']
            targethostgroupname = group[host]['hostgroups'][hostgroup]['target_group_name']
            if targetgid == '-':
                outcome = storage.addhostgroup(targetport,targethostgroupname)
        for hostgroup in group[host]['hostgroups']:
            targetport = group[host]['hostgroups'][hostgroup]['targetport']
            targetgid = group[host]['hostgroups'][hostgroup]['targetgid']
            targethostgroupname = group[host]['hostgroups'][hostgroup]['target_group_name']
            targetresourcegroup = group[host]['resource']['resourceGroupName']
            outcome = storage.addhostgrpresource(targetresourcegroup,targetport,targethostgroupname)
        for hostgroup in group[host]['hostgroups']:
            targetport = group[host]['hostgroups'][hostgroup]['targetport']
            targetgid = group[host]['hostgroups'][hostgroup]['targetgid']
            targethostgroupname = group[host]['hostgroups'][hostgroup]['target_group_name']
            targetresourcegroup = group[host]['resource']['resourceGroupName']
            targethmd = group[host]['hostgroups'][hostgroup]['target_hmd']
            targethmobits = group[host]['hostgroups'][hostgroup]['target_hmo_bits']
            outcome = storage.modifyhostgrp(targetport,targethmd,targethostgroupname,host_mode_opt=targethmobits)
        for hostgroup in group[host]['hostgroups']:
            targetport = group[host]['hostgroups'][hostgroup]['targetport']
            targetgid = group[host]['hostgroups'][hostgroup]['targetgid']
            targethostgroupname = group[host]['hostgroups'][hostgroup]['target_group_name']
            for hba_wwn in group[host]['hostgroups'][hostgroup]['WWNS']:
                outcome = storage.addhbawwn(targetport,hba_wwn,targethostgroupname)
                wwn_nickname = group[host]['hostgroups'][hostgroup]['WWNS'][hba_wwn]['NICK_NAME']
                if re.search(r'\w+',wwn_nickname):
                    outcome = storage.addwwnnickname(targetport,hba_wwn,wwn_nickname,targethostgroupname)

        self.logendtask(host=host,taskid=taskid,status='completed')

    def configureldevs(self, storage: object, host: dict, taskid: int):
        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.jsonin
        endstatus = ''
        self.logtaskstart(taskname,host=self.host,taskid=taskid)

        createfunction = getattr(self,'createldev_'+self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['target_ldev_policy'])(storage,host,taskid)

        self.logendtask(host=host,taskid=taskid,status='completed')

    def createldev_match(self,storage,host,taskid):
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.jsonin
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        step = self.step
        checkmatchingout = self.checkmatching(group[host]['ldevs'])

        if checkmatchingout['error']:
            group[host]['steps'][step]['tasks'][taskid]['error'] = 1
            group[host]['steps'][step]['tasks'][taskid]['messages'].append('Unable to {} -> {}'.format(taskname,checkmatchingout))
            for message in checkmatchingout['messages']:
                group[host]['steps'][step]['tasks'][taskid]['messages'].append(message)
                log.info(message)
            self.logendtask(host=host,taskid=taskid,status='Failed')

            #logendstep(jsonin,migrationgroup,host,step,"FAILED")
            raise Exception('Check matching ldevs failed')

        checkldevsfreeout = self.checkldevsfree(storage,group[host]['ldevs'])
        if checkldevsfreeout['error']:
            group[host]['steps'][step]['tasks'][taskid]['error'] = 1
            group[host]['steps'][step]['tasks'][taskid]['messages'].append('Unable to {} -> {}'.format(taskname,checkldevsfreeout))
            for message in checkldevsfreeout['messages']:
                group[host]['steps'][step]['tasks'][taskid]['messages'].append(message)
                log.info(message)
            self.logendtask(host=host,taskid=taskid,status='Failed')
            #logendstep(jsonin,migrationgroup,host,step,"FAILED")
            raise Exception('Check matching ldevs failed')

        try:
            resourcegroupname = group[host]['resource']['resourceGroupName']
            self.unmapldevs(storage,group[host]['ldevs'])
            self.addldevtoresource(storage,group[host]['ldevs'],resourcegroupname)
            self.mapldevreserves(storage,group[host]['ldevs'])
            self.createldevs(storage,group[host]['ldevs'])
            log.info("Successfully created ldevs")
            self.labelldevs(storage,group[host]['ldevs'])
        except Exception as e:
            raise Exception('Unable to {}, error {}'.format(taskname,str(e)))

    def checkmatching(self,ldevs):
        log = self.log
        out = { 'messages': [], 'error':0 }
        for ldevid in ldevs:
            if ldevs[ldevid]['omit']:
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            if str(ldevid) != str(ldevs[ldevid]['target_ldevid']):
                message = 'ldevid \'{}\' and target_ldevid \'{}\' do not match'.format(ldevid,ldevs[ldevid]['target_ldevid'])
                out['messages'].append(message)
                out['error'] = 1
                ldevs[ldevid]['errmessage'] = message
                ldevs[ldevid]['error'] = 1
        return out

    def checkldevsfree(self,storage,ldevs):
        log = self.log

        out = { 'messages': [], 'error': 0 }
        [storage.getldev(ldevs[ldevid]['target_ldevid']) for ldevid in ldevs]
        for ldevid in ldevs:
            if ldevs[ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            targetldevid = str(ldevs[ldevid]['target_ldevid'])
            voltype = storage.views['_ldevs'][targetldevid]['VOL_TYPE']
            rsgid = storage.views['_ldevs'][targetldevid]['RSGID']
            virtualid = storage.views['_ldevs'][targetldevid].get('VIR_LDEV')

            if voltype != 'NOT DEFINED' or rsgid != '0' or virtualid:
                message = 'Target ldevid \'{}\' is not free. VOL_TYPE \'{}\', RSGID \'{}\', VIR_LDEV \'{}\''.format(targetldevid,voltype,rsgid,virtualid)
                log.error(message)
                out['messages'].append(message)
                out['error'] = 1
                ldevs[ldevid]['errmessage'] = message
                ldevs[ldevid]['error'] = 1
        return out

    def unmapldevs(self,storage,ldevs):
        log = self.log
        for ldevid in ldevs:
            if ldevs[ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            #log.info(storage.getldev(ldevid))
            ldev = str(ldevid)
            targetldevid = ldevs[ldev]['target_ldevid']
            storage.log.info('Storage array {} - Source ldevid {}, unmap target ldev {}'.format(storage.serial,ldev,targetldevid))
            storage.unmapldev(targetldevid,targetldevid)

    def mapldevs(self,storage,ldevs):
        log = self.log
        for targetldevid in ldevs:
            virtualldevid = ldevs[targetldevid]
            storage.log.info('Storage array {} - Source ldevid {}, map target ldev {}'.format(storage.serial,targetldevid,virtualldevid))
            storage.mapldev(targetldevid,virtualldevid)
    
    def addldevtoresource(self,storage,ldevs,resourcegroupname):
        log = self.log
        for ldevid in ldevs:
            if ldevs[ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            ldev = str(ldevid)
            targetldevid = ldevs[ldev]['target_ldevid']
            log.info('Storage array {} - Source ldevid {}, unmap target ldev {}'.format(storage.serial,ldev,targetldevid))
            storage.addldevresource(resourcegroupname,targetldevid)

    def createldevs(self,storage,ldevs):
        storage.apis[storage.useapi].undocmds.insert(0,"sleep 20")
        log = self.log

        for ldevid in ldevs:
            if ldevs[ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            ldev = str(ldevid)
            targetldevid = ldevs[ldev]['target_ldevid']
            targetpool = ldevs[ldev]['target_poolid']
            capacity = ldevs[ldev]['VOL_Capacity(BLK)']
            log.info('Storage array {} - Source ldevid {}, add target ldev {} poolid {} capacity {}'.format(storage.serial,ldev,targetldevid,targetpool,capacity))
            storage.addldev(targetldevid,targetpool,capacity)

    def labelldevs(self,storage,ldevs):

        for ldevid in ldevs:
            if ldevs[ldevid].get('omit',False):
                storage.log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue

            targetldevid = ldevs[ldevid]['target_ldevid']
            if 'target_ldev_naming' in ldevs[ldevid]:
                targetlabel = ldevs[ldevid]['target_ldev_naming']
                if re.search(r'\w', targetlabel):
                    storage.modifyldevname(targetldevid,targetlabel)
                else:
                    storage.log.info('Target ldevid \'{}\' label not present in json'.format(targetldevid))
            else:
                storage.log.info('Target ldevid \'{}\' label key not present in json'.format(targetldevid))

    def modifyldevcapacitysaving(self, storage: object, host: dict, capacity_saving: str, taskid: int):

        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        ldevs = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs']

        self.logtaskstart(taskname,host=self.host,taskid=taskid)
        appendtotask = { 'ldevsaffected': 0 }

        if capacity_saving != 'disable':
            for ldevid in ldevs:
                if ldevs[ldevid].get('omit',False):
                    log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                    continue
                ldev = str(ldevid)
                targetldevid = ldevs[ldev]['target_ldevid']
                storage.modifyldevcapacitysaving(targetldevid,capacity_saving)
                appendtotask['ldevsaffected'] += 1

        self.logendtask(host=host,taskid=taskid,status='completed',extrafields=appendtotask)


    def configureluns(self, storage: object, host: dict, taskid: int):
        log = self.log
        start = self.now()
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname,host=self.host,taskid=taskid)

        migrationgroup = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        for hostgroup in migrationgroup[host]['hostgroups']:
            targetport = migrationgroup[host]['hostgroups'][hostgroup]['targetport']
            targethostgroupname = migrationgroup[host]['hostgroups'][hostgroup]['target_group_name']
            for lunid in migrationgroup[host]['hostgroups'][hostgroup]['LUNS']:
                sourceldevid = migrationgroup[host]['hostgroups'][hostgroup]['LUNS'][lunid]['LDEV']
                targetldevid = migrationgroup[host]['ldevs'][sourceldevid]['target_ldevid']
                if migrationgroup[host]['ldevs'][sourceldevid]['omit']:
                    log.info('Ldevid omitted {}, omit flag {}, skip lun mapping'.format(sourceldevid,migrationgroup[host]['ldevs'][sourceldevid]['omit']))
                    continue
                storage.addlun(targetport,targetldevid,lunid,targethostgroupname)

        self.logendtask(host=host,taskid=taskid,status='completed')

    def muselect(self,availablemus):

        # This muselect is relevant to the GAD migration, it is essentially temporary but this could be used to other GAD therefore 0 is preferred.
        muPreferenceOrder = [0,2,3,1]
        for mu in muPreferenceOrder:
            if str(mu) in availablemus:
                return str(mu)

    def configurecopygrps(self, sourcestorage, targetstorage, host, taskid: int,skipontrue='REMOTE_FLAG'):

        maxcopygrpnamelength = 30
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname,host=self.host,taskid=taskid,storage=sourcestorage)

        # Default mirror unit = 0 for GAD
        mu = "0"
        sourceserial = sourcestorage.serial
        targetserial = targetstorage.serial
        copygrpnamea = self.group
        if 'copygrp_prefix' in self.config and re.search(r'\w',self.config['copygrp_prefix']):
            copygrpprefix = self.config['copygrp_prefix']
            copygrpnamea = '{}.{}'.format(copygrpprefix,self.group)
        maxhostnamelen = maxcopygrpnamelength - len(copygrpnamea)
        hostnamelen = len(host)
        #copygrouphostname = host[:maxhostnamelen]
        copygrouphostname = host
        # Quick and not infallible method for truncating copy group name to 30 chars
        copygrpname = '{}.{}'.format(copygrpnamea,copygrouphostname)
        if len(copygrpname) > 30:
            copygrpname = copygrpname[:20]+str(uuid.uuid4().hex.upper()[0:10])

        devicegrpname = copygrpname
        self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['copy_grp_name'] = copygrpname
        self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['device_grp_name'] = devicegrpname

        if 'mu_free' in self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]:
            log.info("mu_free key present, migrating object must be associated with replicated volumes, selecting free mirror unit")
            mu = self.muselect(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['mu_free'])
            self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['mu_migration'] = mu
            self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['mu_free'].pop(mu, None)
            self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['mu_used'][mu] = copygrpname
            self.log.info("Using mirror_unit number {}".format(mu))

        for ldevid in self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs']:
            
            if self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs'][ldevid]['omit']:
                log.info('Ldevid omitted {}, omit flag {}, skip adding device grp'.format(ldevid,self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs'][ldevid]['omit']))
                continue
            targetldevid = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs'][ldevid]['target_ldevid']
            devicename = 'src_{}_{}_targ_{}_{}'.format(ldevid,sourcestorage.returnldevid(ldevid)['culdev'],targetldevid,sourcestorage.returnldevid(targetldevid)['culdev'])
            self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs'][ldevid]['device_grp_name'] = devicegrpname
            self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs'][ldevid]['device_name'] = devicename
            sourcestorage.adddevicegrp(devicegrpname,devicename,ldevid)
            targetstorage.adddevicegrp(devicegrpname,devicename,targetldevid)

        if len(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs'].keys()):
            sourcestorage.addcopygrp(copygrpname,devicegrpname,mu)
            targetstorage.addcopygrp(copygrpname,devicegrpname,mu)

        self.logendtask(host=host,taskid=taskid,status='completed',storage=sourcestorage)

    def createldev_ldevrange(self,storage,host,taskid):
    
        log = self.log
        jsonin = self.jsonin
        ldevstart = self.config['ldevstart']
        ldevend = self.config['ldevend']
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        resourcegroupname = group[host]['resource']['resourceGroupName']

        self.autocreateldevs(storage,group[host]['ldevs'],ldevstart,ldevend)
        self.unmapldevs(storage,group[host]['ldevs'])
        self.addldevtoresource(storage,group[host]['ldevs'],resourcegroupname)
        self.mapldevreserves(storage,group[host]['ldevs'])
        self.labelldevs(storage,group[host]['ldevs'])

    def createldev_matchfallback(self,storage,host,taskid):

        log = storage.log
        jsonin = storage.jsonin
        ldevstart = self.config['ldevstart']
        ldevend = self.config['ldevend']
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        resourcegroupname = group[host]['resource']['resourceGroupName']

        self.creatematchingldevorfallback(storage,group[host]['ldevs'],ldevstart,ldevend)
        self.unmapldevs(storage,group[host]['ldevs'])
        self.addldevtoresource(storage,group[host]['ldevs'],resourcegroupname)
        self.mapldevreserves(storage,group[host]['ldevs'])
        self.labelldevs(storage,group[host]['ldevs'])

    def checkoffset(self,offset,ldevs,storage):
        log = self.log
        out = { 'messages': [], 'error':0 }
        for ldevid in ldevs:
            if ldevs[ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            
            targetldevid = int(offset) + int(ldevid)
            if ldevs[ldevid]['target_ldevid'] != targetldevid:
                message = 'Input ldevid \'{}\' and target_ldevid \'{}\' do not comply with given offset \'{}\''.format(ldevid,ldevs[ldevid]['target_ldevid'],offset)
                out['messages'].append(message)
                out['error'] = 1
                ldevs[ldevid]['errmessage'] = message
                ldevs[ldevid]['error'] = 1

        return out

    def createldev_ldevoffset(self,storage,host,taskid):

        log = self.log
        jsonin = self.jsonin
        taskname = inspect.currentframe().f_code.co_name
        resourcegroupname = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['resource']['resourceGroupName']
        ldevoffset = self.config['ldevoffset']
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]

        checkoffsetreturn = self.checkoffset(ldevoffset,group[host]['ldevs'],storage)
        if checkoffsetreturn['error']:
            group[host]['steps'][step]['tasks'][taskid]['error'] = 1
            group[host]['steps'][step]['tasks'][taskid]['messages'].append('Unable to {} -> {}'.format(taskname,checkoffsetreturn))
            for message in checkoffsetreturn['messages']:
                group[host]['steps'][step]['tasks'][taskid]['messages'].append(message)
                log.info(message)
            self.logendtask(host=host,taskid=taskid,status='Failed')
            #logendstep(jsonin,migrationgroup,host,step,"FAILED")
            raise Exception('Unable to create ldevs with given offset')

        checkldevsfreeout = self.checkldevsfree(storage,group[host]['ldevs'])
        if checkldevsfreeout['error']:
            group[host]['steps'][step]['tasks'][taskid]['error'] = 1
            group[host]['steps'][step]['tasks'][taskid]['messages'].append('Unable to {} -> {}'.format(taskname,checkldevsfreeout))
            for message in checkldevsfreeout['messages']:
                group[host]['steps'][step]['tasks'][taskid]['messages'].append(message)
                log.info(message)
            self.logendtask(host=host,taskid=taskid,status='Failed')
            #logendstep(jsonin,migrationgroup,host,step,"FAILED")
            raise Exception('Unable to create ldevs with given offset')

        log.info('Number of ldevs to create: {}'.format(len(group[host]['ldevs'].keys() )))

        self.unmapldevs(storage,group[host]['ldevs'])
        self.addldevtoresource(storage,group[host]['ldevs'],resourcegroupname)
        self.mapldevreserves(storage,group[host]['ldevs'])
        self.createldevs(storage,group[host]['ldevs'])
        self.labelldevs(storage,group[host]['ldevs'])
        log.info('Successfully created {} ldevs with offset'.format(len(group[host]['ldevs'].keys() )))

    def autocreateldevs(self,storage,ldevs,ldevstart,ldevend):
        storage.apis[storage.useapi].undocmds.insert(0,"sleep 20")
        jsonin = storage.jsonin
        for ldevid in ldevs:
            if ldevs[ldevid]['omit']:
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            ldev = str(ldevid)
            targetpool = ldevs[ldev]['target_poolid']
            capacityblk = ldevs[ldev]['VOL_Capacity(BLK)']
            targetldevid = storage.addldevauto(poolid=targetpool,capacityblk=capacityblk,resource_id=0,start=ldevstart,end=ldevend)['autoldevid']
            ldevs[ldevid]['target_ldevid'] = targetldevid
            storage.log.info('Storage array {} - Source ldevid {}, autoadded target ldev {} poolid {} capacity {}'.format(storage.serial,ldev,targetldevid,targetpool,capacityblk))

    def creatematchingldevorfallback(self,storage,ldevs,ldevstart,ldevend):
        storage.apis[storage.useapi].undocmds.insert(0,"sleep 20")
        jsonin = self.jsonin
        log = self.log
        for ldevid in ldevs:
            if ldevs[ldevid]['omit']:
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            targetpool = ldevs[ldevid]['target_poolid']
            capacityblk = ldevs[ldevid]['VOL_Capacity(BLK)']
            targetldevid = ldevid

            checkldevfreeout = self.checkldevfree(storage,targetldevid,log)
            if not checkldevfreeout[targetldevid]:
                log.info('targetldevid is free {}, allocate ldevid - checked returned {}'.format(targetldevid,checkldevfreeout))
                storage.addldev(targetldevid,targetpool,capacityblk)
                ldevs[ldevid]['target_ldevid'] = targetldevid
            else:
                log.info('targetldevid is NOT free {}, fallback to autoallocate from range - check returned {}'.format(targetldevid,checkldevfreeout))
                targetldevid = storage.addldevauto(poolid=targetpool,capacityblk=capacityblk,resource_id=0,start=ldevstart,end=ldevend)['autoldevid']
                log.info('Auto allocated ldevid {}'.format(targetldevid))
                ldevs[ldevid]['target_ldevid'] = targetldevid

    def checkldevfree(self,storage,ldevid,log):

        log = self.log

        returnout = { ldevid: 1 }
        storage.getldev(ldevid)
        voltype = storage.views['_ldevs'][ldevid]['VOL_TYPE']
        rsgid = storage.views['_ldevs'][ldevid]['RSGID']
        if voltype == 'NOT DEFINED' and rsgid == '0':
            message = 'Target ldevid \'{}\' is free. VOL_TYPE \'{}\', RSGID \'{}\''.format(ldevid,voltype,rsgid)
            log.info(message)
            returnout = { ldevid: 0 }
        return returnout

    def createvsms(self,storage):
        vsms = {}
        try:

            for group in self.jsonin[self.migrationtype]['migrationgroups']:
                for host in self.jsonin[self.migrationtype]['migrationgroups'][group]:
                    vsms[self.jsonin[self.migrationtype]['migrationgroups'][group][host]['resource']['resourceGroupName']] = { "virtualSerialNumber": self.jsonin[self.migrationtype]['migrationgroups'][group][host]['resource']['virtualSerialNumber'], "virtualModel": self.jsonin[self.migrationtype]['migrationgroups'][group][host]['resource']['virtualModel'] }
            for vsm in vsms:
                self.log.info('Create vsm \'{}\' vsmserial \'{}\' vsmtype \'{}\''.format(vsm,vsms[vsm]['virtualSerialNumber'],vsms[vsm]['virtualModel']))
        
            # Create virtual machines
            self.log.debug('Number of vsms to create: {}'.format(len(vsms)))
            storage.addvsms(vsms)
        except Exception as e:
            raise StorageException('Failed to create vsm(s) {}'.format(str(e)),Storage,self.log,migration=self)

    def usermessage(self,messagekey,acknowledge=False,justreturn=False):
        language = self.env.language
        messenger = messaging(language,self.log)
        message = messenger.message(messagekey)
        if justreturn:
            return message
        self.log.debug('message to user \'{}\''.format(message))
        print('\n{}'.format(message))

        if acknowledge:
            question = messenger.message('acknowledgenextstep')
            ans = ""
            while not re.search('^n$|^y$',ans,re.IGNORECASE):
                ans = input('\n{} (y|n): '.format(question))
                print ('\n')
                if ans == 'y' or ans == 'Y':
                    self.log.info('Acknowledged {}'.format(ans))
                if ans == 'n' or ans == 'N':
                    self.log.info('User declined {}'.format(ans))
                    self.exitroutine()
                else:
                    self.log.info('User answered {}'.format(ans))

    def quietcapacityreport(self):
        # Cheat a capacity report out for create report ;)
        try:
            self.jsonin = self.migrationjson
            for group in self.jsonin[self.migrationtype]['migrationgroups']:
                self.group = group
                self.producecapacityreport(0,False)
                print('\n\n')
        except Exception as e:
            pass 


    def checkldevs(self, storage: object, host: dict):
        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.jsonin
        endstatus = ''
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        ldevpolicy = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['target_ldev_policy']
        errorflag = 0

        log.info("Checkldevs host {} policy {}".format(host,ldevpolicy))

        def checkldevs_ldevoffset():
            ldevoffset = self.config['ldevoffset']
            log.info("Check offset")
            checkoffsetreturn = self.checkoffset(ldevoffset,group[host]['ldevs'],storage)
            log.info("Check ldevs free")
            checkldevsfreeout = self.checkldevsfree(storage,group[host]['ldevs'])
            if checkoffsetreturn['error'] or checkldevsfreeout['error']:
                return 1

        def checkldevs_match():
            checkldevsfreeout = self.checkldevsfree(storage,group[host]['ldevs'])
            if checkldevsfreeout['error']:
                return 1

        def checkldevs_matchfallback():
            return

        def checkldevs_ldevrange():
            return

        checkfunctions = {'ldevoffset':checkldevs_ldevoffset, 'match':checkldevs_match, 'matchfallback':checkldevs_matchfallback, 'ldevrange':checkldevs_ldevrange }
        errorflag = checkfunctions[ldevpolicy]()
            #createfunction = getattr(self.checkldevs,'checkldevs_'+ldevpolicy)(storage,host)

        if errorflag:
            log.error("Identified problems with requested host {} ldevs".format(host))
            raise Exception("Identified problems with requested host {} ldevs".format(host))

    def discoveredgestorage(self):

        jsonin = self.migrationjson
        migrationtype = self.migrationtype
        hostgroupreturnkeys = ['PORT','GID','GROUP_NAME','Serial#','HMD','HMO_BITs','RSGID']
        hbaflag = False
        edgestorages = {}
        

        if not self.config.get('edge_storage_discovery'):
            self.log.info("Edge storage discovery NOT authorised")
            return
        
        if not self.edgestorage:
            self.log.info("NO Edge storage detected")
            return

        self.log.info("Obtaining edge storage logical device info")
        migrationgroups = jsonin[migrationtype]['migrationgroups']
        
        for group in migrationgroups:
            for host in migrationgroups[group]:
                remotehostgroups = {}
                resourcekeys = {}
                for ldev in migrationgroups[group][host]['ldevs']:
                    if 'remote_replication' in migrationgroups[group][host]['ldevs'][ldev]:
                        remoteserial = migrationgroups[group][host]['ldevs'][ldev]['P-Seq#']
                        remoteldevid = migrationgroups[group][host]['ldevs'][ldev]['P-LDEV#']
                        replicationtype = migrationgroups[group][host]['ldevs'][ldev]['remote_replication']
                        remoteldevdetail = self.getldev(self.edgestoragearrays[remoteserial]['storage'],remoteldevid)
                        remoteldevdetail['source_ldevid'] = ldev
                        remoteldevdetail['source_culdev'] = self.target.returnldevid(ldev)['culdev']
                        migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['ldevs'][remoteldevid] = remoteldevdetail
                        migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['ldevs'][remoteldevid]['REMOTE_FLAG'] = True
                        ldevrsgid = migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['ldevs'][remoteldevid]['RSGID']
                        resourcekeys[ldevrsgid] = ldevrsgid
                        migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['resource']['resourceGroupId'] = ldevrsgid

                        self.log.debug(remoteldevdetail)

                        for port in list(remoteldevdetail['PORTs'].keys()):
                            remotehostgroups[port] = remoteserial

                for hostgroup in remotehostgroups:
                    port,gid = self.returnportgid(hostgroup)
                    storage = self.edgestoragearrays[remotehostgroups[hostgroup]]['storage']
                    self.writestoragerole(storage,"edgestorage")
                    #storagehostgrps = storage.gethostgrp(port=port)
                    #groups[groupid][host]['hostgroups'][hostgroup] = groups[groupid][host]['hostgroups'].get(hostgroup, self.gethostgroupresourcegroupids(storage,port,gid,returnkeys=hostgroupreturnkeys))
                    #self.log.info(storagehostgrps)
                    migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup] = self.gethostgroupresourcegroupids(storage,port,gid,returnkeys=hostgroupreturnkeys)
                    migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup]['LUNS'] = migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup].get('LUNS', {})
                    migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup]['WWNS'] = migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup].get('WWNS', {})
                    hostgroupluns = self.edgestoragearrays[remotehostgroups[hostgroup]]['storage'].getluns(ports=[hostgroup])
                    rsgid = migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup]['RSGID']
                    migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['resource']['resourceGroupId'] = rsgid
                    resourcekeys[rsgid] = rsgid

                    try:
                        hostgroupluns = storage.views['_ports'][port]['_host_grps'][gid]['_luns']
                    except:
                        hostgroupluns = []

                    if len(hostgroupluns):
                        for lun in hostgroupluns:
                            luns = migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup]['LUNS']
                            ldevid = hostgroupluns[lun]['LDEV']
                            storage.log.debug('LDEV: {}'.format(ldevid))
                            luns[lun] = {}
                            luns[lun]['LDEV'] = ldevid
                            luns[lun]['CULDEV'] = storage.returnldevid(hostgroupluns[lun]['LDEV'])['culdev']
                            luns[lun]['CM'] = hostgroupluns[lun]['CM']
                            luns[lun]['OPKMA'] = hostgroupluns[lun]['OPKMA']

                    hbawwn = storage.gethbawwns(ports=[hostgroup])
                    
                    #groups[groupid][host]['hostgroups'][hostgroup]['WWNS'] = groups[groupid][host]['hostgroups'][hostgroup].get('WWNS', {})
                    #groups[groupid][host]['hostgroups'][hostgroup]['WWNS'][wwn] = { 'NICK_NAME': storage.views[requiredcustomviews.wwn_view][wwn][hostgroup]['NICK_NAME'] }
                    # "WWNS": {                                "1000000000000080": {                                    "NICK_NAME": "-"                                }
                    if '_hba_wwns' in storage.views['_ports'][port]['_host_grps'][gid]:
                        hbaflag = True
                        edgestorages[remotehostgroups[hostgroup]] = True
                        for wwn in storage.views['_ports'][port]['_host_grps'][gid]['_hba_wwns']:
                            migrationgroups[group][host]['remote_replication'][replicationtype]['targets'][remoteserial]['hostgroups'][hostgroup]['WWNS'][wwn] = { 'NICK_NAME': storage.views['_ports'][port]['_host_grps'][gid]['_hba_wwns'][wwn]['NICK_NAME']}

                if len(resourcekeys) > 1:
                    raise Exception('Unable to handle edge remote_replicated hosts which are associated with more than one resource group'.format(list[resourcekeys.keys()]))

        if hbaflag:
            if self.config.get('edge_storage_discovery_wwn',False):
                self.log.info("User requested edge wwn discovery... please be patient and bring a physical cmd device")
                for storageserial in edgestorages:
                    storage = self.edgestoragearrays[storageserial]['storage']
                    self.indexhbawwns(storage)
                    self.edgepopulatemigrationjson(storage)

    def edgepopulatemigrationjson(self, storage: object):
        
        conf = self.config
        startgroup = conf.get('migration_group_start')
        hostspergroup = conf.get('migration_objects_per_group')
        log = self.log.debug
        groupid = '{}.{}.{}'.format(conf['source_serial'],conf['target_serial'],startgroup)
        groups = { groupid: {}}
        
        hostpergroupcounter = 0
        ldevpopkeys = ['CL','EXP_SPACE','FLA(MB)','F_POOLID','LDEV','MP#','NUM_PORT','OPE_RATE','OPE_TYPE','PORTs','PWSV_S','RSV(MB)','SL','CMP']
        hostgroupreturnkeys = ['PORT','GID','GROUP_NAME','Serial#','HMD','HMO_BITs','RSGID']


        # Obtain a list of wwns from the remote host groups discovered earlier

        jsonin = self.migrationjson
        jsonincopy = copy.deepcopy(jsonin)
        for migrationgroup in jsonincopy[self.migrationtype]['migrationgroups']:
            for host in jsonincopy[self.migrationtype]['migrationgroups'][migrationgroup]:       
                for remote_replication in jsonincopy[self.migrationtype]['migrationgroups'][migrationgroup][host]['remote_replication']:
                    for target in jsonincopy[self.migrationtype]['migrationgroups'][migrationgroup][host]['remote_replication'][remote_replication]['targets']:
                        resourcekeys = {}
                        wwnsdetected = 0
                        shortcut = jsonin[self.migrationtype]['migrationgroups'][migrationgroup][host]['remote_replication'][remote_replication]['targets'][target]
                        copyshortcut = jsonincopy[self.migrationtype]['migrationgroups'][migrationgroup][host]['remote_replication'][remote_replication]['targets'][target]
                        for hostgroup in copyshortcut['hostgroups']:

                            for wwn in copyshortcut['hostgroups'][hostgroup]['WWNS']:
                                if wwn.lower() in storage.views[requiredcustomviews.wwn_view]:
                                    for indexedhostgroup in storage.views[requiredcustomviews.wwn_view][wwn]:
                                        if hostgroup == indexedhostgroup:
                                            for lun in copyshortcut['hostgroups'][indexedhostgroup]['LUNS']:
                                                _ldevid = copyshortcut['hostgroups'][indexedhostgroup]['LUNS'][lun]['LDEV']
                                                shortcut['ldevs'][_ldevid] = shortcut['ldevs'].get(_ldevid, self.getldev(storage,_ldevid,popkeys=ldevpopkeys))
                                            continue
                                        wwnsdetected += 1
                                        log('Located edgehost {} wwn {} in storage array {} in hostgroup {}'.format(host,wwn,storage.serial,indexedhostgroup))
                                        port,gid = self.returnportgid(indexedhostgroup)
                                        shortcut['hostgroups'][indexedhostgroup] = shortcut['hostgroups'].get(indexedhostgroup,self.gethostgroupresourcegroupids(storage,port,gid,returnkeys=hostgroupreturnkeys))
                                        shortcut['hostgroups'][indexedhostgroup]['WWNS'] = shortcut['hostgroups'][indexedhostgroup].get('WWNS',{})
                                        shortcut['hostgroups'][indexedhostgroup]['LUNS'] = shortcut['hostgroups'][indexedhostgroup].get('LUNS',{})
                                        shortcut['hostgroups'][indexedhostgroup]['WWNS'][wwn] = { 'NICK_NAME': storage.views[requiredcustomviews.wwn_view][wwn][indexedhostgroup]['NICK_NAME'] }
                                        rsgid = shortcut['hostgroups'][indexedhostgroup]['RSGID']
                                        shortcut['resource']['resourceGroupId'] = rsgid
                                        resourcekeys[rsgid] = rsgid
                                        storage.getluns(ports=[indexedhostgroup])

                                        #groups[groupid][host] = groups[groupid].get(host, { "hostgroups": {}, "ldevs": {}, "resource": {}, "steps": {}, "edgesteps": {}, "omit": False })
                                        #for hostgroup in storage.views[requiredcustomviews.wwn_view][wwn]:
                                        #port,gid = self.returnportgid(hostgroup)
                                        #groups[groupid][host]['hostgroups'][hostgroup] = groups[groupid][host]['hostgroups'].get(hostgroup, self.gethostgroupresourcegroupids(storage,port,gid,returnkeys=hostgroupreturnkeys))
                                        #groups[groupid][host]['hostgroups'][hostgroup]['WWNS'] = groups[groupid][host]['hostgroups'][hostgroup].get('WWNS', {})
                                        #groups[groupid][host]['hostgroups'][hostgroup]['LUNS'] = groups[groupid][host]['hostgroups'][hostgroup].get('LUNS', {})
                                        #groups[groupid][host]['hostgroups'][hostgroup]['WWNS'][wwn] = { 'NICK_NAME': storage.views[requiredcustomviews.wwn_view][wwn][hostgroup]['NICK_NAME'] }
                                        #rsgid = groups[groupid][host]['hostgroups'][hostgroup]['RSGID']
                                        #groups[groupid][host]['resource']['resourceGroupId'] = rsgid
                                        #resourcekeys[rsgid] = rsgid
                                        #storage.getluns(ports=[hostgroup])

                                        try:
                                            hostgroupluns = storage.views['_ports'][port]['_host_grps'][gid]['_luns']
                                        except:
                                            hostgroupluns = []

                                        if len(hostgroupluns):
                                            for lun in hostgroupluns:
                                                luns = shortcut['hostgroups'][indexedhostgroup]['LUNS']
                                                ldevid = hostgroupluns[lun]['LDEV']
                                                storage.log.debug('LDEV: {}'.format(ldevid))
                                                luns[lun] = {}
                                                luns[lun]['LDEV'] = ldevid
                                                luns[lun]['CULDEV'] = storage.returnldevid(hostgroupluns[lun]['LDEV'])['culdev']
                                                luns[lun]['CM'] = hostgroupluns[lun]['CM']
                                                luns[lun]['OPKMA'] = hostgroupluns[lun]['OPKMA']

                                                shortcut['ldevs'][ldevid] = shortcut['ldevs'].get(ldevid, self.getldev(storage,ldevid,popkeys=ldevpopkeys))

                                else:
                                    log('That is mighty strange, wwn: {} must be on this storage {} ??'.format(wwn,storage.serial))
            
            '''
            if wwnsdetected:
                self.hostcount += 1
                hostpergroupcounter += 1
                if hostpergroupcounter == hostspergroup:
                    hostpergroupcounter = 0
                    startgroup += 1
                if len(groups[groupid][host]['ldevs']):
                    for ldev in groups[groupid][host]['ldevs']:
                        ldevrsgid = groups[groupid][host]['ldevs'][ldev]['RSGID']
                        groups[groupid][host]['resource']['resourceGroupId'] = rsgid
                        resourcekeys[rsgid] = rsgid       
            else:
                continue
            
            if len(resourcekeys) > 1:
                raise Exception('Unable to handle hosts which are associated with more than one resource group'.format(list[resourcekeys.keys()]))
        if self.hostcount < 1:
            self.log.info("None of the supplied wwns could be located, migrating host count: 0")
            sys.exit(0)

        self.migrationjson[self.migrationtype]['migrationgroups'] = groups
        self.migrationjson[self.migrationtype]['storage'] = {}
        '''

    def lockresource(self,storage):
        if self.targeted_rollback:
            setattr(storage.apis[storage.useapi], 'undocmds', self.undocmds[storage.serial]['pre'])
            self.log.info("targeted_rollback: {}, tweak api undocmd destination to writeback to storage class".format(self.targeted_rollback))    
        storage.lockresource()

    def unlockresource(self,storage):
        if self.targeted_rollback:
            setattr(storage.apis[storage.useapi], 'undocmds', self.undocmds[storage.serial]['post'])
            self.log.info("targeted_rollback: {}, tweak api undocmd destination to writeback to storage class".format(self.targeted_rollback))    
        storage.unlockresource()

    def writeundofile(self,storage):
        
        if self.targeted_rollback:
            undodir = '{}{}{}'.format(self.undodir,os.sep,self.group)
            undofile = '{}{}{}.{}.{}.sh'.format(undodir,os.sep,self.scriptname,self.start,storage.serial)
            self.createdir(undodir)
            with open(undofile,"a") as undofile_handler:
                for postcmd in self.undocmds[storage.serial]['post']:
                    undofile_handler.write('{}\n'.format(postcmd))

            for host in self.undocmds[storage.serial]['objects']:
                undocmds = self.undocmds[storage.serial]['objects'][host]
                if len(undocmds):
                    with open(undofile,"a") as undofile_handler:
                        for undocmd in undocmds:
                            undofile_handler.write('{}\n'.format(undocmd))
            
            with open(undofile,"a") as undofile_handler:
                for precmd in self.undocmds[storage.serial]['pre']:
                    undofile_handler.write('{}\n'.format(precmd))

            for host in self.undocmds[storage.serial]['objects']:
                hostundodir = '{}{}{}'.format(undodir,os.sep,host)
                self.createdir(hostundodir)
                undofile = '{}{}{}.{}.{}.sh'.format(hostundodir,os.sep,self.scriptname,self.start,storage.serial)
                if len(undocmds):
                    cmds = self.undocmds[storage.serial]['post'] + undocmds + self.undocmds[storage.serial]['pre']
                    with open(undofile,"w") as undofile_handler:
                        for undocmd in cmds:
                            undofile_handler.write('{}\n'.format(undocmd))                            
        else:
            storage.writeundofile()

    def setpostcleanupdir(self,storage,postcleanupdir):
        if self.targeted_rollback:
            self.postcleanupdir = '{}{}{}{}{}'.format(self.basedir,os.sep,'postcleanup',os.sep,self.group)
        else:
            storage.setpostcleanupdir(postcleanupdir)

    def setpostcleanupfile(self,storage,postcleanupfile):
        if not self.targeted_rollback:
            storage.setpostcleanupfile(self,postcleanupfile)

    def writepostcleanupfile(self,storage,postcleanupcmdregex):
        
        if self.targeted_rollback:
            self.log.debug('Regex {}'.format(postcleanupcmdregex))
            cleanupdir = self.postcleanupdir
            self.createdir(cleanupdir)
            cleanupfile = '{}{}{}.{}.{}.sh'.format(cleanupdir,os.sep,self.scriptname,self.start,storage.serial)
            with open(cleanupfile,"a") as file_handler:
                for postcmd in self.undocmds[storage.serial]['post']:
                    if re.search(postcleanupcmdregex, postcmd):
                        file_handler.write('{}\n'.format(postcmd))

            for host in self.undocmds[storage.serial]['objects']:
                undocmds = self.undocmds[storage.serial]['objects'][host]
                if len(undocmds):
                    with open(cleanupfile,"a") as file_handler:
                        for undocmd in undocmds:
                            if re.search(postcleanupcmdregex, undocmd):
                                file_handler.write('{}\n'.format(undocmd))
            
            with open(cleanupfile,"a") as file_handler:
                for precmd in self.undocmds[storage.serial]['pre']:
                    if re.search(postcleanupcmdregex, precmd):
                        file_handler.write('{}\n'.format(precmd))

            for host in self.undocmds[storage.serial]['objects']:
                hostcleanupdir = '{}{}{}'.format(cleanupdir,os.sep,host)
                self.createdir(hostcleanupdir)
                cleanupfile = '{}{}{}.{}.{}.sh'.format(hostcleanupdir,os.sep,self.scriptname,self.start,storage.serial)
                if len(undocmds):
                    cmds = self.undocmds[storage.serial]['post'] + undocmds + self.undocmds[storage.serial]['pre']
                    with open(cleanupfile,"w") as file_handler:
                        for undocmd in cmds:
                            if re.search(postcleanupcmdregex, undocmd):
                                file_handler.write('{}\n'.format(undocmd))                            
        else:
            storage.writeundofile()

    '''
    def writepostcleanupfile(self,storage,postcleanupcmdregex):
        
        if self.targeted_rollback:
            self.log.debug('Regex {}'.format(postcleanupcmdregex))
            for host in self.undocmds[storage.serial]['objects']:
                cleanupdir = '{}{}{}'.format(self.postcleanupdir,os.sep,host)
                self.createdir(cleanupdir)
                undocmds = self.undocmds[storage.serial]['objects'][host]
                if len(undocmds):
                    cleanupfile = '{}{}{}.{}.{}.sh'.format(cleanupdir,os.sep,self.scriptname,self.start,storage.serial)
                    postcmds = self.undocmds[storage.serial]['post'] + undocmds + self.undocmds[storage.serial]['pre']
                    with open(cleanupfile,"w") as undofile_handler:
                        for postcmd in postcmds:
                            if re.search(postcleanupcmdregex, postcmd):
                                undofile_handler.write('{}\n'.format(postcmd))
        else:
            storage.writepostcleanupfile(postcleanupcmdregex)
    '''

    # TASK LISTS

    def step1tasks(self):
        log = self.log

        def prehostchecks(host):
            log.info('Checking host: {}'.format(host))
            # Check ldevs
            self.checkldevs(self.target,host)


        def processhost(host):
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                log.info('Processing host: {}'.format(host))
                # Configure undocmd list
                if self.targeted_rollback:
                    self.undocmds[self.source.serial]['objects'][host] = self.undocmds[self.source.serial]['objects'].get(host,[])
                    self.undocmds[self.target.serial]['objects'][host] = self.undocmds[self.target.serial]['objects'].get(host,[])
                    setattr(self.source.apis[self.source.useapi], 'undocmds', self.undocmds[self.source.serial]['objects'][host])
                    setattr(self.target.apis[self.target.useapi], 'undocmds', self.undocmds[self.target.serial]['objects'][host])
                    self.log.info("targeted_rollback: {}, tweak api undocmd destination to writeback to storage class".format(self.targeted_rollback))
                    '''
                      File "/scripts/GAD-migration/hiraid/storagemigration.py", line 2907, in step1tasks
    processhost(host)
  File "/scripts/GAD-migration/hiraid/storagemigration.py", line 2871, in processhost
    setattr(self.source.apis[self.source.useapi], 'undocmds', self.undocmds[self.source.serial]['objects'][host]['post'])
TypeError: list indices must be integers or slices, not str
'''
                # Locate host group ids
                self.locatehostgroupgids(self.target,host,3)
                # Configure host groups
                self.configurehostgrps(self.target,host,4)
                # Configure ldevs
                self.configureldevs(self.target,host,5)
                # Set capacity saving
                self.modifyldevcapacitysaving(self.target,host,self.config['capacity_saving'],6)
                # Map target luns
                self.configureluns(self.target,host,7)
                # Create copy groups
                self.configurecopygrps(self.source,self.target,host,8)
                # Update step
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]["steps"][self.step]['status'] = "completed"
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]["steps"][self.step]['endtime'] = self.now()
                # Write host migration files
                hostmigrationfile = '{}{}{}.json'.format(self.migrationdir,self.separator,host)
                file = open(hostmigrationfile,"w")
                file.write(json.dumps(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host], indent=4, sort_keys=True))
            else:
                log.info('Skipping this host, omit key set {}'.format(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']))

        try:
            # Refresh capacity report
            self.capacityreport(self.target,skipifthisstepcomplete=2,taskid=1)
            # Produce capacity report
            if not self.producecapacityreport(2): self.exitroutine()

            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                prehostchecks(host)
            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                    processhost(host)

        except Exception as e:
            raise StorageException('Unable to complete step {}, error {}'.format(self.step,str(e)),Storage,self.log)


    def step2tasks(self):

        log = self.log
        now = self.now()
        source_cci_horcm_inst = self.config['source_cci_horcm_inst']
        target_cci_horcm_inst = self.config['target_cci_horcm_inst']
        horcmfilelist = []
        horcminsts = []
        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,source_cci_horcm_inst))
        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,target_cci_horcm_inst))
        horcminsts.append(source_cci_horcm_inst)
        horcminsts.append(target_cci_horcm_inst)

        try:
            if self.warningreport(refresh=True): self.exitroutine()
            self.source.getcopygrp()
            self.backuphorcmfiles(horcmfilelist,1)
            self.createhorcmfiles(2)
            self.restarthorcminsts(horcminsts,3)
            self.capacityreport(self.target,skipifthisstepcomplete=2,taskid=4)
            if not self.producecapacityreport(5): self.exitroutine()
            pairrequests = self.createpairs(6)['pairrequests']
            self.monitorpairs(pairrequests,7)
            self.capacityreport(self.target,skipifthisstepcomplete=2,taskid=8)
            self.writeundofile(self.source)
            #self.target.writeundofile()
        except Exception as e:
            raise StorageException('Unable to creategadpairs, error \'{}\''.format(str(e)),Storage,self.log)

    def step3tasks(self):

        try:
            horcm_inst = self.config['target_cci_horcm_inst']
            pairevtwaitvolflag = '-ss'

            #for host in self.actioninghostlist:
            #    print("--> Actioning this host: {}".format(host))

            self.log.info('Confirm devices are in PAIR status at source using pairvolchk')
            self.pairvolchk('source',expectedreturn=23,taskid=1)
            self.log.info('Confirm devices are in PAIR status at target using pairvolchk')
            self.pairvolchk('target',expectedreturn=33,taskid=2)
            self.log.info('Pairsplit -RS volumes')
            pairevtwaits = self.splitRS('target',taskid=3)
            self.log.info('Monitor for ssus PAIR status with pairevtwait')
            self.monitorpairevtwaits(self.target,pairevtwaits,'ssus',4,horcm_inst,pairevtwaitvolflag)
        except Exception as e:
            raise StorageException('Failed to complete step {} tasks, error {}'.format(self.step,str(e)),Storage,self.log)


    def step4tasks(self):

        try:
            horcm_inst = self.config['target_cci_horcm_inst']
            pairevtwaitvolflag = '-ss'
            self.log.info('Confirm devices are in PSUS status at source using pairvolchk')
            self.pairvolchk('source',expectedreturn=24,taskid=1)
            self.log.info('Confirm devices are in SSUS status at target using pairvolchk')
            self.pairvolchk('target',expectedreturn=34,taskid=2)
            self.log.info('Pairsplit -R volumes')
            pairevtwaits = self.smplR('target',taskid=3)
            self.log.info('Monitor for smpl PAIR status with pairevtwait')
            self.monitorpairevtwaits(self.target,pairevtwaits,'smpl',4,horcm_inst,pairevtwaitvolflag)
        except Exception as e:
            raise StorageException('Failed to complete step {} tasks, error {}'.format(self.step,str(e)),Storage,self.log)
