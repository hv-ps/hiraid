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

import inspect
from .storagemigration import StorageMigration
from .storageexception import StorageException
from .raidlib import Storage as Storage
from .storagecapabilities import Storagecapabilities as storagecapabilities
import json
import re
import sys
import collections
import uuid
import os
from string import Template
from datetime import datetime
import time

class requiredcustomviews:
    wwn_view = 'wwns_bylcwwnportgid'

class GadEdgeMigration(StorageMigration):


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
                    # edge # if int(step) == 1:
                        # edge # self.log.info("Step {}, nothing to load for host".format(step))
                        # edge # continue
                    self.log.info('Loading all possible hosts ( except those with omit tag )')
                    self.log.info('loading host {}'.format(host))
                    hostinputjsonfile = '{}{}{}.json'.format(self.migrationdir,self.separator,host)
                    try:
                        self.loadhostjson(hostinputjsonfile,host)
                    except Exception as l:
                        if host in inchosts:
                            raise Exception('Unable to load host {} json - error: {}'.format(host,str(l)))
                        else:
                            self.log.warn('Unable to load host {} data, host not included for this step'.format(host))

                    #self.log.info('{}'.format(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['steps']))

                    if self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['steps']['1']['status'] != "Successful":
                        raise Exception('Host {} has not completed step _1_allocate_target_resources, GAD target ldevids will not be allocated, therefore the PVOLS for this edge migration do not exist'.format(host))
                    else:
                        self.log.info('Host {} completed _1_allocate_target_resources, proceeding')

                    # In order to allow users to include and exclude hosts at any step, capture copy_grp names for ALL node regardless otherwise
                    # when horcm is overwritten problems will occur.
                    copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('copy_grp_name')
                    if copy_grp_name:
                        self.copygrps[copy_grp_name] = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('device_grp_name')

                    # Likewise for edge
                    edge_copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('edge_copy_grp_name')
                    if edge_copy_grp_name:
                        self.edgecopygrps[edge_copy_grp_name] = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host].get('edge_device_grp_name')

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


    def checkstep(self):
        errtracker = { 'error': 0 }
        previousstep = int(self.step) - 1
        log = self.log

        def process(host,group,previousstep,errtracker):
            log.info('Check step sequence for host {}, group {}, this step {}, previousstep {}'.format(host,group,self.step,previousstep))
            lasthoststep = 0
            jsonstepslist = sorted(list(self.jsonin[self.migrationtype]['migrationgroups'][group][host]['edgesteps'].keys()))
            if len(jsonstepslist):
                lasthoststep = jsonstepslist[-1]
            log.info('Host {} last executed step {}'.format(host,lasthoststep))
            log.debug('Steps index {}'.format(jsonstepslist))
            if int(lasthoststep) >= int(self.step):
                errormessage = 'This step {}, however step {} has already executed, appear to be running out of sequence'.format(self.step,lasthoststep)
                log.error(errormessage)
                errtracker['error'] = 1
            if previousstep > 0:
                if str(previousstep) not in self.jsonin[self.migrationtype]['migrationgroups'][group][host]['edgesteps']:
                    log.error('Step {} not in steps for host {} in group {}'.format(str(previousstep),host,group))
                    errtracker['error'] = 1
                if str(previousstep) not in self.jsonin[self.migrationtype]['migrationgroups'][group][host]['edgesteps'] or not re.search('successful|completed|ended', self.jsonin[self.migrationtype]['migrationgroups'][group][host]['edgesteps'][str(previousstep)]['status'],re.IGNORECASE):
                    errormessage = 'There is a problem with the previous step {} ( status {} ) for host {} in migration group {}'.format(previousstep,self.jsonin[self.migrationtype]['migrationgroups'][group][host]['edgesteps'][str(previousstep)]['status'],host,group)
                    log.error(errormessage)
                    errtracker['error'] = 1


        for node in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            process(node,self.group,previousstep,errtracker)

        if errtracker['error']:
            message = 'Seem to be running out of step or previous step did not complete appropriately, see log for further details'
            log.error(message)
            raise Exception(message)

        log.info("Step sequence OK")


    def XXXconnectstorage(self,storageserial,horcminst):

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

    def connectedgestorage(self,role: str=None):

        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        storageapi = self.config.get('api','raidcom')
        log = self.log

        for host in group:
            if 'remote_replication' in group[host]:
                for replicationtype in group[host]['remote_replication']:
                    for edgestorageserial in group[host]['remote_replication'][replicationtype]['targets']:
                        if group[host]['remote_replication'][replicationtype]['targets'][edgestorageserial]['remote_replication_type_support']:

                            horcminst = self.config.get('edge_horcm_inst',None)
                            log.info('Establish connectivity with edge target storage {}'.format(edgestorageserial))
                            apiconfig = { 'serial': edgestorageserial, 'horcminst': horcminst }
                            self.edgestoragearrays[edgestorageserial] = { 'storage': {} }
                            self.edgestoragearrays[edgestorageserial]['storage'] = Storage(edgestorageserial,log,useapi=storageapi)
                            getattr(self.edgestoragearrays[edgestorageserial]['storage'],storageapi)(apiconfig=apiconfig)
                            undofile = '{}.{}'.format(self.scriptname,self.start)
                            getattr(self.edgestoragearrays[edgestorageserial]['storage'],'setundofile')(undofile)
                            setattr(self.edgestoragearrays[edgestorageserial]['storage'],'jsonin',self.jsonin)

    def connectedgetargetstorage(self):

        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        storageapi = self.config.get('api','raidcom')
        log = self.log

        def connectstorage(edgetarget):
            horcminst = self.config['edge_horcm_inst']
            log.info('Establish connectivity with edge target storage {}'.format(edgetarget))
            apiconfig = { 'serial': edgetarget, 'horcminst': horcminst }
            self.edgestoragearrays[edgetarget] = { 'storage': Storage(edgetarget,log,useapi=storageapi) }
            #self.edgestoragearrays[edgetarget]['storage'] = Storage(edgestorageserial,log,useapi=storageapi)
            getattr(self.edgestoragearrays[edgetarget]['storage'],storageapi)(apiconfig=apiconfig)
            undofile = '{}.{}'.format(self.scriptname,self.start)
            getattr(self.edgestoragearrays[edgetarget]['storage'],'setundodir')(self.group)
            getattr(self.edgestoragearrays[edgetarget]['storage'],'setundofile')(undofile)
            setattr(self.edgestoragearrays[edgetarget]['storage'],'jsonin',self.jsonin)



        for host in group:
            if 'remote_replication' in group[host]:
                for replicationtype in group[host]['remote_replication']:
                    try:
                        for edgestorageserial in group[host]['remote_replication'][replicationtype]['targets']:
                            if group[host]['remote_replication'][replicationtype]['targets'][edgestorageserial]['remote_replication_type_support']:
                                edgetarget = group[host]['remote_replication'][replicationtype]['targets'][edgestorageserial]['edge_target']
                                self.edgestoragearrays.get(edgetarget,connectstorage(edgetarget))
                                self.edgestoragearrays.get(edgestorageserial,connectstorage(edgestorageserial))
                    except Exception as e:
                        raise StorageException('remote_replication_type_support not present, did you discover edge storage? {}'.format(str(e)),Storage,self.log,migration=self)
                            
    def lockedgestorage(self):
        for storage in self.edgestoragearrays:
            self.edgestoragearrays[storage]['storage'].lockresource()

    def unlockedgestorage(self):
        for storage in self.edgestoragearrays:
            self.edgestoragearrays[storage]['storage'].unlockresource()

    def writeedgeundofiles(self):
        for storage in self.edgestoragearrays:
            self.edgestoragearrays[storage]['storage'].writeundofile()

        '''
        role = 'source | target'
        
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
        
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        storage.raidcom(jsonin['gad']['conf']['source_horcm_inst'])
        # Append undo dir to default /reverse
        source.setundodir(group)
        # Set undo file
        source.setundofile(undofile)

    
        '''    


    def createvsms(self):
        vsmcreate = {}
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]

        if self.config.get('edge_virtualise',False):
            self.log.info("Virtualisation of edge storage required")
            try:
                for host in group:
                    if 'remote_replication' in group[host]:
                        for replicationtype in group[host]['remote_replication']:
                            for edgestorageserial in group[host]['remote_replication'][replicationtype]['targets']:
                                if group[host]['remote_replication'][replicationtype]['targets'][edgestorageserial]['remote_replication_type_support']:
                                    edgetarget = group[host]['remote_replication'][replicationtype]['targets'][edgestorageserial]['edge_target']
                                    self.log.info("Edge target {}".format(edgetarget))

                                    # HORRIBLE HORRIBLE
                                    if int(self.edgestoragearrays[edgetarget]['storage'].micro_ver.split('-')[0]) <= 70:
                                        warning = "VSM creation not supported on this storage type {}".format(self.edgestoragearrays[edgetarget]['storage'].v_id)
                                        self.log.warn(warning)
                                        self.warnings += 1
                                        self.warningmessages.append(warning)
                                        self.endmessage = "Ended with warnings"
                                        continue

                                    vsmcreate[edgetarget] = vsmcreate.get(edgetarget,{})
                                    vsmcreate[edgetarget][group[host]['remote_replication'][replicationtype]['targets'][edgestorageserial]['resource']['resourceGroupName']] = group[host]['remote_replication'][replicationtype]['targets'][edgestorageserial]['resource']
                
                if len(vsmcreate):
                    self.log.info("Create the following virtual storage machines")
                    for edgestorage in vsmcreate:
                        for vsm in vsmcreate[edgestorage]:
                            self.log.info('Create vsm \'{}\' vsmserial \'{}\' vsmtype \'{}\''.format(vsm,vsmcreate[edgestorage][vsm]['virtualSerialNumber'],vsmcreate[edgestorage][vsm]['virtualModel']))
                else:
                    self.log.info("No virtual storage machines required")


                # Lock edge storage arrays
                for edgestorage in vsmcreate:
                    self.log.info("Lock edge storage {}".format(edgestorage))
                    self.edgestoragearrays[edgestorage]['storage'].lockresource()

                # Create virtual machines
                for edgestorage in vsmcreate:
                    self.log.info("Create virtual storage machines {} on storage {}".format(vsmcreate[edgestorage],edgestorage))
                    self.edgestoragearrays[edgestorage]['storage'].addvsms(vsmcreate[edgestorage])
                
                # Unlock edge storage arrays
                for edgestorage in vsmcreate:
                    self.log.info("Unlock edge storage {}".format(edgestorage))
                    self.edgestoragearrays[edgestorage]['storage'].unlockresource()

            except Exception as e:
                raise StorageException('Failed to create vsm(s) {}'.format(str(e)),Storage,self.log,migration=self)
        else:
            self.log.info("Config item edge_virtualise set to '{}', resources to be added to meta_resource".format(self.config.get('edge_virtualise',False)))
    
    def writeundofile(self):
        for edgeserial in self.edgestoragearrays:
            self.edgestoragearrays[edgeserial]['storage'].writeundofile()

    def checkldevs(self, storage: object, host: dict, remotereplicationtype: str, edge_source_serial: str):
        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.jsonin
        endstatus = ''
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        ldevpolicy = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edge_target_ldev_policy']
        errorflag = 0

        log.info("Checkldevs host {} policy {}".format(host,ldevpolicy))

        def checkldevs_ldevoffset():
            ldevoffset = self.config['edge_ldevoffset']
            log.info("Check ldev offset")
            checkoffsetreturn = self.checkoffset(ldevoffset,group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['ldevs'],storage)
            log.info("Check ldevs free")
            checkldevsfreeout = self.checkldevsfree(storage,group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['ldevs'])
            if checkoffsetreturn['error'] or checkldevsfreeout['error']:
                return 1
            else:
                self.log.info("Edge target ldevids passed all checks for host {}".format(host))

        def checkldevs_match():
            checkldevsfreeout = self.checkldevsfree(storage,group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['ldevs'])
            if checkldevsfreeout['error']:
                return 1

        def checkldevs_matchfallback():
            return

        def checkldevs_ldevrange():
            return

        checkfunctions = {'edge_ldevoffset':checkldevs_ldevoffset, 'edge_match':checkldevs_match, 'edge_matchfallback':checkldevs_matchfallback, 'edge_ldevrange':checkldevs_ldevrange }
        errorflag = checkfunctions[ldevpolicy]()
            #createfunction = getattr(self.checkldevs,'checkldevs_'+ldevpolicy)(storage,host)

        if errorflag:
            log.error("Identified problems with requested host {} ldevs".format(host))
            raise Exception("Identified problems with requested host {} ldevs".format(host))

    def hostgroupexists(self, storage: object, host: dict, remotereplicationtype: str, edge_source_serial: str):
        targetportlist = {}
        output = { 'messages':[], 'error': 0}
        jsonin = self.jsonin
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        hostgroups = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['hostgroups']
        edge_fail_on_existing_target_hostgroup = self.config.get('edge_fail_on_existing_target_hostgroup',False)
        
        for hostgroup in hostgroups:
            targetportlist[hostgroups[hostgroup]['targetport']] = {}

        targetporthostgroups = storage.gethostgrps(targetportlist,optviews=['hostgroupsbyportname'])['hostgroupsbyportname']

        for hostgroup in hostgroups:
            port,gid = storage.returnportandgid(hostgroup)
            targetname = hostgroups[hostgroup]['target_group_name']
            targethmobits = hostgroups[hostgroup]['target_hmo_bits']
            targethmd = hostgroups[hostgroup]['target_hmd']
            targetport = hostgroups[hostgroup]['targetport']
            self.log.info('Port: {}, gid: {}, targetname: {}, targethmobits: {}, targethmd: {}, targetport: {}'.format(port,gid,targetname,targethmobits,targethmd,targetport))
            if targetname in targetporthostgroups[targetport]:
                self.log.warn('Hostgroup name {} already exists on target port {}'.format(targetname,targetport))
                hostgroups[hostgroup]['_create'] = 0
                hostgroups[hostgroup]['_exists'] = 1
                hostgroups[hostgroup]['target_gid'] = targetporthostgroups[port][targetname]['GID']
                existingtargetgid = targetporthostgroups[port][targetname]['GID']
                existinghmobits = targetporthostgroups[port][targetname]['HMO_BITs']
                self.log.info('Existing hmobits type {}'.format(type(existinghmobits)))
                existinghmd = targetporthostgroups[port][targetname]['HMD']
                luncount = storage.getlun(port=targetport,gid=existingtargetgid)['metaview']['stats']['luncount']
                if edge_fail_on_existing_target_hostgroup:
                    message = 'edge_fail_on_existing_target_hostgroup set to true and host group exists, must fail'
                    self.log.error(message)
                    output['messages'].append(message)
                    output['error'] = 1
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
                    #luncount = storage.getlun(port=targetport,gid=existingtargetgid)['metaview']['stats']['luncount']
                if luncount:
                    message = 'Target host group {}-{} exists and contains {} lun(s)'.format(targetport,existingtargetgid,luncount)
                    output['messages'].append(message)
                    for requestedlun in group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['hostgroups'][hostgroup]['LUNS']:
                        self.log.info("Requested hostgroup \'{}\' - Lun location \'{}\'".format(hostgroup,requestedlun))
                    for lun in storage.views['_ports'][targetport]['_host_grps'][existingtargetgid]['_luns']:
                        self.log.info("Port \'{}\', gid \'{}\' - Existing Lun \'{}\'".format(targetport,existingtargetgid,lun))
                        if lun in group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['hostgroups'][hostgroup]['LUNS']:
                            message = "Target host group \'{}\' luns already exist in the requested location \'{}\'".format(hostgroup,lun)
                            self.log.error(message)
                            output['messages'].append(message)
                            output['error'] = 1
                    
            else:
                hostgroups[hostgroup]['_create'] = 1

            if output['error']:
                raise Exception("Identified problems with requested host {} hostgroups".format(host))
                
        self.log.info("Target host group passed all checks for migration host {}".format(host))


    def logtaskstart(self,taskname,group='',host='',step='',taskid='',storage: object='',tasklocator='edgesteps'):
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
            jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid] = { "taskname":taskname, "status":"started", "begin":ts, "messages":[] }
            taskref = jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid]
            storetask(taskref)
        else:
            for host in jsonin[migrationtype]['migrationgroups'][group]:
                if not jsonin[migrationtype]['migrationgroups'][group][host]['omit']:
                    jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid] = { "taskname":taskname, "status":"started", "begin":ts, "messages":[] }
                    taskref = jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid]
                    storetask(taskref)


    def logendtask(self,host,taskid,status,extrafields={},storage: object='',tasklocator='edgesteps'):
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
            jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid]['status'] = status
            jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid]['end'] = ts
            includefields(jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid],extrafields)
            removetask()

        else:
            for host in jsonin[migrationtype]['migrationgroups'][group]:
                if not jsonin[migrationtype]['migrationgroups'][group][host]['omit']:
                    jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid]['status'] = status
                    jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid]['end'] = ts
                    includefields(jsonin[migrationtype]['migrationgroups'][group][host][tasklocator][step]['tasks'][taskid],extrafields)
                    removetask()

    def logstartstep(self):
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
        scriptname = self.scriptname

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edgesteps'] = { self.step:{ "name":scriptname, "status": "started", "tasks": {}, "begin":ts } }
                #self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edgesteps'][self.step] = { "name":scriptname, "status": "started", "tasks": {}, "begin":ts }
    
    def logendstep(self,status):
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edgesteps'][self.step]['status'] = status
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edgesteps'][self.step]['end'] = ts
        self.log.info('-- {} --'.format(status))

    def writehostmigrationfile(self,edge='edge'):

        previousstep = int(self.step) - 1
        prepend = '{}__'.format(previousstep)
        log = self.log

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                hostmigrationfile = '{}{}{}.json'.format(self.migrationdir,self.separator,host)
                self.backupfile(hostmigrationfile,prepend)
                file = open(hostmigrationfile,"w")
                file.write(json.dumps(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host], indent=4, sort_keys=True))

    def locatehostgroupgids(self,storage,host,taskid,remotereplicationtype: str, edge_source_serial: str):
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

        def populatejsoningids(gidgroups):
            for gidgroup in gidgroups:
                for sourcehostgroup in gidgroups[gidgroup]['group']:
                    self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['hostgroups'][sourcehostgroup]['targetgid'] = gidgroups[gidgroup]['gid']

        # Task management is a bit of a problem now we're forking the task to go configure edge targets. Requires some deep thought! Comment out for now.
        self.logtaskstart(taskname,host=host,taskid=taskid)
        #jsonin[migrationtype]['migrationgroups'][group][host]['edgesteps'][step]['tasks'][taskid] = { "taskname":taskname, "status":"started", "begin":ts, "messages":[] }

        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        hostgroups = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['hostgroups']

        for hostgroup in hostgroups:

            if hostgroups[hostgroup]['_create']:
                groupname = hostgroups[hostgroup]['GROUP_NAME']
                targetport = hostgroups[hostgroup]['targetport']
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
            log.info('All host groups to have matching gid numbers')
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
                hostgroups[hg]['targetgid'] = "-"

        self.returngids(storage,gidgroups)
        log.info('gidgroups {}'.format(gidgroups))
        populatejsoningids(gidgroups)
        self.logendtask(host=host,taskid=taskid,status='completed')
        
    def configurehostgrps(self, storage: object, host: str, taskid: int, remotereplicationtype: str, edge_source_serial: str):

        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.jsonin

        # Task management is a bit of a problem now we're forking the task to go configure edge targets. Requires some deep thought! Comment out for now.
        self.logtaskstart(taskname,host=host,taskid=taskid)
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        hostgroups = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['hostgroups']

        for hostgroup in hostgroups:
            targetport = hostgroups[hostgroup]['targetport']
            targetgid = hostgroups[hostgroup]['targetgid']
            targethostgroupname = hostgroups[hostgroup]['target_group_name']
            targethmd = hostgroups[hostgroup]['target_hmd']
            targethmobits = hostgroups[hostgroup]['target_hmo_bits']

            storage.addhostgroup(targetport,targethostgroupname,targetgid)

            if self.config.get('edge_virtualise',False):
                targetresourcegroup = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['resource']['resourceGroupName']
                log.info("Add host groups to edge targetresource group: {} at storage {}".format(targetresourcegroup,storage.serial))
                storage.addhostgrpresource(targetresourcegroup,targetport,targethostgroupname)

            storage.modifyhostgrp(targetport,targethmd,targethostgroupname,host_mode_opt=targethmobits)

            for hba_wwn in hostgroups[hostgroup]['WWNS']:
                outcome = storage.addhbawwn(targetport,hba_wwn,targethostgroupname)
                wwn_nickname = hostgroups[hostgroup]['WWNS'][hba_wwn]['NICK_NAME']
                if re.search(r'\w+',wwn_nickname):
                    outcome = storage.addwwnnickname(targetport,hba_wwn,wwn_nickname,targethostgroupname)
        
        self.logendtask(host=host,taskid=taskid,status='completed')

    def configureldevs(self, storage: object, host: dict, taskid: int, remotereplicationtype: str, edge_source_serial: str):
        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        jsonin = self.jsonin
        endstatus = ''

        self.logtaskstart(taskname,host=host,taskid=taskid)
        createfunction = getattr(self,'createldev_'+self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edge_target_ldev_policy'])(storage,host,taskid,remotereplicationtype,edge_source_serial)
        self.logendtask(host=host,taskid=taskid,status='completed')

    def createldev_edge_ldevoffset(self,storage,host,taskid,remotereplicationtype: str, edge_source_serial: str):

        log = self.log
        jsonin = self.jsonin
        taskname = inspect.currentframe().f_code.co_name
        ldevoffset = self.config['edge_ldevoffset']
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        edge = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]
        resourcegroupname = edge['resource']['resourceGroupName']
        #group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['ldevs']

        checkoffsetreturn = self.checkoffset(ldevoffset,edge['ldevs'],storage)
        if checkoffsetreturn['error']:
            self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]
            edge['edgesteps'][step]['tasks'][taskid]['error'] = 1
            edge['edgesteps'][step]['tasks'][taskid]['messages'].append('Unable to {} -> {}'.format(taskname,checkoffsetreturn))
            for message in checkoffsetreturn['messages']:
                edge['edgesteps'][step]['tasks'][taskid]['messages'].append(message)
                log.info(message)
            self.logendtask(host=host,taskid=taskid,status='Failed')
            raise Exception('Unable to create ldevs with given offset')

        checkldevsfreeout = self.checkldevsfree(storage,edge['ldevs'])
        if checkldevsfreeout['error']:
            edge['edgesteps'][step]['tasks'][taskid]['error'] = 1
            edge['edgesteps'][step]['tasks'][taskid]['messages'].append('Unable to {} -> {}'.format(taskname,checkldevsfreeout))
            for message in checkldevsfreeout['messages']:
                edge['edgesteps'][step]['tasks'][taskid]['messages'].append(message)
                log.info(message)
            self.logendtask(host=host,taskid=taskid,status='Failed')
            raise Exception('Unable to create ldevs with given offset')

        log.info('Number of ldevs to create: {}'.format(len(edge['ldevs'].keys() )))

        if self.config.get('edge_virtualise',False):
            self.unmapldevs(storage,edge['ldevs'])
            self.addldevtoresource(storage,edge['ldevs'],resourcegroupname)

        #self.mapldevreserves(storage,edge['ldevs'])
        self.createldevs(storage,edge['ldevs'])
        self.labelldevs(storage,edge['ldevs'])

        if self.config.get('edge_virtualise', False):
            self.mapldevs(storage,edge['ldevs'])
            self.mapldevreserves(storage,edge['ldevs'])
                

        '''
        if self.config.get('edge_virtualise',False):
            ldevmap = {}
            for ldevid in edge['ldevs']:
                if edge['ldevs'][ldevid].get('omit',False):
                    log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,edge['ldevs'][ldevid]['omit']))
                    continue
                ldevmap[ldevid] = edge['ldevs'][ldevid]['target_ldevid']
            self.mapldevs(storage,ldevmap)
        '''

        log.info('Successfully created {} ldevs with offset'.format(len(edge['ldevs'].keys() )))

        #self.logendtask(host=host,taskid=taskid,status='completed')

    #self.mapldevreserves(storage,group[host]['ldevs'])    
    def mapldevs(self,storage,ldevs,doontrue='REMOTE_FLAG'):
        log = self.log
        for ldevid in ldevs:
            ontrue = ldevs[ldevid].get(doontrue,False)
            if ldevs[ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}'.format(ldevid,ldevs[ldevid]['omit']))
                continue
            if not ontrue:
                log.info('Ldevid {} was omitted because doontrue filter: {} = {}'.format(ldevid,doontrue,ontrue))
                continue
            ldev = str(ldevid)
            targetldevid = ldevs[ldev]['target_ldevid']
            virtualldevid = ldevid
            storage.log.info('Storage array {} - Source ldevid {}, targetldevid: {} virtualldevid ldevid {}'.format(storage.serial,ldev,targetldevid,ldevid))
            storage.mapldev(targetldevid,virtualldevid)


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



    def modifyldevcapacitysaving(self, storage: object, host: dict, taskid: int, capacity_saving: str, remotereplicationtype: str, edge_source_serial: str):

        ts = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        #ldevs = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['ldevs']
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        edge = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]
        
        ldevs = edge['ldevs']

        self.logtaskstart(taskname,host=host,taskid=taskid)
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
        else:
            log.info("capacity_saving: {}, nothing to do".format(capacity_saving))

        self.logendtask(host=host,taskid=taskid,status='completed',extrafields=appendtotask)

    def configureluns(self, storage: object, host: dict, taskid: int, remotereplicationtype: str, edge_source_serial: str):
        log = self.log
        start = self.now()
        taskname = inspect.currentframe().f_code.co_name

        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        edge = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]
        self.logtaskstart(taskname,host=host,taskid=taskid)
        
        for hostgroup in edge['hostgroups']:
            targetport = edge['hostgroups'][hostgroup]['targetport']
            targethostgroupname = edge['hostgroups'][hostgroup]['target_group_name']
            for lunid in edge['hostgroups'][hostgroup]['LUNS']:
                sourceldevid = edge['hostgroups'][hostgroup]['LUNS'][lunid]['LDEV']
                targetldevid = edge['ldevs'][sourceldevid]['target_ldevid']
                
                if edge['ldevs'][sourceldevid].get('omit',False):
                    log.info('Ldevid omitted {}, omit flag {}, skip lun mapping'.format(sourceldevid,edge['ldevs'][sourceldevid]['omit']))
                    continue
                storage.addlun(targetport,targetldevid,lunid,targethostgroupname)

        self.logendtask(host=host,taskid=taskid,status='completed')

    def muselect(self,availablemus):

        # This muselect is relevant to the GAD migration, it is essentially temporary but this could be used to other GAD therefore 0 is preferred.
        muPreferenceOrder = [1,2,3,0]
        for mu in muPreferenceOrder:
            if mu in availablemus:
                return str(mu)

    def configurecopygrps_hur(self, sourcestorage, targetstorage, host, taskid: int,remotereplicationtype: str, edge_source_serial: str, parentroot: object, childroot: object, doontrue='REMOTE_FLAG'):

        maxcopygrpnamelength = 30
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        edge = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]


        #sourceldevs = group[host]['ldevs']
        sourceldevs = parentroot['ldevs']

        self.log.info(json.dumps(sourceldevs,indent=4))
        self.logtaskstart(taskname,host=host,taskid=taskid,storage=sourcestorage)

        edgesourceserial = sourcestorage.serial
        edgetargetserial = targetstorage.serial
        if remotereplicationtype == "GAD+UR": copygrpprefix = 'h'
        if remotereplicationtype == "GAD+TC": copygrpprefix = 't'

        copygrpnamea = '{}.{}.{}.{}'.format(copygrpprefix,edgesourceserial,edgetargetserial,self.group)

        maxhostnamelen = maxcopygrpnamelength - len(copygrpnamea)
        copygrouphostname = host
        # Quick and not entirely infallible method for truncating copy group name to 30 chars
        copygrpname = '{}.{}'.format(copygrpnamea,copygrouphostname)
        if len(copygrpname) > 30:
            copygrpname = copygrpname[:20]+str(uuid.uuid4().hex.upper()[0:10])

        devicegrpname = copygrpname
        #group[host]['edge_copy_grp_name'] = copygrpname
        #group[host]['edge_device_grp_name'] = devicegrpname
        #copy_grp_name_key = '{}_{}_{}'.format(sourcestorage.serial,targetstorage.serial,'copy_grp_name')
        #device_grp_name_key = '{}_{}_{}'.format(sourcestorage.serial,targetstorage.serial,'device_grp_name')
        group[host]['copygrp_hur'] = copygrpname
        group[host]['devicegrp_hur'] = devicegrpname
        group[host]['hur_source_serial'] = str(sourcestorage.serial)
        group[host]['hur_target_serial'] = str(targetstorage.serial)

        mu = self.muselect(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['target_mu_free'])

        mu_key = '{}_{}_{}'.format(sourcestorage.serial,targetstorage.serial,'mu')
        

        #self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edge_mu_migration'] = mu
        self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][mu_key] = mu
        self.log.info("Using mirror_unit number {}".format(mu))

        for ldevid in childroot['ldevs']:
            ontrue = childroot['ldevs'][ldevid].get(doontrue,False)
            if childroot['ldevs'][ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}, skip adding device grp'.format(ldevid,childroot['ldevs'][ldevid]['omit']))
                continue
            if not ontrue:
                log.info('Ldevid {} was omitted because doontrue filter: {} = {}'.format(ldevid,doontrue,ontrue))
                continue

            sourceldevid = sourceldevs[childroot['ldevs'][ldevid]['source_ldevid']]['target_ldevid']
            log.info("Edge replication source ldev is GAD target ldevid: {}".format(sourceldevid))
            targetldevid = childroot['ldevs'][ldevid]['target_ldevid']

            #devicename = 'src_{}_targ_{}'.format(sourceldevid,targetldevid)
            devicename = 'src_{}_{}_targ_{}_{}'.format(sourceldevid,sourcestorage.returnldevid(sourceldevid)['culdev'],targetldevid,sourcestorage.returnldevid(targetldevid)['culdev'])
            childroot['ldevs'][ldevid]['device_grp_name'] = devicegrpname
            childroot['ldevs'][ldevid]['device_name'] = devicename
            sourcestorage.adddevicegrp(devicegrpname,devicename,sourceldevid)
            targetstorage.adddevicegrp(devicegrpname,devicename,targetldevid)

        if len(childroot['ldevs'].keys()):
            sourcestorage.addcopygrp(copygrpname,devicegrpname,mu)
            targetstorage.addcopygrp(copygrpname,devicegrpname,mu)

        self.logendtask(host=host,taskid=taskid,status='completed',storage=sourcestorage)

    def configure_copygrps_legacy_dr_gad_migration(self, sourcestorage, targetstorage, host, taskid: int,remotereplicationtype: str, edge_source_serial: str, parentroot: object, childroot: object, doontrue='REMOTE_FLAG'):
        # self.configurecopygrpsedgegad(sourceedge,self.edgestoragearrays[edge_target]['storage'],host,10,replicationtype,edge_source_serial,parentroot=parentroot,childroot=childroot)
        maxcopygrpnamelength = 30
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        #edge = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]


        #sourceldevs = group[host]['ldevs']
        sourceldevs = parentroot['ldevs']

        self.log.info(json.dumps(sourceldevs,indent=4))
        self.logtaskstart(taskname,host=host,taskid=taskid,storage=sourcestorage)

        edgesourceserial = sourcestorage.serial
        edgetargetserial = targetstorage.serial
        if remotereplicationtype == "GAD+UR": copygrpprefix = 'h'
        if remotereplicationtype == "GAD+TC": copygrpprefix = 't'
        if remotereplicationtype == "GAD": copygrpprefix = 'g'

        copygrpnamea = '{}.{}.{}.{}'.format(copygrpprefix,edgesourceserial,edgetargetserial,self.group)

        maxhostnamelen = maxcopygrpnamelength - len(copygrpnamea)
        copygrouphostname = host
        # Quick and not entirely infallible method for truncating copy group name to 30 chars
        copygrpname = '{}.{}'.format(copygrpnamea,copygrouphostname)
        if len(copygrpname) > 30:
            copygrpname = copygrpname[:20]+str(uuid.uuid4().hex.upper()[0:10])

        devicegrpname = copygrpname
        #group[host]['edge_copy_grp_name'] = copygrpname
        #group[host]['edge_device_grp_name'] = devicegrpname
        #copy_grp_name_key = '{}_{}_{}_{}'.format(remotereplicationtype,sourcestorage.serial,targetstorage.serial,'copy_grp_name')
        #device_grp_name_key = '{}_{}_{}_{}'.format(remotereplicationtype,sourcestorage.serial,targetstorage.serial,'device_grp_name')
        #group[host][copy_grp_name_key] = copygrpname
        #group[host][device_grp_name_key] = devicegrpname
        group[host]['copygrp_legacy_dr_gad'] = copygrpname
        group[host]['devicegrp_legacy_dr_gad'] = devicegrpname
        group[host]['legacy_dr_source_serial'] = str(sourcestorage.serial)
        group[host]['legacy_dr_target_serial'] = str(targetstorage.serial)
        mu = self.muselect(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['target_mu_free'])

        mu_key = '{}_{}_{}'.format(sourcestorage.serial,targetstorage.serial,'mu')
        

        #self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edge_mu_migration'] = mu
        self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][mu_key] = mu
        self.log.info("Using mirror_unit number {}".format(mu))

        for ldevid in childroot['ldevs']:
            ontrue = childroot['ldevs'][ldevid].get(doontrue,False)
            if childroot['ldevs'][ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}, skip adding device grp'.format(ldevid,childroot['ldevs'][ldevid]['omit']))
                continue
            if ontrue:
                log.info('Ldevid {} was omitted because doontrue filter: {} = {}'.format(ldevid,doontrue,ontrue))
                continue

            sourceldevid = ldevid
            log.info("Edge replication source ldevid: {}".format(sourceldevid))
            targetldevid = childroot['ldevs'][ldevid]['target_ldevid']

            #devicename = 'src_{}_targ_{}'.format(sourceldevid,targetldevid)
            devicename = 'src_{}_{}_targ_{}_{}'.format(sourceldevid,sourcestorage.returnldevid(sourceldevid)['culdev'],targetldevid,sourcestorage.returnldevid(targetldevid)['culdev'])
            childroot['ldevs'][ldevid]['device_grp_name'] = devicegrpname
            childroot['ldevs'][ldevid]['device_name'] = devicename
            sourcestorage.adddevicegrp(devicegrpname,devicename,sourceldevid)
            targetstorage.adddevicegrp(devicegrpname,devicename,targetldevid)

        if len(childroot['ldevs'].keys()):
            sourcestorage.addcopygrp(copygrpname,devicegrpname,mu)
            targetstorage.addcopygrp(copygrpname,devicegrpname,mu)

        self.logendtask(host=host,taskid=taskid,status='completed',storage=sourcestorage)


    def configure_copygrps_legacy_hur(self, sourcestorage, targetstorage, host, taskid: int,remotereplicationtype: str, edge_source_serial: str, parentroot: object, doontrue='REMOTE_FLAG'):
        # self.configurecopygrpsedgegad(sourceedge,self.edgestoragearrays[edge_target]['storage'],host,10,replicationtype,edge_source_serial,parentroot=parentroot,childroot=childroot)
        maxcopygrpnamelength = 30
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        #edge = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]


        #sourceldevs = group[host]['ldevs']
        sourceldevs = parentroot['ldevs']

        self.log.info(json.dumps(sourceldevs,indent=4))
        self.logtaskstart(taskname,host=host,taskid=taskid,storage=sourcestorage)

        edgesourceserial = sourcestorage.serial
        edgetargetserial = targetstorage.serial
        if remotereplicationtype == "GAD+UR": copygrpprefix = 'h'
        if remotereplicationtype == "GAD+TC": copygrpprefix = 't'
        if remotereplicationtype == "GAD": copygrpprefix = 'g'
        if remotereplicationtype == "LEGACYHUR": copygrpprefix = 'lh'

        copygrpnamea = '{}.{}.{}.{}'.format(copygrpprefix,edgesourceserial,edgetargetserial,self.group)

        maxhostnamelen = maxcopygrpnamelength - len(copygrpnamea)
        copygrouphostname = host
        # Quick and not entirely infallible method for truncating copy group name to 30 chars
        copygrpname = '{}.{}'.format(copygrpnamea,copygrouphostname)
        if len(copygrpname) > 30:
            copygrpname = copygrpname[:20]+str(uuid.uuid4().hex.upper()[0:10])

        devicegrpname = copygrpname
        group[host]['copygrp_legacy_hur'] = copygrpname
        group[host]['devicegrp_legacy_hur'] = devicegrpname
        group[host]['legacy_hur_source_serial'] = str(sourcestorage.serial)
        group[host]['legacy_hur_target_serial'] = str(targetstorage.serial)

        #mu = self.muselect(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['target_mu_free'])
        inboundmukey = 'undefinedcg.{}.{}'.format(sourcestorage.serial,targetstorage.serial)
        for usedmu in self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['mu_used']:
            if self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['mu_used'][usedmu] == inboundmukey:
                mu = usedmu

        mu_key = '{}_{}_{}_{}'.format(copygrpprefix,sourcestorage.serial,targetstorage.serial,'mu')
        
        self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][mu_key] = mu
        self.log.info("Using mirror_unit number {}".format(mu))

        for ldevid in parentroot['ldevs']:

            if parentroot['ldevs'][ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}, skip adding device grp'.format(ldevid,parentroot['ldevs'][ldevid]['omit']))
                continue

            if not ( parentroot['ldevs'][ldevid].get('remote_replication', False) and parentroot['ldevs'][ldevid]['remote_replication'] == "GAD+UR" ):
                log.info('Ldev omitted, as it is not a GAD+UR volume')
                continue



            sourceldevid = ldevid
            log.info("Edge replication source ldevid: {}".format(sourceldevid))
            targetldevid = parentroot['ldevs'][ldevid]['P-LDEV#']
            targetstorageserial = parentroot['ldevs'][ldevid]['P-Seq#']
            if targetstorageserial != targetstorage.serial:
                raise("targetstoragserial {} != targetstorage.serial {}, something went wrong".format(targetstorageserial,targetstorage.serial))

            #devicename = 'src_{}_targ_{}'.format(sourceldevid,targetldevid)
            devicename = 'src_{}_{}_targ_{}_{}'.format(ldevid,sourcestorage.returnldevid(ldevid)['culdev'],targetldevid,sourcestorage.returnldevid(targetldevid)['culdev'])
            parentroot['ldevs'][ldevid]['device_grp_name'] = devicegrpname
            parentroot['ldevs'][ldevid]['device_name'] = devicename
            sourcestorage.adddevicegrp(devicegrpname,devicename,sourceldevid)
            targetstorage.adddevicegrp(devicegrpname,devicename,targetldevid)

        if len(parentroot['ldevs'].keys()):
            sourcestorage.addcopygrp(copygrpname,devicegrpname,mu)
            targetstorage.addcopygrp(copygrpname,devicegrpname,mu)

        self.logendtask(host=host,taskid=taskid,status='completed',storage=sourcestorage)



    def configurecopygrpsXXXXXXX(self, sourcestorage, targetstorage, host, taskid: int,remotereplicationtype: str, edge_source_serial: str, doontrue='REMOTE_FLAG'):

        maxcopygrpnamelength = 30
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        edge = group[host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]
        sourceldevs = group[host]['ldevs']
        self.logtaskstart(taskname,host=host,taskid=taskid,storage=sourcestorage)

        edgesourceserial = sourcestorage.serial
        edgetargetserial = targetstorage.serial
        if remotereplicationtype == "GAD+UR": copygrpprefix = 'h'
        if remotereplicationtype == "GAD+TC": copygrpprefix = 't'

        copygrpnamea = '{}.{}.{}.{}'.format(copygrpprefix,self.group,edgesourceserial,edgetargetserial)

        maxhostnamelen = maxcopygrpnamelength - len(copygrpnamea)
        copygrouphostname = host
        # Quick and not entirely infallible method for truncating copy group name to 30 chars
        copygrpname = '{}.{}'.format(copygrpnamea,copygrouphostname)
        if len(copygrpname) > 30:
            copygrpname = copygrpname[:20]+str(uuid.uuid4().hex.upper()[0:10])

        devicegrpname = copygrpname
        group[host]['edge_copy_grp_name'] = copygrpname
        group[host]['edge_device_grp_name'] = devicegrpname
        mu = self.muselect(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['target_mu_free'])
        self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edge_mu_migration'] = mu
        self.log.info("Using mirror_unit number {}".format(mu))

        for ldevid in edge['ldevs']:
            ontrue = edge['ldevs'][ldevid].get(doontrue,False)
            if edge['ldevs'][ldevid].get('omit',False):
                log.info('Ldevid omitted {}, omit flag {}, skip adding device grp'.format(ldevid,edge['ldevs'][ldevid]['omit']))
                continue
            if not ontrue:
                log.info('Ldevid {} was omitted because doontrue filter: {} = {}'.format(ldevid,doontrue,ontrue))
                continue

            sourceldevid = sourceldevs[edge['ldevs'][ldevid]['source_ldevid']]['target_ldevid']
            log.info("Edge replication source ldev is GAD target ldevid: {}".format(sourceldevid))
            targetldevid = edge['ldevs'][ldevid]['target_ldevid']

            #devicename = 'src_{}_targ_{}'.format(sourceldevid,targetldevid)
            devicename = 'src_{}_{}_targ_{}_{}'.format(ldevid,sourcestorage.returnldevid(ldevid)['culdev'],targetldevid,sourcestorage.returnldevid(targetldevid)['culdev'])
            edge['ldevs'][ldevid]['device_grp_name'] = devicegrpname
            edge['ldevs'][ldevid]['device_name'] = devicename
            sourcestorage.adddevicegrp(devicegrpname,devicename,sourceldevid)
            targetstorage.adddevicegrp(devicegrpname,devicename,targetldevid)

        if len(edge['ldevs'].keys()):
            sourcestorage.addcopygrp(copygrpname,devicegrpname,mu)
            targetstorage.addcopygrp(copygrpname,devicegrpname,mu)

        self.logendtask(host=host,taskid=taskid,status='completed',storage=sourcestorage)

    def createhorcmfiles(self,taskid,sourceinst,targetinst,sourceudpport,targetudpport,copygrpkey,devicegrpkey,sourceserialkey,targetserialkey,remotereplicationtype="GAD+UR"):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        log = self.log

        horcmtemplatefile = self.env.horcmtemplatefile
        sourceccihorcmfile = '{}horcm{}.conf'.format(self.horcmdir,sourceinst)
        targetccihorcmfile = '{}horcm{}.conf'.format(self.horcmdir,targetinst)
        source_horcm_ldevg = []
        target_horcm_ldevg = []
        source_horcm_inst = []
        target_horcm_inst = []
        status = 'completed'
        errors = {}

        self.logtaskstart(taskname,host=self.host,taskid=taskid)

        self.log.info("HELLO")
        def processhorcmcontent(host,copygrpname,devicegrpname,sourcestorage,targetstorage):
            log.info('process horcm content for host {}'.format(host))

            for storagearray in [sourcestorage,targetstorage]:
                if copygrpname not in storagearray.views['_copygrps']:
                    errmessage = 'Copygrp {} not present on storage array!'.format(copygrpname)
                    log.error(errmessage)
                    raise Exception(errmessage)
            source_horcm_ldevg.append('{}\t{}\t{}'.format(copygrpname,devicegrpname,sourcestorage.serial))
            target_horcm_ldevg.append('{}\t{}\t{}'.format(copygrpname,devicegrpname,targetstorage.serial))
            source_horcm_inst.append('{}\tlocalhost\t{}'.format(copygrpname,targetudpport))
            target_horcm_inst.append('{}\tlocalhost\t{}'.format(copygrpname,sourceudpport))

        try:
        
            edgetargets = {}
            for node in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                # No need to check for omit host, that's done at load time.
                if 'remote_replication' in self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]:
                    for edge_source_serial in self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['remote_replication'][remotereplicationtype]['targets']:
                        edge_target = self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['edge_target']
                        edgetargets[edge_target] = True
                        if self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial].get('omit',False):
                            self.log.warn("Remote replication skipped for host {}".format(node))
                        else:
                            if copygrpkey in self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]:
                                cpygrpname = self.jsonin[self.migrationtype]['migrationgroups'][self.group][node][copygrpkey]
                                dvcgrpname = self.jsonin[self.migrationtype]['migrationgroups'][self.group][node][devicegrpkey]
                                source_serial = self.jsonin[self.migrationtype]['migrationgroups'][self.group][node][sourceserialkey]
                                target_serial = self.jsonin[self.migrationtype]['migrationgroups'][self.group][node][targetserialkey]
                                sourcestorage = self.edgestoragearrays[source_serial]['storage']
                                targetstorage = self.edgestoragearrays[target_serial]['storage']
                                targetstorage.getcopygrp()
                                sourcestorage.getcopygrp()
                                # Probably drop this!
                                self.edgetarget = self.edgestoragearrays[edge_target]['storage']

                                processhorcmcontent(node,cpygrpname,dvcgrpname,sourcestorage,targetstorage)
                            else:
                                thishoststeps = sorted(list(self.jsonin[self.migrationtype]['migrationgroups'][self.group][node]['edgesteps'].keys()))
                                log.info('Copy group name cannot be found for host {}, perhaps this host has not run edgestep 1'.format(node))

            if len(edgetargets) > 1:
                raise Exception("Current scripts cannot support more than one edge target, must end")

            if self.config['cci_horcm_ipcmd'] and self.config['storage_ipaddresses'].get(sourcestorage.serial,False):
                srccmddevice = '\\\\.\IPCMD-{}-31001'.format(self.config['storage_ipaddresses'][sourcestorage.serial])
            else:
                srccmddevice = '\\\\.\CMD-{}{}'.format(sourcestorage.serial,(':/dev/sd','')[os.name=='nt'])

            if self.config['cci_horcm_ipcmd'] and self.config['storage_ipaddresses'].get(targetstorage.serial,False):
                trgcmddevice = '\\\\.\IPCMD-{}-31001'.format(self.config['storage_ipaddresses'][targetstorage.serial])
            else:
                trgcmddevice = '\\\\.\CMD-{}{}'.format(targetstorage.serial,(':/dev/sd','')[os.name=='nt'])

            horcmtemplate = open(horcmtemplatefile)
            horcm = Template(horcmtemplate.read())

            # Append copy_grps from different steps in the process.
            for cg in self.edgecopygrps:
                additional_source_horcm_ldevg = '{}\t{}\t{}'.format(cg,self.edgecopygrps[cg],targetstorage.serial)
                if additional_source_horcm_ldevg not in source_horcm_ldevg:
                    if cg not in targetstorage.views['_copygrps']:
                        log.info('Copygrp {} not present on storage, skipping'.format(cg))
                        continue
                    additional_target_horcm_ldevg = '{}\t{}\t{}'.format(cg,self.edgecopygrps[cg],targetstorage.serial)
                    additional_source_horcm_inst = '{}\tlocalhost\t{}'.format(cg,targetudpport)
                    additional_target_horcm_inst = '{}\tlocalhost\t{}'.format(cg,sourceudpport)
                    source_horcm_ldevg.append(additional_source_horcm_ldevg)
                    target_horcm_ldevg.append(additional_target_horcm_ldevg)
                    source_horcm_inst.append(additional_source_horcm_inst)
                    target_horcm_inst.append(additional_target_horcm_inst)

            sourcehorcm = { 'service':sourceudpport, 'serial':sourcestorage.serial, 'cmddevice':srccmddevice, 'horcm_ldevg':'\n'.join(source_horcm_ldevg), 'horcm_inst':'\n'.join(source_horcm_inst) }
            targethorcm = { 'service':targetudpport, 'serial':targetstorage.serial, 'cmddevice':trgcmddevice, 'horcm_ldevg':'\n'.join(target_horcm_ldevg), 'horcm_inst':'\n'.join(target_horcm_inst) }

            sourcehorcmcontent = horcm.substitute(sourcehorcm)
            targethorcmcontent = horcm.substitute(targethorcm)


            self.writehorcmfile(sourceccihorcmfile,sourcehorcmcontent)
            self.writehorcmfile(targetccihorcmfile,targethorcmcontent)

        except Exception as e:
            status = "Failed"
            self.log.error("An exception occured creating horcms: {}".format(str(e)))
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

    def restarthorcminsts(self,instlist,taskid,storageobj):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        status = 'completed'

        self.logtaskstart(taskname,host=self.host,taskid=taskid)

        try:
            for horcminst in instlist:
                storageobj.restarthorcminst(horcminst)
        except Exception as e:
            status = 'Failed'
            message = 'Failed to restart horcm instances, error {}'.format(str(e))
            log.error(message)
            raise Exception(message)
        finally:
            self.logendtask(host=self.host,taskid=taskid,status=status)


    def createhurpairs(self,taskid,pairsourcestorage,copygrpkey,remotereplicationtype):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        pairrequests = []
        status = 'completed'


        def processpairdisplay(host):
            source_inst = self.config['target_hur_pvol_horcm_inst']
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][copygrpkey]
            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(pairsourcestorage,copy_grp_name,source_inst,opts='-fcxe').split('\n')))]
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
                    self.paircreate(pairsourcestorage,copy_grp_name,source_inst,jp=self.config['edge_journal_map'][str(self.target.serial)],js=self.config['edge_journal_map'][str(self.edgetarget.serial)],fence='async')
                    pairdisplayout = self.pairdisplay(pairsourcestorage,copy_grp_name,source_inst,opts='-fcxe')
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

    def createdrgadpairs(self,taskid,horcminst,copygrpkey,storageserialkey,quorum,fence):
    #def pairsplit(self,storage,taskid,horcminst,copygrpkey,storageserialkey,messagekey,opts=''):

        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        pairrequests = []
        status = 'completed'

        def processpair(storage,host,copy_grp_name,horcminst):
            copy_grp_name = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][copygrpkey]

            pairdisplayout = [row.strip() for row in list(filter(None,self.pairdisplay(storage,copy_grp_name,horcminst,opts='-fcxe').split('\n')))]
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
                    storage.paircreate(inst=horcminst,group=copy_grp_name,quorum=quorum,fence=fence)
                    pairdisplayout = self.pairdisplay(storage,copy_grp_name,horcminst,opts='-fcxe')
                    print(pairdisplayout)
                    pairrequests.append(host)
                    #pairevtwaits[copy_grp_name] = { "host":host, "storage":storage }
                elif paircreateans == 'n':
                    self.logendtask(host=self.host,taskid=taskid,status='skipped')
                    log.info('Skipped paircreate for group {}'.format(copy_grp_name))
                    return
                else:
                    log.info('Invalid answer')        

        for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            self.logtaskstart(taskname,host=host,taskid=taskid)
            copygrpname = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][copygrpkey]
            storageserial = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][storageserialkey]
            self.log.info("Connect to storage serial {}".format(storageserial))
            storage = self.connectstorage(storageserial=storageserial,horcminst=horcminst)
            processpair(storage,host,copygrpname,horcminst)

        return { "pairrequests":pairrequests }

        
    def monitorpairs(self,pairrequests,taskid,copygrpkey,horcminst,storageserialkey):
        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        pairevtwaits = {}

        def processmonitor(host):
            monitorans = ""
            horcmgrp = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][copygrpkey]
            self.log.info("DEBUG horcmgrp: {}".format(horcmgrp))
            
            storageserial = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host][storageserialkey]
            self.log.info("DEBUG storageserial: {}".format(storageserial))
            storage = self.connectstorage(storageserial=storageserial,horcminst=horcminst)
            while not re.search('^y$|^n$',monitorans):
                monitorans = input('Monitor horcmgrp paircreate {} - y/n ? : '.format(horcmgrp))
            if monitorans == 'y':
                
                pairevtwaits[horcmgrp] = { "host":host, "storage":storage }
                
            else:
                log.info('Monitoring not required for group {}'.format(horcmgrp))
                self.logtaskstart('monitorpairevtwaits',host=host,taskid=taskid)
                self.logendtask(host=host,taskid=taskid,status='skipped')

        try:
            for host in pairrequests:
                processmonitor(host)
            pairevtwaitmonitor = self.monitorpairevtwaits(pairevtwaits,'pair',taskid,horcm_inst=horcminst)

        except Exception as e:
            message = 'Unable to monitor pairs, error \'{}\''.format(str(e))
            raise Exception(message)

    def monitorpairevtwaits(self,pairevtwaits,status,taskid,horcm_inst=None,pairevtwaitvolumerole='-s'):
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
                storage = pairevtwaits[copy_grp_name]['storage']
                pairevtwaits[copy_grp_name]['cmd'] = 'pairevtwait -g {} -I{} {} {} -t {}'.format(copy_grp_name,horcminst,pairevtwaitvolumerole,status,pairevtwaittimeout)
                self.logtaskstart(taskname,host=pairevtwaits[copy_grp_name]['host'],taskid=taskid)
                pairevtwaits[copy_grp_name]['proc'] = storage.pairevtwaitexec(pairevtwaits[copy_grp_name]['cmd'])
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
                        pairdisplay = storage.pairdisplay(inst=horcminst,group=copy_grp_name,opts='-fcxe')
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
                        print(storage.pairdisplay(inst=horcminst,group=copy_grp_name,opts='-fcxe')['stdout'])
                        log.info('Poll returned None, continuing to loop')

                if len(procs):
                    log.info('Number of pairevtwait processes left to monitor: {}'.format(len(procs)))
                    time.sleep(pairevtwaitpollseconds)

        log.info('returning from monitorpairevtwaits')

        return returncode


    def paircreate(self,storage,copy_grp_name,horcminst,jp,js,fence='never'):
        storage.paircreate(inst=horcminst,group=copy_grp_name,jp=jp,js=js,fence=fence)

    def raidscan(self,storage,host,taskid,remotereplicationtype,edge_source_serial):
        #self.raidscan(self.target,host,8,remotereplicationtype,edge_source_serial)
        begin = self.now()
        log = self.log
        taskname = inspect.currentframe().f_code.co_name
        self.logtaskstart(taskname,host=self.host,taskid=taskid)
        group = self.jsonin[self.migrationtype]['migrationgroups'][self.group]
        for hostgroup in group[host]['hostgroups']:
            targetportgid = '{}-{}'.format(group[host]['hostgroups'][hostgroup]['targetport'],group[host]['hostgroups'][hostgroup]['targetgid'])
            storage.raidscan(port=targetportgid)

        for ldevid in group[host]['ldevs']:
            target_ldevid = str(group[host]['ldevs'][ldevid]['target_ldevid'])
            # Loop raidscan and report un/used mu's
            freemu = []
            usedmu = []
            for mu in storage.getview('_raidscanmu')[target_ldevid]:
                _ = storage.getview('_raidscanmu')[target_ldevid][mu]
                if _['Fence'] != '-':
                    usedmu.append(mu)
                    group[host]['target_mu_used'] = group[host].get('target_mu_used',{})
                    group[host]['target_mu_used'][mu] = True
                else:
                    freemu.append(mu)
                    group[host]['target_mu_free'] = group[host].get('target_mu_free',{})
                    group[host]['target_mu_free'][mu] = True

            group[host]['ldevs'][ldevid]['target_mu_used'] = usedmu
            group[host]['ldevs'][ldevid]['target_mu_free'] = freemu

    def pairsplit(self,taskid,horcminst,copygrpkey,storageserialkey,messagekey,opts=''):

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

            # Split Pairs
            ans = ""
            while not re.search('^y$|^n$',ans):
                ans = input('\n{}'.format(question))
                log.info('User answered {}'.format(ans))
                if ans == 'y':
                    storage.pairsplit(inst=horcminst,group=copy_grp_name,opts=opts)
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

    # EDGE STORAGE TASK LISTS

    def step1edgetasks(self,remotereplicationtype="GAD+UR"):
        log = self.log

        def prehostchecks(host,edge_source_serial,edge_target):
            # Check ldevs
            log.info('Checking edge target ldevs for host: {}'.format(host))
            self.checkldevs(self.edgestoragearrays[edge_target]['storage'],host,remotereplicationtype,edge_source_serial)
            # Check hostgroups
            log.info('Checking edge target host_grps for host: {}'.format(host))
            self.hostgroupexists(self.edgestoragearrays[edge_target]['storage'],host,remotereplicationtype,edge_source_serial)


        def processhost(host,edge_source_serial,edge_target):
            if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
                log.info('Processing host: {}'.format(host))


                # Locate host group ids
                self.locatehostgroupgids(self.edgestoragearrays[edge_target]['storage'],host,3,remotereplicationtype,edge_source_serial)
                # Configure host groups
                self.configurehostgrps(self.edgestoragearrays[edge_target]['storage'],host,4,remotereplicationtype,edge_source_serial)
                # Configure ldevs
                self.configureldevs(self.edgestoragearrays[edge_target]['storage'],host,5,remotereplicationtype,edge_source_serial)
                # Set capacity saving
                self.modifyldevcapacitysaving(self.edgestoragearrays[edge_target]['storage'],host,6,self.config.get('edge_capacity_saving','disable'),remotereplicationtype,edge_source_serial)
                # Map target luns
                self.configureluns(self.edgestoragearrays[edge_target]['storage'],host,7,remotereplicationtype,edge_source_serial)
                # Raidscan for mu
                self.raidscan(self.target,host,8,remotereplicationtype,edge_source_serial)
                
                # Create copy groups for HUR leg
                parentroot = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]
                childroot = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]
                self.configurecopygrps_hur(self.target,self.edgestoragearrays[edge_target]['storage'],host,9,remotereplicationtype,edge_source_serial,parentroot=parentroot,childroot=childroot)

                # Create copy groups for edge GAD migration
                sourceedge = self.edgestoragearrays[edge_source_serial]['storage']
                replicationtype = "GAD"
                self.configure_copygrps_legacy_dr_gad_migration(sourceedge,self.edgestoragearrays[edge_target]['storage'],host,10,replicationtype,edge_source_serial,parentroot=childroot,childroot=childroot)

                # Create copy groups for existing HUR pairs
                replicationtype = "LEGACYHUR"
                self.configure_copygrps_legacy_hur(self.source,self.edgestoragearrays[edge_source_serial]['storage'],host,11,replicationtype,edge_source_serial,parentroot)
                # Update step
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edgesteps'][self.step]['status'] = "completed"
                self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['edgesteps'][self.step]['endtime'] = self.now()

                # This is in outer script
                # Write host migration files
                hostmigrationfile = '{}{}{}.json'.format(self.migrationdir,self.separator,host)

                
                file = open(hostmigrationfile,"w")
                file.write(json.dumps(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host], indent=4, sort_keys=True))
            else:
                log.info('Skipping this host, omit key set {}'.format(self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']))

        try:
            # Refresh capacity report
            #self.capacityreport(self.target,skipifthisstepcomplete=2,taskid=1)
            # Produce capacity report
            #if not self.producecapacityreport(2): self.exitroutine()

            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                if 'remote_replication' in self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]:
                    for edge_source_serial in self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets']:
                        edge_target = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['edge_target']
                        if self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial].get('omit',False):
                            self.log.warn("Remote replication skipped for host {}".format(host))
                        else:
                            prehostchecks(host,edge_source_serial,edge_target)

            self.createvsms()

            for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
                if 'remote_replication' in self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]:
                    for edge_source_serial in self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets']:
                        edge_target = self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial]['edge_target']
                        if self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['remote_replication'][remotereplicationtype]['targets'][edge_source_serial].get('omit',False):
                            self.log.warn("Remote replication skipped for host {}".format(host))
                        else:
                            processhost(host,edge_source_serial,edge_target)
            
            #sys.exit(0)
            #for host in self.jsonin[self.migrationtype]['migrationgroups'][self.group]:
            #    if not self.jsonin[self.migrationtype]['migrationgroups'][self.group][host]['omit']:
            #        processhost(host)

        except Exception as e:
            raise StorageException('Unable to complete step {}, error {}'.format(self.step,str(e)),Storage,self.log)

    def step2tasks(self,remotereplicationtype="GAD+UR"):

        log = self.log
        now = self.now()
        horcmfilelist = []
        horcminsts = []

        self.target_hur_pvol_horcm_inst = self.config['edge_source_cci_horcm_inst']
        self.target_hur_svol_horcm_inst = self.config['edge_target_cci_horcm_inst']
        self.target_hur_pvol_horcm_udp_port = self.config['target_hur_pvol_horcm_udp_port']
        self.target_hur_svol_horcm_udp_port = self.config['target_hur_svol_horcm_udp_port']

        self.legacy_hur_pvol_horcm_inst = self.config['legacy_hur_pvol_horcm_inst']
        self.legacy_hur_svol_horcm_inst = self.config['legacy_hur_svol_horcm_inst']
        self.legacy_hur_pvol_horcm_udp_port = self.config['legacy_hur_pvol_horcm_udp_port']
        self.legacy_hur_svol_horcm_udp_port = self.config['legacy_hur_svol_horcm_udp_port']

        self.legacy_dr_gadmigration_pvol_horcm_inst = self.config['legacy_dr_gadmigration_pvol_horcm_inst']
        self.legacy_dr_gadmigration_svol_horcm_inst = self.config['legacy_dr_gadmigration_svol_horcm_inst']
        self.legacy_dr_gadmigration_pvol_horcm_udp_port = self.config['legacy_dr_gadmigration_pvol_horcm_udp_port']
        self.legacy_dr_gadmigration_svol_horcm_udp_port = self.config['legacy_dr_gadmigration_svol_horcm_udp_port']

        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,self.target_hur_pvol_horcm_inst))
        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,self.target_hur_pvol_horcm_inst))
        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,self.legacy_hur_pvol_horcm_inst))
        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,self.legacy_hur_svol_horcm_inst))
        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,self.legacy_dr_gadmigration_pvol_horcm_inst))
        horcmfilelist.append('{}horcm{}.conf'.format(self.horcmdir,self.legacy_dr_gadmigration_svol_horcm_inst))

        horcminsts.append(self.target_hur_pvol_horcm_inst)
        horcminsts.append(self.target_hur_svol_horcm_inst)
        horcminsts.append(self.legacy_hur_pvol_horcm_inst)
        horcminsts.append(self.legacy_hur_svol_horcm_inst)
        horcminsts.append(self.legacy_dr_gadmigration_pvol_horcm_inst)
        horcminsts.append(self.legacy_dr_gadmigration_svol_horcm_inst)


        drquorum = self.config['dr_quorum']

        #try:
        if True:
        
            #if self.warningreport(refresh=True): self.exitroutine()
            self.backuphorcmfiles(horcmfilelist,1)
            # Create new hur horcmfiles
            self.createhorcmfiles(2,self.target_hur_pvol_horcm_inst,self.target_hur_svol_horcm_inst,self.target_hur_pvol_horcm_udp_port,self.target_hur_svol_horcm_udp_port,'copygrp_hur','devicegrp_hur','hur_source_serial','hur_target_serial')
            # Create legacy hur horcmfiles
            self.createhorcmfiles(3,self.legacy_hur_pvol_horcm_inst,self.legacy_hur_svol_horcm_inst,self.legacy_hur_pvol_horcm_udp_port,self.legacy_hur_svol_horcm_udp_port,'copygrp_legacy_hur','devicegrp_legacy_hur','legacy_hur_source_serial','legacy_hur_target_serial')
            # Create legacy dr gad migration horcmfiles
            self.createhorcmfiles(4,self.legacy_dr_gadmigration_pvol_horcm_inst,self.legacy_dr_gadmigration_svol_horcm_inst,self.legacy_dr_gadmigration_pvol_horcm_udp_port,self.legacy_dr_gadmigration_svol_horcm_udp_port,'copygrp_legacy_dr_gad','devicegrp_legacy_dr_gad','legacy_dr_source_serial','legacy_dr_target_serial')


            self.restarthorcminsts(horcminsts,5,self.target)
            #self.capacityreport(self.target,skipifthisstepcomplete=2,taskid=4)
            #if not self.producecapacityreport(5): self.exitroutine()
            self.log.info("Creating new DR HUR pairs")
            pairrequests = self.createhurpairs(5,self.target,'copygrp_hur',remotereplicationtype)['pairrequests']
            self.log.info("Monitor DR HUR pair progress")
            self.monitorpairs(pairrequests,6,'copygrp_hur',self.target_hur_pvol_horcm_inst,'hur_source_serial')
            
            # Create DR GAD pairs
            self.log.info("Creating new DR GAD pairs")
            pairrequests = self.createdrgadpairs(7,horcminst=self.legacy_dr_gadmigration_pvol_horcm_inst,copygrpkey='copygrp_legacy_dr_gad',storageserialkey='legacy_dr_source_serial',quorum=drquorum,fence='never')['pairrequests']
            self.log.info("Monitor DR GAD pair progress")
            self.monitorpairs(pairrequests,8,'copygrp_legacy_dr_gad',self.legacy_dr_gadmigration_pvol_horcm_inst,'legacy_dr_source_serial')
            #def createdrgadpairs(self,taskid,horcminst,copygrpkey,storageserialkey,quorum,fence):
            #self.monitorpairevtwaits('codefixreq',pairrequests,'pair',8,self.target,storageserialkeydrgad,self.target_hur_pvol_horcm_inst)
            #self.monitorpairevtwaits('codefixreq',pairevtwaits['pairevtwaits'],'ssus',4,self.legacy_dr_gadmigration_pvol_horcm_inst,pairevtwaitvolflag)
            #def monitorpairs(self,pairrequests,taskid,pairsourcestorage,copygrpkey,horcminst):
            #self.capacityreport(self.target,skipifthisstepcomplete=2,taskid=8)
            self.target.writeundofile()
            self.edgetarget.writeundofile()
        #except Exception as e:
            #raise StorageException('Unable to creategadpairs, error \'{}\''.format(str(e)),Storage,self.log)

    def step3tasks(self):
        try:
            horcminst = self.config['legacy_hur_pvol_horcm_inst']
            copygrpkey = 'copygrp_legacy_hur'
            storageserialkey = 'legacy_hur_source_serial'
            pairevtwaitvolflag = '-s'

            #for host in self.actioninghostlist:
            #    print("--> Actioning this host: {}".format(host))

            #self.log.info('Confirm devices are in PAIR status at source using pairvolchk')
            #self.pairvolchk('source',expectedreturn=23,taskid=1)
            #self.log.info('Confirm devices are in PAIR status at target using pairvolchk')
            #self.pairvolchk('target',expectedreturn=33,taskid=2)
            self.log.info('Pairsplit -S volumes')
            pairevtwaits = self.pairsplit(taskid=3,horcminst=horcminst,copygrpkey=copygrpkey,storageserialkey=storageserialkey,messagekey='hurprepairsplitS',opts='-S')
            self.log.info('Monitor for smpl PAIR status with pairevtwait')
            self.monitorpairevtwaits(pairevtwaits['pairevtwaits'],'smpl',4,horcminst,pairevtwaitvolflag)
        except Exception as e:
            raise StorageException('Failed to complete step {} tasks, error {}'.format(self.step,str(e)),Storage,self.log)

    def step4tasks(self):

        try:
            horcminst = self.config['legacy_dr_gadmigration_svol_horcm_inst']
            pairevtwaitvolflag = '-ss'
            copygrpkey = 'copygrp_legacy_dr_gad'
            storageserialkey = 'legacy_dr_target_serial'

            #for host in self.actioninghostlist:
            #    print("--> Actioning this host: {}".format(host))

            #self.log.info('Confirm devices are in PAIR status at source using pairvolchk')
            #self.pairvolchk('source',expectedreturn=23,taskid=1)
            #self.log.info('Confirm devices are in PAIR status at target using pairvolchk')
            #self.pairvolchk('target',expectedreturn=33,taskid=2)
            self.log.info('Pairsplit -RS volumes')
            #self,storage,taskid,horcminst,copygrpkey,storageserialkey,messagekey,opts
            pairevtwaits = self.pairsplit(taskid=3,horcminst=horcminst,copygrpkey=copygrpkey,storageserialkey=storageserialkey,messagekey='prepairsplitRSDRHost',opts='-RS')
            #pairevtwaits = self.splitRS('target',taskid=3)
            self.log.info('Monitor for ssus PAIR status with pairevtwait')
            self.monitorpairevtwaits(pairevtwaits['pairevtwaits'],'ssus',4,horcminst,pairevtwaitvolflag)
            
        except Exception as e:
            raise StorageException('Failed to complete step {} tasks, error {}'.format(self.step,str(e)),Storage,self.log)


    def step5tasks(self):

        try:
            horcminst = self.config['legacy_dr_gadmigration_svol_horcm_inst']
            pairevtwaitvolflag = '-ss'
            copygrpkey = 'copygrp_legacy_dr_gad'
            storageserialkey = 'legacy_dr_target_serial'

            #for host in self.actioninghostlist:
            #    print("--> Actioning this host: {}".format(host))

            #self.log.info('Confirm devices are in PAIR status at source using pairvolchk')
            #self.pairvolchk('source',expectedreturn=23,taskid=1)
            #self.log.info('Confirm devices are in PAIR status at target using pairvolchk')
            #self.pairvolchk('target',expectedreturn=33,taskid=2)
            self.log.info('Pairsplit -R DR host volumes')
            #self,storage,taskid,horcminst,copygrpkey,storageserialkey,messagekey,opts
            pairevtwaits = self.pairsplit(taskid=3,horcminst=horcminst,copygrpkey=copygrpkey,storageserialkey=storageserialkey,messagekey='prepairsplitRSDRHost',opts='-R')
            #pairevtwaits = self.splitRS('target',taskid=3)
            self.log.info('Monitor for ssus PAIR status with pairevtwait')
            self.monitorpairevtwaits(pairevtwaits['pairevtwaits'],'smpl',4,horcminst,pairevtwaitvolflag)
            
        except Exception as e:
            raise StorageException('Failed to complete step {} tasks, error {}'.format(self.step,str(e)),Storage,self.log)