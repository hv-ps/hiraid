#!/usr/bin/python3.6
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.02
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
# 14/01/2020    v1.1.00     Initial Release - DEC
#
# 24/01/2020    v1.1.01     Add functions getportlogin and getrcu - CM
#
# 03/11/2020    v1.1.02     Add functions getsnapshotgroup_default, getsnapshotgroup, addsnapshotgroup, createsnapshot,
#                           unmapsnapshotsvol, resyncsnapshotmu, snapshotevtwait, deletesnapshotmu - CM
#
# -----------------------------------------------------------------------------------------------------------------------------------

import subprocess
import time
from .raidcomparser import Raidcomparser
import inspect
import os
import json
import re

class raidcom:
    def __init__(self,storage,instance,path="/usr/bin/",cciextension='.sh'):

        self.storage = storage
        self.serial = storage.serial
        self.log = storage.log
        self.instance = instance
        self.path = path
        self.parser = Raidcomparser(storage)
        self.successfulcmds = []
        self.undocmds = []
        self.cciextension = cciextension
        self.cmdoutput = False

    def verbose(self,on=True):
        self.cmdoutput = on
        self.log.info('raidcom execute verbose {}'.format(on))

    def raidqry(self):
        cmd = '{}raidqry -l -I{}'.format(self.path,self.instance)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.raidqry(cmdreturn['stdout'])
        return cmdreturn
    
    def lockresource(self):
        self.log.info('lockresource {}'.format(self.serial))
        cmd = '{}raidcom lock resource -I{} -s {}'.format(self.path,self.instance,self.serial)
        undocmd = ['{}raidcom unlock resource -I{} -s {}'.format(self.path,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)

    def unlockresource(self):
        self.log.info('unlockresource {}'.format(self.serial))
        cmd = '{}raidcom unlock resource -I{} -s {}'.format(self.path,self.instance,self.serial)
        undocmd = ['{}raidcom lock resource -I{} -s {}'.format(self.path,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)

    def identify(self):
        self.log.info('Identify self {}'.format(self.serial))
        resourcegroups = self.getresource()
        #return resourcegroups['views']['defaultview']['0']['V_ID']
        return resourcegroups

    def getresource(self, optviews: list=[]) -> dict:
        
        self.log.info('getresource {}'.format(self.serial))
        cmd = '{}raidcom get resource -key opt -I{} -s {}'.format(self.path,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        if cmdreturn['return']: self.log.info('Command error: {}'.format(cmdreturn['stderr']))
        self.log.debug(cmdreturn['stdout'])
        cmdreturn['views'] = self.parser.getresource(cmdreturn['stdout'],optviews)
        return cmdreturn

    def getldev(self,ldevid, optviews: list=[]) -> dict:
        self.log.debug('getldev {}'.format(ldevid))
        cmd = '{}raidcom get ldev -ldev_id {} -I{} -s {}'.format(self.path,ldevid,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getldev(cmdreturn['stdout'],optviews)
        return cmdreturn
    
    def getport(self,optviews: list=[]) -> dict:
        cmd = '{}raidcom get port -I{} -s {}'.format(self.path,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getport(cmdreturn['stdout'])
        return cmdreturn

    def getportlogin(self,port: str,optviews: list=[]):
        cmd = '{}raidcom get port -port {} -I{} -s {}'.format(self.path,port,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getportlogin(cmdreturn['stdout'],optviews)
        return cmdreturn

    #
    def gethostgrptcscan(self,port,gid=None,name=None,optviews=[]):

        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid or name: raise Exception('Fully qualified port requires no gid{} or name{}'.format(gid,name))
            cmdparam = ''
        else:
            if gid is None and name is None: raise Exception("gethostgrptcscan get requires one argument gid or name, both are None")
            if gid and name: raise Exception('gethostgrptcscan gid and name are mutually exclusive: gid{}, name{}'.format(gid,name))
            cmdparam = ("-"+str(gid)," "+str(name))[gid is None]

        cmd = '{}raidscan -p {}{} -ITC{} -s {} -CLI'.format(self.path,port,cmdparam,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.gethostgrptcscan(cmdreturn['stdout'],optviews)

        return cmdreturn
    #

    def raidscanremote(self,port,gid=None,name=None,mode='TC',optviews=[]):
    
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid or name: raise Exception('Fully qualified port requires no gid{} or name{}'.format(gid,name))
            cmdparam = ''
        else:
            if gid is None and name is None: raise Exception("raidscanremote requires one argument gid or name, both are None")
            if gid and name: raise Exception('raidscanremote gid and name are mutually exclusive: gid{}, name{}'.format(gid,name))
            cmdparam = ("-"+str(gid)," "+str(name))[gid is None]
        

        cmd = '{}raidscan -p {}{} -I{}{} -s {} -CLI'.format(self.path,port,cmdparam,mode,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.raidscanremote(cmdreturn['stdout'],optviews)

        return cmdreturn
    
    def raidscanmu(self,port,gid=None,mu=None,mode='',validmu=[0,1,2,3],optviews=[]):
    
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid: raise Exception('Fully qualified port requires no gid{}'.format(gid))
            cmdparam = ''
        else:
            if gid is None: raise Exception("raidscan requires gid if port is not fully qualified but it is set to none")
            cmdparam = "-"+str(gid)

        if mu == None or mu not in validmu: raise Exception("Please specify valid mu for raidscanmu")
        
        cmd = '{}raidscan -p {}{} -I{}{} -s {} -CLI -mu {}'.format(self.path,port,cmdparam,mode,self.instance,self.serial,mu)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.raidscanmu(cmdreturn['stdout'],mu,optviews)

        return cmdreturn

    def getcopygrp(self,optviews: list=[]) -> dict:
        cmd = '{}raidcom get copy_grp -I{} -s {}'.format(self.path,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getcopygrp(cmdreturn['stdout'])
        return cmdreturn

    def getrcu(self,optviews: list=[]) -> dict:
        cmd = '{}raidcom get rcu -I{} -s {}'.format(self.path,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getrcu(cmdreturn['stdout'])
        return cmdreturn

    def gethostgrp(self,port: str,optviews: list=[]):
        cmd = '{}raidcom get host_grp -port {} -I{} -s {}'.format(self.path,port,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.gethostgrp(cmdreturn['stdout'],optviews)
        return cmdreturn

    def gethostgrprgid(self,port: str,resourcegroupid: int, optviews: list=[]):
        cmd = '{}raidcom get host_grp -port {} -resource {} -I{} -s {}'.format(self.path,port,resourcegroupid,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.gethostgrprgid(cmdreturn['stdout'],resourcegroupid,optviews)
        return cmdreturn

    def gethostgrpkeyhostgrprgid(self,port: str,resourcegroupid: int, optviews: list=[]):
        cmd = '{}raidcom get host_grp -port {} -resource {} -key host_grp -I{} -s {}'.format(self.path,port,resourcegroupid,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.gethostgrpkeyhostgrprgid(cmdreturn['stdout'],resourcegroupid,optviews)
        return cmdreturn

    def gethbawwn(self,port,gid=None,name=None,optviews=[]): 
        
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid or name: raise Exception('Fully qualified port requires no gid{} or name{}'.format(gid,name))
            cmdparam = ''
        else:
            if gid is None and name is None: raise Exception("gethbawwn get requires one argument gid or name, both are None")
            if gid and name: raise Exception('getlun gid and name are mutually exclusive: gid{}, name{}'.format(gid,name))
            cmdparam = ("-"+str(gid)," "+str(name))[gid is None]

        cmd = '{}raidcom get hba_wwn -port {}{} -I{} -s {}'.format(self.path,port,cmdparam,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.gethbawwn(cmdreturn['stdout'],optviews)
        return cmdreturn

    def getlun(self,port,gid=None,name=None,optviews=[]):

        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid or name: raise Exception('Fully qualified port requires no gid{} or name{}'.format(gid,name))
            cmdparam = ''
        else:
            if gid is None and name is None: raise Exception("getlun get requires one argument gid or name, both are None")
            if gid and name: raise Exception('getlun gid and name are mutually exclusive: gid{}, name{}'.format(gid,name))
            cmdparam = ("-"+str(gid)," "+str(name))[gid is None]

        cmd = '{}raidcom get lun -port {}{} -I{} -s {} -key opt'.format(self.path,port,cmdparam,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getlun(cmdreturn['stdout'],optviews)
        self.log.debug('Port: {}{} lun count: {}'.format(port,cmdparam,cmdreturn['views']['metaview']['stats']['luncount']))

        return cmdreturn

    def getpool(self,opts='',optviews=[]):
        cmd = '{}raidcom get pool -I{} -s {} {}'.format(self.path,self.instance,self.serial,opts)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getpool(cmdreturn['stdout'],optviews)
        return cmdreturn

    def execute(self,cmd,undocmds=[],expectedreturn=0):
        self.log.info('Executing: {}'.format(cmd))
        self.log.debug('Expecting return code {}'.format(expectedreturn))
        #proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = proc.communicate()
        # Get the name of the calling function
        #parse = getattr(raidcomparser,inspect.currentframe().f_back.f_code.co_name)(stdout)
        if proc.returncode and proc.returncode != expectedreturn:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+stdout)
            self.log.error("Stderr > "+stderr)
            message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
            raise Exception('Unable to execute Command "{}". Command dump > {}'.format(cmd,message))

        if len(undocmds):
            for undocmd in undocmds: 
                echo = 'echo "Executing: {}"'.format(undocmd)
                self.undocmds.insert(0,undocmd)
                self.undocmds.insert(0,echo)

        if self.cmdoutput:
            self.log.info(stdout)

        return {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }

    def pairevtwaitexec(self,cmd):
        self.log.info('Executing: {}'.format(cmd))
        proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return proc

    def restarthorcminst(self,inst):
        self.log.info('Restarting horcm instance {}'.format(inst))
        cmd = '{}horcmshutdown{} {}'.format(self.path,self.cciextension,inst)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = proc.communicate()
        if proc.returncode:
            if re.search(r'Can\'t be attached to HORC manager',stderr):
                self.log.warn('OK - Looks like horcm inst {} is already stopped'.format(inst))
            else:
                self.log.error("Return > "+str(proc.returncode))
                self.log.error("Stdout > "+stdout)
                self.log.error("Stderr > "+stderr)
                message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
                raise Exception('Unable to shutdown horcm inst: {}. Command dump > {}'.format(cmd,message))
                
        # Now start the instance
        time.sleep(2)
        cmd = '{}horcmstart{} {}'.format(self.path,self.cciextension,inst)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = proc.communicate()
        if proc.returncode:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+stdout)
            self.log.error("Stderr > "+stderr)
            message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
            raise Exception('Unable to start horcm inst: {}. Command dump > {}'.format(cmd,message))


    def checkport(self,port):
        if not re.search(r'^cl\w-\D+\d?$',port,re.IGNORECASE): raise Exception('Malformed port: {}'.format(port))
        return port

    def checkportgid(self,portgid):
        if not re.search(r'cl\w-\D+\d?-\d+',portgid,re.IGNORECASE): raise Exception('Malformed portgid: {}'.format(portgid))
        return portgid

    def addresource(self,resourceGroupName: str,virtualSerialNumber: str=None,virtualModel: str=None):
        cmd = '{}raidcom add resource -resource_name \'{}\' -virtual_type {} {} -I{} -s {}'.format(self.path,resourceGroupName,virtualSerialNumber,virtualModel,self.instance,self.serial)
        undocmd = ['{}raidcom delete resource -resource_name \'{}\' -I{} -s {}'.format(self.path,resourceGroupName,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addhostgrpresource(self,port: str,resource_name: str, host_grp_name=''):
        cmd = '{}raidcom add resource -resource_name \'{}\' -port {} {} -I{} -s {}'.format(self.path,resource_name,port,host_grp_name,self.instance,self.serial)
        undocmd = ['{}raidcom delete resource -resource_name \'{}\' -port {} {} -I{} -s {}'.format(self.path,resource_name,port,host_grp_name,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn
    
    def addldevresource(self, resource_name: str, ldevid: str):
        cmd = '{}raidcom add resource -resource_name \'{}\' -ldev_id {} -I{} -s {}'.format(self.path,resource_name,ldevid,self.instance,self.serial)
        undocmd = ['{}raidcom delete resource -resource_name \'{}\' -ldev_id {} -I{} -s {}'.format(self.path,resource_name,ldevid,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn
    
    def deleteldevresource(self, resource_name: str, ldevid: str):
        cmd = '{}raidcom delete resource -resource_name \'{}\' -ldev_id {} -I{} -s {}'.format(self.path,resource_name,ldevid,self.instance,self.serial)
        undocmd = ['{}raidcom add resource -resource_name \'{}\' -ldev_id {} -I{} -s {}'.format(self.path,resource_name,ldevid,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addhostgroup(self,port: str,hostgroupname: str):
        cmd = '{}raidcom add host_grp -host_grp_name \'{}\' -port {} -I{} -s {}'.format(self.path,hostgroupname,port,self.instance,self.serial)
        undocmd = ['{}raidcom delete host_grp -port {} \'{}\' -I{} -s {}'.format(self.path,port,hostgroupname,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addldev(self,ldevid: str,poolid: int,capacityblk: int):
        self.resetcommandstatus()
        cmd = '{}raidcom add ldev -ldev_id {} -pool {} -capacity {} -I{} -s {}'.format(self.path,ldevid,poolid,capacityblk,self.instance,self.serial)
        undocmd = ['{}raidcom delete ldev -ldev_id {} -pool {} -capacity {} -I{} -s {}'.format(self.path,ldevid,poolid,capacityblk,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        self.getcommandstatus()
        return cmdreturn

    def addldevauto(self,poolid: int,capacityblk: int,resource_id: int,start: int,end: int):
        ldev_range = '{}-{}'.format(start,end)
        self.resetcommandstatus()
        cmd = '{}raidcom add ldev -ldev_id auto -request_id auto -ldev_range {} -pool {} -capacity {} -I{} -s {}'.format(self.path,ldev_range,poolid,capacityblk,self.instance,self.serial)
        #undocmd = ['{}raidcom delete ldev -ldev_id {} -pool {} -capacity {} -I{} -s {}'.format(self.path,ldev_range,poolid,capacityblk,self.instance,self.serial)]
        cmdreturn = self.execute(cmd)
        reqid = cmdreturn['stdout'].rstrip().split(' : ')
        if not re.search(r'REQID',reqid[0]):
            raise Exception('Unable to obtain REQID from stdout {}.'.format(cmdreturn))
        cmdreturn = self.getcommandstatus(reqid[1])
        cmdreturn['views'] = self.parser.getcommandstatus(cmdreturn['stdout'])
        requestid = list(cmdreturn['views']['metaview']['data'].keys())[0]
        autoldevid = cmdreturn['views']['metaview']['data'][requestid]['ID']
        cmdreturn['views']['autoldevid'] = autoldevid
        self.log.info('created ldevid {}'.format(autoldevid))
        undocmd = ['{}raidcom delete ldev -ldev_id {} -pool {} -capacity {} -I{} -s {}'.format(self.path,autoldevid,poolid,capacityblk,self.instance,self.serial)]
        #requestid = cmdreturn['views']['metaview']['data'].keys()[0] 
        echo = 'echo "Executing: {}"'.format(undocmd[0])
        self.undocmds.insert(0,undocmd[0])
        self.undocmds.insert(0,echo)
        # Reset command status
        self.resetcommandstatus(reqid[1])
        return cmdreturn
    
    def resetcommandstatus(self, request_id: str='', requestid_cmd=''):
        if request_id:
            requestid_cmd = '-request_id {}'.format(request_id)
        cmd = '{}raidcom reset command_status {} -I{} -s {}'.format(self.path,requestid_cmd,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def getcommandstatus(self,request_id: str='', requestid_cmd=''):
        if request_id:
            requestid_cmd = '-request_id {}'.format(request_id)
        cmd = '{}raidcom get command_status {} -I{} -s {}'.format(self.path,requestid_cmd,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def addlun(self, port: str, ldevid: str, lun_id: int='', host_grp_name: str='', gid: int='' ):
        cmd = '{}raidcom add lun -port {} {} -ldev_id {} -lun_id {} -I{} -s {}'.format(self.path,port,host_grp_name,ldevid,lun_id,self.instance,self.serial)
        undocmd = ['{}raidcom delete lun -port {} {} -ldev_id {} -lun_id {} -I{} -s {}'.format(self.path,port,host_grp_name,ldevid,lun_id,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn
    
    def deletelun(self, port: str, ldevid: str, lun_id: int='', host_grp_name: str='', gid: int='' ):
        cmd = '{}raidcom delete lun -port {} {} -ldev_id {} -lun_id {} -I{} -s {}'.format(self.path,port,host_grp_name,ldevid,lun_id,self.instance,self.serial)
        undocmd = ['{}raidcom add lun -port {} {} -ldev_id {} -lun_id {} -I{} -s {}'.format(self.path,port,host_grp_name,ldevid,lun_id,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def unmapldev(self,ldevid: str,virtual_ldev_id: str):
        cmd = '{}raidcom unmap resource -ldev_id {} -virtual_ldev_id {} -I{} -s {}'.format(self.path,ldevid,virtual_ldev_id,self.instance,self.serial)
        undocmd = ['{}raidcom map resource -ldev_id {} -virtual_ldev_id {} -I{} -s {}'.format(self.path,ldevid,virtual_ldev_id,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def mapldev(self,ldevid: str,virtual_ldev_id: str):
        cmd = '{}raidcom map resource -ldev_id {} -virtual_ldev_id {} -I{} -s {}'.format(self.path,ldevid,virtual_ldev_id,self.instance,self.serial)
        undocmd = ['{}raidcom unmap resource -ldev_id {} -virtual_ldev_id {} -I{} -s {}'.format(self.path,ldevid,virtual_ldev_id,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def modifyldevname(self,ldevid: str,ldev_name: str):
        cmd = '{}raidcom modify ldev -ldev_id \'{}\' -ldev_name \'{}\' -I{} -s {}'.format(self.path,ldevid,ldev_name,self.instance,self.serial)
        #undocmd = '{}raidcom add ldev -ldev_id \'{}\' -pool {} -capacity {} -I{} -s {}'.format(self.path,ldevid,poolid,capacityblk,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def modifyldevcapacitysaving(self,ldevid: str,capacity_saving: str):
        cmd = '{}raidcom modify ldev -ldev_id \'{}\' -capacity_saving {} -I{} -s {}'.format(self.path,ldevid,capacity_saving,self.instance,self.serial)
        undocmd = ['{}raidcom modify ldev -ldev_id \'{}\' -capacity_saving {} -I{} -s {}'.format(self.path,ldevid,'disable',self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)

        return cmdreturn

    def modifyhostgrp(self,port: str,host_mode: str, host_grp_name: str='', host_mode_opt: list=[]):
        hostmodeopt = ''
        if len(host_mode_opt):
            hostmodeopt = '-host_mode_opt {}'.format(' '.join(map(str,host_mode_opt)))
        cmd = '{}raidcom modify host_grp -port {} {} -host_mode {} {} -I{} -s {}'.format(self.path,port,host_grp_name,host_mode,hostmodeopt,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def adddevicegrp(self, device_grp_name: str, device_name: str, ldev_id: str):
        cmd = '{}raidcom add device_grp -device_grp_name {} {} -ldev_id {} -I{} -s {}'.format(self.path,device_grp_name,device_name,ldev_id,self.instance,self.serial)
        undocmd = ['{}raidcom delete device_grp -device_grp_name {} {} -ldev_id {} -I{} -s {}'.format(self.path,device_grp_name,device_name,ldev_id,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addcopygrp(self, copy_grp_name: str, device_grp_name: str, mirror_id: str=None):

        cmdparam = ''
        if mirror_id != None:
            cmdparam = "-mirror_id "+mirror_id
        cmd = '{}raidcom add copy_grp -copy_grp_name {} -device_grp_name {} {} -I{} -s {}'.format(self.path,copy_grp_name,device_grp_name,cmdparam,self.instance,self.serial)
        undocmd = ['{}raidcom delete copy_grp -copy_grp_name {} -device_grp_name {} -I{} -s {}'.format(self.path,copy_grp_name,device_grp_name,self.instance,self.serial)]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addhbawwn(self,port: str, hba_wwn: str, host_grp_name: str=''):
        cmd = '{}raidcom add hba_wwn -port {} {} -hba_wwn {} -I{} -s {}'.format(self.path,port,host_grp_name,hba_wwn,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def addwwnnickname(self,port: str, hba_wwn: str, wwn_nickname: str, host_grp_name: str=''):
        cmd = '{}raidcom set hba_wwn -port {} {} -hba_wwn {} -wwn_nickname {} -I{} -s {}'.format(self.path,port,host_grp_name,hba_wwn,wwn_nickname,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        return cmdreturn

    # CCI
    def pairdisplay(self,inst: int,group: str,mode='',opts='',optviews: list=[]) -> dict:
        cmd = '{}pairdisplay -g {} -I{}{} {} -CLI'.format(self.path,group,mode,inst,opts)
        cmdreturn = self.execute(cmd)
        #cmdreturn['views'] = self.parser.pairdisplay(cmdreturn['stdout'],optviews)
        return cmdreturn
    
    def pairvolchk(self,inst: int,group: str,device: str,expectedreturn: int):
        cmd = '{}pairvolchk -g {} -d {} -I{} -ss'.format(self.path,group,device,inst)
        cmdreturn = self.execute(cmd,expectedreturn=expectedreturn)
        return cmdreturn

    def paircreate(self, inst: int, group: str, mode='', quorum='', jp='', js='', fence=''):
        undocmd = []
        modifier = ''
        if re.search(r'\d',str(quorum)):
            modifier = '-jq {}'.format(quorum)
            undocmd.insert(0,'{}pairsplit -g {} -I{}{}'.format(self.path,group,mode,inst))
            undocmd.insert(0,'{}pairsplit -g {} -I{}{} -S'.format(self.path,group,mode,inst))

        if re.search(r'\d',str(jp)) and re.search(r'\d',str(js)):
            modifier = '-jp {} -js {}'.format(jp,js)
            undocmd.insert(0,'{}pairsplit -g {} -I{}{} -S'.format(self.path,group,mode,inst))

        cmd = '{}paircreate -g {} -vl {} -f {} -I{}{}'.format(self.path,group,modifier,fence,mode,inst)
        #self.log.info('Paircreate: {}'.format(cmd))
        
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def horctakeover(self, inst: int, group: str):
        cmd = '{}horctakeover -g {} -I{}'.format(self.path,group,inst)
        cmdreturn = self.execute(cmd,expectedreturn=1)
        return cmdreturn

    def pairresyncswaps(self, inst: int, group: str):
        cmd = '{}pairresync -swaps -g {} -I{}'.format(self.path,group,inst)
        cmdreturn = self.execute(cmd,expectedreturn=1)
        return cmdreturn

    def pairsplit(self, inst: int, group: str, opts=''):
        cmd = '{}pairsplit -g {} -I{} {}'.format(self.path,group,inst,opts)
        cmdreturn = self.execute(cmd)
        return cmdreturn
    
    def pairresync(self, inst: int, group: str, opts=''):
        cmd = '{}pairresync -g {} -I{} {}'.format(self.path,group,inst,opts)
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def getsnapshot(self,optviews: list=[]) -> dict:
        cmd = '{}raidcom get snapshot -I{} -s {} -format_time'.format(self.path,self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getsnapshot(cmdreturn['stdout'], optviews)
        return cmdreturn


    def getsnapshotgroup(self, snapshotgroup, optviews: list=[]) -> dict:
        cmd = '{}raidcom get snapshot -snapshotgroup {} -I{} -s {} -format_time -fx'.format(self.path, snapshotgroup, self.instance,self.serial)
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.getsnapshotgroup(cmdreturn['stdout'], optviews)
        return cmdreturn


    def addsnapshotgroup(self, pvol, svol, pool, snapshotgroup, optviews: list=[]) -> dict:
        cmd = '{}raidcom add snapshot -ldev_id {} {} -pool {} -snapshotgroup {} -I{} -s {} '.format(self.path, pvol, svol, pool, snapshotgroup, self.instance, self.serial)
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn


    def createsnapshot(self, snapshotgroup, optviews: list=[]) -> dict:
        cmd = '{}raidcom modify snapshot -snapshotgroup {} -snapshot_data create -I{} -s {} '.format(self.path, snapshotgroup, self.instance, self.serial)
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn


    def unmapsnapshotsvol(self, svol, optviews: list=[]) -> dict:
        cmd = '{}raidcom unmap snapshot -ldev_id {} -I{} -s {} '.format(self.path, svol, self.instance, self.serial)
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn   


    def resyncsnapshotmu(self, pvol, mu, optviews: list=[]) -> dict:
        cmd = '{}raidcom modify snapshot -ldev_id {} -mirror_id {} -snapshot_data resync -I{} -s {} '.format(self.path, pvol, mu, self.instance, self.serial)
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn     

    def snapshotevtwait(self, pvol, mu, status, waittime, optviews: list=[]) -> dict:
        cmd = '{}raidcom get snapshot -ldev_id {} -mirror_id {} -check_status {} -time {} -I{} -s {} '.format(self.path, pvol, mu, status, waittime, self.instance, self.serial)
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn 


    def snapshotgroupevtwait(self, groupName, status, waittime, optviews: list=[]) -> dict:
        cmd = '{}raidcom get snapshot -snapshotgroup {} -check_status {} -time {} -I{} -s {} '.format(self.path, groupName, status, waittime, self.instance, self.serial)
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn 


    def deletesnapshotmu(self, pvol, mu, optviews: list=[]) -> dict:
        cmd = '{}raidcom delete snapshot -ldev_id {} -mirror_id {} -I{} -s {} '.format(self.path, pvol, mu, self.instance, self.serial)
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn 
