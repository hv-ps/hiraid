#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Hitachi Vantara, Inc. All rights reserved.
# Author: Darren Chambers <@Darren-Chambers>
# Author: Giacomo Chiapparini <@gchiapparini-hv>
# Author: Clive Meakin
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

import re
import time
import logging
import subprocess
import collections
import concurrent.futures
from .raidcomparser import Raidcomparser
from .cmdview import Cmdview,CmdviewConcurrent
from .raidcomstats import Raidcomstats
from .storagecapabilities import Storagecapabilities


version = "v1.0.32"

class Raidcom:    

    version = version
    
    def __init__(self,serial,instance,path="/usr/bin/",cciextension='.sh',log=logging,username=None,password=None):

        self.serial = serial
        self.log = log
        self.instance = instance
        self.path = path
        self.cciextension = cciextension
        self.username = username
        self.password = password
        self.cmdoutput = False
        self.views = {}
        self.data = {}
        self.stats = {}
        self.successfulcmds = []
        self.undocmds = []
        self.undodefs = []
        self.parser = Raidcomparser(self,log=self.log)
        self.updatestats = Raidcomstats(self,log=self.log)
        self.login()
        self.identify()
        self.limitations()

    def updateview(self,view: dict,viewupdate: dict) -> dict:
        ''' Update dict view with new dict data '''
        for k, v in viewupdate.items():
            if isinstance(v,collections.abc.Mapping):
                view[k] = self.updateview(view.get(k,{}),v)
            else:
                view[k] = v
        return view

    
    def checkport(self,port):
        if not re.search(r'^cl\w-\D+\d?$',port,re.IGNORECASE): raise Exception('Malformed port: {}'.format(port))
        return port
        
    def checkportgid(self,portgid):
        if not re.search(r'cl\w-\D+\d?-\d+',portgid,re.IGNORECASE): raise Exception('Malformed portgid: {}'.format(portgid))
        return portgid

    def getcommandstatus(self,request_id: str=None, **kwargs) -> object:
        '''
        raidcom get command_status\n
        request_id = <optional request_id>
        '''
        requestid_cmd = ('',f"-request_id {request_id}")[request_id is not None]
        cmd = f"{self.path}raidcom get command_status {requestid_cmd} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        return cmdreturn

    def resetcommandstatus(self, request_id: str='', requestid_cmd='', **kwargs) -> object:
        '''
        raidcom reset command_status
        request_id = <optional request_id>
        '''
        if request_id:
            requestid_cmd = f"-request_id {request_id}"
        cmd = f"{self.path}raidcom reset command_status {requestid_cmd} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        return cmdreturn

    def lockresource(self, **kwargs) -> object:
        '''
        raidcom lock resource -time <seconds>\n
        arguments\n
        time = <seconds>\n
        '''
        time = ('',f"-time {kwargs.get('time')}")[kwargs.get('time') is not None]
        cmd = f"{self.path}raidcom lock resource {time} -I{self.instance} -s {self.serial}"
        undocmd = ['{}raidcom unlock resource -I{} -s {}'.format(self.path,self.instance,self.serial)]
        return self.execute(cmd,undocmd,**kwargs)

    def unlockresource(self, **kwargs) -> object:
        cmd = f"{self.path}raidcom unlock resource -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom lock resource -I{self.instance} -s {self.serial}"]
        return self.execute(cmd,undocmd,**kwargs)

    def identify(self, view_keyname: str='_identity', **kwargs) -> object:
        self.getresource()
        self.raidqry()
        cmdreturn = self.parser.identify()
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.log.info(f"Storage identity: {cmdreturn.view}")
        return cmdreturn

    def raidqry(self, view_keyname: str='_raidqry', **kwargs) -> object:
        '''
        raidqry\n
        examples:\n
        rq = raidqry()\n
        rq = raidqry(datafilter={'Serial#':'350147'})\n
        rq = raidqry(datafilter={'callable':lambda a : int(a['Cache(MB)']) > 50000})\n\n
        Returns Cmdview():\n
        rq.data\n
        rq.view\n
        rq.cmd\n
        rq.returncode\n
        rq.stderr\n
        rq.stdout\n
        rq.stats\n
        '''
        cmd = f"{self.path}raidqry -l -I{self.instance}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.raidqry(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def login(self,**kwargs):
        if self.username and self.password:
            cmd = f"{self.path}raidcom -login {self.username} {self.password} -I{self.instance}"
            return self.execute(cmd,**kwargs)

    def logout(self,**kwargs):
        cmd = f"{self.path}raidcom -logout -I{self.instance} -s {self.serial}"
        return self.execute(cmd,**kwargs)

    def limitations(self):
        for limitation in Storagecapabilities.default_limitations:
            setattr(self,limitation,Storagecapabilities.limitations.get(self.v_id,{}).get(limitation,Storagecapabilities.default_limitations[limitation]))

    def getresource(self, view_keyname: str='_resource_groups', **kwargs) -> object:
        cmd = f"{self.path}raidcom get resource -key opt -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getresource(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def getresourcebyname(self,view_keyname: str='_resource_groups_named', **kwargs) -> object:
        cmd = f"{self.path}raidcom get resource -key opt -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getresourcebyname(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def getldev(self,ldev_id: str, view_keyname: str='_ldevs', update_view=True, **kwargs) -> object:
        '''
        getldev(ldev_id=1000)
        getldev(ldev_id=1000-11000)
        getldev(ldev_id=1000-11000,datafilter={'LDEV_NAMING':'Test_Label_1'})
        getldev(ldev_id=1000-11000,datafilter={'Anykey_when_val_is_callable':lambda a : float(a.get('Used_Block(GB)',0)) > 10})
        '''
        cmd = f"{self.path}raidcom get ldev -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.getldev(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updateview(self.data,{view_keyname:cmdreturn.data})
            self.updatestats.ldevcounts()
        return cmdreturn

    def getldevlist(self, ldevtype: str, view_keyname: str='_ldevlist', update_view=True, key='', **kwargs) -> object:
        '''
        ldevtype = dp_volume | external_volume | journal | pool | parity_grp | mp_blade | defined | undefined | mapped | mapped_nvme | unmapped
        * Some of these options require additional parameters, for example 'pool' requires pool_id = $poolid
        options = { 'key':'front_end' }
        ldevs = getldevlist(ldevtype="mapped",datafilter={'Anykey_when_val_is_callable':lambda a : float(a['Used_Block(GB)']) > 10})\n
        '''
        #options = " ".join([f"-{k} {v}" for k,v in kwargs.get('options',{}).items()])
        key_opt,attr = '',''

        if len(key):
            key_opt = f"-key {key}"
            attr = f"_{key}"

        cmd = f"{self.path}raidcom get ldev -ldev_list {ldevtype} {key_opt} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        #self.parser.getldevlist(cmdreturn,datafilter=kwargs.get('datafilter',{}))

        getattr(self.parser,f'getldevlist{attr}')(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        #self.parser.getattr() getldevlist(cmdreturn,datafilter=kwargs.get('datafilter',{}))

        if update_view:
            self.updateview(self.views,{view_keyname:{ldevtype:cmdreturn.view}})
            
            self.updatestats.ldevcounts()
        return cmdreturn

    def getport(self,view_keyname: str='_ports', update_view=True, **kwargs) -> object:
        '''
        raidcom get port\n
        examples:\n
        ports = getport()\n
        ports = getport(datafilter={'PORT':'CL1-A'})\n
        ports = getport(datafilter={'TYPE':'FIBRE'})\n
        ports = getport(datafilter={'Anykey_when_val_is_callable':lambda a : a['TYPE'] == 'FIBRE' and 'TAR' in a['ATTR']})\n\n
        Returns Cmdview():\n
        ports.data\n
        ports.view\n
        ports.cmd\n
        ports.returncode\n
        ports.stderr\n
        ports.stdout\n
        ports.stats\n
        '''
        cmd = f"{self.path}raidcom get port -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        #self.parser.getport(cmdreturn,datafilter=kwargs.get('datafilter',{}),**kwargs)
        self.parser.getport(cmdreturn,**kwargs)
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updateview(self.data,{view_keyname:cmdreturn.data})
            self.updatestats.portcounters()

        #self.portcounters()
        #raidcomstats.portcounters(self)
        #self.raidcomstats.portcounters()
        
        return cmdreturn




    def gethostgrp(self,port: str, view_keyname: str='_ports', update_view: bool=True, **kwargs) -> object:
        '''
        raidcom get host_grp\n
        Better to use gethostgrp_key_detail rather than this function.\n
        You will instead obtain unused host groups and more importantly the resource group id.\n
        raidcom host_grp\n
        examples:\n
        host_grps = gethostgrp(port="cl1-a")\n
        host_grps = gethostgrp(port="cl1-a",datafilter={'HMD':'VMWARE_EX'})\n
        host_grps = gethostgrp(port="cl1-a",datafilter={'GROUP_NAME':'MyGostGroup})\n
        host_grps = gethostgrp(port="cl1-a",datafilter={'Anykey_when_val_is_callable':lambda a : 'TEST' in a['GROUP_NAME'] })\n
        \n
        Returns Cmdview():\n
        host_grps.data\n
        host_grps.view\n
        host_grps.cmd\n
        host_grps.returncode\n
        host_grps.stderr\n
        host_grps.stdout\n
        host_grps.stats\n
        '''
        cmd = f"{self.path}raidcom get host_grp -port {port} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.gethostgrp(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def gethostgrp_key_detail(self,port: str, view_keyname: str='_ports', update_view: bool=True, **kwargs) -> object:
        '''
        raidcom host_grp -key detail\n
        examples:\n
        host_grps = gethostgrp_key_detail(port="cl1-a")\n
        host_grps = gethostgrp_key_detail(port="cl1-a-140")\n
        host_grps = gethostgrp_key_detail(port="cl1-a",host_grp_name="MyHostGroup")\n
        host_grps = gethostgrp_key_detail(port="cl1-a",datafilter={'HMD':'VMWARE_EX'})\n
        host_grps = gethostgrp_key_detail(port="cl1-a",datafilter={'GROUP_NAME':'MyGostGroup})\n
        host_grps = gethostgrp_key_detail(port="cl1-a",datafilter={'Anykey_when_val_is_callable':lambda a : 'TEST' in a['GROUP_NAME'] })\n
        \n
        Returns Cmdview():\n
        host_grps.data\n
        host_grps.view\n
        host_grps.cmd\n
        host_grps.returncode\n
        host_grps.stderr\n
        host_grps.stdout\n
        host_grps.stats\n
        '''
        
        '''
        raidcom get host_grp -key detail\n
        Differs slightly from raidcom\n
        If port format cl-port-gid or host_grp_name is supplied with cl-port host_grp is filtered.
        '''
        #cmdparam = ""
        
        host_grp_name = kwargs.get('host_grp_name')
        resourceparam = ""
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if host_grp_name: raise Exception(f"Fully qualified port {port} does not require host_grp_name parameter: {host_grp_name}")
            kwargs['datafilter'] = { 'HOST_GRP_ID': port.upper() } 
        elif host_grp_name:
            #cmdparam = f" -host_grp_name '{host_grp_name}' "
            kwargs['datafilter'] = { 'GROUP_NAME': host_grp_name }

        resource_param = ("",f" -resource {kwargs.get('resource')} ")[kwargs.get('resource') is not None]
            
        cmd = f"{self.path}raidcom get host_grp -port {port} -key detail {resource_param} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.gethostgrp_key_detail(cmdreturn,datafilter=kwargs.get('datafilter',{}))

        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.hostgroupcounters()

        return cmdreturn

    def getlun(self,port: str,view_keyname: str='_ports', update_view=True, **kwargs) -> object:
        '''
        raidcom get lun\n
        examples:\n
        luns = getlun(port="cl1-a-1")\n
        luns = getlun(port="cl1-a",host_grp_name="MyHostGroup")\n
        luns = getlun(port="cl1-a",gid=1)\n
        luns = getlun(port="cl1-a-1",datafilter={'LDEV':'12000'})\n
        luns = getlun(port="cl1-a-1",datafilter={'LDEV':['12001','12002']})\n
        luns = getlun(port="cl1-e-1",datafilter={'Anykey_when_val_is_callable':lambda a : int(a['LUN']) > 1})\n
        luns = getlun(port="cl1-e-1",datafilter={'Anykey_when_val_is_callable':lambda a : int(a['LDEV']) > 12000})\n
        \n
        Returns Cmdview():\n
        luns.data\n
        luns.view\n
        luns.cmd\n
        luns.returncode\n
        luns.stderr\n
        luns.stdout\n
        '''
        cmdparam = self.cmdparam(port=port,**kwargs)
        cmd = f"{self.path}raidcom get lun -port {port}{cmdparam} -I{self.instance} -s {self.serial} -key opt"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getlun(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.luncounters()
        return cmdreturn

    def cmdparam(self,**kwargs):
        cmdparam = ""
        if re.search(r'cl\w-\D+\d?-\d+',kwargs['port'],re.IGNORECASE):
            if kwargs.get('gid') or kwargs.get('host_grp_name'): raise Exception(f"Fully qualified port {kwargs['port']} does not require gid or host_grp_name '{kwargs}'")
        else:
            if kwargs.get('gid') is None and kwargs.get('host_grp_name') is None: raise Exception("'gid' or 'host_grp_name' is required when port is not fully qualified (cluster-port-gid)")
            if kwargs.get('gid') and kwargs.get('host_grp_name'): raise Exception(f"'gid' and 'host_grp_name' are mutually exclusive, please supply one or the other > 'gid': {kwargs.get('gid')}, 'host_grp_name': {kwargs.get('host_grp_name')}")
            cmdparam = ("-"+str(kwargs.get('gid'))," "+str(kwargs.get('host_grp_name')))[kwargs.get('gid') is None]
        return cmdparam

    def gethbawwn(self,port,view_keyname: str='_ports', update_view=True, **kwargs) -> object:
        '''
        raidcom get hbawwn\n
        examples:\n
        hbawwns = Raidcom.gethbawwn(port="cl1-a-1")\n
        hbawwns = Raidcom.gethbawwn(port="cl1-a",host_grp_name="MyHostGroup")\n
        hbawwns = Raidcom.gethbawwn(port="cl1-a",gid=1)\n
        \n
        Returns Cmdview():\n
        hbawwns.data\n
        hbawwns.view\n
        hbawwns.cmd\n
        hbawwns.returncode\n
        hbawwns.stderr\n
        hbawwns.stdout\n
        '''

        cmdparam = self.cmdparam(port=port,**kwargs)
        cmd = f"{self.path}raidcom get hba_wwn -port {port}{cmdparam} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.gethbawwn(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.hbawwncounters()
        return cmdreturn


    def getportlogin(self,port: str, view_keyname: str='_ports', update_view=True, **kwargs) -> object:
        '''
        raidcom get port -port {port}\n
        Creates view: self.views['_ports'][port]['PORT_LOGINS'][logged_in_wwn_list].\n
        View is refreshed each time the function is called.\n
        '''
        cmd = f"{self.path}raidcom get port -port {port} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getportlogin(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.portlogincounters()

        return cmdreturn

    def getpool(self, key: str=None, view_keyname: str='_pools', **kwargs) -> object:
        '''
        pools = getpool()\n
        pools = getpool(datafilter={'POOL_NAME':'MyPool'})\n
        pools = getpool(datafilter={'Anykey_when_val_is_callable':lambda a : a['PT'] == 'HDT' or a['PT'] == 'HDP'})\n
        '''
        
        keyswitch = ("",f"-key {key}")[key is not None]
        cmd = f"{self.path}raidcom get pool -I{self.instance} -s {self.serial} {keyswitch}"
        cmdreturn = self.execute(cmd,**kwargs)
        getattr(self.parser,f"getpool_key_{key}")(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updateview(self.data,{view_keyname:cmdreturn.data})
        self.updatestats.poolcounters()
        return cmdreturn

    def getcopygrp(self, view_keyname: str='_copygrps', **kwargs) -> object:
        cmd = f"{self.path}raidcom get copy_grp -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getcopygrp(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def getpath(self,view_keyname: str='_paths', update_view=True, **kwargs) -> object:
        '''
        raidcom get path\n
        examples:\n
        paths = getpath()\n
        paths = getpath(datafilter={'Serial#':'53511'})\n
        paths = getport(datafilter={'Anykey_when_val_is_callable':lambda a : a['CM'] != 'NML'})\n\n
        Returns Cmdview():\n
        paths.data\n
        paths.view\n
        paths.cmd\n
        paths.returncode\n
        paths.stderr\n
        paths.stdout\n
        paths.stats\n
        '''
        cmd = f"{self.path}raidcom get path -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getpath(cmdreturn,datafilter=kwargs.get('datafilter',{}),**kwargs)
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.portcounters()
        
        return cmdreturn
    
    def getparitygrp(self,view_keyname: str='_parity_grp', update_view=True, **kwargs) -> object:
        '''
        raidcom get parity_grp\n
        examples:\n
        parity_grps = getparitygrp()\n
        parity_grps = getparitygrp(datafilter={'R_TYPE':'14D+2P'})\n
        parity_grps = getparitygrp(datafilter={'Anykey_when_val_is_callable':lambda a : a['DRIVE_TYPE'] != 'DKS5E-J900SS'})\n\n
        Returns Cmdview():\n
        parity_grps.serial\n
        parity_grps.data\n
        parity_grps.view\n
        parity_grps.cmd\n
        parity_grps.returncode\n
        parity_grps.stderr\n
        parity_grps.stdout\n
        parity_grps.stats\n
        '''
        cmd = f"{self.path}raidcom get parity_grp -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getparitygrp(cmdreturn,datafilter=kwargs.get('datafilter',{}),**kwargs)
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.portcounters()
        
        return cmdreturn

    def getlicense(self,view_keyname: str='_license', update_view=True, **kwargs) -> object:
        '''
        raidcom get license\n
        examples:\n
        licenses = getlicense()\n
        licenses = getlicense(datafilter={'Type':'PER'})\n
        licenses = getlicense(datafilter={'STS':'INS'})\n
        licenses = getlicense(datafilter={'Anykey_when_val_is_callable':lambda l : 'Migration' in l['Name']})\n\n
        Returns Cmdview():\n
        parity_grps.serial\n
        parity_grps.data\n
        parity_grps.view\n
        parity_grps.cmd\n
        parity_grps.returncode\n
        parity_grps.stderr\n
        parity_grps.stdout\n
        parity_grps.stats\n
        '''
        cmd = f"{self.path}raidcom get license -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getlicense(cmdreturn,datafilter=kwargs.get('datafilter',{}),**kwargs)
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.portcounters()
        
        return cmdreturn
    # Snapshots

    def getsnapshot(self, view_keyname: str='_snapshots', **kwargs) -> object:
        cmd = f"{self.serial}raidcom get snapshot -I{self.instance} -s {self.serial} -format_time"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getsnapshot(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn


    def getsnapshotgroup(self, snapshotgroup: str, fx: str=None, view_keyname: str='_snapshots', **kwargs) -> object:
        fxarg = ("",f"-fx")[fx is not None]
        cmd = f"{self.path}raidcom get snapshot -snapshotgroup {snapshotgroup} -I{self.instance} -s {self.serial} -format_time {fxarg}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getsnapshotgroup(cmdreturn)
        return cmdreturn

    def addsnapshotgroup(self, pvol: str, svol: str, pool: str, snapshotgroup: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add snapshot -ldev_id {pvol} {svol} -pool {pool} -snapshotgroup {snapshotgroup} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.getcommandstatus()
        return cmdreturn

    def createsnapshot(self, snapshotgroup: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom modify snapshot -snapshotgroup {snapshotgroup} -snapshot_data create -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.getcommandstatus()
        return cmdreturn

    def unmapsnapshotsvol(self, svol: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom unmap snapshot -ldev_id {svol} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.getcommandstatus()
        return cmdreturn   

    def resyncsnapshotmu(self, pvol: str, mu: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom modify snapshot -ldev_id {pvol} -mirror_id {mu} -snapshot_data resync -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.getcommandstatus()
        return cmdreturn     

    def snapshotevtwait(self, pvol: str, mu: int, checkstatus: str, waittime: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom get snapshot -ldev_id {pvol} -mirror_id {mu} -check_status {checkstatus} -time {waittime} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.getcommandstatus()
        return cmdreturn 

    def snapshotgroupevtwait(self, snapshotgroup: str, checkstatus: str, waittime: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom get snapshot -snapshotgroup {snapshotgroup} -check_status {checkstatus} -time {waittime} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.getcommandstatus()
        return cmdreturn 


    def deletesnapshotmu(self, pvol: str, mu: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom delete snapshot -ldev_id {pvol} -mirror_id {mu} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.getcommandstatus()
        return cmdreturn 

    '''
    commands
    '''
    def XXXaddldev(self,ldev_id: str,poolid: int,capacity: int, return_ldev: bool=True, **kwargs) -> object:
        '''
        raidcom add ldev -ldev_id <Ldev#> -pool <ID#> -capacity <block_size>\n
        examples:\n
        ldev = Raidcom.addldev(ldev_id=12025,poolid=0,capacity=2097152)\n
        ldev = Raidcom.addldev(ldev_id=12025,poolid=0,capacity="1g")\n
        \n
        Returns Cmdview():\n
        ldev.data\n
        ldev.view\n
        ldev.cmd\n
        ldev.undocmds\n
        ldev.returncode\n
        ldev.stderr\n
        ldev.stdout\n
        '''
        cmdparam, ucmdparam, cmddict, ucmddict = '','',{},{}
        options = { 'capacity_saving': ['compression','deduplication_compression','disable'], 'compression_acceleration':['enable','disable'], 'capacity_saving_mode':['inline','postprocess']}

        for arg in kwargs:
            if arg in options:
                if kwargs[arg] not in options[arg]:
                    raise Exception(f"Optional command argument {arg} has incorrect value {kwargs[arg]}, possible options are {options[arg]}")
                cmdparam = f"{cmdparam} -{arg} {kwargs[arg]} "
                cmddict[arg] = kwargs[arg]
            if arg == "capacity_saving" and kwargs[arg] != "disable":
                ucmdparam = f"-operation initialize_capacity_saving"
                ucmddict['operation'] = 'initialize_capacity_saving'

        cmd = f"{self.path}raidcom add ldev -ldev_id auto -ldev_range {ldev_id}-{ldev_id} -pool {poolid} -capacity {capacity} {cmdparam} -request_id auto -I{self.instance} -s {self.serial}"
        cmddef = { 'cmddef': 'addldev', 'args':{ 'ldev_id':ldev_id, 'poolid':poolid, 'capacity':capacity }.update(cmddict)}

        undocmd = [f"{self.path}raidcom delete ldev -ldev_id {ldev_id} -pool {poolid} -capacity {capacity} {ucmdparam} -I{self.instance} -s {self.serial}"]
        undodef = [{ 'undodef': 'deleteldev', 'args':{ 'ldev_id':ldev_id }.update(ucmddict)}]

        cmdreturn = self.execute(cmd=cmd,undocmds=undocmd,undodefs=undodef,raidcom_asyncronous=False,**kwargs)
        print(cmdreturn.stdout)
        reqid = cmdreturn.stdout.rstrip().split(' : ')
        
        if not re.search(r'REQID',reqid[0]):
            raise Exception(f"Unable to obtain REQID from stdout {cmdreturn}.")
        try:
            self.getcommandstatus(request_id=reqid[1])
        except Exception as e:
            raise Exception(f"Failed to create ldev {ldev_id}, request_id {reqid[1]} error {e}")

        #self.resetcommandstatus(request_id=reqid[1])

        if not kwargs.get('noexec') and return_ldev:
            getldev = self.getldev(ldev_id=ldev_id)
            cmdreturn.data = getldev.data
            cmdreturn.view = getldev.view
        return cmdreturn
    
    def addldev(self,ldev_id: str,poolid: int,capacity: int, return_ldev: bool=True, start: int=None, end: int=None, **kwargs) -> object:
        '''
        raidcom add ldev -ldev_id <Ldev#> -pool <ID#> -capacity <block_size>\n
        examples:\n
        ldev = Raidcom.addldev(ldev_id=12025,poolid=0,capacity=2097152)\n
        ldev = Raidcom.addldev(ldev_id=12025,poolid=0,capacity="1g")\n
        ldev = Raidcom.addldev(ldev_id='auto',poolid=0,start=1000,end=2000,capacity="1g",capacity_saving="compression")\n
        \n
        Returns Cmdview():\n
        ldev.data\n
        ldev.view\n
        ldev.cmd\n
        ldev.undocmds\n
        ldev.returncode\n
        ldev.stderr\n
        ldev.stdout\n
        '''
        cmdparam, ucmdparam, cmddict, ucmddict = '','',{},{}
        options = { 'capacity_saving': ['compression','deduplication_compression','disable'], 'compression_acceleration':['enable','disable'], 'capacity_saving_mode':['inline','postprocess']}

        for arg in kwargs:
            if arg in options:
                if kwargs[arg] not in options[arg]:
                    raise Exception(f"Optional command argument {arg} has incorrect value {kwargs[arg]}, possible options are {options[arg]}")
                cmdparam = f"{cmdparam} -{arg} {kwargs[arg]} "
                cmddict[arg] = kwargs[arg]
            if arg == "capacity_saving" and kwargs[arg] != "disable":
                ucmdparam = f"-operation initialize_capacity_saving"
                ucmddict['operation'] = 'initialize_capacity_saving'

        if ldev_id == 'auto':
            if not start or not end:
                raise Exception(f"When ldev_id is specified as 'auto' range_start and range_end ldev_ids must also be supplied")
            else:
                cmd = f"{self.path}raidcom add ldev -ldev_id auto -ldev_range {start}-{end} -pool {poolid} -capacity {capacity} {cmdparam} -request_id auto -I{self.instance} -s {self.serial}"
                cmdreturn = self.execute(cmd=cmd,raidcom_asyncronous=False,**kwargs)
        else:
            cmd = f"{self.path}raidcom add ldev -ldev_id auto -ldev_range {ldev_id}-{ldev_id} -pool {poolid} -capacity {capacity} {cmdparam} -request_id auto -I{self.instance} -s {self.serial}"
            cmdreturn = self.execute(cmd=cmd,raidcom_asyncronous=False,**kwargs)
            
        reqid = cmdreturn.stdout.rstrip().split(' : ')
        
        if not re.search(r'REQID',reqid[0]):
            raise Exception(f"Unable to obtain REQID from stdout {cmdreturn}.")
        try:
            getcommandstatus = self.getcommandstatus(request_id=reqid[1])
            self.parser.getcommandstatus(getcommandstatus)
            auto_ldev_id = getcommandstatus.data[0]['ID']
            undocmd = f"{self.path}raidcom delete ldev -ldev_id {auto_ldev_id} -pool {poolid} -capacity {capacity} {ucmdparam} -I{self.instance} -s {self.serial}"
            #undodef = { 'undodef': 'deleteldev', 'args':{ 'ldev_id':auto_ldev_id }.update(ucmddict)}
            undodef = { 'undodef': 'deleteldev', 'args':{ 'ldev_id':auto_ldev_id }}
            cmdreturn.undocmds.insert(0,undocmd)
            cmdreturn.undodefs.insert(0,undodef)
            echo = f'echo "Executing: {undocmd}"'
            self.undocmds.insert(0,undocmd)
            self.undocmds.insert(0,echo)
            self.resetcommandstatus(request_id=reqid[1])
        except Exception as e:
            raise Exception(f"Failed to create ldev {ldev_id}, request_id {reqid[1]} error {e}")

        if not kwargs.get('noexec') and return_ldev:
            getldev = self.getldev(ldev_id=auto_ldev_id)
            cmdreturn.data = getldev.data
            cmdreturn.view = getldev.view
        
        return cmdreturn

    def extendldev(self, ldev_id: str, capacity: int, **kwargs) -> object:
        '''
        ldev_id   = Ldevid to extend\n
        capacity = capacity in blk\n
        Where 'capacity' will add specified blks to current capacity 
        '''
        #self.resetcommandstatus()
        cmd = f"{self.path}raidcom extend ldev -ldev_id {ldev_id} -capacity {capacity} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,raidcom_asyncronous=True,**kwargs)
        #self.getcommandstatus()
        return cmdreturn

    def populateundo(self,undodef,undocmds,undodefs):
        undocmds.insert(0,getattr(self,undodef['undodef'])(noexec=True,**undodef['args']).cmd)
        undodefs.insert(0,undodef)

    def deleteldev_undo(self,ldev_id: str, **kwargs):
        print(f"deleteldev_undo {ldev_id}")
        ldev = self.getldev(ldev_id=ldev_id)
        undocmds = []
        undodefs = []
        if len(ldev.data[0].get('LDEV_NAMING',"")):
            print("populateundo - LDEV_NAMING")
            self.populateundo({'undodef':'modifyldevname','args':{'ldev_id':ldev_id,'ldev_name':ldev.data[0]['LDEV_NAMING']}},undocmds,undodefs)
        if ldev.data[0]['VOL_TYPE'] != "NOT DEFINED":
            print("populateundo - NOT DEFINED")
            self.populateundo({'undodef':'addldev','args':{'ldev_id':ldev_id,'capacity':ldev.data[0]['VOL_Capacity(BLK)'],'poolid':ldev.data[0]['B_POOLID']}},undocmds,undodefs)
        print("returning from populateundo")
        return undocmds,undodefs,ldev
            
    def XXdeleteldev(self, ldev_id: str, **kwargs) -> object:
        print(f"deleteldev {ldev_id}, call deleteldev_undo")
        undocmds,undodefs,ldev = self.deleteldev_undo(ldev_id=ldev_id)
        print(f"after deleteldev_undo")
        #self.resetcommandstatus()
        cmd = f"{self.path}raidcom delete ldev -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"

        cmdreturn = self.execute(cmd,undocmds,undodefs,raidcom_asyncronous=True,**kwargs)
        cmdreturn.view = ldev.view
        cmdreturn.data = ldev.data
        #self.getcommandstatus()
        return cmdreturn

    def deleteldev(self,ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom delete ldev -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,raidcom_asyncronous=True,**kwargs)
        return cmdreturn

    def addresource(self,resource_name: str,virtualSerialNumber: str=None,virtualModel: str=None, **kwargs) -> object:
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -I{self.instance} -s {self.serial}"]
        undodef = [{'undodef':'deleteresource','args':{'resource_name':resource_name}}]
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -virtual_type {virtualSerialNumber} {virtualModel} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,undocmd,undodef,**kwargs)
        return cmdreturn

    def deleteresource_undo(self,resource_name: str,**kwargs):
        undocmds = []
        undodefs = []
        resource_data = self.getresourcebyname()
        rgrp = resource_data.view[resource_name]
        self.populateundo({'undodef':'addresource','args':{'resource_name':rgrp['RS_GROUP'],'virtualSerialNumber':rgrp['V_Serial#'],'virtualModel':rgrp['V_ID']}},undocmds,undodefs)
        return undocmds,undodefs
    
    def deleteresource(self,resource_name: str, **kwargs) -> object:
        undocmds,undodefs = self.deleteresource_undo(resource_name=resource_name,**kwargs)
        cmd = f"{self.path}raidcom delete resource -resource_name '{resource_name}' -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,undocmds,undodefs,**kwargs)
        return cmdreturn

    def addhostgrpresource(self,port: str,resource_name: str, **kwargs) -> object:
        cmdparam = self.cmdparam(port=port,**kwargs)
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -port {port}{cmdparam} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -port {port}{cmdparam} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def deletehostgrpresourceid(self,port: str,resource_id: str, **kwargs) -> object:
        cmdparam = self.cmdparam(port=port,**kwargs)
        resource_name = self.views['_resource_groups'][str(resource_id)]['RS_GROUP']
        cmd = f"{self.path}raidcom delete resource -resource_name '{resource_name}' -port {port}{cmdparam} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom add resource -resource_name '{resource_name}' -port {port}{cmdparam} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addhostgrpresourceid(self,port: str,resource_id: str, **kwargs) -> object:
        cmdparam = self.cmdparam(port=port,**kwargs)
        resource_name = self.views['_resource_groups'][str(resource_id)]['RS_GROUP']
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -port {port}{cmdparam} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -port {port}{cmdparam} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn
    
    def addldevresource(self, resource_name: str, ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn
    
    def deleteldevresourceid(self, resource_id: int, ldev_id: str, **kwargs) -> object:
        #resource_name = self.getresource().view[str(resource_id)]['RS_GROUP']
        resource_name = self.views['_resource_groups'][str(resource_id)]['RS_GROUP']
        cmdreturn = self.deleteldevresource(resource_name=resource_name,ldev_id=ldev_id,**kwargs)
        return cmdreturn

    def deleteldevresource(self, resource_name: str, ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom delete resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom add resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addhostgrp(self,port: str,host_grp_name: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add host_grp -host_grp_name '{host_grp_name}' -port {port} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete host_grp -port {'-'.join(port.split('-')[:2])} '{host_grp_name}' -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        if not kwargs.get('noexec'):
            host_grp = self.gethostgrp_key_detail(port='-'.join(port.split('-')[:2]),host_grp_name=host_grp_name)
            cmdreturn.data = host_grp.data
            cmdreturn.view = host_grp.view
        return cmdreturn

    def addhostgroup(self,port: str,hostgroupname: str, **kwargs) -> object:
        '''Deprecated in favour of addhostgrp'''
        return self.addhostgrp(port=port,host_grp_name=hostgroupname,**kwargs)

    def deletehostgrp_undo(self,port:str, **kwargs) -> object:
        undocmds = []
        undodefs = []

        def populateundo(undodef):
            undocmds.insert(0,getattr(self,undodef['undodef'])(noexec=True,**undodef['args']).cmd)
            undodefs.insert(0,undodef)
        
        _host_grp_detail = self.gethostgrp_key_detail(port=port,**kwargs)

        if len(_host_grp_detail.data) < 1 or _host_grp_detail.data[0]['GROUP_NAME'] == "-":
            self.log.warning(f"Host group does not exist > port: '{port}', kwargs: '{kwargs}', data: {_host_grp_detail.data}")
            return undocmds, undodefs
        if len(_host_grp_detail.data) > 1:
            raise Exception(f"Incorrect number of host groups returned {len(_host_grp_detail.data)}, cannot exceed 1. {_host_grp_detail.data}")

        for host_grp in _host_grp_detail.data:
            populateundo({'undodef':'addhostgrp', 'args':{'port':host_grp['HOST_GRP_ID'], 'host_grp_name':host_grp['GROUP_NAME']}})

            if len(host_grp['HMO_BITs']):
                populateundo({'undodef':'modifyhostgrp', 'args':{'port':host_grp['PORT'], 'host_grp_name':host_grp['GROUP_NAME'], 'host_mode': host_grp['HMD'].replace('/IRIX',''), 'host_mode_opt':host_grp['HMO_BITs']}})
                
            if host_grp['RGID'] != "0":
                resource_name = self.views['_resource_groups'][host_grp['RGID']]['RS_GROUP']
                populateundo({'undodef':'addhostgrpresource', 'args':{'port':host_grp['PORT'], 'host_grp_name':host_grp['GROUP_NAME'], 'resource_name':resource_name}})
            
            # luns
            luns = self.getlun(port=port,**kwargs)
            for lun in luns.data:
                populateundo({'undodef':'addlun','args':{'port':lun['PORT'],'host_grp_name':host_grp['GROUP_NAME'],'ldev_id':lun['LDEV'],'lun_id':lun['LUN']}})
            
            # hba_wwns
            hbawwns = self.gethbawwn(port=port,**kwargs)
            for hbawwn in hbawwns.data:
                populateundo({'undodef':'addhbawwn','args':{'port':hbawwn['PORT'],'host_grp_name':host_grp['GROUP_NAME'],'hba_wwn':hbawwn['HWWN']}})
                if hbawwn['NICK_NAME'] != "-":
                    populateundo({'undodef':'setwwnnickname','args':{'port':hbawwn['PORT'],'host_grp_name':host_grp['GROUP_NAME'],'hba_wwn':hbawwn['HWWN'],'wwn_nickname':hbawwn['NICK_NAME']}})

        return undocmds,undodefs

    def deletehostgrp(self,port: str, **kwargs) -> object:
        cmdparam = self.cmdparam(port=port,**kwargs)
        '''
        cmdparam = ""
        host_grp_name = kwargs.get('host_grp_name')
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if host_grp_name: raise Exception(f"Fully qualified port {port} does not require host_grp_name parameter: {host_grp_name}")
        else:
            if not host_grp_name:
                raise Exception("Without a fully qualified port (cluster-port-gid) host_grp_name parameter is required.")
            else:
                cmdparam = f" '{host_grp_name}' "
        '''
        undocmds,undodefs = self.deletehostgrp_undo(port=port,**kwargs)
        cmd = f"{self.path}raidcom delete host_grp -port {port}{cmdparam} -I{self.instance} -s {self.serial}"    
        if len(undocmds):
            cmdreturn = self.execute(cmd,undocmds,undodefs,**kwargs)
        else:
            self.log.warning(f"Host group does not appear to exist - port: '{port}', kwargs: '{kwargs}'. Returning quietly")
            cmdreturn = self.execute(cmd,undocmds,undodefs,noexec=True)

        return cmdreturn

    def resethostgrp(self,port: str, **kwargs) -> object:
        self.deletehostgrp(port=port,**kwargs)
        
    #def dismantlehostgrp(self,port: str, **kwargs) -> object:

    def addldevauto(self,poolid: int,capacity: int,start: int,end: int, **kwargs):
        ldev_range = '{}-{}'.format(start,end)
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom add ldev -ldev_id auto -request_id auto -ldev_range {ldev_range} -pool {poolid} -capacity {capacity} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        reqid = cmdreturn.stdout.rstrip().split(' : ')
        if not re.search(r'REQID',reqid[0]):
            raise Exception(f"Unable to obtain REQID from stdout {cmdreturn}.")
        getcommandstatus = self.getcommandstatus(request_id=reqid[1])
        self.parser.getcommandstatus(getcommandstatus)
        auto_ldev_id = getcommandstatus.data[0]['ID']
        undodef = {'undodef':'deleteldev','args':{'ldev_id':auto_ldev_id}}

        print(f"auto_ldev_id: {auto_ldev_id}")
        print(f"Autoldev requires a little work for an undo..")
        #undo = self.deleteldev(ldev_id=auto_ldev_id,noexec=True)
        #undocmd = [f"{self.path}raidcom delete ldev -ldev_id {auto_ldev_id} -I{self.instance} -s {self.serial}"]
        #undodef = [{ 'undodef': 'deleteldev', 'args':{ 'ldev_id':auto_ldev_id }.update(ucmddict)}]
        print("After deleteldev")
        cmdreturn.view = undo.view
        cmdreturn.data = undo.data
        cmdreturn.undocmds.insert(0,undo.cmd)
        cmdreturn.undodefs.insert(0,undodef)
        self.undocmds.insert(0,cmdreturn.undocmds[0])
        self.undocmds.insert(0,f'echo "Executing: {cmdreturn.undocmds[0]}"')
        self.undodefs.insert(0,cmdreturn.undodefs[0])
        # Reset command status
        self.resetcommandstatus(reqid[1])
        return cmdreturn

    #def addlun(self, port: str, ldev_id: str, lun_id: int='', host_grp_name: str='', gid: int='', **kwargs) -> object:

    def addlun(self, port: str, ldev_id: str, **kwargs) -> object:


        cmdparam = self.cmdparam(port=port, ldev_id=ldev_id, **kwargs)
        if kwargs.get('lun_id'):
            lun_id = kwargs['lun_id']
            cmd = f"{self.path}raidcom add lun -port {port}{cmdparam} -ldev_id {ldev_id} -lun_id {kwargs['lun_id']} -I{self.instance} -s {self.serial}"
            
            #echo = f'echo "Executing: {undocmd}"'
            #self.undocmds.insert(0,undocmd)
            #self.undocmds.insert(0,echo)
            #cmdreturn.undocmds.insert(0,undocmd)
            
            cmdreturn = self.execute(cmd=cmd,**kwargs)
        else:
            cmd = f"{self.path}raidcom add lun -port {port}{cmdparam} -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
            cmdreturn = self.execute(cmd=cmd,**kwargs)
            lun = re.match('^raidcom: LUN \d+\((0x[0-9-af]+)\) will be used for adding',cmdreturn.stdout,re.I)
            if lun:
                #print(lun.group(1))
                lun_id = int(lun.group(1),16)
            else:
                raise Exception(f"Unable to extract lun information while mapping ldev_id {ldev_id} to {port}{cmdparam}")

        undocmd = f"{self.path}raidcom delete lun -port {port}{cmdparam} -ldev_id {ldev_id} -lun_id {lun_id} -I{self.instance} -s {self.serial}"
        echo = f'echo "Executing: {undocmd}"'
        self.undocmds.insert(0,undocmd)
        self.undocmds.insert(0,echo)
        cmdreturn.undocmds.insert(0,undocmd)    
        
        if not kwargs.get('noexec') and kwargs.get('return_lun'):
            getlun = self.getlun(port=f"{port}",lun_filter={ 'LUN': str(lun_id) },**kwargs)
            cmdreturn.data = getlun.data
            cmdreturn.view = getlun.view

        return cmdreturn
    #def gethbawwn(self,port,gid=None,name=None,view_keyname: str='_ports', update_view=True, **kwargs) -> object:
    
    def XXXXcmdparam(self,**kwargs):
        cmdparam = ""
        if re.search(r'cl\w-\D+\d?-\d+',kwargs['port'],re.IGNORECASE):
            if kwargs.get('gid') or kwargs.get('host_grp_name'): raise Exception(f"Fully qualified port {kwargs['port']} does not require gid or host_grp_name '{kwargs}'")
        else:
            if kwargs.get('gid') is None and kwargs.get('host_grp_name') is None: raise Exception("'gid' or 'host_grp_name' is required when port is not fully qualified (cluster-port-gid)")
            if kwargs.get('gid') and kwargs.get('host_grp_name'): raise Exception(f"'gid' and 'host_grp_name' are mutually exclusive, please supply one or the other > 'gid': {kwargs.get('gid')}, 'host_grp_name': {kwargs.get('host_grp_name')}")
            cmdparam = ("-"+str(kwargs.get('gid'))," "+str(kwargs.get('host_grp_name')))[kwargs.get('gid') is None]
        return cmdparam


    def deletelun(self, port: str, ldev_id: str, lun_id: int='', host_grp_name: str='', gid: int='', **kwargs) -> object:
        cmd = f"{self.path}raidcom delete lun -port {port} {host_grp_name} -ldev_id {ldev_id} -lun_id {lun_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom add lun -port {port} {host_grp_name} -ldev_id {ldev_id} -lun_id {lun_id} -I{self.instance} -s {self.serial}"]
        undodef = [{'undodef':'addlun','args':{'port':port, 'host_grp_name':host_grp_name, 'ldev_id':ldev_id,'lun_id':lun_id}}]
        cmdreturn = self.execute(cmd,undocmd,undodef,**kwargs)
        return cmdreturn

    def unmapldev(self,ldev_id: str,virtual_ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom unmap resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom map resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def mapldev(self,ldev_id: str,virtual_ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom map resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom unmap resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def modifyldevname(self,ldev_id: str,ldev_name: str, **kwargs) -> object:
        #self.resetcommandstatus()
        cmd = f'{self.path}raidcom modify ldev -ldev_id {ldev_id} -ldev_name "{ldev_name}" -I{self.instance} -s {self.serial}'
        cmdreturn = self.execute(cmd,raidcom_asyncronous=False,**kwargs)
        #self.getcommandstatus()
        return cmdreturn

    def modifyldevcapacitysaving(self,ldev_id: str,capacity_saving: str, undo_saving: str="disable", **kwargs) -> object:
        #self.resetcommandstatus()
        '''
        Not fetching the previous capacity saving setting and defaulting to disable for undo.
        '''
        cmd = f"{self.path}raidcom modify ldev -ldev_id {ldev_id} -capacity_saving {capacity_saving} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom modify ldev -ldev_id {ldev_id} -capacity_saving {undo_saving} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,raidcom_asyncronous=True,**kwargs)
        #self.getcommandstatus()
        return cmdreturn

    def modifyhostgrp(self,port: str,host_mode: str, host_grp_name: str='', host_mode_opt: list=[], **kwargs) -> object:
        host_mode_opt_arg = ("",f"-set_host_mode_opt {' '.join(map(str,host_mode_opt))}")[len(host_mode_opt) > 0]
        cmd = f"{self.path}raidcom modify host_grp -port {port} {host_grp_name} -host_mode {host_mode} {host_mode_opt_arg} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        return cmdreturn

    def adddevicegrp(self, device_grp_name: str, device_name: str, ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add device_grp -device_grp_name {device_grp_name} {device_name} -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete device_grp -device_grp_name {device_grp_name} {device_name} -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addcopygrp(self, copy_grp_name: str, device_grp_name: str, mirror_id: str=None, **kwargs) -> object:
        mirror_id_arg = ("",f"-mirror_id {mirror_id}")[mirror_id is not None] 
        cmd = f"{self.path}raidcom add copy_grp -copy_grp_name {copy_grp_name} -device_grp_name {device_grp_name} {mirror_id_arg} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete copy_grp -copy_grp_name {copy_grp_name} -device_grp_name {device_grp_name} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addhbawwn(self,port: str, hba_wwn: str, **kwargs) -> object:
        '''
        raidcom add hba_wwn\n
        examples:\n
        addhbawwn = Raidcom.addhbawwn(port="cl1-a-1")\n
        addhbawwn = Raidcom.addhbawwn(port="cl1-a",host_grp_name="MyHostGroup")\n
        addhbawwn = Raidcom.addhbawwn(port="cl1-a",gid=1)\n
        \n
        Returns Cmdview():\n
        addhbawwn.cmd\n
        addhbawwn.returncode\n
        addhbawwn.stderr\n
        addhbawwn.stdout\n
        '''
        cmdparam = self.cmdparam(port=port,**kwargs)
        cmd = f"{self.path}raidcom add hba_wwn -port {port}{cmdparam} -hba_wwn {hba_wwn} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete hba_wwn -port {port}{cmdparam} -hba_wwn {hba_wwn} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addwwnnickname(self,port: str, hba_wwn: str, wwn_nickname: str, **kwargs) -> object:
        '''
        Deprecated in favour of setwwnnickname
        '''
        cmdparam = self.cmdparam(port=port,**kwargs)
        cmd = f"{self.path}raidcom set hba_wwn -port {port}{cmdparam} -hba_wwn {hba_wwn} -wwn_nickname {wwn_nickname} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def setwwnnickname(self,port: str, hba_wwn: str, wwn_nickname: str, **kwargs) -> object:
        '''
        raidcom set hba_wwn -port <port> [<host group name>] -hba_wwn <WWN strings> -wwn_nickname <WWN nickname>\n
        examples:\n
        setwwnnickname = Raidcom.setwwnnickname(port="cl1-a-1","hba_wwn":"1010101010101010","wwn_nickname":"BestWwnEver")\n
        setwwnnickname = Raidcom.setwwnnickname(port="cl1-a",host_grp_name="MyHostGroup","hba_wwn":"1010101010101010","wwn_nickname":"BestWwnEver")\n
        setwwnnickname = Raidcom.setwwnnickname(port="cl1-a",gid=1,host_grp_name="MyHostGroup","hba_wwn":"1010101010101010","wwn_nickname":"BestWwnEver")\n
        \n
        Returns Cmdview():\n
        setwwnnickname.cmd\n
        setwwnnickname.returncode\n
        setwwnnickname.stderr\n
        setwwnnickname.stdout\n
        '''
        cmdparam = self.cmdparam(port=port,**kwargs)
        cmd = f"{self.path}raidcom set hba_wwn -port {port}{cmdparam} -hba_wwn {hba_wwn} -wwn_nickname '{wwn_nickname}' -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def gethostgrptcscan(self,port: str, gid: str=None, view_keyname='_replicationTC', **kwargs) -> object:

        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid: raise Exception('Fully qualified port requires no gid{}'.format(gid))
            cmdarg = ''
        else:
            if gid is None: raise Exception("raidscan requires gid if port is not fully qualified but it is set to none")
            cmdarg = "-"+str(gid)

        cmd = f"{self.path}raidscan -p {port}{cmdarg} -ITC{self.instance} -s {self.serial} -CLI"
        #getattr(self.parser,f"getpool_key_{key}")(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.gethostgrptcscan(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def raidscanremote(self,port: str, gid=None, mode='TC', view_keyname='_remotereplication', **kwargs) -> object:
    
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid: raise Exception('Fully qualified port requires no gid{}'.format(gid))
            cmdarg = ''
        else:
            if gid is None: raise Exception("raidscan requires gid if port is not fully qualified but it is set to none")
            cmdarg = "-"+str(gid)
        
        cmd = f"{self.path}raidscan -p {port}{cmdarg} -I{mode}{self.instance} -s {self.serial} -CLI"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.raidscanremote(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn
    
    def raidscanmu(self,port,gid=None,mu=None,mode='',validmu=[0,1,2,3], view_keyname='_raidscanmu', **kwargs) -> object:
    
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid: raise Exception('Fully qualified port requires no gid{}'.format(gid))
            cmdarg = ''
        else:
            if gid is None: raise Exception("raidscan requires gid if port is not fully qualified but it is set to none")
            cmdarg = "-"+str(gid)

        if mu == None or mu not in validmu: raise Exception("Please specify valid mu for raidscanmu")
        
        cmd = f"{self.path}raidscan -p {port}{cmdarg} -I{mode}{self.instance} -s {self.serial} -CLI -mu {mu}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.raidscanmu(cmdreturn,mu)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def getrcu(self, view_keyname: str='_rcu', **kwargs) -> dict:
        cmd = f"{self.path}raidcom get rcu -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getrcu(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def gethostgrprgid(self,port: str,resource_group_id: int, view_keyname='_ports', **kwargs) -> object:
        cmd = f"{self.path}raidcom get host_grp -port {port} -resource {resource_group_id} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.gethostgrprgid(cmdreturn,resource_group_id)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def gethostgrp_key_detail_rgid(self,port: str,resource_group_id: int, view_keyname='_ports', **kwargs) -> object:
        cmd = f"{self.path}raidcom get host_grp -port {port} -resource {resource_group_id} -key detail -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.gethostgrp_key_detail(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def getquorum(self, view_keyname='_quorum', **kwargs) -> object:
        cmd = f"{self.path}raidcom get quorum -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        self.parser.getquorum(cmdreturn,datafilter=kwargs.get('datafilter',{}))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    '''
    concurrent_{functions}
    '''

    def concurrent_gethostgrps(self,ports: list=[], max_workers: int=30, view_keyname: str='_ports', **kwargs) -> object:
        '''
        host_grps = concurrent_gethostgrps(ports=['cl1-a','cl2-a'])\n
        host_grps = concurrent_gethostgrps(ports=['cl1-a','cl2-a'],datafilter={'HMD':'VMWARE_EX'})\n
        host_grps = concurrent_gethostgrps(port=['cl1-a','cl2-a'],datafilter={'GROUP_NAME':'MyGostGroup})\n
        host_grps = gethostgrp(port=['cl1-a','cl2-a'],datafilter={'Anykey_when_val_is_callable':lambda a : 'TEST' in a['GROUP_NAME'] })\n
        '''
        cmdreturn = CmdviewConcurrent()
        for port in ports: self.checkport(port)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.gethostgrp_key_detail,port=port,update_view=False,**kwargs): port for port in ports}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                cmdreturn.data.extend(future.result().data)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.serial = self.serial
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updateview(self.data,{view_keyname:cmdreturn.data})
        self.updatestats.hostgroupcounters()

        return cmdreturn

    def concurrent_gethbawwns(self,portgids: list=[], max_workers: int=30, view_keyname: str='_ports', **kwargs) -> object:
        ''' e.g. \n
        ports=['cl1-a-3','cl1-a-4'] \n
        '''
        cmdreturn = CmdviewConcurrent()
        
        for portgid in portgids: self.checkportgid(portgid)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.gethbawwn,port=portgid,update_view=False,**kwargs): portgid for portgid in portgids}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                cmdreturn.data.extend(future.result().data)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.serial = self.serial
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))    
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updateview(self.data,{view_keyname:cmdreturn.data})
        self.updatestats.hbawwncounters()
        return cmdreturn

    def concurrent_getluns(self,portgids: list=[], max_workers: int=30, view_keyname: str='_ports', **kwargs) -> object:
        ''' e.g. \n
        ports=['cl1-a-3','cl1-a-4'] \n
        '''
        cmdreturn = CmdviewConcurrent()
        
        for portgid in portgids: self.checkportgid(portgid)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.getlun,port=portgid,update_view=False,**kwargs): portgid for portgid in portgids}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                cmdreturn.data.extend(future.result().data)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.serial = self.serial
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updateview(self.data,{view_keyname:cmdreturn.data})
        self.updatestats.luncounters()
        return cmdreturn

    def concurrent_getldevs(self,ldev_ids: list=[], max_workers: int=30, view_keyname: str='_ldevs', **kwargs) -> object:
        '''
        ldev_ids = [1234,1235,1236]\n
        '''
        cmdreturn = CmdviewConcurrent()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.getldev,ldev_id=ldev_id,update_view=False,**kwargs): ldev_id for ldev_id in ldev_ids}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                cmdreturn.data.extend(future.result().data)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.serial = self.serial
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updatestats.ldevcounts()
        return cmdreturn

    def concurrent_getportlogins(self,ports: list=[], max_workers: int=30, view_keyname: str='_ports', **kwargs) -> object:
        ''' e.g. \n
        ports=['cl1-a','cl1-a'] \n
        '''
        cmdreturn = CmdviewConcurrent()
        for port in ports: self.checkport(port)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.getportlogin,port=port,update_view=False,**kwargs): port for port in ports}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                cmdreturn.data.extend(future.result().data)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.serial = self.serial
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updateview(self.data,{view_keyname:cmdreturn.data})
        self.updatestats.portlogincounters()
        return cmdreturn


    def concurrent_raidscanremote(self,portgids: list=[], max_workers: int=30, view_keyname: str='_remotereplication', **kwargs) -> object:
        '''
        ldev_ids = [1234,1235,1236]\n
        mode='TC', view_keyname='_remotereplication',
        '''
        #def raidscanremote(self,port: str, gid=None, mode='TC', view_keyname='_remotereplication', **kwargs) -> object:
        cmdreturn = CmdviewConcurrent()
        for portgid in portgids: self.checkportgid(portgid)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.raidscanremote,port=portgid,update_view=False,**kwargs): portgid for portgid in portgids}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                cmdreturn.data.extend(future.result().data)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.serial = self.serial
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updatestats.ldevcounts()
        return cmdreturn

    def concurrent_addluns(self,lun_data: list=[{}], max_workers=20) -> object:
        '''
        lun_data: [{'PORT':CL1-A|CL1-A-1, 'GID':None|1, 'host_grp_name':'Name', 'LUN':0,'LDEV':1000}]
        
        lun_data = [
            {'PORT':'CL1-A', 'LUN':0, 'LDEV':46100, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':1, 'LDEV':46101, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':2, 'LDEV':46102, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':3, 'LDEV':46103, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':4, 'LDEV':46104, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':5, 'LDEV':46105, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':6, 'LDEV':46106, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':7, 'LDEV':46107, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':8, 'LDEV':46108, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':9, 'LDEV':46109, 'host_grp_name':'testing123'},
            {'PORT':'CL1-A', 'LUN':10, 'LDEV':46110, 'host_grp_name':'testing123'}]
        '''
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.addlun,port=lun['PORT'],lun_id=lun['LUN'],ldev_id=lun['LDEV'],host_grp_name=lun['host_grp_name']): lun for lun in lun_data}
            for future in concurrent.futures.as_completed(future_out):
                print(future.result().data)

    def concurrent_addldevs(self,ldev_data: list=[], return_ldevs: bool=True, max_workers=20) -> object:
        '''
        ldev_data = [
            {'LDEV|ldev_id':46115,'VOL_Capacity(BLK)|capacity':204800,'B_POOLID|poolid':0},
            {'LDEV|ldev_id':46116,'VOL_Capacity(BLK)|capacity':204800,'B_POOLID|poolid':0},
            {'LDEV|ldev_id':46117,'VOL_Capacity(BLK)|capacity':204800,'B_POOLID|poolid':0},
            {'LDEV|ldev_id':46118,'VOL_Capacity(BLK)|capacity':204800,'B_POOLID|poolid':0},
            {'LDEV|ldev_id':46119,'VOL_Capacity(BLK)|capacity':204800,'B_POOLID|poolid':0}
        ]
        '''
        cmdreturn = CmdviewConcurrent()
        request_data = []
        for d in ldev_data:
            request_data.append({'ldev_id':d.get('LDEV',d.get('ldev_id')), 'capacity': d.get('VOL_Capacity(BLK)',d.get('capacity')), 'poolid':d.get('B_POOLID',d.get('poolid'))})
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.addldev,ldev_id=ldev['ldev_id'],capacity=ldev['capacity'],poolid=ldev['poolid'],return_ldev=return_ldevs): ldev for ldev in request_data}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                cmdreturn.data.extend(future.result().data)
                cmdreturn.undocmds.extend(future.result().undocmds)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        return cmdreturn

    def obfuscatepwd(self,cmd):
        if re.search(r' -login ',cmd):
            c = cmd.split()
            c[2] = "******"
            c[3] = "******"
            return ' '.join(c)
        else:
            return cmd
        
    def execute(self,cmd,undocmds=[],undodefs=[],expectedreturn=0,**kwargs) -> object:

        cmdreturn = Cmdview(cmd=cmd)
        cmdreturn.expectedreturn = expectedreturn
        cmdreturn.serial = self.serial
        if kwargs.get('noexec'):
            return cmdreturn
        if kwargs.get('raidcom_asyncronous'):
            self.resetcommandstatus()
        self.log.debug(f"Executing: {self.obfuscatepwd(cmd)}")
        #self.log.info(f"Expecting return code {expectedreturn}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        cmdreturn.stdout, cmdreturn.stderr = proc.communicate()
        cmdreturn.returncode = proc.returncode
        cmdreturn.executed = True
        
        if proc.returncode and proc.returncode != expectedreturn:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+cmdreturn.stdout)
            self.log.error("Stderr > "+cmdreturn.stderr)
            message = {'return':proc.returncode,'stdout':cmdreturn.stdout, 'stderr':cmdreturn.stderr }
            raise Exception(f"Unable to execute Command '{self.obfuscatepwd(cmd)}'. Command dump > {message}")

        for undocmd in undocmds: 
            echo = f'echo "Executing: {undocmd}"'
            self.undocmds.insert(0,undocmd)
            self.undocmds.insert(0,echo)
            cmdreturn.undocmds.insert(0,undocmd)
                
        for undodef in undodefs:
            self.undodefs.insert(0,undodef)
            cmdreturn.undodefs.insert(0,undodef)

        if self.cmdoutput:
            self.log.info(f"stdout: {cmdreturn.stdout}")

        if kwargs.get('raidcom_asyncronous'):
            self.getcommandstatus()

        return cmdreturn



    # BELOW IS OLD
    '''

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




 

    # CCI
    def pairdisplay(self,inst: int,group: str,mode='',opts='',optviews: list=[]) -> dict:
        cmd = '{}pairdisplay -g {} -I{}{} {} -CLI'.format(self.path,group,mode,inst,opts)
        cmdreturn = self.execute(cmd)
        #cmdreturn['views'] = self.parser.pairdisplay(cmdreturn['stdout'],optviews)
        return cmdreturn
    
    def XXXpairvolchk(self,inst: int,group: str,device: str,expectedreturn: int):
        cmd = '{}pairvolchk -g {} -d {} -I{} -ss'.format(self.path,group,device,inst)
        cmdreturn = self.execute(cmd,expectedreturn=expectedreturn)
        return cmdreturn

    def pairvolchk(self,inst: int,group: str,device: str=None,expectedreturn: int=23,opts=''):
	
        check_device = ''
        if device:
            check_device = f'-d {device}'
        cmd = '{}pairvolchk -g {} {} -I{} -ss {}'.format(self.path,group,check_device,inst,opts)
        cmdreturn = self.execute(cmd,expectedreturn=expectedreturn)
        return cmdreturn

    def paircreate(self, inst: int, group: str, mode='', quorum='', jp='', js='', fence='', copy_pace=15):
        undocmd = []
        modifier = ''
        if re.search(r'\d',str(quorum)):
            modifier = '-jq {}'.format(quorum)
            undocmd.insert(0,'{}pairsplit -g {} -I{}{}'.format(self.path,group,mode,inst))
            undocmd.insert(0,'{}pairsplit -g {} -I{}{} -S'.format(self.path,group,mode,inst))

        if re.search(r'\d',str(jp)) and re.search(r'\d',str(js)):
            modifier = '-jp {} -js {}'.format(jp,js)
            undocmd.insert(0,'{}pairsplit -g {} -I{}{} -S'.format(self.path,group,mode,inst))

        cmd = '{}paircreate -g {} -vl {} -f {} -c {} -I{}{}'.format(self.path,group,modifier,fence,copy_pace,mode,inst)
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

    def verbose(self,on=True):
        self.cmdoutput = on
    '''
