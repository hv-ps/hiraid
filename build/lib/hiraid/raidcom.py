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


version = "v1.0.0"

class Raidcom:    

    version = version
    
    def __init__(self,serial,instance,path="/usr/bin/",cciextension='.sh',log=logging):

        self.serial = serial
        self.log = log
        self.instance = instance
        self.path = path
        self.cciextension = cciextension
        self.cmdoutput = False
        self.views = {}
        self.stats = {}
        self.successfulcmds = []
        self.undocmds = []
        self.undodefs = []
        self.parser = Raidcomparser(self,log=self.log)
        self.updatestats = Raidcomstats(self,log=self.log)
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
        requestid_cmd = ('',f"-request_id {request_id}")[request_id is not None]
        cmd = f"{self.path}raidcom get command_status {requestid_cmd} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def resetcommandstatus(self, request_id: str='', requestid_cmd='', **kwargs) -> object:
        if request_id:
            requestid_cmd = f"-request_id {request_id}"
        cmd = f"{self.path}raidcom reset command_status {requestid_cmd} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        return cmdreturn

    def verbose(self,on=True):
        self.cmdoutput = on
    
    def lockresource(self, **kwargs) -> object:
        cmd = f"{self.path}raidcom lock resource -I{self.instance} -s {self.serial}"
        undocmd = ['{}raidcom unlock resource -I{} -s {}'.format(self.path,self.instance,self.serial)]
        return self.execute(cmd,undocmd)

    def unlockresource(self, **kwargs) -> object:
        cmd = f"{self.path}raidcom unlock resource -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom lock resource -I{self.instance} -s {self.serial}"]
        return self.execute(cmd,undocmd)

    def identify(self, view_keyname: str='_identity', **kwargs) -> object:
        self.getresource()
        self.raidqry()
        cmdreturn = self.parser.identify()
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.log.info(f"Storage identity: {cmdreturn.view}")
        return cmdreturn

    def limitations(self):
        for limit in Storagecapabilities.limitations.get(self.v_id):
            setattr(self,limit,Storagecapabilities.limitations[self.v_id][limit])

    def getresource(self, view_keyname: str='_resource_groups', **kwargs) -> object:
        cmd = f"{self.path}raidcom get resource -key opt -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.getresource(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def raidqry(self, view_keyname: str='_raidqry', **kwargs) -> object:
        cmd = f"{self.path}raidqry -l -I{self.instance}"
        cmdreturn = self.execute(cmd)
        self.parser.raidqry(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def getldev(self,ldev_id: str, view_keyname: str='_ldevs', update_view=True, **kwargs) -> object:
        '''
        e.g.
        ldev_id: 10000|27:10
        '''
        cmd = f"{self.path}raidcom get ldev -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.getldev(cmdreturn)
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.ldevcounts()
        return cmdreturn

    def getldevlist(self, ldevtype: str, view_keyname: str='_ldevlist', update_view=True, **kwargs) -> object:
        '''
        ldevtype = dp_volume | external_volume | journal | pool | parity_grp | mp_blade | defined | undefined | mapped | mapped_nvme | unmapped
        * Some of these options require additional parameters, for example 'pool' requires pool_id = $poolid
        '''
        options = " ".join([f"-{k} {v}" for k,v in kwargs.items()])
        cmd = f"{self.path}raidcom get ldev -ldev_list {ldevtype} {options} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.getldevlist(cmdreturn)
        if update_view:
            self.updateview(self.views,{view_keyname:{ldevtype:cmdreturn.view}})
            self.updatestats.ldevcounts()
        return cmdreturn

    def getport(self,view_keyname: str='_ports', **kwargs) -> object:
        '''
            Raidcom get port  
            -> object:
            getport.stats: dict
            getport.data: list
            getport.header: str
            getport.headers: list
            getport.view: dict
        '''
        cmd = f"{self.path}raidcom get port -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.getport(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        #self.portcounters()
        #raidcomstats.portcounters(self)
        #self.raidcomstats.portcounters()
        self.updatestats.portcounters()
        return cmdreturn

    def gethostgrp(self,port: str, view_keyname: str='_ports', **kwargs) -> object:
        '''
        raidcom get host_grp\n
        Better to use gethostgrp_key_detail rather than this function.\n
        You will instead obtain unused host groups and more importantly the resource group id.\n
        '''
        cmd = f"{self.path}raidcom get host_grp -port {port} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.gethostgrp(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def gethostgrp_key_detail(self,port: str, view_keyname: str='_ports', update_view=True, **kwargs) -> object:
        '''
        raidcom get host_grp -key detail\n
        Differs slightly from raidcom\n
        If port format cl-port-gid or host_grp_name is supplied with cl-port host_grp is filtered.
        '''
        cmdparam = ""
        host_grp_name = kwargs.get('host_grp_name')
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if host_grp_name: raise Exception(f"Fully qualified port {port} does not require host_grp_name parameter: {host_grp_name}")
            kwargs['host_grp_filter'] = { 'HOST_GRP_ID': port.upper() } 
        elif host_grp_name:
            cmdparam = f" -host_grp_name '{host_grp_name}' "
            kwargs['host_grp_filter'] = { 'GROUP_NAME': host_grp_name }

        cmd = f"{self.path}raidcom get host_grp -port {port} -key detail -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.gethostgrp_key_detail(cmdreturn,host_grp_filter=kwargs.get('host_grp_filter',{}))

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
        \n
        Returns Cmdview():\n
        luns.data\n
        luns.view\n
        luns.cmd\n
        luns.returncode\n
        luns.stderr\n
        luns.stdout\n
        '''
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if kwargs.get('gid') or kwargs.get('host_grp_name'): raise Exception(f"Fully qualified port {port} does not require gid or host_grp_name '{kwargs}'")
            cmdparam = ''
        else:
            if kwargs.get('gid') is None and kwargs.get('host_grp_name') is None: raise Exception("Without a fully qualified port (cluster-port-gid) getlun requires additional gid or name")
            if kwargs.get('gid') and kwargs.get('host_grp_name'): raise Exception(f"getlun gid and name are mutually exclusive, please supply one or the other: gid={kwargs.get('gid')}, name={kwargs.get('host_grp_name')}")
            cmdparam = ("-"+str(kwargs.get('gid'))," "+str(kwargs.get('host_grp_name')))[kwargs.get('gid') is None]

        cmd = f"{self.path}raidcom get lun -port {port}{cmdparam} -I{self.instance} -s {self.serial} -key opt"
        cmdreturn = self.execute(cmd)
        self.parser.getlun(cmdreturn)
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.luncounters()
        return cmdreturn

    #def gethbawwn(self,port,gid=None,name=None,view_keyname: str='_ports', update_view=True, **kwargs) -> object:
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
        cmdreturn = self.execute(cmd)
        self.parser.gethbawwn(cmdreturn)
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
        cmdreturn = self.execute(cmd)
        self.parser.getportlogin(cmdreturn)
        
        if update_view:
            self.updateview(self.views,{view_keyname:cmdreturn.view})
            self.updatestats.portlogincounters()

        return cmdreturn

    def getpool(self, key: str=None, view_keyname: str='_pools', **kwargs) -> object:
        
        keyswitch = ("",f"-key {key}")[key is not None]
        cmd = f"{self.path}raidcom get pool -I{self.instance} -s {self.serial} {keyswitch}"
        cmdreturn = self.execute(cmd)
        getattr(self.parser,f"getpool_key_{key}")(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updatestats.poolcounters()
        return cmdreturn

    def getcopygrp(self, view_keyname: str='_copygrps', **kwargs) -> object:
        cmd = f"{self.path}raidcom get copy_grp -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.getcopygrp(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn


    # Snapshots

    def getsnapshot(self, view_keyname: str='_snapshots', **kwargs) -> object:
        cmd = f"{self.serial}raidcom get snapshot -I{self.instance} -s {self.serial} -format_time"
        cmdreturn = self.execute(cmd)
        self.parser.getsnapshot(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn


    def getsnapshotgroup(self, snapshotgroup: str, fx: str=None, view_keyname: str='_snapshots', **kwargs) -> object:
        fxarg = ("",f"-fx")[fx is not None]
        cmd = f"{self.path}raidcom get snapshot -snapshotgroup {snapshotgroup} -I{self.instance} -s {self.serial} -format_time {fxarg}"
        cmdreturn = self.execute(cmd)
        self.parser.getsnapshotgroup(cmdreturn)
        return cmdreturn

    def addsnapshotgroup(self, pvol: str, svol: str, pool: str, snapshotgroup: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add snapshot -ldev_id {pvol} {svol} -pool {pool} -snapshotgroup {snapshotgroup} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn

    def createsnapshot(self, snapshotgroup: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom modify snapshot -snapshotgroup {snapshotgroup} -snapshot_data create -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn

    def unmapsnapshotsvol(self, svol: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom unmap snapshot -ldev_id {svol} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn   

    def resyncsnapshotmu(self, pvol: str, mu: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom modify snapshot -ldev_id {pvol} -mirror_id {mu} -snapshot_data resync -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn     

    def snapshotevtwait(self, pvol: str, mu: int, checkstatus: str, waittime: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom get snapshot -ldev_id {pvol} -mirror_id {mu} -check_status {checkstatus} -time {waittime} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn 

    def snapshotgroupevtwait(self, snapshotgroup: str, checkstatus: str, waittime: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom get snapshot -snapshotgroup {snapshotgroup} -check_status {checkstatus} -time {waittime} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn 


    def deletesnapshotmu(self, pvol: str, mu: int, **kwargs) -> object:
        cmd = f"{self.path}raidcom delete snapshot -ldev_id {pvol} -mirror_id {mu} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn 

    '''
    commands
    '''
    def addldev(self,ldev_id: str,poolid: int,capacity: int, **kwargs) -> object:
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom add ldev -ldev_id {ldev_id} -pool {poolid} -capacity {capacity} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete ldev -ldev_id {ldev_id} -pool {poolid} -capacity {capacity} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        self.getcommandstatus()
        return cmdreturn

    def extendldev(self, ldev_id: str, capacity: int, **kwargs) -> object:
        '''
        ldev_id   = Ldevid to extend\n
        capacity = capacity in blk\n
        Where 'capacity' will add specified blks to current capacity 
        '''
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom extend ldev -ldev_id {ldev_id} -capacity {capacity} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn

    def modifyldevname(self,ldev_id: str,ldev_name: str, **kwargs) -> object:
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom modify ldev -ldev_id {ldev_id} -ldev_name '{ldev_name}' -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn

    def deleteldev_undo(self,ldev_id: str, **kwargs):
        _ldev_data = self.getldev(ldev_id=ldev_id).view
        _ldev = _ldev_data[next(iter(_ldev_data))]
        undocmds = []
        undodefs = []
        if len(_ldev.get('LDEV_NAMING',"")):
            undocmds.append(f"{self.path}raidcom modify ldev -ldev_id {ldev_id} -ldev_name {_ldev['LDEV_NAMING']} -I{self.instance} -s {self.serial}")
            undodefs.append({'undodef':'modifyldevname','args':{'ldev_id':ldev_id,'ldev_name':_ldev['LDEV_NAMING']}})
        if _ldev['VOL_TYPE'] != "NOT DEFINED":
            undocmds.append(f"{self.path}raidcom add ldev -ldev_id {ldev_id} -capacity {_ldev['VOL_Capacity(BLK)']} -pool {_ldev['B_POOLID']} -I{self.instance} -s {self.serial}")
            undodefs.append({'undodef':'addldev','args':{'ldev_id':ldev_id,'capacity':_ldev['VOL_Capacity(BLK)'],'poolid':_ldev['B_POOLID']}})
        return undocmds,undodefs
            
    def deleteldev(self, ldev_id: str, **kwargs) -> object:
        undocmds,undodefs = self.deleteldev_undo(ldev_id=ldev_id)
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom delete ldev -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,undocmds,undodefs)
        self.getcommandstatus()
        return cmdreturn

    def addresource(self,resource_name: str,virtualSerialNumber: str=None,virtualModel: str=None, **kwargs) -> object:
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -virtual_type {virtualSerialNumber} {virtualModel} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addhostgrpresource(self,port: str,resource_name: str, host_grp_name='', **kwargs) -> object:
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -port {port} {host_grp_name} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -port {port} {host_grp_name} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addhostgrpresourceid(self,port: str,resource_id: str, host_grp_name='', **kwargs) -> object:
        resource_name = self.views['_resource_groups'][str(resource_id)]['RS_GROUP']
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -port {port} {host_grp_name} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -port {port} {host_grp_name} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn
    
    def addldevresource(self, resource_name: str, ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn
    
    def deleteldevresourceid(self, resource_id: int, ldev_id: str, **kwargs) -> object:
        #resource_name = self.getresource().view[str(resource_id)]['RS_GROUP']
        resource_name = self.views['_resource_groups'][str(resource_id)]['RS_GROUP']
        cmdreturn = self.deleteldevresource(resource_name=resource_name,ldev_id=ldev_id)
        return cmdreturn

    def deleteldevresource(self, resource_name: str, ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom delete resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom add resource -resource_name '{resource_name}' -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addhostgrp(self,port: str,host_grp_name: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add host_grp -host_grp_name '{host_grp_name}' -port {port} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete host_grp -host_grp_name '{host_grp_name}' -port {port} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addhostgroup(self,port: str,hostgroupname: str, **kwargs) -> object:
        '''Deprecated in favour of gethostgrp'''
        cmd = f"{self.path}raidcom add host_grp -host_grp_name '{hostgroupname}' -port {port} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete host_grp -host_grp_name '{hostgroupname}' -port {port} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn        

    def deletehostgrp_undo(self,port:str, **kwargs) -> object:
        undocmds = []
        undodefs = []

        def populateundo(undodef):
            undocmds.append(getattr(self,undodef['undodef'])(noexec=True,**undodef['args']).cmd)
            undodefs.append(undodef)
        
        _host_grp_detail = self.gethostgrp_key_detail(port=port,**kwargs)

        if len(_host_grp_detail.data) < 1 or _host_grp_detail.data[0]['GROUP_NAME'] == "-":
            self.log.warn(f"Host group does not exist > port: '{port}', kwargs: '{kwargs}', data: {_host_grp_detail.data}")
            return undocmds, undodefs
        if len(_host_grp_detail.data) > 1:
            raise Exception(f"Incorrect number of host groups returned {len(_host_grp_detail.data)}, cannot exceed 1. {_host_grp_detail.data}")

        for host_grp in _host_grp_detail.data:
            populateundo({'undodef':'addhostgrp', 'args':{'port':host_grp['PORT'], 'host_grp_name':host_grp['GROUP_NAME']}})

            if len(host_grp['HMO_BITs']):
                populateundo({'undodef':'modifyhostgrp', 'args':{'port':host_grp['PORT'], 'host_grp_name':host_grp['GROUP_NAME'], 'host_mode': host_grp['HMD'], 'host_mode_opt':host_grp['HMO_BITs']}})
                
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



        print(f"{undocmds}")
        import sys
        sys.exit(0)
        # luns
            
        return undocmds,undodefs

    def deletehostgrp(self,port: str, **kwargs) -> object:
        
        cmdparam = ""
        host_grp_name = kwargs.get('host_grp_name')
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if host_grp_name: raise Exception(f"Fully qualified port {port} does not require host_grp_name parameter: {host_grp_name}")
        else:
            if not host_grp_name:
                raise Exception("Without a fully qualified port (cluster-port-gid) host_grp_name parameter is required.")
            else:
                cmdparam = f" '{host_grp_name}' "

        undocmds,undodefs = self.deletehostgrp_undo(port=port,**kwargs)
        cmd = f"{self.path}raidcom delete host_grp -port {port}{cmdparam}-I{self.instance} -s {self.serial}"    
        if len(undocmds):
            cmdreturn = self.execute(cmd,undocmds,undodefs)
        else:
            self.log.warning(f"Host group does not appear to exist - port: '{port}', passed host_grp_name: '{host_grp_name}'. Returning quietly")
            cmdreturn = self.execute(cmd,undocmds,undodefs,noexec=True)

        return cmdreturn

    def resethostgrp(self,port: str, **kwargs) -> object:
        self.deletehostgrp(port=port,**kwargs)
        
    #def dismantlehostgrp(self,port: str, **kwargs) -> object:

    def addldevauto(self,poolid: int,capacityblk: int,resource_id: int,start: int,end: int, **kwargs):
        ldev_range = '{}-{}'.format(start,end)
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom add ldev -ldev_id auto -request_id auto -ldev_range {ldev_range} -pool {poolid} -capacity {capacityblk} -I{self.instance} -s {self.serial}"
        
        cmdreturn = self.execute(cmd)
        reqid = cmdreturn.stdout.rstrip().split(' : ')
        if not re.search(r'REQID',reqid[0]):
            raise Exception(f"Unable to obtain REQID from stdout {cmdreturn}.")
        cmdreturn = self.getcommandstatus(request_id=reqid[1])
        self.parser.getcommandstatus(cmdreturn)
        requestid = list(cmdreturn.view.keys())[0]
        
        autoldevid = cmdreturn['views']['metaview']['data'][requestid]['ID']
        autoldevid = cmdreturn.view[requestid]['ID']
        
        #cmdreturn['views']['autoldevid'] = autoldevid
        #self.log.info('created ldevid {}'.format(autoldevid))
        undocmd = [f"{self.path}raidcom delete ldev -ldev_id {autoldevid} -pool {poolid} -capacity {capacityblk} -I{self.instance} -s {self.serial}"]
        #requestid = cmdreturn['views']['metaview']['data'].keys()[0] 
        echo = f'echo "Executing: {undocmd[0]}"'
        self.undocmds.insert(0,undocmd[0])
        self.undocmds.insert(0,echo)
        # Reset command status
        self.resetcommandstatus(reqid[1])
        return cmdreturn

    def addlun(self, port: str, ldev_id: str, lun_id: int='', host_grp_name: str='', gid: int='', **kwargs) -> object:
        cmd = f"{self.path}raidcom add lun -port {port} {host_grp_name} -ldev_id {ldev_id} -lun_id {lun_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete lun -port {port} {host_grp_name} -ldev_id {ldev_id} -lun_id {lun_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn
    
    def deletelun(self, port: str, ldev_id: str, lun_id: int='', host_grp_name: str='', gid: int='', **kwargs) -> object:
        cmd = f"{self.path}raidcom delete lun -port {port} {host_grp_name} -ldev_id {ldev_id} -lun_id {lun_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom add lun -port {port} {host_grp_name} -ldev_id {ldev_id} -lun_id {lun_id} -I{self.instance} -s {self.serial}"]
        undodef = [{'undodef':'addlun','args':{'port':port, 'host_grp_name':host_grp_name, 'ldev_id':ldev_id,'lun_id':lun_id}}]
        cmdreturn = self.execute(cmd,undocmd,undodef,**kwargs)
        return cmdreturn

    def unmapldev(self,ldev_id: str,virtual_ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom unmap resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom map resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def mapldev(self,ldev_id: str,virtual_ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom map resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom unmap resource -ldev_id {ldev_id} -virtual_ldev_id {virtual_ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def modifyldevname(self,ldev_id: str,ldev_name: str, **kwargs) -> object:
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom modify ldev -ldev_id {ldev_id} -ldev_name '{ldev_name}' -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.getcommandstatus()
        return cmdreturn

    def modifyldevcapacitysaving(self,ldev_id: str,capacity_saving: str, **kwargs) -> object:
        self.resetcommandstatus()
        cmd = f"{self.path}raidcom modify ldev -ldev_id '{ldev_id}' -capacity_saving {capacity_saving} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom modify ldev -ldev_id '{ldev_id}' -capacity_saving {capacity_saving} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        self.getcommandstatus()
        return cmdreturn

    def modifyhostgrp(self,port: str,host_mode: str, host_grp_name: str='', host_mode_opt: list=[], **kwargs) -> object:
        host_mode_opt_arg = ("",f"-set_host_mode_opt {' '.join(map(str,host_mode_opt))}")[len(host_mode_opt) > 0]
        cmd = f"{self.path}raidcom modify host_grp -port {port} {host_grp_name} -host_mode {host_mode} {host_mode_opt_arg} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd,**kwargs)
        return cmdreturn

    def adddevicegrp(self, device_grp_name: str, device_name: str, ldev_id: str, **kwargs) -> object:
        cmd = f"{self.path}raidcom add device_grp -device_grp_name {device_grp_name} {device_name} -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete device_grp -device_grp_name {device_grp_name} {device_name} -ldev_id {ldev_id} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
        return cmdreturn

    def addcopygrp(self, copy_grp_name: str, device_grp_name: str, mirror_id: str=None, **kwargs) -> object:
        mirror_id_arg = ("",f"-mirror_id {mirror_id}")[mirror_id is not None] 
        cmd = f"{self.path}raidcom add copy_grp -copy_grp_name {copy_grp_name} -device_grp_name {device_grp_name} {mirror_id_arg} -I{self.instance} -s {self.serial}"
        undocmd = [f"{self.path}raidcom delete copy_grp -copy_grp_name {copy_grp_name} -device_grp_name {device_grp_name} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd)
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
        undocmd = [f"{self.path}raidcom add hba_wwn -port {port}{cmdparam} -hba_wwn {hba_wwn} -I{self.instance} -s {self.serial}"]
        cmdreturn = self.execute(cmd,undocmd,**kwargs)
        return cmdreturn

    def addwwnnickname(self,port: str, hba_wwn: str, wwn_nickname: str, host_grp_name: str, **kwargs) -> object:
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

    def gethostgrptcscan(self,port: str, gid=None, name=None, view_keyname='_replicationTC', **kwargs) -> object:

        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid or name: raise Exception('Fully qualified port requires no gid{} or name{}'.format(gid,name))
            cmdarg = ''
        else:
            if gid is None and name is None: raise Exception("gethostgrptcscan get requires one argument gid or name, both are None")
            if gid and name: raise Exception('gethostgrptcscan gid and name are mutually exclusive: gid{}, name{}'.format(gid,name))
            cmdarg = ("-"+str(gid)," "+str(name))[gid is None]

        cmd = f"{self.path}raidscan -p {port}{cmdarg} -ITC{self.instance} -s {self.serial} -CLI"
        cmdreturn = self.execute(cmd)
        self.parser.gethostgrptcscan(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def raidscanremote(self,port: str, gid=None, name=None, mode='TC', view_keyname='_remotereplication', **kwargs) -> object:
    
        if re.search(r'cl\w-\D+\d?-\d+',port,re.IGNORECASE):
            if gid or name: raise Exception('Fully qualified port requires no gid{} or name{}'.format(gid,name))
            cmdarg = ''
        else:
            if gid is None and name is None: raise Exception("raidscanremote requires one argument gid or name, both are None")
            if gid and name: raise Exception('raidscanremote gid and name are mutually exclusive: gid{}, name{}'.format(gid,name))
            cmdarg = ("-"+str(gid)," "+str(name))[gid is None]
        
        cmd = f"{self.path}raidscan -p {port}{cmdarg} -I{mode}{self.instance} -s {self.serial} -CLI"
        cmdreturn = self.execute(cmd)
        self.parser.raidscanremote(cmdreturn)
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
        cmdreturn = self.execute(cmd)
        self.parser.raidscanmu(cmdreturn,mu)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def getrcu(self, view_keyname: str='_rcu', **kwargs) -> dict:
        cmd = f"{self.path}raidcom get rcu -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.getrcu(cmdreturn)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def gethostgrprgid(self,port: str,resource_group_id: int, view_keyname='_ports', **kwargs) -> object:
        cmd = f"{self.path}raidcom get host_grp -port {port} -resource {resource_group_id} -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.gethostgrprgid(cmdreturn,resource_group_id)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    def gethostgrpkeyhostgrprgid(self,port: str,resource_group_id: int, view_keyname='_portskeyhostgrp', **kwargs) -> object:
        cmd = f"{self.path}raidcom get host_grp -port {port} -resource {resource_group_id} -key host_grp -I{self.instance} -s {self.serial}"
        cmdreturn = self.execute(cmd)
        self.parser.gethostgrpkeyhostgrprgid(cmdreturn,resource_group_id)
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        return cmdreturn

    '''
    concurrent_{functions}
    '''

    def concurrent_gethostgrps(self,ports: list=[], max_workers: int=30, view_keyname: str='_ports', **kwargs) -> object:
        ''' e.g. \n
        ports=['cl1-a','cl2-a'] \n
        hostgrp_filter=['cl1-a-3','cl2-a'3]
        '''
        cmdreturn = CmdviewConcurrent()
        for port in ports: self.checkport(port)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.gethostgrp_key_detail,port=port,update_view=False,**kwargs): port for port in ports}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updatestats.hostgroupcounters()

        return cmdreturn

    def concurrent_gethbawwns(self,portgids: list=[], max_workers: int=30, view_keyname: str='_ports', **kwargs) -> object:
        ''' e.g. \n
        ports=['cl1-a-3','cl1-a-4'] \n
        '''
        cmdreturn = CmdviewConcurrent()
        
        for portgid in portgids: self.checkportgid(portgid)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.gethbawwn,port=portgid,update_view=False): portgid for portgid in portgids}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))    
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updatestats.hbawwncounters()
        return cmdreturn

    def concurrent_getluns(self,portgids: list=[], max_workers: int=30, view_keyname: str='_ports', **kwargs) -> object:
        ''' e.g. \n
        ports=['cl1-a-3','cl1-a-4'] \n
        '''
        cmdreturn = CmdviewConcurrent()
        
        for portgid in portgids: self.checkportgid(portgid)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.getlun,port=portgid,update_view=False): portgid for portgid in portgids}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updatestats.luncounters()
        return cmdreturn

    def concurrent_getldevs(self,ldev_ids: list=[], max_workers: int=30, view_keyname: str='_ldevs', **kwargs) -> object:
        '''
        ldev_ids = [1234,1235,1236]\n
        '''
        cmdreturn = CmdviewConcurrent()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_out = { executor.submit(self.getldev,ldev_id=ldev_id,update_view=False): ldev_id for ldev_id in ldev_ids}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                self.updateview(cmdreturn.view,future.result().view)
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
            future_out = { executor.submit(self.getportlogin,port=port,update_view=False): port for port in ports}
            for future in concurrent.futures.as_completed(future_out):
                cmdreturn.stdout.append(future.result().stdout)
                cmdreturn.stderr.append(future.result().stderr)
                self.updateview(cmdreturn.view,future.result().view)
        cmdreturn.view = dict(sorted(cmdreturn.view.items()))
        self.updateview(self.views,{view_keyname:cmdreturn.view})
        self.updatestats.portlogincounters()
        return cmdreturn

    def execute(self,cmd,undocmds=[],undodefs=[],expectedreturn=0,**kwargs) -> object:
        self.log.info(f"Executing: {cmd}")
        self.log.debug(f"Expecting return code {expectedreturn}")
        cmdreturn = Cmdview(cmd=cmd)
        cmdreturn.expectedreturn = expectedreturn
        if kwargs.get('noexec'):
            return cmdreturn
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        cmdreturn.stdout, cmdreturn.stderr = proc.communicate()
        cmdreturn.returncode = proc.returncode
        cmdreturn.executed = True
        
        if proc.returncode and proc.returncode != expectedreturn:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+cmdreturn.stdout)
            self.log.error("Stderr > "+cmdreturn.stderr)
            message = {'return':proc.returncode,'stdout':cmdreturn.stdout, 'stderr':cmdreturn.stderr }
            raise Exception(f"Unable to execute Command '{cmd}'. Command dump > {message}")

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
    '''