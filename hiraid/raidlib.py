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
# 24/01/2020    v1.1.01     Add function getportlogin and getrcu - CM
#
# 03/11/2020    v1.1.02     Add functions getsnapshotgroup_default, getsnapshotgroup, addsnapshotgroup, createsnapshot,
#                           unmapsnapshotsvol, resyncsnapshotmu, snapshotevtwait, deletesnapshotmu - CM

#
# -----------------------------------------------------------------------------------------------------------------------------------

from .storageexception import StorageException
from . import raidcom
from . import hvutil
from .v_id import VId as v_id
from .storagecapabilities import Storagecapabilities as storagecapabilities

import collections
import importlib
import inspect
import json
import re
import os
from datetime import datetime



class Storage:
    instances = []
    lockedstorage = []
    def __init__(self,serial,log,scriptname="",jsonin="",useapi="raidcom",basedir=os.getcwd()):
        
        self.apis = {}
        self.views = { '_ports':{}, '_resourcegroups':{}, '_ldevs':{} }
        self.datetime = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
        self.taskref = None
        self.jsonin = jsonin
        self.serial = serial
        self.log = log
        self.useapi = useapi
        self.basedir = basedir
        self.separator = ('/','\\')[os.name == 'nt']
        self.configdir = '{}{}{}'.format(self.basedir,self.separator,'etc')
        self.cachefilename = '{}_cachefile.json'.format(self.serial)
        self.cachedir = '{}{}{}'.format(self.basedir,self.separator,'cache')
        self.cachefile = '{}{}{}'.format(self.cachedir,self.separator,self.cachefilename)
        self.undodir = '{}{}{}'.format(self.basedir,self.separator,'reverse')
        self.postcleanupdir = '{}{}{}'.format(self.basedir,self.separator,'postcleanup')
        self.scriptname = scriptname
        self.storagetypelookup = v_id
        self.storagecapabilities = storagecapabilities
        self.__class__.instances.append(self)
    
    def raidcom(self,horcminst=None,path='/usr/bin/',cciextension='.sh',horcmdir='/etc/',apiconfig=None):
        '''Define raidcom instance and set storage object interface to raidcom
        horcminst = horcm instance number
        path = path to cci / raidcom binaries e.g /usr/bin OR for windows C:\\HORCM\\etc\\
        cciextension = Usually '.sh' for nix, '.exe' for Windows
        horcmdir = Usually '/etc/' for nix, 'C:\\Windows\\'
        Optionally send / override apiconfig = { 'instance':0, 'path':'/usr/bin/', ...}
        '''
        if apiconfig:
            horcminst = apiconfig.get('horcminst',horcminst)
            path = apiconfig.get('path',path)
            cciextension = apiconfig.get('cciextension',cciextension)
            horcmdir = apiconfig.get('horcmdir',horcmdir)

        self.apis['raidcom'] = raidcom.raidcom(self,horcminst,path=path,cciextension=cciextension)
        self.horcminst = horcminst
        self.horcmdir = horcmdir
        self.raidqry()
        self.identify()
        self.setcapabilities()
        self.setmigrationpath()

    def verbose(self,on=True):
        self.apis[self.useapi].verbose(on)

    def raidcom2(self,requirements: dict):
        ''' Define raidcom instance and set storage object interface to raidcom '''
        horcminst = requirements['horcminst']
        self.apis['raidcom'] = raidcom.raidcom(self,horcminst)
        self.raidqry()
        self.identify()
        
    #def cmrest(self,protocol,resthost,port,storagedeviceid):
    #    ''' Define cmrest as storage interface api '''
    #    self.apis['cmrest'] = cmrest.cmrest(self.serial,self.log,protocol,resthost,port,storagedeviceid)
    #    self.storagedeviceid = storagedeviceid
    #    self.identify()


    def cmrest(self,protocol,resthost,port,storagedeviceid,userid="cmrest",password="cmrest"):
        ''' Define cmrest as storage interface api '''
        from . import cmrest
        self.apis['cmrest'] = cmrest.cmrest(self,self.serial,self.log,protocol,resthost,port,storagedeviceid,userid,password)
        self.storagedeviceid = storagedeviceid
        self.useapi = 'cmrest'

        self.cmidentify()

    def setundodir(self,undodir):
        self.undodir = '{}{}{}'.format(self.undodir,self.separator,undodir)
    
    def setundofile(self,undofile):
        self.undofile = '{}{}{}.{}.sh'.format(self.undodir,self.separator,undofile,self.serial)
        self.log.info('Set undofile: {}'.format(self.undofile))

    def setpostcleanupdir(self,postcleanupdir):
        self.postcleanupdir = '{}{}{}'.format(self.postcleanupdir,self.separator,postcleanupdir)

    def setpostcleanupfile(self,postcleanupfile):
        self.postcleanupfile = '{}{}{}.{}.sh'.format(self.postcleanupdir,self.separator,postcleanupfile,self.serial)
        self.log.info('Set postcleanupfile: {}'.format(postcleanupfile))

    def setjsonin(self,jsonin: object):
        self.jsonin = jsonin

    def storetaskref(self,ref):
        self.taskref = ref

    def writemessagetotaskref(self,message):
        if self.taskref:
            self.taskref['error'] = 1
            self.taskref['errormessage'] = message
            self.taskref['status'] = "failed"
            self.taskref['end'] = self.now()

    def removetaskref(self):
        self.taskref = None

    def raidqry(self):
        # TODO horcm_ver to new int
        serial = str(self.serial)
        try:
            apiresponse = self.apis[self.useapi].raidqry()
            self.views['raidqry'] = apiresponse['views']
            if serial not in apiresponse['views']:
                self.log.error("Unable to locate self {} in raidqry, am I defined in horcminst {}?".format(serial,self.horcminst))
            self.micro_ver = apiresponse['views'][serial]['Micro_ver']
            self.cache = apiresponse['views'][serial]['Cache(MB)']
            self.horcm_ver = apiresponse['views'][serial]['HORCM_ver']
        except Exception as e:
            raise StorageException('Unable to obtain raidqry: {}'.format(e),Storage,self.log,self)

              
    def identify(self):
        ''' Identify storage array '''
        apiresponse = self.apis[self.useapi].identify()
        if apiresponse['views']['defaultview']['0']['V_ID'] == "-":
            self.log.info("Ambiguous identifier ({}), probably from older R700 'get resource'. Falling back to identification by micro_ver".format(apiresponse['views']['defaultview']['0']['V_ID']))
            identifier = self.storagetypelookup.micro_ver[self.micro_ver.split('-')[0]]['v_id']
        else:
            identifier = apiresponse['views']['defaultview']['0']['V_ID']
            
        self.v_id = self.storagetypelookup.models[identifier]['v_id']
        self.vtype = self.storagetypelookup.models[identifier]['type']
        self.model = " - ".join(self.storagetypelookup.models[identifier]['model'])
        
        if not self.vtype:
            raise Exception("Unable to identify self, check v_id.py for models supported by this function")
        self.log.info('Identity > {}, Model > {}, v_id > {}'.format(self.vtype,self.model,self.v_id))

    def cmidentify(self):
        apiresponse = self.apis[self.useapi].identify()
        self.log.info(apiresponse)
        self.model = apiresponse['model']
        self.v_id = self.storagetypelookup.models[self.model]['v_id']
        self.vtype = self.storagetypelookup.models[self.model]['type']

    def setcapabilities(self):
        ''' Set list of key capabilites based upon microcode '''
        self.capabilities = []
        micro_ver_int = self.micro_ver.replace('-','').split('/')[0]
        if self.v_id in self.storagecapabilities.microcode_capabilities:
            for micro_code in self.storagecapabilities.microcode_capabilities[self.v_id]:
                if micro_ver_int >= micro_code.replace('-','').split('/')[0]:
                    self.capabilities.extend(self.storagecapabilities.microcode_capabilities[self.v_id][micro_code])

    def setmigrationpath(self):
        ''' Set method of migration required to migrate from this storage array '''
        self.migrationpath = None
        if self.v_id in self.storagecapabilities.migration_path:
            self.migrationpath = self.storagecapabilities.migration_path[self.v_id]
            
    def testerror(self):
            raise StorageException("In class no worky",Storage,self.log,self)
            
    def lockresource(self):
        ''' Lock storage array '''
        try:
            apiresponse = self.apis[self.useapi].lockresource()
            self.__class__.lockedstorage.append(self)
        except Exception as e:
            raise StorageException('Unable to lock resource: {}'.format(e),Storage,self.log,self)

    def unlockresource(self):
        ''' Unlock storage array '''
        try:
            aliresponse = self.apis[self.useapi].unlockresource()
        except Exception as e:
            raise StorageException('Unable to lock resource'.format(e),Storage,self.log,self)

    def updateview(self,view: dict,viewupdate: dict) -> dict:
        ''' Update dict view with new dict data '''
        for k, v in viewupdate.items():
            if isinstance(v,collections.Mapping):
                view[k] = self.updateview(view.get(k,{}),v)
            else:
                view[k] = v
        return view

    def getresource(self,optviews: list=[]) -> dict:
        ''' Return resource view '''
        apiresponse = self.apis[self.useapi].getresource(optviews=optviews)
        return apiresponse['views']
        #self.views['resourcegroups'] = apiresponse['views']['defaultview']

    def getldev(self,ldevid: int, optviews: list=[]) -> dict:
        ''' Return ldev view \n
        ldevid can be a string or an integer formed as either a singular decimal ldevid or range e.g. 1000-1010
        '''
        #decimalldevid = self.returnldevid(ldevid)['decimal']
        apiresponse = self.apis[self.useapi].getldev(ldevid)
        self.log.debug('apiresponse ldevid {}'.format(apiresponse))
        return apiresponse['views']

    def getldevs(self,ldevs: list=[], optviews: list=[]) -> dict:
        ''' e.g. \n
        ports=['cl1-a-1','cl2-a-2'] \n
        optviews=['customview_1','customview_2'] \n '''
        returnldevs = { 'views': {} }
        try:
            for ldevid in ldevs:
                self.log.debug('Ldev: {}'.format(ldevid))
                returnldevs['views'] = self.getldev(ldevid=ldevid,optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to getldevs: {}'.format(e),Storage,self.log,self)
        return returnldevs['views']

    def getport(self,optviews: list=[]) -> dict:
        ''' Return storage port view '''
        apiresponse = self.apis[self.useapi].getport(optviews=optviews)
        return apiresponse['views']

    def getportlogin(self,port: str,optviews: list=[]) -> dict:
        ''' Specify port. To access default view: getportlogin(port)['defaultview'] '''
        # Need to loop through the resource groups in order to understand which resource groups the host groups live.
        apiresponse = self.apis[self.useapi].getportlogin(port=port,optviews=optviews)
        return apiresponse['views']

    #
    def gethostgrptcscan(self,port: str, gid: int=None, optviews: list=[]) -> dict:
        ''' e.g. \n
        port="CL1-A-1" \n
        port="CL1-A",gid=1 \n
        optviews=['customview_1','customview_2'] \n
        To access default view: getlun(args)['defaultview'] '''
        apiresponse = self.apis[self.useapi].gethostgrptcscan(port=port,gid=gid,optviews=optviews)
        return apiresponse['views']
    #

    def raidscanremote(self,port: str, gid: int=None, optviews: list=[]) -> dict:
        ''' e.g. \n
        port="CL1-A-1" \n
        port="CL1-A",gid=1 \n
        optviews=['customview_1','customview_2'] \n
        To access default view: getlun(args)['defaultview'] '''
        apiresponse = self.apis[self.useapi].raidscanremote(port=port,gid=gid,optviews=optviews)
        return apiresponse['views']

    def raidscan(self,port: str, gid: int=None, mirrorunits: list=[0,1,2,3], optviews: list=[]) -> dict:
        ''' e.g. \n
        port="CL1-A-1" \n
        port="CL1-A",gid=1 \n
        mu=[0,1,2,3] \n
        optviews=['customview_1','customview_2'] \n
        To access default view: getlun(args)['defaultview'] '''
        for mu in mirrorunits:
            apiresponse = self.apis[self.useapi].raidscanmu(port=port,gid=gid,mu=mu,optviews=optviews)
            self.log.info(apiresponse)
        #return apiresponse['views']

    def getcopygrp(self,optviews: list=[]) -> dict:
        ''' Return get copy_grp view '''
        apiresponse = self.apis[self.useapi].getcopygrp(optviews=optviews)
        return apiresponse['views']

    def getrcu(self,optviews: list=[]) -> dict:
        ''' Return get copy_grp view '''
        apiresponse = self.apis[self.useapi].getrcu(optviews=optviews)
        return apiresponse['views']

    def gethbawwn(self,port: str, gid: int=None, name: str=None, optviews: list=[]) -> dict:
        ''' Port [ host group gid (integer) | host group name ( str ) ] \n
        To access default view: gethbawwn(args)['defaultview'] '''
        apiresponse = self.apis[self.useapi].gethbawwn(port=port,gid=gid,name=name,optviews=optviews)
        return apiresponse['views']

    def gethbawwns(self,ports: list=[], optviews: list=[]) -> dict:
        ''' e.g. \n
        ports=['cl1-a-1','cl2-a-2'] \n
        optviews=['customview_1','customview_2'] \n '''
        hbawwns = { 'views': {} }
        try:
            for port in ports:
                self.checkportgid(port)
            for port in ports:
                hbawwns['views'] = self.gethbawwn(port=port,optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to gethbawwns: {}'.format(e),Storage,self.log,self)
        return hbawwns['views']

    def gethostgrp(self,port: str,optviews: list=[]) -> dict:
        ''' Specify port. To access default view: gethostgrp(port)['defaultview'] '''
        # Need to loop through the resource groups in order to understand which resource groups the host groups live.
        apiresponse = self.apis[self.useapi].gethostgrp(port=port,optviews=optviews)
        return apiresponse['views']

    def gethostgrprgid(self,port: str,optviews: list=[]) -> dict:
        ''' Specify port. To access default view: gethostgrp(port)['defaultview'] '''
        # Need to loop through the resource groups in order to understand which resource groups the host groups live.
        hostgroups = { 'views':{} }
        for resourcegroupid in self.views['_resourcegroups']:
            self.updateview(hostgroups['views'],self.apis[self.useapi].gethostgrprgid(port=port,resourcegroupid=resourcegroupid,optviews=optviews))

        return hostgroups['views']

    def gethostgrps(self,ports: list=[], optviews: list=[]) -> dict:
        ''' e.g. \n
        ports=['cl1-a','cl2-a'] \n
        optviews=['customview_1','customview_2'] \n '''
        hostgroups = { 'views': {} }
        try:
            for port in ports: self.checkport(port)
            for port in ports:
                self.updateview(hostgroups['views'],self.gethostgrp(port,optviews=optviews))
                #hostgroups['views'] = self.gethostgrp(port=port,optviews=optviews)
                #self.log.info("Port {} Returned to gethostgrps {}".format(port,hostgroups['views']))
        except Exception as e:
            raise StorageException('Unable to gethostgrps: {}'.format(e),Storage,self.log,self)

        return hostgroups['views']

    def gethostgrpkeyhostgrprgid(self,port: str,resourcegrpid: int=None, optviews: list=[]) -> dict:
        ''' Specify port. To access default view: gethostgrp(port)['defaultview'] '''
        # Need to loop through the resource groups in order to understand which resource groups the host groups live.
        hostgroups = { 'views':{} }
        if resourcegrpid is not None:
            self.updateview(hostgroups['views'],self.apis[self.useapi].gethostgrpkeyhostgrprgid(port=port,resourcegroupid=resourcegrpid,optviews=optviews))
        else:
            for resourcegroupid in self.views['_resourcegroups']:
                self.updateview(hostgroups['views'],self.apis[self.useapi].gethostgrpkeyhostgrprgid(port=port,resourcegroupid=resourcegroupid,optviews=optviews))
        return hostgroups['views']

    def gethostgrpsrgid(self,ports: list=[], optviews: list=[]) -> dict:
        ''' e.g. \n
        ports=['cl1-a','cl2-a'] \n
        optviews=['customview_1','customview_2'] \n '''
        hostgroups = { 'views': {} }
        try:
            for port in ports: self.checkport(port)
            for port in ports:
                self.updateview(hostgroups['views'],self.gethostgrprgid(port,optviews=optviews))
        except Exception as e:
            raise StorageException('Unable to gethostgrpsrgid: {}'.format(e),Storage,self.log,self)

        return hostgroups['views']

    def getlun(self,port: str, gid: int=None, name: str=None,optviews: list=[]) -> dict:
        
        ''' e.g. \n
        port="CL1-A-1" \n
        port="CL1-A",gid=1 \n
        port="CL1-A",name="hostgroupname" \n
        optviews=['customview_1','customview_2'] \n
        To access default view: getlun(args)['defaultview'] '''
        apiresponse = self.apis[self.useapi].getlun(port=port,gid=gid,name=name,optviews=optviews)
        return apiresponse['views']

    def getluns(self,ports: list=[], optviews: list=[]) -> dict:
        ''' e.g. \n
        ports=['cl1-a-2','cl2-a-2'] \n
        optviews=['customview_1','customview_2']'''
        # views = { 'defaultview': {}, 'list': [], 'metaview': { 'data': {}, 'stats': {} } }
        views = {}
        try:
            for port in ports: self.checkportgid(port)
            for port in ports:
                luns = self.getlun(port=port,optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to getluns: {}'.format(e),Storage,self.log,self)

        return luns

    def getpool(self,opts='',optviews: list=[]) -> dict:
        try:
            apiresponse = self.apis[self.useapi].getpool(opts=opts,optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to get pools: {}'.format(e),Storage,self.log,self)

        return apiresponse['views']

    def getview(self,view: str) -> dict:
        ''' Return specified view '''
        return self.views[view]

    def showviews(self) -> list:
        ''' Return list of possible views '''
        return self.views.keys()

    def checkport(self,port):
        if not re.search(r'^cl\w-\D+\d?$',port,re.IGNORECASE): raise Exception('Malformed port: {}'.format(port))
        return

    def checkportgid(self,portgid):
        if not re.search(r'cl\w-\D+\d?-\d+',portgid,re.IGNORECASE): raise Exception('Malformed portgid: {}'.format(portgid))
        return

    def writecache(self):
        hvutil.createdir(self.cachedir)
        cachefile = self.cachefile
        self.log.info('Cachefile: {}'.format(cachefile))
        file = open(cachefile,"w")
        file.write(json.dumps(self.views,indent=4))

    def readcache(self):
        cachefile = self.cachefile
        self.log.debug('Reading cachefile {}'.format(cachefile))
        try:
            with open(cachefile) as json_file:
                self.views = json.load(json_file)
        except Exception as e:
            raise Exception('Unable to load cachefile {}'.format(cachefile))

    def returnldevid(self,value):
        out = { "in":value }
        pattern = re.compile('\w{2}:\w{2}')
        if pattern.match(str(value)):
            self.log.info('Matched storage hexadecimal: {}'.format(value))
            out['culdev'] = value
            out['decimal'] = int(value.replace(':',''),16)
        else:
            self.log.debug('Decimal input: {}'.format(value))
            out['decimal'] = value
            hexadecimal = format(int(value), '02x')
            while len(hexadecimal) < 4:
                hexadecimal = "0" + hexadecimal
            out['culdev'] = hexadecimal[:2] + ":" + hexadecimal[2:]
        return out

    def addhostgroup(self, port: str, hostgroupname: str, gid: int=None,):
        self.log.debug('{} -> port {}, hostgroupname {}, gid {}'.format(inspect.currentframe().f_code.co_name,port,hostgroupname,gid))
        if gid:
            port = '{}-{}'.format(port,gid)
        try:
            apiresponse = self.apis[self.useapi].addhostgroup(port=port,hostgroupname=hostgroupname)
        except Exception as e:
            raise StorageException('Unable to add host group: {}'.format(e),Storage,self.log,self)

        self.log.debug('addhostgroup {}'.format(apiresponse))

    def modifyhostgrp(self,port: str, host_mode: str, host_grp_name: str='', gid: int='', host_mode_opt: list=[]):
        # TO DO: Validate inputs!
        hostmodelookup = { 'LINUX/IRIX':'LINUX' }
        if host_mode in hostmodelookup:
            host_mode = hostmodelookup[host_mode]
        try:
            apiresponse = self.apis[self.useapi].modifyhostgrp(port=port,host_grp_name=host_grp_name,host_mode=host_mode,host_mode_opt=host_mode_opt)
        except Exception as e:
            raise StorageException('Unable to modify host group: {}'.format(e),Storage,self.log,self)

    def adddevicegrp(self, device_grp_name: str, device_name: str, ldev_id: str):
        try:
            apiresponse = self.apis[self.useapi].adddevicegrp(device_grp_name=device_grp_name,device_name=device_name,ldev_id=ldev_id)
        except Exception as e:
            raise StorageException('Unable to create device_grp {}, device_grp_name {}, ldev_id {} - error {}'.format(device_grp_name,device_name,ldev_id,e),Storage,self.log,self)
        
    def addcopygrp(self, copy_grp_name: str, device_grp_name: str, mirror_id=None):
        try:
            apiresponse = self.apis[self.useapi].addcopygrp(copy_grp_name=copy_grp_name,device_grp_name=device_grp_name,mirror_id=mirror_id)
        except Exception as e:
            raise StorageException('Unable to create copy_grp_name {}, device_grp_name {}, mirror_id {} - error {}'.format(copy_grp_name,device_grp_name,mirror_id,e),Storage,self.log,self)
        

    def addhbawwn( self, port: str, hba_wwn: str, host_grp_name: str='', gid: int='') -> dict:
        try:
            apiresponse = self.apis[self.useapi].addhbawwn(port=port,host_grp_name=host_grp_name,hba_wwn=hba_wwn)
        except Exception as e:
            raise StorageException('Unable to add hba_wwn {}'.format(e),Storage,self.log,self)
    
    def addwwnnickname(self, port: str, hba_wwn: str, wwn_nickname: str, host_grp_name: str='', gid: int='') -> dict:
        try:
            apiresponse = self.apis[self.useapi].addwwnnickname(port=port,host_grp_name=host_grp_name,hba_wwn=hba_wwn,wwn_nickname=wwn_nickname)
        except Exception as e:
            raise StorageException('Unable to add hba wwn_nickname {}'.format(e),Storage,self.log,self)

    def addldev(self, ldevid: str, poolid: int, capacityblk: int) -> dict:
        try:
            apiresponse = self.apis[self.useapi].addldev(ldevid=ldevid,poolid=poolid,capacityblk=capacityblk)
        except Exception as e:
            raise StorageException('Unable to add ldevid {}'.format(e),Storage,self.log,self)

    def addldevauto(self, poolid, capacityblk: int, resource_id=0, start=0, end=65279 ) -> dict:
        try:
            apiresponse = self.apis[self.useapi].addldevauto(poolid=poolid,capacityblk=capacityblk,resource_id=resource_id,start=start,end=end)
        except Exception as e:
            raise StorageException('Unable to add ldev auto'.format(e),Storage,self.log,self)
        return apiresponse['views']

    def addlun(self, port: str, ldevid: str, lun_id: int='', host_grp_name: str='', gid: int='' ) -> dict:
        try:
            apiresponse = self.apis[self.useapi].addlun(port=port,ldevid=ldevid,lun_id=lun_id,host_grp_name=host_grp_name)
        except Exception as e:
            raise StorageException('Unable to add lun {}'.format(e),Storage,self.log,self)

    def deletelun(self, port: str, ldevid: str, lun_id: int='', host_grp_name: str='', gid: int='' ) -> dict:
        try:
            apiresponse = self.apis[self.useapi].deletelun(port=port,ldevid=ldevid,lun_id=lun_id,host_grp_name=host_grp_name)
        except Exception as e:
            raise StorageException('Unable to delete lun {}'.format(e),Storage,self.log,self)

    def unmapldev(self, ldevid: str, virtual_ldev_id: str) -> dict:
        try:
            apiresponse = self.apis[self.useapi].unmapldev(ldevid=ldevid,virtual_ldev_id=virtual_ldev_id)
        except Exception as e:
            raise StorageException('Unable to unmap ldevid {}'.format(e),Storage,self.log,self)

    def mapldev(self, ldevid: str, virtual_ldev_id: str) -> dict:
        try:
            apiresponse = self.apis[self.useapi].mapldev(ldevid=ldevid,virtual_ldev_id=virtual_ldev_id)
        except Exception as e:
            raise StorageException('Unable to map ldevid {} error {}'.format(ldevid,e),Storage,self.log,self)

    def modifyldevname(self, ldevid: str, ldev_name: str):
        try:
            apiresponse = self.apis[self.useapi].modifyldevname(ldevid=ldevid,ldev_name=ldev_name)
        except Exception as e:
            raise StorageException('Unable to modify ldev_name \'{}\', ldevid \'{}\' - exception {}'.format(ldev_name,ldevid,e),Storage,self.log,self)

    def modifyldevcapacitysaving(self, ldevid: str, capacity_saving: str):
        try:
            apiresponse = self.apis[self.useapi].modifyldevcapacitysaving(ldevid=ldevid,capacity_saving=capacity_saving)
        except Exception as e:
            raise StorageException('Unable to set ldev capacity_saving \'{}\', ldevid \'{}\' - exception {}'.format(capacity_saving,ldevid,e),Storage,self.log,self)
    
    def addresource(self,resourceGroupName: str,virtualSerialNumber: str=None,virtualModel: str=None):
        apiresponse = self.apis[self.useapi].addresource(resourceGroupName=resourceGroupName,virtualSerialNumber=virtualSerialNumber,virtualModel=virtualModel)
        #self.updateview(self.views['_ports'],apiresponse['views']['defaultview'])
        #return apiresponse['views']
        return apiresponse

    def addhostgrpresource(self, resource_name: str, port: str, host_grp_name: str='') -> dict:
        try:
            apiresponse = self.apis[self.useapi].addhostgrpresource(port=port,resource_name=resource_name,host_grp_name=host_grp_name)
        except Exception as e:
            raise StorageException('Unable to add hba wwn_nickname {}'.format(e),Storage,self.log,self)

    def addldevresource(self, resource_name: str, ldevid: str) -> dict:
        try:
            apiresponse = self.apis[self.useapi].addldevresource(resource_name=resource_name,ldevid=ldevid)
        except Exception as e:
            raise StorageException('Unable to add ldevid {} to resource name {} - error {}'.format(ldevid,resource_name,e),Storage,self.log,self)
    
    def deleteldevresource(self, resource_name: str, ldevid: str) -> dict:
        try:
            apiresponse = self.apis[self.useapi].deleteldevresource(resource_name=resource_name,ldevid=ldevid)
        except Exception as e:
            raise StorageException('Unable to delete ldevid {} from resource name {} - error {}'.format(ldevid,resource_name,e),Storage,self.log,self)

    def addvsms(self,vsmdict: dict) -> dict:
        output = { "presentok": 0, "presentmismatched": 0, "created": 0, "error": 0, "messages": []}
        if len(vsmdict) < 1:
            self.log.info("No vsms in list")
        for vsm in vsmdict:
            vsmdict[vsm]['present'] = 0
            for configuredvsm in self.views['_resourcegroups']:
                if self.views['_resourcegroups'][configuredvsm]['RS_GROUP'] == vsm:
                    self.log.info('VSM name {} already present'.format(vsm))
                    if ( self.views['_resourcegroups'][configuredvsm]['V_Serial#'] == vsmdict[vsm]['virtualSerialNumber'] ) and ( self.views['_resourcegroups'][configuredvsm]['V_ID'] == vsmdict[vsm]['virtualModel'] ):
                        self.log.info('VSM \'{}\' with serial \'{}\' of type \'{}\' is already present at storage serial \'{}\''.format(vsm,vsmdict[vsm]['virtualSerialNumber'],self.views['_resourcegroups'][configuredvsm]['V_ID'],self.serial))
                        output['presentok'] += 1
                        vsmdict[vsm]['present'] = 1
                    else:
                        output['presentmismatched'] += 1
                        vsmdict[vsm]['present'] = 0
                        output['error'] = 1
                        message = 'vsm \'{}\' present but requested attributes mismatch, requested: virtualSerialNumber \'{}\' virtualModel \'{}\', present virtualSerialNumber \'{}\' virtualModel \'{}\''.format(vsm,vsmdict[vsm]['virtualSerialNumber'],vsmdict[vsm]['virtualModel'],self.views['_resourcegroups'][configuredvsm]['V_Serial#'],self.views['_resourcegroups'][configuredvsm]['V_ID'])
                        output['messages'].append(message)
                        self.log.info(message)
                    
        if output['error']:
            raise Exception(str(output['messages']))
            return output
        
        for vsm in vsmdict:
            self.log.info("Create VSM {} from data {}".format(vsm,vsmdict))
            if not vsmdict[vsm]['present']:
                vsmserial = vsmdict[vsm]['virtualSerialNumber']
                self.log.debug("VSM model: "+vsmdict[vsm]['virtualModel'])
                vsmmodel = self.storagetypelookup.models[vsmdict[vsm]['virtualModel']]['type']
                response = self.addresource(vsm,vsmserial,vsmmodel)
                self.log.info('vsm {} response {}'.format(vsm,response))
    
        # Check vsm is actually present
        self.getresource(optviews=['resourcegroupsbyname'])
        self.log.debug(json.dumps(self.getview('resourcegroupsbyname')))
        for vsm in vsmdict:
            if not vsmdict[vsm]['present']:
                # Should now be present
                if vsm in self.views['resourcegroupsbyname']:
                    output['created'] += 1
                else:
                    output['error'] = 1
                    message = 'Failed to create vsm \'{}\' requested: virtualSerialNumber \'{}\' virtualModel \'{}\', present virtualSerialNumber \'{}\' virtualModel \'{}\''.format(vsm,vsmdict[vsm]['virtualSerialNumber'],vsmdict[vsm]['virtualModel'],self.views['_resourcegroups'][configuredvsm]['V_Serial#'],self.views['_resourcegroups'][configuredvsm]['V_ID'])
                    output['messages'].append(message)
        return output

    def writeundofile(self):
        undofile = self.undofile
        hvutil.createdir(self.undodir)
        undocmds = self.apis[self.useapi].undocmds
        if len(undocmds):
            self.log.info('Write undo commands to log and file: {}'.format(undofile))
            with open(undofile,"w") as undofile_handler:
                for undocmd in self.apis[self.useapi].undocmds:
                    undofile_handler.write('{}\n'.format(undocmd))
                    self.log.info(undocmd)
        return self.undofile

    def writepostcleanupfile(self, regex: str):
        self.log.debug('Regex {}'.format(regex))
        postcleanupfile = self.postcleanupfile
        hvutil.createdir(self.postcleanupdir)
        undocmds = self.apis[self.useapi].undocmds
        if len(undocmds):
            self.log.info('Log and file postcleanup cmds: {}'.format(postcleanupfile))
            with open(postcleanupfile,"w") as postcleanupfile_handler:
                for undocmd in self.apis[self.useapi].undocmds:
                    if re.search(regex, undocmd):
                        postcleanupfile_handler.write('{}\n'.format(undocmd))
                        self.log.info('Postcleanup cmd: {}'.format(undocmd))
        return self.postcleanupfile

    def dumpjsonin(self):
        self.log.debug(json.dumps(self.jsonin,indent=4,sort_keys=True))

    def returnportandgid(self,portgid):
        port = '-'.join(portgid.split('-')[:2])
        gid = portgid.split('-')[-1]
        return port,gid

    def restarthorcminst(self,inst):
        try:
            apiresponse = self.apis[self.useapi].restarthorcminst(inst=inst)
        except Exception as e:
            raise StorageException('Unable to restart horcm instance {} - error {}'.format(inst,e),Storage,self.log,self)

    def pairdisplay(self,inst,group,mode='',opts='',optviews: list=[]):
        try:
            apiresponse = self.apis[self.useapi].pairdisplay(inst=inst,mode=mode,group=group,opts=opts,optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to pairdisplay inst {} mode {} group {} - error {}'.format(inst,mode,group,e),Storage,self.log,self)

        return apiresponse

    def pairvolchk(self,inst,group,device,expectedreturn):
        try:
            apiresponse = self.apis[self.useapi].pairvolchk(inst=inst,group=group,device=device,expectedreturn=expectedreturn)
        except Exception as e:
            raise StorageException('Unable to pairvolchk or volumes not in expected state - error {}'.format(str(e)),Storage,self.log,self)

    def paircreate(self,inst,group,fence,quorum='',mode='', jp='', js=''):
        try:
            apiresponse = self.apis[self.useapi].paircreate(inst=inst,fence=fence,mode=mode,group=group,quorum=quorum,jp=jp,js=js)
        except Exception as e:
            raise StorageException('Unable to paircreate inst {} mode {} group {} fence {} quorum {} jp {} js {} - error {}'.format(inst,mode,group,fence,quorum,jp,js,e),Storage,self.log,self)

    def horctakeover(self,inst,group):
        try:
            apiresponse = self.apis[self.useapi].horctakeover(inst=inst,group=group)
        except Exception as e:
            raise StorageException('Unable to horctakeover inst {}  group {} - error {}'.format(inst,group,e),Storage,self.log,self)
        
    def pairresyncswaps(self,inst,group):
        try:
            apiresponse = self.apis[self.useapi].pairresyncswaps(inst=inst,group=group)
        except Exception as e:
            raise StorageException('Unable to pairresyncswaps inst {}  group {} - error {}'.format(inst,group,e),Storage,self.log,self)

    def pairresync(self,inst,group,opts=''):
        try:
            apiresponse = self.apis[self.useapi].pairresync(inst=inst,group=group,opts=opts)
        except Exception as e:
            raise StorageException('Unable to pairresync inst {} group {} opts {} - error {}'.format(inst,group,opts,e),Storage,self.log,self)
        
    def pairsplit(self,inst,group,opts=''):
        try:
            apiresponse = self.apis[self.useapi].pairsplit(inst=inst,group=group,opts=opts)
        except Exception as e:
            raise StorageException('Unable to pairsplit inst {} group {} opts {} - error {}'.format(inst,group,opts,e),Storage,self.log,self)

    def pairevtwaitexec(self,pairevtwaits):
        try:
            apiresponse = self.apis[self.useapi].pairevtwaitexec(pairevtwaits)
        except Exception as e:
            raise StorageException('Unable to monitor pairevtwaits {} - error {}'.format(pairevtwaits,e),Storage,self.log,self)
        return apiresponse

    def now(self):
        return datetime.now().strftime('%d-%m-%Y_%H.%M.%S')

    def blkstomb(self,blks):
        MB = int(blks) / 2048
        GB = MB / 1024
        TB = GB / 1024
        PB = TB / 1024
        return { 'blks':blks, 'MB':round(MB), 'GB':round(GB,2), 'TB':round(TB,2), 'PB':round(PB,2) }

    def caps(self,capacity,denominator):
    
        if denominator == "GB":
            GB = capacity
            MB = GB * 1024
            TB = GB / 1024
            PB = TB / 1024
            blks = MB * 2048
        if denominator == "MB":
            MB = capacity
            GB = MB / 1024
            TB = GB / 1024
            PB = TB / 1024
            blks = MB * 2048
        if denominator == "blks":
            blks = capacity
            MB = int(blks) / 2048
            GB = MB / 1024
            TB = GB / 1024
            PB = TB / 1024
        if denominator == "TB":
            TB = capacity
            PB = TB / 1024
            GB = TB * 1024
            MB = GB * 1024
            blks = MB * 2048
        if denominator == "PB":
            PB = capacity
            TB = PB * 1024
            GB = TB * 1024
            MB = GB * 1024
            blks = MB * 2048

        return { 'blks':round(blks), 'MB':round(MB), 'GB':round(GB,2), 'TB':round(TB,2), 'PB':round(PB,2) }
        

    def deletecache(self):
        cachefile = self.cachefile
        self.log.debug('Deleting cachefile {}'.format(cachefile))
        try:
            os.remove(self.cachefile)
        except Exception as e:
            print('Unable to load cachefile {}'.format(cachefile))

    def getsnapshot(self,optviews: list=[]) -> dict:
        ''' Return get snapshot view '''
        apiresponse = self.apis[self.useapi].getsnapshot(optviews=optviews)
        return apiresponse['views']


    def getsnapshotgroup(self, snapshotgroup, optviews: list=[]) -> dict:
        ''' Return get snapshot view '''
        apiresponse = self.apis[self.useapi].getsnapshotgroup(snapshotgroup, optviews=optviews)
        return apiresponse['views']   


    def addsnapshotgroup(self, pvol, svol, pool, snapshotgroup, optviews: list=[]) -> dict:
        ''' Return add snapshot  '''
        try:
            apiresponse = self.apis[self.useapi].addsnapshotgroup(pvol, svol, pool, snapshotgroup, optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to add snapshot group {}'.format(e),Storage,self.log,self)
        

    def createsnapshot(self, snapshotgroup, optviews: list=[]) -> dict:
        ''' Return create snapshot  '''
        try:
            apiresponse = self.apis[self.useapi].createsnapshot(snapshotgroup, optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to create snapshot {}'.format(e),Storage,self.log,self)

    def unmapsnapshotsvol(self, svol, optviews: list=[]) -> dict:
        ''' Return Unmap snapshot  '''
        try:
            apiresponse = self.apis[self.useapi].unmapsnapshotsvol(svol, optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to unmap snapshot Svol {}'.format(e),Storage,self.log,self)  

    def resyncsnapshotmu(self, pvol, mu, optviews: list=[]) -> dict:
        ''' Resync an umapped (orphaned) snapshot '''
        try:
            apiresponse = self.apis[self.useapi].resyncsnapshotmu(pvol, mu, optviews=optviews)
        except Exception as e:
            raise StorageException('Unable to resync unmapped (orphaned) Svol {}'.format(e),Storage,self.log,self) 


    def snapshotevtwait(self, pvol, mu, status, waittime, optviews: list=[]) -> dict:
        ''' snapshot event wait '''
        try:
            apiresponse = self.apis[self.useapi].snapshotevtwait(pvol, mu, status, waittime, optviews=optviews)
        except Exception as e:
            raise StorageException('Snapshot event wait failed {}'.format(e),Storage,self.log,self) 


    def snapshotgroupevtwait(self, groupName, status, waittime, optviews: list=[]) -> dict:
        ''' snapshotgroup event wait '''
        try:
            apiresponse = self.apis[self.useapi].snapshotgroupevtwait(groupName, status, waittime, optviews=optviews)
        except Exception as e:
            raise StorageException('Snapshotgroup event wait failed {}'.format(e),Storage,self.log,self) 


    def deletesnapshotmu(self, pvol, mu, optviews: list=[]) -> dict:
        ''' snapshot event wait '''
        try:
            apiresponse = self.apis[self.useapi].deletesnapshotmu(pvol, mu, optviews=optviews)
        except Exception as e:
            raise StorageException('Snapshot event wait failed {}'.format(e),Storage,self.log,self) 

    '''
    Def Aliases
        Component - Function
        i.e. Snapshot - Add
            Snapshot - Delete
    '''

    def snapshotget(self,optviews: list=[]) -> dict: self.getsnapshot(optviews)

    def snapshotgetgroup(self, snapshotgroup, optviews: list=[]) -> dict: self.getsnapshotgroup(snapshotgroup, optviews)
 
    def snapshotaddgroup(self, pvol, svol, pool, snapshotgroup, optviews: list=[]) -> dict: self.addsnapshotgroup(pvol, svol, pool, snapshotgroup, optviews)

    def snapshotcreate(self, snapshotgroup, optviews: list=[]) -> dict: self.createsnapshot(snapshotgroup, optviews)

    def snapshotunmapsvol(self, svol, optviews: list=[]) -> dict: self.unmapsnapshotsvol(svol, optviews)

    def snapshotresyncmu(self, pvol, mu, optviews: list=[]) -> dict: self.resyncsnapshotmu(pvol, mu, optviews) 

    def snapshotdeletemu(self, pvol, mu, optviews: list=[]) -> dict: self.deletesnapshotmu(pvol, mu, optviews)









