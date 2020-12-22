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
# 14/01/2020    v1.1.00     Initial Release - DEC
#
# 24/01/2020    v1.1.01     Add function getportlogin and getrcu - CM
#
# -----------------------------------------------------------------------------------------------------------------------------------

from .hvutil import configlog as configlog
from .raidlib import Storage as Storage

class Storageadmin(Storage):

    def __init__(self,serial,horcminst):
        identity = 'storageadmin'
        import pprint
        log = configlog(identity,'/tmp/logs','storageadmin.log')
        super(Storageadmin, self).__init__(serial,log,useapi='raidcom')
        self.raidcom(horcminst)
        self.pp = pprint.PrettyPrinter(indent=4).pprint
        self.ldevstates = {}

    def getport(self,optviews: list=[]) -> dict:
        return self.pp(super(Storageadmin,self).getport()['defaultview']['_ports'])
        #output = super(Storageadmin,self).getport()
        #return self.pp(output['defaultview']['_ports'])

    def getldev(self,ldevid: int, optviews: list=[]) -> dict:
        return self.pp(super(Storageadmin,self).getldev(ldevid=ldevid)['defaultview'])

    def unmapldev(self,ldevid,ldev):
        virtualLdevId = ldev[str(ldevid)].get('VIR_LDEV',False)
        if virtualLdevId:
            if virtualLdevId == "65534":
                self.log.info("Ldev \'{}\' already unmapped".format(ldevid))
                return
            virtualLdevId = (virtualLdevId,'reserve')[virtualLdevId == "65535"]
            super(Storageadmin,self).unmapldev(ldevid=ldevid,virtual_ldev_id=virtualLdevId)

    def mapldev(self,ldevid,ldev):
        virtualLdevId = ldev[str(ldevid)].get('VIR_LDEV',False)
        self.log.info(virtualLdevId)
        if virtualLdevId and str(virtualLdevId) == '65534':
            super(Storageadmin,self).mapldev(ldevid=ldevid,virtual_ldev_id=ldevid)

    def getresourcename(self,resourcegroupid):
        #return self.getresource()['defaultview'][str(resourcegroupid)]['RS_GROUP']
        return self.views['_resourcegroups'][str(resourcegroupid)]['RS_GROUP']

    def deleteldevresource(self,ldevid,ldev):
        rsgid = int(ldev[str(ldevid)].get('RSGID'))
        if rsgid:
            resourcename = self.getresourcename(rsgid)
            super(Storageadmin,self).deleteldevresource(resource_name=resourcename,ldevid=ldevid)

    def deletelun(self,ldevid,ldev):
        numport = int(ldev[str(ldevid)].get('NUM_PORT'))
        if numport:
            for port in ldev[str(ldevid)]['PORTs']:
                lun_id = ldev[str(ldevid)]['PORTs'][port]['lun']
                super(Storageadmin,self).deletelun(port=port,ldevid=ldevid,lun_id=lun_id)

    def addlun(self,ldevid,ldev):
        numport = int(ldev[str(ldevid)].get('NUM_PORT'))
        if numport:
            for port in ldev[str(ldevid)]['PORTs']:
                lun_id = ldev[str(ldevid)]['PORTs'][port]['lun']
                super(Storageadmin,self).addlun(port=port,ldevid=ldevid,lun_id=lun_id)
    
    def gethostgrps(self,ports: list,optviews: list=[]) -> dict:
        ''' Specify port. To access default view: gethostgrp(port)['defaultview'] '''
        return super(Storageadmin,self).gethostgrps(ports=ports,optviews=optviews)

    def nextgid(self,ports: list,gids: list=[]):
        data = super(Storageadmin,self).gethostgrps(ports=ports)['metaview']['data']
        for port in data:
            gids.extend([int(k) for k in data[port]])

        for g in range(1,254):
            if g not in gids:
                return { 'gid':g, 'error':0 }

        return {'error':1, 'message': 'No gids available'}

    def addhostgroups(self,ports: list, hostgroupname: str):

        gid = self.nextgid(ports)['gid']
        for port in ports:
            self.addhostgroup(port,hostgroupname,gid)

    def showundocmds(self):
        return self.apis[self.useapi].undocmds

    # ADMIN HELPERS TO STRAIGHTEN OUT DEMO RESOURCES
    def fixreserveldevs(self,ldevids: list=[]):
        for ldevid in ldevids:
            self.fixreserveldev(ldevid)

    def fixreserveldev(self,ldevid: int):
        self.ldevstates['original'] = super(Storageadmin,self).getldev(ldevid=ldevid)['defaultview']
        self.log.info('LDEV START:\n')
        self.pp(self.ldevstates['original'])
        self.deletelun(ldevid,self.ldevstates['original'])
        self.unmapldev(ldevid,self.ldevstates['original'])
        self.ldevstates['current'] = super(Storageadmin,self).getldev(ldevid=ldevid)['defaultview']
        #self.deleteldevresource(ldevid,ldev)
        self.mapldev(ldevid,self.ldevstates['current'])
        self.addlun(ldevid,self.ldevstates['original'])
        self.ldevstates['final'] = super(Storageadmin,self).getldev(ldevid=ldevid)['defaultview']
        self.log.info('LDEV NOW:\n')
        self.pp(self.ldevstates['final'])

