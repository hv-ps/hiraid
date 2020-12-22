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
# 24/01/2020    v1.1.01     Add custom view function gethostgrptcscan__replicationTCState
#
# -----------------------------------------------------------------------------------------------------------------------------------

import inspect
from .cmviews import Cmviews

class Cmcustomviews(Cmviews):

    def gethostgrp_byportgid(self,metaview,viewname):
        view = {}
        for port in metaview['data']:
            for gid in metaview['data'][port]:
                portgid = port+"-"+gid
                view[portgid] = metaview['data'][port][gid]
        
        customview = { viewname: view }
        self.updateview(self.storage.views,customview)
        self.log.debug('CUSTOMVIEW: {}'.format(customview))
        return view

    def gethostgrprgid_byportgid(self,metaview,viewname):
        view = {}
        for port in metaview['data']:
            for gid in metaview['data'][port]:
                portgid = port+"-"+gid
                view[portgid] = metaview['data'][port][gid]
        
        customview = { viewname: view }
        self.updateview(self.storage.views,customview)
        self.log.debug('CUSTOMVIEW: {}'.format(customview))
        return view

    def gethostgrptcscan__replicationTCState(self,metaview, viewname):
        view = {}

        for hostgrpid in metaview['data']:
            for ldevid in metaview['data'][hostgrpid]:
                replicationStatus = metaview['data'][hostgrpid][ldevid]['Status']
                if  replicationStatus not in view:
                    view[replicationStatus] = {}
                view[replicationStatus][ldevid] = metaview['data'][hostgrpid][ldevid]

        customview = { viewname: view }
        self.updateview(self.storage.views,customview)
        self.log.debug('CUSTOMVIEW: {}'.format(customview))
        return view

    def raidscanremote__remotereplicationbyldevid(self,metaview, viewname):
        view = {}

        for hostgrpid in metaview['data']:
            for ldevid in metaview['data'][hostgrpid]:
                view[ldevid] = metaview['data'][hostgrpid][ldevid]

        customview = { viewname: view }
        self.updateview(self.storage.views,customview)
        self.log.debug('CUSTOMVIEW: {}'.format(customview))
        return view


    def gethostgrp_hostgroupsbyportname(self,metaview,viewname):
        view = {}
        for port in metaview['data']:
            for gid in metaview['data'][port]:
                if port not in view:
                    view[port] = {}
                view[port][metaview['data'][port][gid]['GROUP_NAME']] = metaview['data'][port][gid]
        
        customview = { viewname: view }
        self.updateview(self.storage.views,customview)
        self.log.debug('CUSTOMVIEW: {}'.format(customview))
        return view

    def gethbawwn_wwns_bylcwwnportgid(self,metaview,viewname):
        view = {}
        self.log.debug(metaview)
        for port in metaview['data']:
            for gid in metaview['data'][port]:
                portgid = port+"-"+gid
                for wwn in metaview['data'][port][gid]:
                    view[wwn] = { portgid: {} }
                    for heading in metaview['data'][port][gid][wwn]:
                        view[wwn][portgid][heading] = metaview['data'][port][gid][wwn][heading]
                    self.log.debug('Populate {} port {} gid {} wwn {}'.format(inspect.currentframe().f_code.co_name,port,gid,wwn)) 

        customview = { viewname: view }
        self.log.debug("{} updating view {}".format(inspect.currentframe().f_code.co_name,customview))
        self.updateview(self.storage.views,customview)      
        self.log.debug("Returned from updateview, returning {}".format(view))  
        return view

    def getresource_resourcegroupsbyname(self,metaview,viewname):
        self.log.debug("view_keyname: "+viewname)
        view = { viewname: {} }
        for rgid in metaview['data']:
            view[viewname][metaview['data'][rgid]['RS_GROUP']] = metaview['data'][rgid]
        self.updateview(self.storage.views,view)
        return view[viewname]

    def getresource_resourcesnewview(self,metaview,viewname):
        self.log.debug("view_keyname: "+viewname)
        print('dumpmetaview {}'.format(metaview))
        return metaview

    def Xgetcommandstatus_autoldev(self,metaview,viewname):
        self.log.debug("view_keyname: "+viewname)
        view = {}

        if len(metaview['data']) > 1:
            raise Exception("There can only be a single command_status requestid, something has gone very wrong")

        requestid = list(metaview['data'].keys())[0]
        autoldevid = metaview['data'][requestid]['ID']
        view[autoldevid] = metaview['data'][requestid]

        customview = { viewname: view }
        self.updateview(self.storage.views,customview)
        self.log.debug('CUSTOMVIEW: {}'.format(customview))
        return view

    def Xgetcommandstatus_thisautocreatedldev(self,metaview,viewname):
        self.log.debug("view_keyname: "+viewname)
        view = {}

        if len(metaview['data']) > 1:
            raise Exception("There can only be a single command_status requestid, something has gone very wrong")

        requestid = list(metaview['data'].keys())[0]
        view = metaview['data'][requestid]['ID']

        customview = { viewname: view }
        self.updateview(self.storage.views,customview)
        self.log.debug('CUSTOMVIEW: {}'.format(customview))
        return view

    def gethostgrp_custom1(self,metaview):
        self.log.debug("custom1")

    def gethostgrp_custom2(self,metaview):
        self.log.debug("custom2")

    def getlun_custom1(self,metaview):
        return {'test':'test'}
        self.log.debug("getlun_custom1")



    ''' View Handlers '''

