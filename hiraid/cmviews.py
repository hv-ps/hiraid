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
# 24/01/2020    v1.1.01     Add functions getportlogin and getrcu - CM
#
# -----------------------------------------------------------------------------------------------------------------------------------

import copy
import collections

class Cmviews:

    def __init__(self,storage):
        self.storage = storage
        self.log = storage.log

    def updateview(self,view: dict,viewupdate: dict) -> dict:
        ''' Update dict view with new dict data '''
        for k, v in viewupdate.items():
            if isinstance(v,collections.Mapping):
                view[k] = self.updateview(view.get(k,{}),v)
            else:
                view[k] = v
        return view

    def updateview_defaultview(self,views: dict,viewupdate: dict):
        self.updateview(views['defaultview'],viewupdate['defaultview'])
        return
    
    def updateview_metaview(self,views: dict,viewupdate: dict):
        if 'data' not in views['metaview']: views['metaview']['data'] = {}
        self.updateview(views['metaview']['data'],viewupdate['metaview']['data'])
        if 'stats' not in views['metaview']: views['metaview']['stats'] = {}
        for stat in viewupdate['metaview']['stats']:
            if stat not in views['metaview']['stats']:
                views['metaview']['stats'][stat] = 0
            views['metaview']['stats'][stat] += viewupdate['metaview']['stats'][stat]
        return

    def updateview_list(self,view: dict,viewupdate: dict) -> dict:
        if len(view['list']) and len(viewupdate['list']):
            # Header already in place at the top of view['list']
            viewupdate['list'].pop(0)

        view['list'].extend(viewupdate['list'])
        return

    def runtest(self,a):
        print('{}'.format(a))
        
    def getport_default(self,metaview,default_view_keyname: str='_ports'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view

    def getcopygrp_default(self,metaview,default_view_keyname: str='_copygrps'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view

    def getrcu_default(self,metaview,default_view_keyname: str='_rcu'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view

    def getresource_default(self,metaview,default_view_keyname: str='_resourcegroups'):
        self.log.debug("default_view_keyname: "+default_view_keyname)
        view = {}
        view[default_view_keyname] = metaview['data']
        self.updateview(self.storage.views,view)
        return view[default_view_keyname]

    def gethostgrp_default(self,metaview,default_view_keyname: str='_ports'):
        view = {}
        for port in metaview['data']:
            view[port] = { '_host_grps': {} }
            for gid in metaview['data'][port]:
                view[port]['_host_grps'][gid] = {}
                for heading in metaview['data'][port][gid]:
                    view[port]['_host_grps'][gid][heading] = metaview['data'][port][gid][heading]

        defaultview = { default_view_keyname: view }
        self.updateview(self.storage.views,defaultview)
        return view
    
    def getportlogin_default(self,metaview,default_view_keyname: str='_ports'):
        view = {}
        for port in metaview['data']:
            view[port] = { '_port_logins': {} }
            for wwn in metaview['data'][port]:
                view[port]['_port_logins'][wwn] = True
        defaultview = { default_view_keyname: view }
        self.updateview(self.storage.views,defaultview)
        return view

    #
    def gethostgrptcscan_default(self,metaview,default_view_keyname: str='_replicationTC'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view
    #

    def raidscanremote_default(self,metaview,default_view_keyname: str='_remotereplication'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view

    def raidscanmu_default(self,metaview,default_view_keyname: str='_raidscanmu'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view

    def gethostgrprgid_default(self,metaview,default_view_keyname: str='_ports'):
        view = {}
        for port in metaview['data']:
            view[port] = { '_host_grps': {} }
            for gid in metaview['data'][port]:
                view[port]['_host_grps'][gid] = {}
                for heading in metaview['data'][port][gid]:
                    view[port]['_host_grps'][gid][heading] = metaview['data'][port][gid][heading]

        defaultview = { default_view_keyname: view }
        self.updateview(self.storage.views,defaultview)
        return view

    def gethostgrpkeyhostgrprgid_default(self,metaview,default_view_keyname: str='_portskeyhostgrp'):
        view = {}
        for port in metaview['data']:
            view[port] = { '_host_grps': {} }
            for gid in metaview['data'][port]:
                view[port]['_host_grps'][gid] = {}
                for heading in metaview['data'][port][gid]:
                    view[port]['_host_grps'][gid][heading] = metaview['data'][port][gid][heading]

        defaultview = { default_view_keyname: view }
        self.updateview(self.storage.views,defaultview)
        return view

    def getlun_default(self,metaview,default_view_keyname: str='_ports'):
        view = {}
        for port in metaview['data']:
            view[port] = { '_host_grps': {} }
            for gid in metaview['data'][port]:
                view[port]['_host_grps'][gid] = { '_luns': {} }
                for lun in metaview['data'][port][gid]:
                    view[port]['_host_grps'][gid]['_luns'][lun] = {}
                    for heading in metaview['data'][port][gid][lun]:
                        view[port]['_host_grps'][gid]['_luns'][lun][heading] = metaview['data'][port][gid][lun][heading]
        defaultview = { default_view_keyname: view }

        self.updateview(self.storage.views,defaultview)
        return view

    def gethbawwn_default(self,metaview,default_view_keyname: str='_ports'):
        ''' metaview['data'][port][gid][wwn][head] = value '''
        view = {}
        for port in metaview['data']:
            view[port] = { '_host_grps': {} }
            for gid in metaview['data'][port]:
                view[port]['_host_grps'][gid] = { '_hba_wwns': {} }
                for wwn in metaview['data'][port][gid]:
                    view[port]['_host_grps'][gid]['_hba_wwns'][wwn] = {}
                    for heading in metaview['data'][port][gid][wwn]:
                        view[port]['_host_grps'][gid]['_hba_wwns'][wwn][heading] = metaview['data'][port][gid][wwn][heading]

        defaultview = { default_view_keyname: view }
        self.updateview(self.storage.views,defaultview)
        return view

    def getldev_default(self,metaview,default_view_keyname: str='_ldevs'):
        view = {}
        for ldev in metaview['data']:
            ldevid = ldev['ldevId']
            view[ldevid] = metaview['data'][ldev]

        defaultview = { default_view_keyname: view }
        self.updateview(self.storage.views,defaultview)
        return view

    def getpool_default(self,metaview,default_view_keyname: str='_pools'):
        view = {}
        view[default_view_keyname] = metaview['data']
        #self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view

    def getcommandstatus_default(self,metaview,default_view_keyname: str='_commandstatus'):
        view = {}
        view[default_view_keyname] = metaview['data']
        #self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.storage.views,view)
        return view

    def pairdisplay_default(self,metaview,default_view_keyname: str='_lastpairdisplay'):
        # THIS IS NOT IN USE
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        #self.storage.views[default_view_keyname] = metaview['data']
        self.updateview(self.storage.views,view)
        return view

