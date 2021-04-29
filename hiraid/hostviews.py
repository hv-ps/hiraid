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
# 12/11/2020    v1.1.00     Initial Release - DEC
#
# -----------------------------------------------------------------------------------------------------------------------------------

import copy
import collections

class Views:

    def __init__(self,host):
        self.host = host
        self.log = host.log

    def updateview(self,view: dict,viewupdate: dict) -> dict:
        ''' Update dict view with new dict data '''
        #self.host.views = { '_ports':{},
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
        
    def multipathll_default(self,metaview,default_view_keyname: str='_multipathll'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.host.views,view)
        return view

    def devdiskbypath_default(self,metaview,default_view_keyname: str='_devdiskbypath'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.host.views,view)
        return view

    def getmultipathing_default(self,metaview,default_view_keyname: str='_multipathing'):
        view = {}
        view[default_view_keyname] = metaview['data']
        self.log.debug("view: {}, default_view_keyname {}".format(view,default_view_keyname))
        self.updateview(self.host.views,view)
        return view

        

    

