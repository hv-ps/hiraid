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

import json
import re
import inspect
import sys
from . import cmcustomviews

class Cmrestparser:
    def __init__(self,storage):
        self.storage = storage
        self.log = storage.log
        self.serial = storage.serial
        self.setview = cmcustomviews.Cmcustomviews(storage)

    def storages(self,apiresponse):
        datadict = {}
        for storage in apiresponse['data']:
            datadict[storage['storage_device_id']] = storage
        return datadict

    def getldev(self,apiresponse,optviews=[]):

        viewsdict = { 'defaultview': {}, 'list': [], 'metaview': { 'data':{}, 'stats': {} }, 'header': [] }
        viewsdict['metaview']['data'] = apiresponse['data']
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])

        for view in optviews:
            self.log.debug("Customview "+view)
            viewsdict[view] = getattr(cmcustomviews,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    def getport(self,apiresponse,optviews=[]):

        viewsdict = { 'defaultview': {}, 'list': [], 'metaview': { 'data':{}, 'stats': {} }, 'header': [] }
        viewsdict['metaview']['data'] = apiresponse['data']
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])

        for view in optviews:
            self.log.debug("Customview "+view)
            viewsdict[view] = getattr(cmcustomviews,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

