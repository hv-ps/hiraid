#!/usr/bin/python3.6
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.03
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
# 13/01/2022    v1.1.00     Initial Release - DEC
#
# -----------------------------------------------------------------------------------------------------------------------------------
import json
import copy

class LdevState:

    def __init__(self,log,storage,ldevid):
        self.log = log
        self.storage = storage
        self.ldevid = ldevid
        self.checks = { "pool": self.pool, "capacity": self.capacity, "rgid":self.rgid, "virtual_ldevid":self.virtual_ldevid }
        self.state = { 'ldevid':self.ldevid, 'change_state':'NODIFF', 'ldevstate':{}}
        self.header_arg_map = { "capacity":"VOL_Capacity(BLK)", "ldevid":"LDEV", "rgid":"RSGID", "pool":"B_POOLID", "virtual_ldevid":"VIR_LDEV", "virtual_culdevid":"CULDEV_VIR_LDEV" }

    def pool(self):
        
        directstate = self.state['ldevstate']['pool']
        self.log.info(f"Compare pool current state: {directstate['current_state']} -> requested state: {directstate['requested_state']}")
        self.state['goto_view'][self.header_arg_map['pool']] = str(directstate['requested_state'])

        if directstate['current_state']:
            directstate['current_state'] = int(directstate['current_state'])
        
        if directstate['requested_state'] != directstate['current_state']:
            if directstate['current_state'] is None:
                directstate['change_state'] = "CHANGE"
            else:
                directstate['change_state'] = "ERROR"
                directstate['message'] = "Switching pools is not currently supported, sorry."
                self.state['change_state'] = "ERROR"
        else:
            directstate['change_state'] = "NODIFF"
        
        if self.state['change_state'] != "ERROR":
            if directstate['change_state'] == "CHANGE":
                self.state['change_state'] = directstate['change_state']

        self.log.info(f"Pool change_state: {directstate['change_state']}")
        
    def capacity(self): 
        
        directstate = self.state['ldevstate']['capacity']
        directstate['requested_state'] = int(directstate['requested_state'])
        self.log.info(f"Compare capacity current state: {directstate['current_state']} -> requested state: {directstate['requested_state']}")
        self.state['goto_view'][self.header_arg_map['capacity']] = str(directstate['requested_state'])

        if directstate['current_state']:
            directstate['current_state'] = int(directstate['current_state'])
        
        if directstate['requested_state'] != directstate['current_state']:
            if directstate['current_state'] is None:
                directstate['change_state'] = "CHANGE"
            else:
                
                self.log.info("{} {}".format(directstate['requested_state'],directstate['current_state']))
                
                if directstate['requested_state'] < directstate['current_state']:
                    directstate['change_state'] = "ERROR"
                    directstate['message'] = "Reducing capacity is not currently supported, sorry"
                    self.state['change_state'] = "ERROR"
                else:
                    self.log.info("Ldev {} size to increase from {} to {}".format(self.state['current_view'],directstate['current_state'],directstate['requested_state']))
                    directstate['change_state'] = "INCREASE"
        else:
            directstate['change_state'] = "NODIFF"

        if self.state['change_state'] != "ERROR":
            if directstate['change_state'] == "CHANGE":
                self.state['change_state'] = directstate['change_state']
        self.log.info(f"Capacity change_state: {directstate['change_state']}")

    def rgid(self):
        directstate = self.state['ldevstate']['rgid']
        directstate['current_state'] = int(directstate['current_state'])
        self.state['goto_view'][self.header_arg_map['rgid']] = str(directstate['requested_state'])
        self.log.info(f"Compare rgid current state: {directstate['current_state']} -> requested state: {directstate['requested_state']}")

        # New
        if 'resource_view' not in self.state:
            self.state['resource_view'] = self.storage.getresource()['defaultview']
            

        if str(directstate['requested_state']) not in self.state['resource_view']:
            self.log.error(f"Requested resource group {directstate['requested_state']} does not exist on storage array")
            directstate['change_state'] = "ERROR"
            self.state['change_state'] = directstate['change_state']
            self.log.info(f"RGID change_state: {directstate['change_state']}")
            return
        
        #to_resource_name = self.state['resource_view'][directstate['requested_state']]['RS_GROUP']
        to_resource_serial = self.state['resource_view'][str(directstate['requested_state'])]['V_Serial#']
        present_resource_serial = self.storage.serial

        if present_resource_serial == to_resource_serial:
            self.state['rg_serial_matches_meta'] = True

        # end new
        if directstate['requested_state'] == directstate['current_state']:
            directstate['change_state'] = "NODIFF"
        else:
            directstate['change_state'] = "CHANGE"
        
        if self.state['change_state'] != "ERROR":
            if directstate['change_state'] == "CHANGE":
                self.state['change_state'] = directstate['change_state']
        self.log.info(f"RGID change_state: {directstate['change_state']}")

    def virtual_ldevid(self):
        directstate = self.state['ldevstate']['virtual_ldevid']
        directstate['current_state'] = int(directstate['current_state'])
        self.state['goto_view'][self.header_arg_map['virtual_ldevid']] = str(directstate['requested_state'])
        self.state['goto_view'][self.header_arg_map['virtual_culdevid']] = str(self.storage.returnldevid(directstate['requested_state'])['culdev'])



        self.log.info(f"Compare virtual_ldevid current state: {directstate['current_state']} -> requested state: {directstate['requested_state']}")

        # Not so simple, VIR_LDEV will disappear if the ldev resource is assigned to a resource group with same serial as the parent storage array ( rgid 0 )
        # Trial an expected state
        if not self.state['resource_view']:
            self.state['resource_view'] = self.storage.getresource()

        if directstate['requested_state'] == directstate['current_state']:
            directstate['change_state'] = "NODIFF"
        else:
            directstate['change_state'] = "CHANGE"
        
        if self.state['change_state'] != "ERROR":
            if directstate['change_state'] == "CHANGE":
                self.state['change_state'] = directstate['change_state']

        self.log.info(f"virtual Ldevid change_state: {directstate['change_state']}")

    def putldev(self,ldevid,**kwargs):
        self.log.info(f"putldev: {ldevid}, arguments: {kwargs}")
        #header_arg_map = { "VOL_Capacity(BLK)":"capacity", "LDEV":"ldevid", "RSGID":"rgid", "B_POOLID":"pool" }
        header_arg_map = { "capacity":"VOL_Capacity(BLK)", "ldevid":"LDEV", "rgid":"RSGID", "pool":"B_POOLID", "virtual_ldevid":"VIR_LDEV"  }
        test_order = ['rgid','pool','capacity','virtual_ldevid']

        #state = { 'change_state':'NOCHANGE', 'ldevstate':{}}
        self.log.debug(f"Obtain current state of ldev {ldevid}, use a custom view to force VIR_LDEV to always be present.")
        currentLdevState = self.storage.getldev(ldevid,optviews=['forcevirtualldevid'])['forcevirtualldevid'].get(str(ldevid))
        #self.log.info(f"LDEV: {json.dumps(currentLdevState,indent=4)}")

        # Set the initial view
        if 'initial_view' not in self.state:
            self.state['initial_view'] = currentLdevState
            
        if 'goto_view' not in self.state:
            self.state['goto_view'] = copy.deepcopy(currentLdevState)    

        self.state['current_view'] = currentLdevState
        
        #state['current_view'] = currentLdevState
        self.log.debug(currentLdevState)
        for k,v in kwargs.items():
            self.state['ldevstate'][k] = { 'current_state':currentLdevState.get(header_arg_map[k]), 'requested_state':v }
            self.log.info(f"{k}:{v}")
        
        #if currentLdevState.get('VOL_TYPE') == "NOT DEFINED" and self.state['ldevstate'].get('capacity'):
        #    state['change_state'] = 'NEW'
        #    #return state
        for test in test_order:
            if test in kwargs:
                self.checks[test]()
        #for requested in kwargs:
        #    self.checks[requested]()
        return self.state
        self.log.info(state)
        
