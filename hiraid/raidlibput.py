 

from cmd import PROMPT
from xml.sax.handler import property_xml_string
from .raidlib import Storage
from .storagestate import LdevState as ldevstate
from .storageexception import StorageException
import json
import os
import sys


class StoragePut(Storage):
    '''
    Def PUT functions
    Introducing idempotency
    PUTs can be dangerous, adding prompting
    '''
    def __init__(self,serial,log,scriptname="",jsonin="",useapi="raidcom",basedir=os.getcwd(),prompting=True,summary=True):
        super().__init__(serial,log,scriptname,jsonin,useapi,basedir)
        self.prompting = prompting
        self.summary = summary
        
    #def putldev(self, ldevid: str, poolid: int, capacityblk: int) -> dict:
    
    def prompt(self):
        if self.prompting:
            reply = input("Do you wish to continue? y/n")
            if reply != "y":
                return 1

    def summarise(self,change_state):
        shortsummary = True
        if self.summary:
            self.log.info("summarise state: ")
            self.log.info(json.dumps(change_state.state,indent=4))
            self.log.info(f"LdevId: {change_state.state['ldevid']} Change_Request: {change_state.state['change_state']}")

            if shortsummary:
                for k,v in change_state.state['initial_view'].items():
                    if v != change_state.state['goto_view'][k]:
                        self.log.info(f"{k}: {v} > {change_state.state['goto_view'][k]}")
            else:
                for k,v in change_state.state['initial_view'].items():
                    self.log.info(f"{k}: {v} > {change_state.state['goto_view'][k]}")

    def putldev(self, ldevid: str, **kwargs) -> dict:

        change_state = ldevstate(self.log,self,ldevid)
        max_state_change_attempts = 5
        self.log.info("LdevId: {}".format(ldevid))
        loop_states = ["CHANGE"]

        def pool(ldevid,change_state,**kwargs):
            if change_state.state['ldevstate']['pool']['change_state'] == "CHANGE":
                self.addldev(ldevid,kwargs['pool'],kwargs['capacity'])
                # if successful ( we don't know because you haven't coded! )
                if change_state.state['ldevstate']['capacity']['change_state'] == "CHANGE":
                    change_state.state['ldevstate']['capacity']['change_state'] = "NODIFF"

        def capacity(ldevid,change_state,**kwargs):
            if change_state.state['ldevstate']['capacity']['change_state'] == "CHANGE":
                self.log.info("Add ldev: {}, pool: {}, capacity: {}".format(ldevid,kwargs['pool'],kwargs['capacity']))
                self.addldev(ldevid,kwargs['pool'],kwargs['capacity'])

                # if successful
                if change_state.state['ldevstate']['pool']['change_state'] == "CHANGE":
                    change_state.state['ldevstate']['pool']['change_state'] = "NODIFF"

            elif change_state.state['ldevstate']['capacity']['change_state'] == "INCREASE":
                self.log.info("Increase capacity ldev: {}, pool: {}, capacity: {}".format(ldevid,kwargs['pool'],kwargs['capacity']))
                self.extendvolume(ldevid,kwargs['capacity'])

        def rgid(ldevid,change_state,**kwargs):
            if change_state.state['ldevstate'].get('rgid',{}).get('change_state') == "CHANGE" or change_state.state['ldevstate'].get('virtual_ldevid',{}).get('change_state') == "CHANGE":
                self.log.info(f"Modifying ldevid {ldevid} rgid / virtual_ldevid")

                self.log.info("INITIAL_VIEW: {}".format(change_state.state['initial_view']))
                self.log.info("Add ldev: {} to resource group: {}".format(ldevid,kwargs['rgid']))
                resourcegroups = self.getresource()
                self.log.debug(f"ResourceGroups: {resourcegroups}")
                
                to_resource_name = resourcegroups['defaultview'].get(str(kwargs['rgid']),{}).get('RS_GROUP')
                to_resource_serial = resourcegroups['defaultview'].get(str(kwargs['rgid']),{}).get('V_Serial#')

                # Landing in a resource other than rgid 0 but having same serial number as rgid 0 yields no VIR_LDEV 
                self.log.info(f"Destination resource serial: {to_resource_serial}")
                self.log.info(f"Parent resource serial: {self.serial}")

                
                from_virtual_ldevid = change_state.state['initial_view'].get('VIR_LDEV') if change_state.state['initial_view'].get('VIR_LDEV') else ldevid
                to_virtual_ldevid = change_state.state['initial_view'].get('VIR_LDEV') if change_state.state['initial_view'].get('VIR_LDEV') else ldevid

                if kwargs.get('virtual_ldevid'):
                    to_virtual_ldevid = kwargs['virtual_ldevid']

                if int(from_virtual_ldevid) == 65534:
                    self.log.info(f"ldevid virtual ldev id already unmapped: {from_virtual_ldevid}")
                else:
                    self.unmapldev(ldevid,from_virtual_ldevid)
                
                self.addldevresource(resource_name=to_resource_name,ldevid=ldevid)
                
                if int(to_virtual_ldevid) == 65534:
                    self.log.info(f"ldevid virtual ldev id should remain unmapped: {from_virtual_ldevid}")
                else:
                    self.mapldev(ldevid,to_virtual_ldevid)
            else:
                self.log.info("No changes required")   

        def alua(ldevid,change_state,**kwargs):
            if change_state.state['ldevstate']['alua']['change_state'] == "CHANGE":
                self.addldev(ldevid,kwargs['pool'],kwargs['capacity'])
                # if successful ( we don't know because you haven't coded! )
                
        
        def runtasks(ldevid,change_state,**kwargs):

            self.log.info("KWARGS: {}".format(kwargs))
            for task in tasks:
                if task in kwargs:
                    self.log.info("==== Running TASK -> {}".format(task))
                    
                    tasks[task](ldevid,change_state,**kwargs)
                    # forcibly reset overall state
                    if change_state.state['change_state'] != "ERROR":
                        change_state.state['change_state'] = "RESET"    
                    change_state.putldev(ldevid,**kwargs)


        tasks = { "rgid":rgid, "pool":pool, "capacity":capacity, "virtual_ldevid":rgid }
        
        try:
            #currentLdev = self.getldev(ldevid)['defaultview'].get(str(ldevid))
            state_change_attempt_count = 0
            self.log.info(f"Initialise state object: {change_state.state['change_state']}")
            self.log.info(f"Run state check for ldevid {ldevid}")
            change_state.putldev(ldevid,**kwargs)
            self.log.info(f"---------------Ldev {ldevid} initial   state: {json.dumps(change_state.state['initial_view'])}")
            self.log.info(f"---------------Ldev {ldevid} goto      state: {json.dumps(change_state.state['goto_view'])}")
            
            self.log.info(change_state.state['change_state'])
            
            self.summarise(change_state)

            if change_state.state['change_state'] == "NODIFF":
                self.log.info("Nothing to do, exit") 
                sys.exit(0)

            if change_state.state['change_state'] == "ERROR":
                self.log.info("There is a problem with the request, exit") 
                sys.exit(1)

            if self.prompt():
                self.log.info("User ended operation")
                sys.exit(0)

            self.summarise(change_state)
            
            self.log.info("Proceeding to make changes")
            while (change_state.state['change_state'] in loop_states and state_change_attempt_count < max_state_change_attempts) or state_change_attempt_count < 1:
                self.log.info("Looping - Overall Current Change State: {}, attribute states: {}".format(change_state.state['change_state'],json.dumps(change_state.state,indent=4)))
                state_change_attempt_count += 1
                self.log.info("--------- state_change_attempt_count ->: {}".format(state_change_attempt_count))
                runtasks(ldevid,change_state,**kwargs)
                
            if change_state.state['change_state'] == "ERROR":
                self.log.error("Declined to put ldev due to error state: {}".format(json.dumps(change_state.state['change_state'],indent=4)))

        except Exception as e:
            raise StorageException('Unable to add ldevid {}'.format(e),Storage,self.log,self)    

        finally:
            self.log.info(f"Ldev {ldevid} initial   state: {json.dumps(change_state.state['initial_view'])}")
            self.log.info(f"Ldev {ldevid} goto      state: {json.dumps(change_state.state['goto_view'])}")
            self.log.info(f"Ldev {ldevid} final     state: {json.dumps(change_state.state['current_view'])}")
            #self.log.info(f"STATE: {json.dumps(change_state.state,indent=4)}")




