#!/usr/bin/python3.6
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.01
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
# 19/01/2021    v1.1.01     Bug fix, reference to log object typo
#
# -----------------------------------------------------------------------------------------------------------------------------------

import inspect
from .storagemigration import StorageMigration
from .storageexception import StorageException
from .raidlib import Storage as Storage
from .storagecapabilities import Storagecapabilities as storagecapabilities
import json
import re


class GadMigration(StorageMigration):

    def preparejsonformigration(self,storage):
        conf = self.config
        jsonin = self.migrationjson
        log = self.log.debug

        vsm_prefix = conf.get('virtual_storage_machine_prefix')
        vsm_name_policy = conf.get('virtual_storage_machine_name_policy')
        defaulthmobits = conf.get('default_host_mode_options')
        virtual_storage_machine_name_override = conf.get('virtual_storage_machine_name_override')

        migrationgroups = jsonin[self.migrationtype]['migrationgroups']
        for group in migrationgroups:
            log('Group: '.format(group))
            for host in migrationgroups[group]:
                resourcegroupid = migrationgroups[group][host]['resource']['resourceGroupId']
                if 'target_ldev_policy' in self.migrationhosts[host]:
                    migrationgroups[group][host]['target_ldev_policy'] = self.migrationhosts[host]['target_ldev_policy']
                else:
                    migrationgroups[group][host]['target_ldev_policy'] = conf['target_ldev_policy']
                target_ldev_policy = migrationgroups[group][host]['target_ldev_policy']

                for hostgroup in migrationgroups[group][host]['hostgroups']:
                    port,gid = self.returnportgid(hostgroup)
                    hmobits = None
                    hmobitsunique = None
                    hostgrp = migrationgroups[group][host]['hostgroups'][hostgroup]

                    hostgrp['target_hmd'] = hostgrp['HMD']
                    hmobits = hostgrp['HMO_BITs'].copy()
                    hmobits.extend(defaulthmobits)
                    hmobitsunique = dict.fromkeys(hmobits)
                    hostgrp['target_hmo_bits'] = sorted(list(hmobitsunique.keys()))
                    hostgrp['target_group_name'] = hostgrp['GROUP_NAME']

                    
                    if ('source_to_target_port_map' in self.migrationhosts[host]) and (port in self.migrationhosts[host]['source_to_target_port_map']):
                        self.log.info("PORT IS IN HOSTS")
                        hostgrp['targetport'] = self.migrationhosts[host]['source_to_target_port_map'][port]
                    elif port in conf['source_to_target_port_map']:
                        self.log.info("PORT IS IN CONF")
                        hostgrp['targetport'] = conf['source_to_target_port_map'][port]
                    else:
                        self.log.info("PORT IS NOT IN CONF OR HOSTS")
                        hostgrp['targetport'] = port
                
                if not len(migrationgroups[group][host]['ldevs'].keys()):
                    migrationgroups[group][host]['omit'] = True
                    self.log.info("!! Host lun count is 0 - automatically omitting this host")
                    
                for ldevid in migrationgroups[group][host]['ldevs']:
                    ldev = migrationgroups[group][host]['ldevs'][ldevid]
                    ldev['target_ldev_naming'] = ldev['LDEV_NAMING']
                    if 'target_poolid' in self.migrationhosts[host]:
                        ldev['target_poolid'] = str(self.migrationhosts[host]['target_poolid'])
                    else:
                        ldev['target_poolid'] = str(conf['default_target_poolid'])
                    
                    self.log.info('Host {} target_ldev_policy {}'.format(host,target_ldev_policy))
                    if target_ldev_policy == 'match':
                        ldev['target_ldevid'] = ldevid
                    elif target_ldev_policy == 'ldevoffset':
                        ldev['target_ldevid'] = int(ldevid) + int(conf['ldevoffset'])
                    else:
                        ldev['target_ldevid'] = ''

                    if ldev['target_ldevid']:
                        ldev['target_culdev'] = storage.returnldevid(ldev['target_ldevid'])['culdev']

                    if ldevid in self.migrationhosts[host].get('omit_ldevids',[]) or int(ldevid) in self.migrationhosts[host].get('omit_ldevids',[]):
                        ldev['omit'] = True
                    else:
                        ldev['omit'] = False

                    if 'CMD' in ldev['VOL_ATTR']:
                        self.log.warn('Ldevid \'{}\' is CMD, omit ldev from migration'.format(ldevid))
                        ldev['omit'] = True

                if storage.views['_resourcegroups'][resourcegroupid]['RS_GROUP'] == "meta_resource" or vsm_name_policy == 'newname':
                    migrationgroups[group][host]['resource']['resourceGroupName'] = vsm_prefix+storage.views['_resourcegroups'][resourcegroupid]['V_Serial#']
                else:
                    migrationgroups[group][host]['resource']['resourceGroupName'] = re.sub(r'[\s]', '_', storage.views['_resourcegroups'][resourcegroupid]['RS_GROUP'])

                if vsm_name_policy == 'override':
                    migrationgroups[group][host]['resource']['resourceGroupName'] = virtual_storage_machine_name_override
                migrationgroups[group][host]['resource']['virtualSerialNumber'] = storage.views['_resourcegroups'][resourcegroupid]['V_Serial#']
                migrationgroups[group][host]['resource']['virtualModel'] = storage.storagetypelookup.models[storage.views['_resourcegroups'][resourcegroupid]['V_ID']]['v_id']

        return

    def preparejsonforedgereplication(self):
        conf = self.config
        jsonin = self.migrationjson
        log = self.log.debug

        vsm_prefix = conf.get('virtual_storage_machine_prefix')
        vsm_name_policy = conf.get('virtual_storage_machine_name_policy')
        defaulthmobits = conf.get('edge_default_host_mode_options')
        virtual_storage_machine_name_override = conf.get('edge_virtual_storage_machine_name_override')
        edge_storage_migration = conf.get('edge_storage_migration',False)

        if not edge_storage_migration:
            log("Edge storage migration not required")
            return
        
        if not self.config.get('edge_storage_discovery'):
            log("Edge storage discovery NOT authorised, insufficient data for this function")
            return
        
        if not self.edgestorage:
            self.log.info("NO Edge storage detected")
            return
            
        migrationgroups = jsonin[self.migrationtype]['migrationgroups']
        
        for group in migrationgroups:
            log('Group: '.format(group))
            
            for host in migrationgroups[group]:
                if 'remote_replication' not in migrationgroups[group][host]:
                    self.log.info("Migration object {} has no remote replication".format(host))
                    continue
                if 'edge_target_ldev_policy' in self.migrationhosts[host]:
                    migrationgroups[group][host]['edge_target_ldev_policy'] = self.migrationhosts[host]['edge_target_ldev_policy']
                else:
                    migrationgroups[group][host]['edge_target_ldev_policy'] = conf['edge_target_ldev_policy']
                    
                edge_target_ldev_policy = migrationgroups[group][host]['edge_target_ldev_policy']

                for remotereplicationtype in migrationgroups[group][host]['remote_replication']:
                    for edgestorageserial in migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets']:

                        remotereplicationsupport = (False,True)[remotereplicationtype in self.source.capabilities and remotereplicationtype in self.target.capabilities]
                        migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['remote_replication_type_support'] = remotereplicationsupport
                        
                        if remotereplicationsupport:
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['edge_target'] = conf['edge_storage_map'][remotereplicationtype][edgestorageserial]
                        if remotereplicationsupport and ('edge_storage_map' in self.migrationhosts[host]):
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['edge_target'] = self.migrationhosts[host]['edge_storage_map'][remotereplicationtype][edgestorageserial]
                        
                        resourcegroupid = migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['resource']['resourceGroupId']
                        for edgehostgroup in migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['hostgroups']:
                            port,gid = self.returnportgid(edgehostgroup)
                            hmobits = None
                            hmobitsunique = None
                            hostgrp = migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['hostgroups'][edgehostgroup]
                            
                            hostgrp['target_hmd'] = hostgrp['HMD']
                            hmobits = hostgrp['HMO_BITs'].copy()
                            hmobits.extend(defaulthmobits)
                            hmobitsunique = dict.fromkeys(hmobits)
                            hostgrp['target_hmo_bits'] = sorted(list(hmobitsunique.keys()))
                            hostgrp['target_group_name'] = hostgrp['GROUP_NAME']

                            
                            if ('edge_source_to_edge_target_port_map' in self.migrationhosts[host]) and (port in self.migrationhosts[host]['edge_source_to_edge_target_port_map']):
                                hostgrp['targetport'] = self.migrationhosts[host]['edge_source_to_edge_target_port_map'][port]
                            elif port in conf['edge_source_to_edge_target_port_map']:
                                hostgrp['targetport'] = conf['edge_source_to_edge_target_port_map'][port]
                            else:
                                hostgrp['targetport'] = port

                        if not len(migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['ldevs'].keys()):
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['omit'] = True
                            self.log.info("!! Host lun count is 0 - automatically omitting this host")
                        else:
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['omit'] = False

                        for ldevid in migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['ldevs']:
                            ldev = migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['ldevs'][ldevid]
                            ldev['target_ldev_naming'] = ldev['LDEV_NAMING']

                            ldev['target_poolid'] = self.migrationhosts[host].get('edge_target_poolid',str(conf['edge_default_target_poolid']))

                            self.log.info('Host {} target_ldev_policy {}'.format(host,edge_target_ldev_policy))
                            if edge_target_ldev_policy == 'edge_match':
                                ldev['target_ldevid'] = ldevid
                            elif edge_target_ldev_policy == 'edge_ldevoffset':
                                ldev['target_ldevid'] = int(ldevid) + int(conf['edge_ldevoffset'])
                            else:
                                ldev['target_ldevid'] = ''

                            if ldev['target_ldevid']:
                                ldev['target_culdev'] = self.source.returnldevid(ldev['target_ldevid'])['culdev']

                            #if ldevid in self.migrationhosts[host].get('omit_ldevids',[]) or int(ldevid) in self.migrationhosts[host].get('omit_ldevids',[]):
                            #    ldev['omit'] = True
                            #else:
                            #    ldev['omit'] = False

                            if 'CMD' in ldev['VOL_ATTR']:
                                self.log.warn('Ldevid \'{}\' is CMD, omit ldev from migration'.format(ldevid))
                                ldev['omit'] = True

                        if self.edgestoragearrays[edgestorageserial]['storage'].views['_resourcegroups'][resourcegroupid]['RS_GROUP'] == "meta_resource" or vsm_name_policy == 'newname':
                            
                            vSerial = self.edgestoragearrays[edgestorageserial]['storage'].views['_resourcegroups'][resourcegroupid]['V_Serial#']
                            virtualSerial = (vSerial,edgestorageserial)[vSerial == "-"]
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['resource']['resourceGroupName'] = vsm_prefix+virtualSerial
                        else:
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['resource']['resourceGroupName'] = re.sub(r'[\s]', '_', self.edgestoragearrays[edgestorageserial]['storage'].views['_resourcegroups'][resourcegroupid]['RS_GROUP'])

                        if vsm_name_policy == 'override':
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['resource']['resourceGroupName'] = virtual_storage_machine_name_override
                        
                        vSerial = self.edgestoragearrays[edgestorageserial]['storage'].views['_resourcegroups'][resourcegroupid]['V_Serial#']
                        virtualSerial = (vSerial,edgestorageserial)[vSerial == "-"]
                        migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['resource']['virtualSerialNumber'] = virtualSerial
                        
                        vID = self.edgestoragearrays[edgestorageserial]['storage'].views['_resourcegroups'][resourcegroupid]['V_ID']

                        if vID == "-":
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['resource']['virtualModel'] = self.edgestoragearrays[edgestorageserial]['storage'].v_id
                        else:
                            migrationgroups[group][host]['remote_replication'][remotereplicationtype]['targets'][edgestorageserial]['resource']['virtualModel'] = self.edgestoragearrays[edgestorageserial]['storage'].storagetypelookup.models[vID]['v_id']

        return


    def paircreate(self,storage,copy_grp_name,horcminst,quorum,fence='never'):
        storage.paircreate(inst=horcminst,group=copy_grp_name,quorum=quorum,fence=fence)

    def warningreport(self,refresh=False):
        alerttable = { 'ldevs': { 'NO': { 'VOL_ATTR': ['HORC','MRCF','UR'] }, 'MUST': {}, 'CHECKCAPABILITIES': { 'VOL_ATTR': ['HORC','MRCF','UR'] } } }
        log = self.log
        jsonin = self.migrationjson

        for group in jsonin[self.migrationtype]['migrationgroups']:
            for host in jsonin[self.migrationtype]['migrationgroups'][group]:
                for ldevid in jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs']:
                    capabilityRequirement = None
                    
                    if refresh:
                        log.info('Refreshing ldev {} data'.format(ldevid))
                        vol_attr = self.getldev(self.source,ldevid,refresh=True,returnkeys=['VOL_ATTR'])
                        log.debug("Refreshed VOL_ATTR {}".format(vol_attr))
                        jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid]['VOL_ATTR'] = vol_attr['VOL_ATTR']
                    '''
                    for key in alerttable['ldevs']['NO']:
                        if type(alerttable['ldevs']['NO'][key]) is list:
                            checkvar = jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid].get(key)
                            for attribute in alerttable['ldevs']['NO'][key]:
                                if attribute in checkvar:
                                    self.warnings += 1
                                    warning = 'Step {} - Ldevid {} {} has attribute {} which is not currently compatible with GAD migration'.format(self.step,ldevid,key,attribute)
                                    log.warn(warning)
                                    jsonin[self.migrationtype]['reports']['warnings'] = jsonin[self.migrationtype]['reports'].get('warnings', { 'ldevs': {} })
                                    jsonin[self.migrationtype]['reports']['warnings']['ldevs'][ldevid] = jsonin[self.migrationtype]['reports']['warnings']['ldevs'].get(ldevid, [])
                                    jsonin[self.migrationtype]['reports']['warnings']['ldevs'][ldevid].append(warning)
                                    self.warningmessages.append(warning)
                                    self.endmessage = "Ended with warnings"
                    '''
                    for key in alerttable['ldevs']['CHECKCAPABILITIES']:
                        if type(alerttable['ldevs']['CHECKCAPABILITIES'][key]) is list:
                            checkvar = jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid].get(key)
                            for attribute in alerttable['ldevs']['CHECKCAPABILITIES'][key]:
                                if attribute in checkvar:
                                    log.info("Checking ldev {} key {} attribute {} supported by this storage {} type {} microcode {}".format(ldevid,key,attribute,self.source.serial,self.source.v_id,self.source.micro_ver))
                                    try:
                                        capabilityRequirement = storagecapabilities.migration_type_requirements[self.migrationtype]['ldevs']['VOL_ATTR'][attribute]["Fence"][jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid].get('Fence')]
                                        log.info("CapabilityRequirement: {}".format(capabilityRequirement))
                                        if capabilityRequirement not in self.source.capabilities:
                                            self.warnings += 1
                                            warning = 'Ldevid {} {} has attribute {} and fence {} requiring source storage capability {} not present on source storage {} type {} at this micro_ver {}'.format(ldevid,key,attribute,jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid].get('Fence'),capabilityRequirement,self.source.serial,self.source.v_id,self.source.micro_ver)
                                            log.warn(warning)
                                            jsonin[self.migrationtype]['reports']['warnings'] = jsonin[self.migrationtype]['reports'].get('warnings', { 'ldevs': {} })
                                            jsonin[self.migrationtype]['reports']['warnings']['ldevs'][ldevid] = jsonin[self.migrationtype]['reports']['warnings']['ldevs'].get(ldevid, [])
                                            jsonin[self.migrationtype]['reports']['warnings']['ldevs'][ldevid].append(warning)
                                            self.warningmessages.append(warning)
                                            self.endmessage = "Ended with warnings"
                                        else:
                                            #self.warnings += 1
                                            log.info('Source storage meets capability requirements')
                                            warning = 'NO CAPACITY CHECK IS IN PLACE FOR EDGE STORAGE'
                                            log.warn(warning)
                                            self.warningmessages.append(warning)

                                        if capabilityRequirement not in self.target.capabilities:
                                            #self.warnings += 1
                                            warning = 'Ldevid {} {} has attribute {} and fence {} requiring target storage capability {} not present on target storage {} type {} at this micro_ver {}'.format(ldevid,key,attribute,jsonin[self.migrationtype]['migrationgroups'][group][host]['ldevs'][ldevid].get('Fence'),capabilityRequirement,self.target.serial,self.source.v_id,self.target.micro_ver)
                                            log.warn(warning)
                                            jsonin[self.migrationtype]['reports']['warnings'] = jsonin[self.migrationtype]['reports'].get('warnings', { 'ldevs': {} })
                                            jsonin[self.migrationtype]['reports']['warnings']['ldevs'][ldevid] = jsonin[self.migrationtype]['reports']['warnings']['ldevs'].get(ldevid, [])
                                            jsonin[self.migrationtype]['reports']['warnings']['ldevs'][ldevid].append(warning)
                                            self.warningmessages.append(warning)
                                            self.endmessage = "Ended with warnings"
                                        else:
                                            log.info('Target storage meets capability requirements')

                                    except Exception as e:
                                        capabilityRequirement = "NOT_CAPABLE"
                                        log.info("{} {}".format(capabilityRequirement,str(e)))

        return self.warnings
                
