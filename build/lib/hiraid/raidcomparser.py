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
# 14/01/2020    v1.1.00     Initial Release - DC
#
# 24/01/2020    v1.1.01     Add functions getportlogin getrcu - CM
#
# 10/08/2022    v1.1.02     Bug fix, gethbawwn broken view missing wwns - DC
#
# 11/02/2023    v1.1.03     Added getdrive - DC
#
# 08/03/2024    v1.1.04     Updated gethostgrp_key_detail to allow gethostgrp to be dropped - DC
#
# 18/04/2024    v1.1.05     Ditched some junk and fixed getldevlist_front_end
#
# -----------------------------------------------------------------------------------------------------------------------------------

import re
import collections
from typing import Callable
from .historutils.historutils import Storcapunits
from .historutils.historutils import Ldevid
from .cmdview import Cmdview
from .v_id import VId
import copy

        
class Raidcomparser:
    def __init__(self,raidcom,log):
        self.log = log
        self.raidcom = raidcom
    
    def updateview(self,view: dict,viewupdate: dict) -> dict:
        ''' Update dict view with new dict data '''
        for k, v in viewupdate.items():
            if isinstance(v,collections.abc.Mapping):
                view[k] = self.updateview(view.get(k,{}),v)
            else:
                view[k] = v
        return view

    def altview(self,cmdreturn,altview):
        if altview:
            self.updateview(self.raidcom.views,{altview(cmdreturn):cmdreturn.altview})

    def applyfilter(self,row,_filter):
        if _filter:
            for key, val in _filter.items():
                if key not in row and not callable(val) :
                    return False
            for key, val in _filter.items():
                if isinstance(val,str):
                    if row[key] != val:
                        return False
                elif isinstance(val,list):
                    if row[key] not in val:
                        return False
                elif callable(val):
                    return val(row)
                else:
                    return False
        return True
            
    #def initload(self,cmdreturn,header='',keys=[],replaceHeaderChars={}):
    def initload(self,cmdreturn,header='',keys=[],maxsplit=-1):

        cmdreturn.rawdata = [row.strip() for row in list(filter(None,cmdreturn.stdout.split('\n')))]
        cmdreturn.headers = []
        if not cmdreturn.rawdata:
            return
        else:
            cmdreturn.header = cmdreturn.rawdata.pop(0)
            #cmdreturn.headers = [header.translate(str.maketrans(replaceHeaderChars)) for header in cmdreturn.header.split()]
            cmdreturn.headers = cmdreturn.header.split(maxsplit=maxsplit)

    def translate_headers(self,replaceHeaderChars={}):
        corrected_dict = { k.replace(':', ''): v for k, v in ori_dict.items() }

    #sample_string.translate(str.maketrans(char_to_replace))

    def identify(self, view_keyname: str='_identity'):

        micro_ver = self.raidcom.views['_raidqry'][str(self.raidcom.serial)]['Micro_ver']
        if self.raidcom.views['_resource_groups']['0']['V_ID'] == "-":
            identifier = VId.micro_ver[micro_ver.split('-')[0]]['v_id']
        else:
            identifier = self.raidcom.views['_resource_groups']['0']['V_ID']
        
        self.raidcom.v_id = VId.models.get(identifier,{}).get('v_id',None)
        self.raidcom.vtype = VId.models.get(identifier,{}).get('type',None)
        self.raidcom.model = " - ".join(VId.models.get(identifier,{}).get('model',[]))
        self.raidcom.micro_ver = micro_ver
        self.raidcom.cache = self.raidcom.views['_raidqry'][str(self.raidcom.serial)]['Cache(MB)']
        self.raidcom.horcm_ver = self.raidcom.views['_raidqry'][str(self.raidcom.serial)]['HORCM_ver']
        #self.raidcom.cmd_device = self.raidcom.views['_horcctl'][self.raidcom.unitid]
        self.raidcom.cmd_device = self.raidcom.views['_horcctl'][self.raidcom.instance]['current_control_device']
        self.raidcom.cmd_device_type = ('FIBRE','IP')['IPCMD' in self.raidcom.cmd_device]
        self.raidcom.cmd_device_ldevid = None
        self.raidcom.cmd_device_culdev = None
        self.raidcom.cmd_device_port = None
        if not self.raidcom.vtype:
            raise Exception(f"Unable to identify self, check v_id.py for supported models, dump raidqry: {self.raidcom.views['_raidqry'][str(self.raidcom.serial)]}")
        
        # if inqraid was successful, we can also return cmd_device_ldevid
        try:
            if self.raidcom.cmd_device_type == "FIBRE":
                device_file =  self.raidcom.cmd_device.split('/')[-1]
                self.raidcom.cmd_device_ldevid = self.raidcom.views['_inqraid'][device_file]['LDEV']
                self.raidcom.cmd_device_culdev = Ldevid(self.raidcom.cmd_device_ldevid).culdev
                self.raidcom.cmd_device_port = self.raidcom.views['_inqraid'][device_file]['PORT']
        except:
            self.log.warn("Unable to derive cmd_device ldev_id")

        view = {
            'v_id': self.raidcom.v_id,
            'vtype': self.raidcom.vtype,
            'model': self.raidcom.model,
            'cache': self.raidcom.cache,
            'serial': self.raidcom.serial,
            'micro_ver': self.raidcom.micro_ver,
            'horcm_ver': self.raidcom.horcm_ver,
            'horcm_inst': self.raidcom.instance,
            'cmd_device': self.raidcom.cmd_device,
            'cmd_device_type': self.raidcom.cmd_device_type,
            'cmd_device_port': self.raidcom.cmd_device_port,
            'cmd_device_ldevid': self.raidcom.cmd_device_ldevid,
            'cmd_device_culdev': self.raidcom.cmd_device_culdev
        }
        
        cmdreturn = Cmdview("identify")
        cmdreturn.stdout = view
        cmdreturn.view = view
        return cmdreturn

    def raidqry(self, cmdreturn: object,datafilter: dict={}):
        self.initload(cmdreturn)
        
        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                serial = datadict['Serial#']
                cmdreturn.view[serial] = datadict
            cmdreturn.stats['storages'] = len(cmdreturn.view)

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.split()
            prefilter.append(dict(zip(cmdreturn.headers, row)))
        
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn

   
    def getresource(self, cmdreturn: object, datafilter: dict={}, altview: Callable=None, **kwargs ) -> dict:
        '''
        stdout as input from get resource command
        '''
        self.initload(cmdreturn)
        cmdreturn.stats = { 'resource_group_count':0 }

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                #self.log.info(datadict)
                rgid = datadict['RGID']
                cmdreturn.view[rgid] = datadict
                cmdreturn.stats['resource_group_count'] += 1

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.rsplit(maxsplit=5)
            prefilter.append(dict(zip(cmdreturn.headers, row)))

        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        self.altview(cmdreturn,altview)
        return cmdreturn

    def concurrent_getresource(self, cmdreturns: list, datafilter: dict={}, altview: Callable=None, **kwargs ) -> dict:

        newcmdreturn = Cmdview("concurrent_getresource")
        newcmdreturn.rawdata = []
        newcmdreturn.stats = { 'resource_group_count':0 }
        prefilter_dict = {}
        precombine = []

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                #self.log.info(datadict)
                rgid = datadict['RGID']
                cmdreturn.view[rgid] = datadict
                cmdreturn.stats['resource_group_count'] += 1

        for cmdreturn in cmdreturns:
            prefilter = []
            self.initload(cmdreturn)
            
            for line in cmdreturn.rawdata:
                row = line.split()
                for item in row:
                    precombine.append(dict(zip(cmdreturn.headers, row)))
        
        for r in precombine:
            rgid = r['RGID']
            prefilter_dict[rgid] = prefilter_dict.get(rgid,{})
            for k, v in r.items():
                prefilter_dict[rgid][k] = v

        prefilter = [value for value in prefilter_dict.values()]
        newcmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(newcmdreturn)
        return newcmdreturn



    def getresourcebyname(self, cmdreturn: object, datafilter: dict={}, **kwargs) -> dict:
        '''
        stdout as input from get resource command
        '''
        self.initload(cmdreturn)
        cmdreturn.stats = { 'resource_group_count':0 }

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                self.log.info(datadict)
                key = datadict['RS_GROUP']
                cmdreturn.view[key] = datadict
                cmdreturn.stats['resource_group_count'] += 1

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.rsplit(maxsplit=5)
            prefilter.append(dict(zip(cmdreturn.headers, row)))

        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn

    def getport(self,cmdreturn: object, datafilter: dict={}, **kwargs ) -> object:
        '''
        cmdreturn: getport cmdreturn object as input
        default_view_keyname: default _ports
        '''
        self.initload(cmdreturn)
        cmdreturn.headers.append('UNITID')
        cmdreturn.stats = { 'portcount':0 }

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                port = datadict['PORT']
                if port not in cmdreturn.view:
                    cmdreturn.view[port] = copy.deepcopy(datadict)
                    cmdreturn.view[port]['ATTR'] = []
                    cmdreturn.stats['portcount'] += 1
                cmdreturn.view[port]['ATTR'].append(datadict['ATTR'])

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.split()
            #row[0] = re.sub(r'\d+$','',row[0])
            port = re.sub(r'\d+$','',row[0])
            unitid = (row[0].replace(port,''),'0')[row[0].replace(port,'') == '']
            row[0] = port
            row.append(unitid)
            prefilter.append(dict(zip(cmdreturn.headers, row)))
            
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn

    def splitportgid(self,hsd):
        clport,gid = hsd.rsplit(maxsplit=1)
        clport = re.sub(r'\d+$','',clport)
        return clport,gid

    def cleanhsd(self,hsd):
        cl,port,gid = hsd.split('-')
        port = re.sub(r'\d+$','',port)
        return f"{cl}-{port}-{gid}"

    def getldev(self,cmdreturn: object,datafilter: dict={}) -> object:
        ldevdata = [dict(map(str.strip, row.split(':', 1)) for row in list(filter(None,ldev.split('\n')))) for ldev in cmdreturn.stdout.split('\n\n')]
        prefilter = []

        def createview(cmdreturn):
            cmdreturn.stats = { 'ldevcount':0 }
            for datadict in cmdreturn.data:
                ldev_id = datadict['LDEV']
                cmdreturn.view[ldev_id] = datadict
                cmdreturn.stats['ldevcount'] += 1

        def PORTs(**kwargs):
            port_data = {}
            num_ports = int(kwargs['ldevdata']['NUM_PORT'])
            if num_ports > 0:
                if (num_ports -1) != kwargs['value'].count(' : '):
                    message = f"Unable to parse malformed PORTs ( NUM_PORT: {num_ports} ) {kwargs['value']}. Possible \':\' in hostgroup name?"
                    self.log.error(message)
                    raise Exception(message)
                ports = [pdata.strip() for pdata in list(filter(None,kwargs['value'].split(' : ')))]
                for hsd_lun_name in ports:
                    hsd,lun,name = hsd_lun_name.split(maxsplit=2)
                    hsd = self.cleanhsd(hsd)
                    port_data[hsd] = { "portId":hsd.rsplit('-',1)[0], "hostGroupNumber": hsd.split('-')[-1], "lun":lun, "hostgroupid":hsd, "hostGroupNameAbv": name }
                    #if len(name) > 15:
                    try:
                        port_data[hsd]['hostGroupName'] = self.raidcom.views['_ports'][hsd.rsplit('-',1)[0]]['_GIDS'][hsd.split('-')[-1]]['GROUP_NAME']
                    except KeyError as e:
                        self.raidcom.gethostgrp_key_detail(hsd.rsplit('-',1)[0])
                        port_data[hsd]['hostGroupName'] = self.raidcom.views['_ports'][hsd.rsplit('-',1)[0]]['_GIDS'][hsd.split('-')[-1]]['GROUP_NAME']  

            if len(port_data):
                kwargs['ldevout']['PORTs'] = port_data
            else:
                kwargs['ldevout']['PORTs'] = kwargs['value']

        def VOL_ATTR(**kwargs):
            kwargs['ldevout']['VOL_ATTR'] = kwargs['value'].split(' : ')

        def VOL_Capacity(**kwargs):
            capacity = Storcapunits(kwargs['value'],'blk')
            for denom in ['BLK','MB','GB','TB']:
                kwargs['ldevout'][f'VOL_Capacity({denom})'] = str(getattr(capacity,denom))

        def Used_Block(**kwargs):
            capacity = Storcapunits(kwargs['value'],'blk')
            for denom in ['BLK','MB','GB','TB']:
                kwargs['ldevout'][f'Used_Block({denom})'] = str(getattr(capacity,denom))

        def LDEV(**kwargs):
            l = kwargs['value'].split()
            kwargs['ldevout'][kwargs['key']] = l[0]
            if len(l) > 1:
                kwargs['ldevout'][l[1]] = l[3]
            
        def RSGID(**kwargs):
            kwargs['ldevout'][kwargs['key']] = kwargs['value']
            kwargs['ldevout']['RS_GROUP'] = self.raidcom.views['_resource_groups'][kwargs['value']]['RS_GROUP']

        specialfields = {'LDEV': LDEV, 'PORTs': PORTs,'VOL_ATTR': VOL_ATTR, 'VOL_Capacity(BLK)': VOL_Capacity, 'Used_Block(BLK)': Used_Block, 'RSGID': RSGID }
        for ldev in ldevdata:
            ldevout = {}
            for k, v in ldev.items():
                if k in specialfields:
                    specialfields[k](key=k,value=v,ldevdata=ldev,ldevout=ldevout)
                else:
                    ldevout[k] = v
            prefilter.append(ldevout)
        
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        
        createview(cmdreturn)
        return cmdreturn
        
        
    def getldevlist(self,cmdreturn: object, datafilter: dict={}, **kwargs) -> object:
        

        def createview(cmdreturn):
            cmdreturn.stats = { 'ldevcount':0 }
            for datadict in cmdreturn.data:
                #self.log.info(datadict)
                ldev_id = datadict['LDEV']
                cmdreturn.view[ldev_id] = datadict
                cmdreturn.stats['ldevcount'] += 1

        prefilter = []
        listofldevs = list(filter(None,cmdreturn.stdout.split('\n\n')))
        for ldev in listofldevs:
            ldevobj = type('obj', (object,), {'stdout' : ldev, 'view': {}})
            parsedldev = self.getldev(ldevobj)
            #self.updateview(cmdreturn.view,parsedldev.view)
            #cmdreturn.data.extend(parsedldev.data)
            prefilter.extend(parsedldev.data)
            
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn

    def getldevlist_front_end(self,cmdreturn: object, datafilter: dict={}, **kwargs) -> object:

        self.initload(cmdreturn,maxsplit=9)
        cmdreturn.stats = { 'ldev_count':0 }

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                #self.log.info(datadict)
                ldevid = datadict['LDEV#']
                cmdreturn.view[ldevid] = datadict
                cmdreturn.stats['ldev_count'] += 1

        prefilter = []
        for line in cmdreturn.rawdata:
            
            row = line.rsplit(maxsplit=9)
            prefilter.append(dict(zip(cmdreturn.headers, row)))

        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn
        
    def gethostgrp(self,cmdreturn: object, datafilter: dict={}) -> object:

        '''
        host_grp_filter = { 'KEY': 'VALUE' | ['VALUE1','VALUE2'], 'KEY2': 'VALUE' | ['VALUE1','VALUE2'] }\n
        e.g. host_grp_filter = { 'HMD': ['LINUX/IRIX','VMWARE_EX'] } filters host groups where HMD is LINUX/IRIX or VMWARE_EX\n
        host_grp_filter is case sensitive in both the key and the value and allows host_grps through only if ALL criteria matches.\n
        Remember that pretty much all of the values are parsed into strings, RGID for example is a string.\n
        '''
        self.initload(cmdreturn)
        cmdreturn.stats = { '_GIDS':0, '_GIDS_UNUSED':0 }

        def hostgrpsview(gid_key,data):
            for host_grp_dict in data:
                #self.log.info(host_grp_dict)
                port = host_grp_dict['PORT']
                gid = host_grp_dict['GID']
                cmdreturn.view[port] = cmdreturn.view.get(port,{})
                cmdreturn.view[port][gid_key] = cmdreturn.view[port].get(gid_key,{})
                cmdreturn.view[port][gid_key][gid] = host_grp_dict
                cmdreturn.data.append(host_grp_dict)
                cmdreturn.stats[gid_key] += 1

        prefiltered_host_grps = []
        cmdreturn.headers.insert(0,"HOST_GRP_ID")
        for line in cmdreturn.rawdata:
            hostgroupName = re.findall(r'"([^"]*)"', line)
            if hostgroupName:
                line = line.replace(f'"{hostgroupName[0]}"','-')
            port,gid,rgid,nameSpace,serial,hostmode,host_mode_options = line.split()
            port = re.sub(r'\d+$','', port)
            host_grp_id = f"{port}-{gid}"

            if hostgroupName:
                nameSpace = hostgroupName[0]
            if host_mode_options == "-":
                host_mode_options = []
            else:
                #host_mode_options = sorted([int(host_mode) for host_mode in host_mode_options.split(':')])
                host_mode_options = [str(hmo) for hmo in sorted([int(opt) for opt in host_mode_options.split(':')])]

            values = (host_grp_id,port,gid,rgid,nameSpace,serial,hostmode,host_mode_options)
            
            prefiltered_host_grps.append(dict(zip(cmdreturn.headers, values)))

        filtered_host_grps = list(filter(lambda l: self.applyfilter(l,datafilter),filter_unused_grps))
        #filtered_host_grps = list(filter(filter_host_grps,prefiltered_host_grps))
        used_host_grps = list(filter(lambda x: (x['GROUP_NAME'] != '-'),filtered_host_grps))
        #unused_host_grps = list(filter(lambda x: (x['GROUP_NAME'] == '-'),filtered_host_grps))
        hostgrpsview('_GIDS',used_host_grps)
        #hostgrpsview('_GIDS_UNUSED',unused_host_grps)
        #if cmdreturn.header: cmdreturn.rawdata.insert(0,cmdreturn.header)
        return cmdreturn

    def gethostgrp_key_detail(self,cmdreturn: object, datafilter: dict={}, hostgrp_usage: list=['_GIDS','_GIDS_UNUSED']) -> object:

        '''
        host_grp_filter = { 'KEY': 'VALUE' | ['VALUE1','VALUE2'], 'KEY2': 'VALUE' | ['VALUE1','VALUE2'] }\n
        e.g. host_grp_filter = { 'HMD': ['LINUX/IRIX','VMWARE_EX'] } filters host groups where HMD is LINUX/IRIX or VMWARE_EX\n
        host_grp_filter is case sensitive in both the key and the value and allows host_grps through only if ALL criteria matches.\n
        Remember that pretty much all of the values are parsed into strings, RGID for example is a string.\n
        '''
        self.initload(cmdreturn)
        cmdreturn.stats = { '_GIDS':0, '_GIDS_UNUSED':0 }

        def hostgrpsview(gid_key,data):
            for host_grp_dict in data:
                #self.log.info(host_grp_dict)
                port = host_grp_dict['PORT']
                gid = host_grp_dict['GID']
                cmdreturn.view[port] = cmdreturn.view.get(port,{})
                cmdreturn.view[port][gid_key] = cmdreturn.view[port].get(gid_key,{})
                cmdreturn.view[port][gid_key][gid] = host_grp_dict
                cmdreturn.data.append(host_grp_dict)
                cmdreturn.stats[gid_key] += 1

        prefiltered_host_grps = []
        cmdreturn.headers.insert(0,"HOST_GRP_ID")
        for line in cmdreturn.rawdata:
            hostgroupName = re.findall(r'"([^"]*)"', line)
            if hostgroupName:
                line = line.replace(f'"{hostgroupName[0]}"','-')
            port,gid,rgid,nameSpace,serial,hostmode,host_mode_options = line.split()
            port = re.sub(r'\d+$','', port)
            host_grp_id = f"{port}-{gid}"

            if hostgroupName:
                nameSpace = hostgroupName[0]
            if host_mode_options == "-":
                host_mode_options = []
            else:
                #host_mode_options = sorted([int(host_mode) for host_mode in host_mode_options.split(':')])
                host_mode_options = [str(hmo) for hmo in sorted([int(opt) for opt in host_mode_options.split(':')])]

            values = (host_grp_id,port,gid,rgid,nameSpace,serial,hostmode,host_mode_options)
            
            prefiltered_host_grps.append(dict(zip(cmdreturn.headers, values)))

        filtered_host_grps = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered_host_grps))
        host_grps_usage = { '_GIDS': list(filter(lambda x: (x['GROUP_NAME'] != '-'),filtered_host_grps)), '_GIDS_UNUSED': list(filter(lambda x: (x['GROUP_NAME'] == '-'),filtered_host_grps)) }

        for usage in hostgrp_usage:    
            hostgrpsview(usage,host_grps_usage[usage])

        #if cmdreturn.header: cmdreturn.rawdata.insert(0,cmdreturn.header)
        return cmdreturn

    def getlun(self,cmdreturn: object,datafilter: dict={}) -> object:

        self.initload(cmdreturn)
        cmdreturn.stats = { 'luncount':0 }
        
        def createview(data):
            for datadict in data:
                port = datadict['PORT']
                gid = datadict['GID']
                lun = datadict['LUN']
                cmdreturn.view[port] = cmdreturn.view.get(port,{})
                cmdreturn.view[port]['_GIDS'] = cmdreturn.view[port].get('_GIDS',{})
                cmdreturn.view[port]['_GIDS'][gid] = cmdreturn.view[port]['_GIDS'].get(gid,{'_LUNS':{}})
                cmdreturn.view[port]['_GIDS'][gid]['_LUNS'][lun] = datadict
                cmdreturn.stats['luncount'] += 1 

        prefiltered_luns = []
        cmdreturn.headers.insert(0,"HOST_GRP_ID")
        for line in cmdreturn.rawdata:
            #self.log.info(f"{line}")
            values = line.strip().split(maxsplit=9)
            if len(values) > 9:
                #values[9] = sorted([int(hmo) for hmo in values[9].split()])
                values[9] = [str(inthmo) for inthmo in sorted([int(hmo) for hmo in values[9].split()])]
            else:
                values.append([])
            port = values[0] = re.sub(r'\d+$','', values[0])
            gid = values[1]
            host_grp_id = f"{port}-{gid}"            
            values.insert(0,host_grp_id)

            prefiltered_luns.append(dict(zip(cmdreturn.headers, values)))
        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered_luns))
        createview(cmdreturn.data)

        return cmdreturn

    def gethbawwn(self,cmdreturn: object, datafilter: dict={}) -> object:

        self.initload(cmdreturn)
        cmdreturn.stats = { 'hbawwncount':0 }

        # Quick fix for when hba_wwn is requested from ELUN port
        if not len(cmdreturn.rawdata) or re.search(r'^PORT\s+WWN',cmdreturn.header):
            return cmdreturn
        
        def createview(data):
            for datadict in data:
                #self.log.info(datadict)
                port = datadict['PORT']
                gid = datadict['GID']
                wwn = datadict['HWWN']

                #cmdreturn.view[port] = cmdreturn.view.get(port,{ '_GIDS': { gid:{'_WWNS':{}}} })
                #cmdreturn.view[port]['_GIDS'][gid]['_WWNS'][wwn] = {}
                #cmdreturn.stats['hbawwncount'] += 1

                cmdreturn.view[port] = cmdreturn.view.get(port,{ '_GIDS': { gid:{'_WWNS':{}}} })
                cmdreturn.view[port]['_GIDS'][gid]['_WWNS'][wwn] = datadict
                cmdreturn.stats['hbawwncount'] += 1
#                for value,head in zip(values,cmdreturn.headers):
#                    cmdreturn.view[port]['_GIDS'][gid]['_WWNS'][wwn][head] = value

#                cmdreturn.view[port]['_GIDS'] = cmdreturn.view[port].get('_GIDS',{})
#                cmdreturn.view[port]['_GIDS'][gid] = cmdreturn.view[port]['_GIDS'].get(gid,{'_LUNS':{}})
#                cmdreturn.view[port]['_GIDS'][gid]['_LUNS'][lun] = datadict
#                cmdreturn.stats['luncount'] += 1 
        
        prefiltered = []
        cmdreturn.headers.insert(0,"HOST_GRP_ID")
        for line in cmdreturn.rawdata:
            sline = line.strip()
            regex = r'\s(\w{16}\s{3,4}'+str(self.raidcom.serial)+r')(?:\s+)(.*$)'
            capture = re.search(regex,sline)
            wwn,serial = capture.group(1).split()
            wwn = wwn.lower()
            wwn_nickname = capture.group(2)
            row = sline.split()
            port,gid = row[0],row[1]
            extractHostGroupNameRegex = r'(?:^'+port+r'\s+'+gid+r'\s+)(.*?)(?:\s+'+capture.group(1)+r'\s+'+capture.group(2)+r')'
            hostgroupName = re.search(extractHostGroupNameRegex,sline).group(1)
            port = re.sub(r'\d+$','', port)
            host_grp_id = f"{port}-{gid}"
            values = (host_grp_id,port,gid,hostgroupName,wwn,serial,wwn_nickname)
            
            #cmdreturn.view[port] = cmdreturn.view.get(port,{ '_GIDS': { gid:{'_WWNS':{}}} })
            #cmdreturn.view[port]['_GIDS'][gid]['_WWNS'][wwn] = {}
            #cmdreturn.stats['hbawwncount'] += 1
            prefiltered.append(dict(zip(cmdreturn.headers, values)))

        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
            #for value,head in zip(values,cmdreturn.headers):
            #    cmdreturn.view[port]['_GIDS'][gid]['_WWNS'][wwn][head] = value
        createview(cmdreturn.data)
        return cmdreturn

    def getportlogin(self,cmdreturn,datafilter: dict={}):

        self.initload(cmdreturn)
        cmdreturn.stats = { 'loggedinhostcount':0 }

        def createview(data):
            for datadict in data:
                #self.log.info(datadict)
                port = datadict['PORT']
                login_wwn = datadict['LOGIN_WWN']
                cmdreturn.view[port]['_PORT_LOGINS'][login_wwn] = { "Serial#": serial }
                cmdreturn.stats['loggedinhostcount'] += 1

        prefiltered = []
        for line in cmdreturn.rawdata:
            col = line.split()
            port = re.sub(r'\d+$','',col[0])
            cmdreturn.view[port] = cmdreturn.view.get(port,{'_PORT_LOGINS':{}})
            login_wwn,serial,dash = col[1],col[2],col[3]
            values = (port,login_wwn,serial,dash)
            prefiltered.append(dict(zip(cmdreturn.headers, values)))

        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn.data)
        return cmdreturn
    
    def getpool_key_None(self,cmdreturn,datafilter: dict={}) -> dict:
        
        self.initload(cmdreturn)
        cmdreturn.stats = { 'poolcount':0 }

        def createview(data):
            for datadict in data:
                poolid = str(int(datadict['PID']))
                cmdreturn.view[poolid] = datadict
            cmdreturn.stats['poolcount'] = len(cmdreturn.view)

        prefiltered = []
        for line in cmdreturn.rawdata:
            values = line.split()
            prefiltered.append(dict(zip(cmdreturn.headers,values)))
        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn.data)
        return cmdreturn

    def getpool_key_opt(self,cmdreturn,datafilter: dict={}) -> dict:
        
        self.initload(cmdreturn)
        cmdreturn.stats = { 'poolcount':0 }

        def createview(data):
            for datadict in data:
                poolid = str(int(datadict['PID']))
                cmdreturn.view[poolid] = datadict
                cmdreturn.stats['poolcount'] += 1

        prefiltered =[]
        for line in cmdreturn.rawdata:
            values = line.split()
            pid,pols,u,seq,num,ldev,h,vcap,typ,pm,pt,auto_add_plv = values[0],values[1],values[2],values[-9],values[-8],values[-7],values[-6],values[-5],values[-4],values[-3],values[-2],values[-1]
            revealpoolnameregex = r'(?:^'+pid+r'\s+'+pols+r'\s+'+u+r'\s+)(.*?)(?:\s'+seq+r'\s+'+num+r'\s+'+ldev+r'\s+'+h+r'\s+'+vcap+r'\s+'+typ+r'\s+'+pm+r'\s+'+pt+r'\s+'+auto_add_plv+r')'
            poolname = re.search(revealpoolnameregex,line).group(1).strip()
            values = (pid,pols,u,poolname,seq,num,ldev,h,vcap,typ,pm,pt,auto_add_plv)

            prefiltered.append(dict(zip(cmdreturn.headers,values)))
        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn.data)
        return cmdreturn

    def getpool_key_fmc(self,cmdreturn,datafilter={}) -> object:
        self.getpool_key_None(cmdreturn,datafilter={})
        return cmdreturn

    def getpool_key_saving(self,cmdreturn,datafilter={}) -> object:
        self.getpool_key_None(cmdreturn,datafilter={})
        return cmdreturn
    
    def getpool_key_basic(self,cmdreturn,datafilter={}) -> dict:
        
        self.initload(cmdreturn)
        cmdreturn.stats = { 'poolcount':0 }

        def createview(data):
            for datadict in data:
                poolid = str(int(datadict['PID']))
                cmdreturn.view[poolid] = datadict
            cmdreturn.stats['poolcount'] = len(cmdreturn.view)

        prefiltered = []
        for line in cmdreturn.rawdata:
            values = line.split(maxsplit=22)
            prefiltered.append(dict(zip(cmdreturn.headers,values)))

        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn.data)
        return cmdreturn

    def getpool_key_powersave(self,cmdreturn,datafilter={}) -> object:
        self.getpool_key_None(cmdreturn,datafilter={})
        return cmdreturn

    def getpool_key_total_saving(self,cmdreturn,datafilter={}) -> object:
        self.getpool_key_None(cmdreturn,datafilter=datafilter)
        return cmdreturn

    def getpool_key_software_saving(self,cmdreturn,datafilter={}) -> object:
        self.getpool_key_None(cmdreturn,datafilter=datafilter)
        return cmdreturn

    def getpool_key_efficiency(self,cmdreturn: object, datafilter={}) -> object:
        self.getpool_key_None(cmdreturn,datafilter=datafilter)
        return cmdreturn

    def getcopygrp(self,cmdreturn: object,datafilter: dict={}) -> dict:
        
        self.initload(cmdreturn)
        cmdreturn.stats = { 'copygrpcount':0 }

        def createview(data):
            for datadict in data:
                #self.log.info(datadict)
                copy_grp = datadict['COPY_GROUP']
                ldev_grp = datadict['LDEV_GROUP']
                cmdreturn.view[copy_grp] = cmdreturn.view.get(copy_grp,{})
                cmdreturn.view[copy_grp][ldev_grp] = datadict
            cmdreturn.stats['copygrpcount'] = len(cmdreturn.view)

        prefiltered = []
        for line in cmdreturn.rawdata:
            values = line.split()
            # Can't support copy_grps with spaces atm, sorry
            if len(values) != len(cmdreturn.headers): raise("header and data length mismatch, unable to support copy_grps with spaces, especially if device_grp also has spaces")
            prefiltered.append(dict(zip(cmdreturn.headers,values)))

        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn.data)
        return cmdreturn

    def getdevicegrp(self,cmdreturn: object,datafilter: dict={}) -> dict:
        
        self.initload(cmdreturn)
        cmdreturn.stats = { 'devicecount':0 }

        '''
        LDEV_GROUP                     LDEV_NAME                       LDEV#     Serial#
        MIG_350147.358149.1.BB8E5CFBEA MIG_s_34000_84:d0_t_34000_84:d0 34000     350147
        MIG_350147.358149.1.BB8E5CFBEA MIG_s_34001_84:d1_t_34001_84:d1 34001     350147
        MIG_350147.358149.1.BB8E5CFBEA MIG_s_34002_84:d2_t_34002_84:d2 34002     350147
        MIG_350147.358149.1.BB8E5CFBEA MIG_s_34003_84:d3_t_34003_84:d3 34003     350147
        '''
        def createview(data):
            for datadict in data:
                #self.log.info(datadict)
                ldev_grp = datadict['LDEV_GROUP']
                ldev = datadict['LDEV#']
                cmdreturn.view[ldev_grp] = cmdreturn.view.get(ldev_grp,{})
                cmdreturn.view[ldev_grp][ldev] = datadict
                cmdreturn.stats['devicecount'] += 1

        prefiltered = []
        for line in cmdreturn.rawdata:
            values = line.split()
            # Can't support copy_grps with spaces atm, sorry
            if len(values) != len(cmdreturn.headers): raise("header and data length mismatch, unable to support copy_grps with spaces, especially if device_grp also has spaces")
            prefiltered.append(dict(zip(cmdreturn.headers,values)))

        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn.data)
        return cmdreturn



    def getpath(self,cmdreturn: object, datafilter: dict={}, **kwargs ) -> object:
        '''
        cmdreturn: getpath cmdreturn object as input
        default_view_keyname: default _ports
        '''
        self.initload(cmdreturn)
        cmdreturn.stats = { 'pathcount':0 }

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                #PHG GROUP STS CM IF MP# PORT   WWN                 PR LUN PHS  Serial# PRODUCT_ID LB PM DM QD TO(s) PBW(s)
                phg = datadict['PHG']
                group = datadict['GROUP']
                port = datadict['PORT']
                cmdreturn.view[phg] = cmdreturn.view.get(phg,{})
                cmdreturn.view[phg][group] = cmdreturn.view[phg].get(group,{})
                cmdreturn.view[phg][group][port] = cmdreturn.view[phg][group].get(port,datadict)
                cmdreturn.stats['pathcount'] += 1

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.split()
            prefilter.append(dict(zip(cmdreturn.headers, row)))
            
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn

    def getparitygrp(self,cmdreturn: object, datafilter: dict={}, **kwargs ) -> object:
        '''
        cmdreturn: getparitygrp cmdreturn object as input
        default_view_keyname: default _parity_grps
        '''
        self.initload(cmdreturn)
        cmdreturn.stats = { 'parity_grp_count':0 }

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                #PHG GROUP STS CM IF MP# PORT   WWN                 PR LUN PHS  Serial# PRODUCT_ID LB PM DM QD TO(s) PBW(
                group = datadict['GROUP']
                cmdreturn.view[group] = datadict
                cmdreturn.stats['parity_grp_count'] += 1

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.split()
            prefilter.append(dict(zip(cmdreturn.headers, row)))
            
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn

    def getlicense(self,cmdreturn: object, datafilter: dict={}, **kwargs ) -> object:
        '''
        cmdreturn: getlicense cmdreturn object as input
        default_view_keyname: default _license
        '''
        self.initload(cmdreturn)
        cmdreturn.stats = { 'installed_licenses':0 }

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                license = datadict['Name']
                cmdreturn.view[license] = datadict
                if datadict['STS'] == "INS":
                    cmdreturn.stats['installed_licenses'] += 1

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.split()
            row = line.strip().split(maxsplit=8)
            row[8] = row[8].replace('"','')
            cmdreturn.headers.append("Serial#")
            row.append(cmdreturn.serial)
            prefilter.append(dict(zip(cmdreturn.headers, row)))
            
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn

    def getcommandstatus(self,cmdreturn) -> dict:

        self.initload(cmdreturn)
        
        def withreqid(data):
            #REQID    R SSB1    SSB2    Serial#      ID  Description\n
            #00000019 -    -       -     358149   10011  -\n
            for line in cmdreturn.rawdata:             
                values = (reqid,r,ssb1,ssb2,serial,id,description) = line.split(maxsplit=6)
                cmdreturn.view[reqid] = cmdreturn.view.get(reqid,{})
                cmdreturn.data.append(dict(zip(cmdreturn.headers, values)))
                for value,head in zip(values,cmdreturn.headers):
                    cmdreturn.view[reqid][head] = value

        def noreqid(data):
            #HANDLE   SSB1    SSB2    ERR_CNT        Serial#     Description\n
            # 00de        -       -          0         358149     -\n
            for line in cmdreturn.rawdata:                
                values = (handle,ssb1,ssb2,errcnt,serial,description) = line.split(maxsplit=5)
                cmdreturn.data.append(dict(zip(cmdreturn.headers, values)))
                for value,head in zip(values,cmdreturn.headers):
                    cmdreturn.view[head] = value

        if 'REQID' in cmdreturn.headers:
            withreqid(cmdreturn.rawdata)
        else:
            noreqid(cmdreturn.rawdata)

        return cmdreturn

    def getsnapshot(self,cmdreturn) -> object:
        self.initload(cmdreturn)
        cmdreturn.stats = { 'snapshotcount':0 }

        for line in cmdreturn.rawdata:
            values = (snapshot_name,ps,stat,serial,ldev,mu,pldev,pid,percent,mode,splttime) = line.split()
            if len(values) != len(cmdreturn.headers): raise("header and data length mismatch")
            cmdreturn.view[snapshot_name] = cmdreturn.view.get(snapshot_name,{})
            cmdreturn.stats['snapshotcount'] += 1

        return cmdreturn

    def getsnapshotgroup(self,cmdreturn) -> object:
        self.initload(cmdreturn)
        cmdreturn.stats = { 'snapshotcount':0 }

        for line in cmdreturn.rawdata:
            values = (snapshot_name,ps,stat,serial,ldev,mu,pldev,pid,percent,mode,splttime) = line.split()
            if len(values) != len(cmdreturn.headers): raise("header and data length mismatch")
            cmdreturn.view[snapshot_name] = cmdreturn.view.get(snapshot_name,{})
            for value,head in zip(values,cmdreturn.headers):
                cmdreturn.view[head] = value
            cmdreturn.stats['snapshotcount'] += 1

        return cmdreturn


    def gethostgrptcscan(self,cmdreturn,datafilter: dict={}) -> object:

        '''        self.initload(cmdreturn)
        cmdreturn.stats = { 'loggedinhostcount':0 }

        def createview(data):
            for datadict in data:
                #self.log.info(datadict)
                port = datadict['PORT']
                cmdreturn.view[port]['_PORT_LOGINS'][login_wwn] = { "Serial#": serial }
                cmdreturn.stats['loggedinhostcount'] += 1

        prefiltered = []
        for line in cmdreturn.rawdata:
            col = line.split()
            port = re.sub(r'\d+$','',col[0])
            cmdreturn.view[port] = cmdreturn.view.get(port,{'_PORT_LOGINS':{}})
            login_wwn,serial,dash = col[1],col[2],col[3]
            values = (port,login_wwn,serial,dash)
            prefiltered.append(dict(zip(cmdreturn.headers, values)))

        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn.data)
        return cmdreturn'''



      
        self.initload(cmdreturn)

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                port = datadict['PORT#']
                ldevid = datadict['LDEV#']
                cmdreturn.view[port] = cmdreturn.view.get(port,{})
                cmdreturn.view[port][ldevid] = cmdreturn.view[port].get(ldevid,datadict)
                if cmdreturn.view[port][ldevid]['P/S'] == 'SMPL':
                    cmdreturn.view[port][ldevid]['Status'] = 'SMPL'

        prefiltered = []

        for headingIndex in range(0, len(cmdreturn.headers)):
            if cmdreturn.headers[headingIndex] == '/ALPA/C':
                x = re.split(r'/', cmdreturn.headers[headingIndex])
                cmdreturn.headers[headingIndex] = x[1]
                cmdreturn.headers.insert(headingIndex+1, x[2]) 

        for line in cmdreturn.rawdata:
            values = line.split()
            hsdkeys = values[0].split('-')
            hsdkeys[1] = re.sub(r'\d+$','',hsdkeys[1])
            values[0] = '-'.join(hsdkeys)
            if len(values) != len(cmdreturn.headers): raise("header and data length mismatch")
            prefiltered.append(dict(zip(cmdreturn.headers, values)))
        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefiltered))
        createview(cmdreturn)
        return cmdreturn
   
    def raidscanremote(self,cmdreturn,datafilter: dict={}) -> object:
        self.gethostgrptcscan(cmdreturn,datafilter)
        return cmdreturn

    def raidscanmu(self,cmdreturn,mu) -> dict:
    
        self.initload(cmdreturn)
        for headingIndex in range(0, len(cmdreturn.headers)):
            if cmdreturn.headers[headingIndex] == '/ALPA/C':
                x = re.split(r'/', cmdreturn.headers[headingIndex])
                cmdreturn.headers[headingIndex] = x[1]
                cmdreturn.headers.insert(headingIndex+1, x[2])  

        cmdreturn.headers.append('mu')

        for line in cmdreturn.rawdata:
            values = line.split()
            values.append(mu)
            hsdkeys = values[0].split('-')
            hsdkeys[1] = re.sub(r'\d+$','',hsdkeys[1])
            values[0] = '-'.join(hsdkeys)
            if len(values) != len(cmdreturn.headers): raise("header and data length mismatch")
            prikey = ldevid = values[7]
            seckey = mu
            cmdreturn.view[prikey] = cmdreturn.view.get(prikey,{})
            cmdreturn.view[prikey][seckey] = cmdreturn.view[prikey].get(seckey,{})

            for value,head in zip(values,cmdreturn.headers):
                cmdreturn.view[prikey][seckey][head] = value  

            if cmdreturn.view[prikey][seckey]['P/S'] == 'SMPL':
                cmdreturn.view[prikey][seckey]['Status'] = 'SMPL'

        return cmdreturn

    def getrcu(self,cmdreturn) -> object:
        
        # stdout: some comments
        # Create list and simultaneously filter out empties
        # Return dictionary with rcu as key
        self.initload(cmdreturn)
        cmdreturn.stats = { 'rcucount':0 }

        for line in cmdreturn.rawdata:
            values = line.split()
            if len(values) != len(cmdreturn.headers): raise("header and data length mismatch")
            serial = values[0]
            cmdreturn.view[serial] = cmdreturn.view.get(serial,{})
            for item,head in zip(values,cmdreturn.headers):
                cmdreturn.view[serial][head] = item
            cmdreturn.stats['rcucount'] += 1

        return cmdreturn

    def gethostgrprgid(self,cmdreturn: object, resource_group_id: int) -> object:

        self.initload(cmdreturn)
        cmdreturn.stats = { 'hostgroupcount':0 }
        
        for line in cmdreturn.rawdata:
            hmos = ""
            hmolist = []
            sline = line.strip()
            hmoregex = r'(.*?)([\s\d]+$)'
            capture = re.search(hmoregex,sline)
            try:
                hmos = str(capture.group(2).strip())
                sline = capture.group(1)
                hmolist = hmos.split()
            except:
                pass
            row = sline.split()
            port,gid,serial,hmd = row[0],row[1],row[-2],row[-1]
            revealHostGroupNameRegex = r'(?:^'+port+r'\s+'+gid+r'\s+)(.*?)(?:\s+'+serial+r'\s+'+hmd+r')'
            hostgroupName = re.search(revealHostGroupNameRegex,sline).group(1)
            port = re.sub(r'\d+$','', port)
            
            values = (port,gid,hostgroupName,serial,hmd,sorted(hmolist),resource_group_id)
            cmdreturn.headers.append('RSGID')

            cmdreturn.view[port] = cmdreturn.view.get(port,{'_GIDS':{}})
            cmdreturn.view[port]['_GIDS'][gid] = {}
            cmdreturn.stats['hostgroupcount'] += 1

            for value,head in zip(values,cmdreturn.headers):
                cmdreturn.view[port]['_GIDS'][gid][head] = value

        return cmdreturn
    
    def getquorum(self,cmdreturn: object,datafilter: dict={}) -> object:

        self.initload(cmdreturn)
        cmdreturn.stats = { 'quorumcount':0 }
        quorum_prefilter = [dict(map(str.strip, row.split(':', 1)) for row in list(filter(None,quorum.split('\n')))) for quorum in cmdreturn.stdout.split('\n\n')]
        cmdreturn.data = list(filter(lambda q: self.applyfilter(q,datafilter),quorum_prefilter))

        def createview(data):
            for datadict in data:
                qrdid = datadict['QRDID']
                cmdreturn.view[qrdid] = cmdreturn.view.get(qrdid,datadict)
                cmdreturn.stats['quorumcount'] += 1

        createview(cmdreturn.data)
        return cmdreturn

    def getdrive(self,cmdreturn: object,datafilter: dict={}) -> object:

        self.initload(cmdreturn)
        cmdreturn.stats = { 'drivecount':0 }
        
        def createview(data):
            for datadict in data:
                #group = datadict['GROUP']
                location = datadict['LOCATION']
                cmdreturn.view[location] = datadict
                cmdreturn.stats['drivecount'] += 1 

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.split()
            prefilter.append(dict(zip(cmdreturn.headers, row)))

        cmdreturn.data = list(filter(lambda l: self.applyfilter(l,datafilter),prefilter))
        createview(cmdreturn.data)

        return cmdreturn
    
    def gethostgrpkeyhostgrprgid(self,cmdreturn: object,resourcegroupid):

        self.initload(cmdreturn)
        cmdreturn.stats = { 'hostgroupcount':0 }

        for line in cmdreturn.rawdata:
            hmos = ""
            hmolist = []
            sline = line.strip()
            self.log.debug(sline)
            hmoregex = r'(.*?)([\s\d]+$)'
            capture = re.search(hmoregex,sline)
            try:
                hmos = str(capture.group(2).strip())
                sline = capture.group(1)
                hmolist = hmos.split()
            except:
                pass
            row = sline.split()
            port,gid,serial,hmd = row[0],row[1],row[-2],row[-1]
            revealHostGroupNameRegex = r'(?:^'+port+r'\s+'+gid+r'\s+)(.*?)(?:\s+'+serial+r'\s+'+hmd+r')'
            hostgroupName = re.search(revealHostGroupNameRegex,sline).group(1)
            port = re.sub(r'\d+$','', port)
            self.log.debug("Port: '"+port+"', Gid: '"+gid+"', HostgroupName: '"+hostgroupName+"', serial: '"+serial+"', hostmode: '"+hmd+"', hmos: '"+hmos+"', RSGID: '"+str(resourcegroupid)+"'" )
            values = (port,gid,hostgroupName,serial,hmd,sorted(hmolist),resourcegroupid)
            cmdreturn.headers.append('RSGID')

            if port not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'] = { port:{} }
            viewsdict['metaview']['data'][port][gid] = {}
            viewsdict['metaview']['stats']['hostgroupcount'] += 1

            for value,head in zip(values,headings):
                viewsdict['metaview']['data'][port][gid][head] = value  

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict
