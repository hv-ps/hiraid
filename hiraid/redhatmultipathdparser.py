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
# 14/01/2020    v1.1.00     Initial Release - DC
#
# -----------------------------------------------------------------------------------------------------------------------------------

import re
import inspect
import sys
import json
from . import customhostviews

class RedhatMultipathdParser:
    def __init__(self,host):
        self.host = host
        self.log = host.log
        #self.serial = storage.serial
        self.setview = customhostviews.Customviews(host)
         
    def initload(self,stdout,header='',keys=[]):
        data = [row.strip() for row in list(filter(None,stdout.split('\n')))]
        viewsdict = { 'defaultview': {}, 'list': data, 'metaview': { 'data':{}, 'stats': {} } }
        return viewsdict, data, keys

    def returnldevid(self,value):
        out = { "in":value }
        pattern = re.compile('\w{2}:\w{2}')
        if pattern.match(str(value)):
            out['culdev'] = value
            out['decimal'] = int(value.replace(':',''),16)
        else:
            out['decimal'] = value
            hexadecimal = format(int(value), '02x')
            while len(hexadecimal) < 4:
                hexadecimal = "0" + hexadecimal
            out['culdev'] = hexadecimal[:2] + ":" + hexadecimal[2:]
        return out

    def multipathll(self,stdout,optviews: list=[]) -> dict:
        '''
        stdout as input
        '''
        mpout = {}
        seekwwid = r'.*HITACHI,OPEN.*'
        seeksize = r'^size=|policy'
        seekpath = r'\d+:\d+:\d+:\d+\s'
        key = None

        viewsdict, data, keys = self.initload(stdout)
        #viewsdict['metaview']['stats']['resourcegroupcount'] = 0
        def parsewwid(line,mpout):
            regex = r'(.*?)\s\((\w{33})\)\s(dm-\d+)\s(HITACHI,OPEN.*)$'
            capture = re.search(regex,line)
            culdev = capture.group(2)[-4:-2]+":"+capture.group(2)[-2:]
            ldev_id = self.returnldevid(culdev)['decimal']
            mpout[capture.group(3)] = { "alias":capture.group(1), "wwid":capture.group(2), "sysfsname":capture.group(3), "vendor":capture.group(4).split(',')[0], "emulation":capture.group(4).split(',')[1], "culdev":culdev, "ldev_id":ldev_id }
            #print(json.dumps(out,indent=4))
            return capture.group(3)

        def parseattributes(line,key,mpout):
            regex = r'[\w]+=\'{0,1}.+?\'{0,1}(?=\s[\w]+=|$)'
            capture = re.findall(regex,line.strip())
            for item in capture:
                header,value = item.split('=')
                mpout[key][header] = value.strip('\'')

        def parsepath(line,key,mpout):
            path = line.split()
            mpout[key]['paths'] = mpout[key].get('paths',{})
            #mpout[key]['paths'][path[2]] = bypathjson[path[2]]
            mpout[key]['paths'][path[2]] = mpout[key]['paths'].get(path[2], {})
            mpout[key]['paths'][path[2]]['major_minor'] = path[2]
            mpout[key]['paths'][path[2]]['device'] = path[2]
            mpout[key]['paths'][path[2]]['major_minor'] = path[3]
            mpout[key]['paths'][path[2]]['host_chan_scsiid_lun'] = path[1]
            mpout[key]['paths'][path[2]]['dm_path_state'] = path[4]
            mpout[key]['paths'][path[2]]['physical_path_state'] = path[5]
            mpout[key]['paths'][path[2]]['path_state'] = path[6]

        for line in data:
            if re.search(seekwwid,line):
                key = parsewwid(line,mpout)
            if re.search(seeksize,line):
                parseattributes(line,key,mpout)
            if re.search(seekpath,line):
                parsepath(line,key,mpout)

        viewsdict['metaview']['data'] = mpout
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict.get('header'): viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    def devdiskbypath(self,stdout,optviews: list=[]) -> dict:
        
        # stdout: some comments
        # Create list and simultaneously filter out empties
        # Drop numeric characters from back of port placed there based upon location in horcm
        # Return dictionary with port as key
        viewsdict, data, headings = self.initload(stdout)
        #viewsdict['metaview']['stats']['portcount'] = 0

        output = {}
        for line in data:
            row = line.split()
            hosthba = None
            storagehba = None
            lun = None
            for item in row:
                bypathregex = r'/dev/disk/by-path/fc-0x[\w]{16}-0x[\w]{16}-lun-[\d]+'
                deviceregex = r'../../sd[\w]+'

                if re.search(bypathregex,item):
                    bypath = item.split('/')[-1].split('-')
                    hosthba = bypath[1].lstrip('0x')
                    storagehba = bypath[2].lstrip('0x')
                    lun = bypath[-1]
                    continue
                if re.search(deviceregex,item):
                    device = item.lstrip('../../')
                    #print("HostHBA: {}, StorageHBA: {}, Lun: {}, Device: {}".format(hosthba,storagehba,lun,device))
                    output[device] = { "hosthba":hosthba, "storagehba":storagehba, "lun":lun }
        
        viewsdict['metaview']['data'] = output
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict.get('header'): viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    def getmultipathing(self,joindicts,optviews: list=[]) -> dict:

        viewsdict = { 'defaultview': {}, 'metaview': { 'data':{}, 'stats': {} } }
        viewsdict['metaview']['data'] = joindicts['multipathllview'].copy()
        
        for sysfsname in viewsdict['metaview']['data']:
            for device in viewsdict['metaview']['data'][sysfsname]['paths']:
                viewsdict['metaview']['data'][sysfsname]['paths'][device].update(joindicts['devdiskbypathview'][device])
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)
        return viewsdict

    def getcopygrp(self,stdout,optviews: list=[]) -> dict:
        
        # stdout: some comments
        # Create list and simultaneously filter out empties
        # Return dictionary with copy_grp as key
        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['copygrpcount'] = 0

        for line in data:
            sline = line.split()
            if len(sline) != len(headings): raise("header and data length mismatch")
            if sline[0] not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'][sline[0]] = {}
            for item,head in zip(sline,headings):
                viewsdict['metaview']['data'][sline[0]][head] = item
                # This won't work for multi use ports! The count will be incorrect
                # Also, concatenate port type to a list
            viewsdict['metaview']['stats']['copygrpcount'] += 1

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    def getrcu(self,stdout,optviews: list=[]) -> dict:
        
        # stdout: some comments
        # Create list and simultaneously filter out empties
        # Return dictionary with rcu as key
        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['rcu'] = 0

        for line in data:
            sline = line.split()
            if len(sline) != len(headings): raise("header and data length mismatch")
            if sline[0] not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'][sline[0]] = {}
            for item,head in zip(sline,headings):
                viewsdict['metaview']['data'][sline[0]][head] = item
                # This won't work for multi use ports! The count will be incorrect
                # Also, concatenate port type to a list
            viewsdict['metaview']['stats']['rcu'] += 1

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    def getportlogin(self,stdout,optviews=[]):

        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['loggedinhostcount'] = 0

        for line in data:
            sline = line.split()
            if len(sline) != len(headings): raise("header and data length mismatch")
            sline[0] = re.sub(r'\d+$','',sline[0])
            if sline[0] not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'][sline[0]] = {}

            login_wwn,serial,dash = sline[1],sline[2],sline[3]

            if login_wwn not in viewsdict['metaview']['data'][sline[0]]:
                viewsdict['metaview']['data'][sline[0]][login_wwn] = login_wwn
                viewsdict['metaview']['stats']['loggedinhostcount'] += 1

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict


    def gethostgrp(self,stdout,optviews=[]):

        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['hostgroupcount'] = 0

        for line in data:
            hmos = ""
            hmolist = []
            sline = line.strip()
            self.log.debug(sline)
            hmoregex = r'(.*?)([\s\d]+$)'
            capture = re.search(hmoregex,sline)
            try:
                hmos = str(capture.group(2).strip()).split()
                sline = capture.group(1)
                hmolist = hmos.split()
            except:
                pass
            
            row = sline.split()
            port,gid,serial,hmd = row[0],row[1],row[-2],row[-1]
            revealHostGroupNameRegex = r'(?:^'+port+r'\s+'+gid+r'\s+)(.*?)(?:\s+'+serial+r'\s+'+hmd+r')'
            hostgroupName = re.search(revealHostGroupNameRegex,sline).group(1)
            port = re.sub(r'\d+$','', port)
            self.log.debug('Port {}, gid {}, hostgroupname {}, serial {}, hostmode {}, hmos {}, hmolist {}'.format(port,gid,hostgroupName,serial,hmd,hmos,hmolist))
            values = (port,gid,hostgroupName,serial,hmd,sorted(hmolist))
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


    def gethostgrprgid(self,stdout,resourcegroupid,optviews=[]):

        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['hostgroupcount'] = 0

        for line in data:
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
            self.log.debug("Port: '"+port+"', Gid: '"+gid+"', HostgroupName: '"+hostgroupName+"', serial: '"+serial+"', hostmode: '"+hmd+"', hmos: '"+hmos+"', RSGID: '"+resourcegroupid+"'" )
            values = (port,gid,hostgroupName,serial,hmd,sorted(hmolist),resourcegroupid)
            headings.append('RSGID')

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

    def gethostgrpkeyhostgrprgid(self,stdout,resourcegroupid,optviews=[]):

        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['hostgroupcount'] = 0

        for line in data:
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
            headings.append('RSGID')

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



    def getlun(self,stdout,optviews=[]) -> dict:

        self.log.debug('Entered Raidcomparser: {}'.format(inspect.currentframe().f_code.co_name))
        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['luncount'] = 0

        for line in data:
            hmos = ""
            hmolist = []
            sline = line.strip()
            self.log.debug(sline)
            hmoregex = r'(.*?)([\s\d{2,3}]+$)'
            capture = re.search(hmoregex,sline)
            try:
                hmos = str(capture.group(2).strip())
                sline = capture.group(1)
                hmolist = hmos.split()
            except:
                pass
            port,gid,hmd,lun,num,ldev,cm,serial,opkma = sline.split()
            values = sline.split()
            values.append(sorted(hmolist))
            values[0] = re.sub(r'\d+$','', values[0])
            port = re.sub(r'\d+$','', port)
            self.log.debug("Port: '"+port+"', Gid: '"+gid+"', hostmode: '"+hmd+"', lun: '"+lun+"', num: '"+num+"', ldev: '"+ldev+"', cm: '"+cm+"', serial: '"+serial+"', opkma: '"+opkma+"', hmos: '"+hmos+"'")
            if port not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'] = { port:{} }
            if gid not in viewsdict['metaview']['data'][port]:
                viewsdict['metaview']['data'][port][gid] = {}

            viewsdict['metaview']['data'][port][gid][lun] = {}
            viewsdict['metaview']['stats']['luncount'] += 1

            for value,head in zip(values,headings):
                viewsdict['metaview']['data'][port][gid][lun][head] = value  

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    #
    def gethostgrptcscan(self,stdout,optviews=[]) -> dict:

        self.log.debug('Entered Raidcomparser: {}'.format(inspect.currentframe().f_code.co_name))
        viewsdict, data, headings = self.initload(stdout)
        #viewsdict['metaview']['stats']['luncount'] = 0
        for headingIndex in range(0, len(headings)):
            if headings[headingIndex] == '/ALPA/C':
                x = re.split(r'/', headings[headingIndex])
                headings[headingIndex] = x[1]
                headings.insert(headingIndex+1, x[2]) 

        for line in data:
            sline = line.split()
            keys = sline[0].split('-')
            keys[1] = re.sub(r'\d+$','',keys[1])
            sline[0] = '-'.join(keys)
            if len(sline) != len(headings): raise("header and data length mismatch")
            prikey = sline[0]
            seckey = sline[7]

            if prikey not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'] = { prikey:{} }
            if seckey not in viewsdict['metaview']['data'][prikey]:
                viewsdict['metaview']['data'][prikey][seckey] = {}

            for value,head in zip(sline,headings):
                viewsdict['metaview']['data'][prikey][seckey][head] = value 

            if viewsdict['metaview']['data'][prikey][seckey]['P/S'] == 'SMPL':
                viewsdict['metaview']['data'][prikey][seckey]['Status'] = 'SMPL'

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict
    #

    def raidscanremote(self,stdout,optviews=[]) -> dict:
    
        self.log.debug('Entered Raidcomparser: {}'.format(inspect.currentframe().f_code.co_name))
        viewsdict, data, headings = self.initload(stdout)
        #viewsdict['metaview']['stats']['luncount'] = 0
        for headingIndex in range(0, len(headings)):
            if headings[headingIndex] == '/ALPA/C':
                x = re.split(r'/', headings[headingIndex])
                headings[headingIndex] = x[1]
                headings.insert(headingIndex+1, x[2]) 

        for line in data:
            sline = line.split()
            keys = sline[0].split('-')
            keys[1] = re.sub(r'\d+$','',keys[1])
            sline[0] = '-'.join(keys)
            if len(sline) != len(headings): raise("header and data length mismatch")
            prikey = sline[0]
            seckey = sline[7]

            if prikey not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'] = { prikey:{} }
            if seckey not in viewsdict['metaview']['data'][prikey]:
                viewsdict['metaview']['data'][prikey][seckey] = {}

            for value,head in zip(sline,headings):
                viewsdict['metaview']['data'][prikey][seckey][head] = value 

            if viewsdict['metaview']['data'][prikey][seckey]['P/S'] == 'SMPL':
                viewsdict['metaview']['data'][prikey][seckey]['Status'] = 'SMPL'

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    def raidscanmu(self,stdout,mu,optviews=[]) -> dict:
    
        self.log.debug('Entered Raidcomparser: {}'.format(inspect.currentframe().f_code.co_name))
        viewsdict, data, headings = self.initload(stdout)
        #viewsdict['metaview']['stats']['luncount'] = 0
        for headingIndex in range(0, len(headings)):
            if headings[headingIndex] == '/ALPA/C':
                x = re.split(r'/', headings[headingIndex])
                headings[headingIndex] = x[1]
                headings.insert(headingIndex+1, x[2]) 

        headings.append('mu')

        for line in data:
            sline = line.split()
            sline.append(mu)
            keys = sline[0].split('-')
            keys[1] = re.sub(r'\d+$','',keys[1])
            sline[0] = '-'.join(keys)
            if len(sline) != len(headings): raise("header and data length mismatch")
            ldevid = sline[7]
            prikey = ldevid
            seckey = mu

            if prikey not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'][prikey] = {}
            if seckey not in viewsdict['metaview']['data'][prikey]:
                viewsdict['metaview']['data'][prikey][seckey] = {}

            for value,head in zip(sline,headings):
                viewsdict['metaview']['data'][prikey][seckey][head] = value 

            if viewsdict['metaview']['data'][prikey][seckey]['P/S'] == 'SMPL':
                viewsdict['metaview']['data'][prikey][seckey]['Status'] = 'SMPL'

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict


    def getldev(self,stdout,optviews=[]): 

        helperfunctions = {'gethostgroup':self.gethostgrp }
        viewsdict = { 'defaultview': {}, 'list': [], 'metaview': { 'data':{}, 'stats': {} }, 'header': [] }
        ldevdict = {}

        def returnldevid(self,value):
            out = { "in":value }
            pattern = re.compile('\w{2}:\w{2}')
            if pattern.match(str(value)):
                self.log.debug('Matched storage hexadecimal: {}'.format(value))
                out['culdev'] = value
                out['decimal'] = int(value.replace(':',''),16)
            else:
                self.log.debug('Decimal input: {}'.format(value))
                out['decimal'] = value
                hexadecimal = format(int(value), '02x')
                while len(hexadecimal) < 4:
                    hexadecimal = "0" + hexadecimal
                out['culdev'] = hexadecimal[:2] + ":" + hexadecimal[2:]
            self.log.debug("returning: {}".format(out))
            return out

        def sanitisehostgroupid(hostgroupid):
            output = {}
            output['portid'] = '-'.join(hostgroupid.split('-')[:2])
            output['portid'] = re.sub(r'\d+$','',output['portid'])
            output['gid'] = hostgroupid.split('-')[-1]
            output['hostgroupid'] = '{}-{}'.format(output['portid'],output['gid'])
            return output

        def PORTs(values,ldevdict,row):
            hostgroupids = {}
            if int(ldevdict['NUM_PORT']) > 0:
                if (int(ldevdict['NUM_PORT']) -1) != values.count(' : '):
                    message = 'Malformed PORTs row {}, unable to parse ldev {}. Possible \':\' in hostgroup name?'.format(row,ldevdict['LDEV'])
                    self.log.info(message)
                    raise Exception(message)

                portdata = [row.strip() for row in list(filter(None,values.split(' : ')))]
                for portlunname in portdata:
                    portlunnamelist = portlunname.split()
                    cleanportdata = sanitisehostgroupid(portlunnamelist[0])
                    hostgroupids[cleanportdata['hostgroupid']] = { "portId":cleanportdata['portid'], "hostGroupNumber": cleanportdata['gid'], "lun":portlunnamelist[1], "hostgroupid":cleanportdata['hostgroupid'],"hostGroupNameAbv":" ".join(portlunnamelist[2:]) }
                    try:
                        hostgroupids[cleanportdata['hostgroupid']]['hostGroupName'] = self.storage.views['_ports'][cleanportdata['portid']]['_host_grps'][cleanportdata['gid']]['GROUP_NAME']
                    except KeyError:
                        self.log.info("Unable to populate true host group name from self, fetching data")
                        self.storage.gethostgrp(cleanportdata['portid'])
                        hostgroupids[cleanportdata['hostgroupid']]['hostGroupName'] = self.storage.views['_ports'][cleanportdata['portid']]['_host_grps'][cleanportdata['gid']]['GROUP_NAME']

            return hostgroupids
            #portregex = r'(^CL\w-\D+\d?-\d+\s\d+\s[\w\s:-_]{1,16})(\s:\s|$)'

        def VOL_ATTR(values,ldevdict,row):
            return values.split(' : ')

        specialfields = {'PORTs': PORTs,'VOL_ATTR': VOL_ATTR}
        
        data = [row.strip() for row in list(filter(None,stdout.split('\n')))]
        
        #for i, row in enumerate(data): 
        #    if re.search("^LDEV : ", row):
        #        ldevrow = data.pop(i)
        #        break

        #ldevfix = ldevrow.split()
        #ldevid = ldevfix[2]
        #culdev = returnldevid(self,ldevid)
        #ldevdict[ldevid] = {}

        #if len(ldevfix) > 3:
        #    data.insert(0," ".join(ldevfix[3:]))
        #    data.insert(0," ".join(ldevfix[:3]))
        #else:
        #    data.insert(0,ldevrow)
 
        headervalueregex = r'(^.*?)(?:\s:?)(.*$)'
        for row in data:
            self.log.debug("ROW: "+row)
            capture = re.search(headervalueregex,row)
            header = capture.group(1)
            value = capture.group(2).strip()

            if re.search("^Serial#",row):
                serialrow = row
                continue

            if re.search("^LDEV : ",row):
                ldevfix = row.split()
                ldevid = ldevfix[2]
                culdev = returnldevid(self,ldevid)['culdev']
                ldevdict[ldevid] = {}
                ldevdict[ldevid][ldevfix[0]] = ldevfix[2]
                ldevdict[ldevid]['CULDEV'] = culdev
                viewsdict['header'].append(ldevfix[0])
                viewsdict['header'].append('CULDEV')
                
                if len(ldevfix) > 3:
                    ldevdict[ldevid][ldevfix[0]] = ldevfix[2]
                    ldevdict[ldevid][ldevfix[3]] = ldevfix[5]
                    virtualculdev = returnldevid(self,ldevfix[5])['culdev']
                    additionalldevheader = 'CULDEV_{}'.format(ldevfix[3])
                    ldevdict[ldevid][additionalldevheader] = virtualculdev
                    viewsdict['header'].append(ldevfix[3])
                    viewsdict['header'].append(additionalldevheader)

                serial = serialrow.split()
                ldevdict[ldevid][serial[0]] = serial[2]

                continue

            viewsdict['header'].append(header)

            try:
                parsedfield = specialfields[header](value,ldevdict[ldevid],row)
                ldevdict[ldevid][header] = parsedfield
            except KeyError:
                ldevdict[ldevid][header] = value
                pass

        viewsdict['metaview']['data'] = ldevdict
        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])

        for view in optviews:
            self.log.debug("Customview "+view)
            viewsdict[view] = getattr(customviews,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        self.log.debug(ldevdict)
        return viewsdict

    def gethbawwn(self,stdout,optviews=[]) -> dict:

        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['hbawwncount'] = 0

        # Quick and dirty fix for when hba_wwn is requested from ELUN port
        if re.search(r'^PORT\s+WWN',viewsdict['header']):
            self.log.debug("Skipping ELUN hba_wwn header: "+viewsdict['header'])
            return viewsdict

        for line in data:
            sline = line.strip()
            self.log.debug(sline)
            regex = r'\s(\w{16}\s{3,4}'+str(self.serial)+r')(?:\s+)(.*$)'
            capture = re.search(regex,sline)
            wwn,serial = capture.group(1).split()
            wwn = wwn.lower()
            wwn_nickname = capture.group(2)
            row = sline.split()
            port,gid = row[0],row[1]
            extractHostGroupNameRegex = r'(?:^'+port+r'\s+'+gid+r'\s+)(.*?)(?:\s+'+capture.group(1)+r'\s+'+capture.group(2)+r')'
            hostgroupName = re.search(extractHostGroupNameRegex,sline).group(1)
            port = re.sub(r'\d+$','', port)
            self.log.debug("Port: '"+port+"', Gid: '"+gid+"', HostgroupName: '"+hostgroupName+"', wwn: '"+wwn+"', serial: '"+serial+"', wwn_nickname: '"+wwn_nickname+"'" )
            values = (port,gid,hostgroupName,wwn,serial,wwn_nickname)
            if port not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'] = { port:{} }
            if gid not in viewsdict['metaview']['data'][port]:
                viewsdict['metaview']['data'][port][gid] = {}
            if wwn not in viewsdict['metaview']['data'][port][gid]:
                viewsdict['metaview']['data'][port][gid][wwn] = {}
            viewsdict['metaview']['stats']['hbawwncount'] += 1

            for value,head in zip(values,headings):
                viewsdict['metaview']['data'][port][gid][wwn][head] = value

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict


    def getpool(self,stdout,optviews: list=[]) -> dict:
        
        # stdout: some comments
        # Create list and simultaneously filter out empties
        # Drop numeric characters from back of port placed there based upon location in horcm
        # Return dictionary with port as key
        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['poolcount'] = 0

        def nokeyopt(data):
            for line in data:
                sline = line.split()
                if len(sline) != len(headings): raise("header and data length mismatch")
                sline[0] = str(int(sline[0]))
                if sline[0] not in viewsdict['metaview']['data']:
                    viewsdict['metaview']['data'][sline[0]] = {}
                for item,head in zip(sline,headings):
                    viewsdict['metaview']['data'][sline[0]][head] = item    
                viewsdict['metaview']['stats']['poolcount'] += 1

        def keyopt(data):
            for line in data:
                row = line.split()
                pid,pols,u,seq,num,ldev,h,vcap,typ,pm,pt,auto_add_plv = row[0],row[1],row[2],row[-9],row[-8],row[-7],row[-6],row[-5],row[-4],row[-3],row[-2],row[-1]
                revealpoolnameregex = r'(?:^'+pid+r'\s+'+pols+r'\s+'+u+r'\s+)(.*?)(?:\s'+seq+r'\s+'+num+r'\s+'+ldev+r'\s+'+h+r'\s+'+vcap+r'\s+'+typ+r'\s+'+pm+r'\s+'+pt+r'\s+'+auto_add_plv+r')'
                poolname = re.search(revealpoolnameregex,line).group(1).strip()
                poolid = str(int(pid))
                values = (pid,pols,u,poolname,seq,num,ldev,h,vcap,typ,pm,pt,auto_add_plv)
                if pid not in viewsdict['metaview']['data']:
                    viewsdict['metaview']['data'][poolid] = {}
                for value,head in zip(values,headings):
                    viewsdict['metaview']['data'][poolid][head] = value    
                viewsdict['metaview']['stats']['poolcount'] += 1

        if 'POOL_NAME' in headings:
            keyopt(data)
        else:
            nokeyopt(data)

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict

    def getcommandstatus(self,stdout,optviews: list=[]) -> dict:

        viewsdict, data, headings = self.initload(stdout)
        
        def withreqid(data):
            #REQID    R SSB1    SSB2    Serial#      ID  Description\n00000019 -    -       -     358149   10011  -\n
            for line in data:
                row = line.split()
                reqid,r,ssb1,ssb2,serial,ID = row[0],row[1],row[2],row[3],row[4],row[5]
                revealmessageregex = r'(?:^'+reqid+r'\s+'+r+r'\s+'+ssb1+r'\s+'+ssb2+r'\s+'+serial+r'\s+'+ID+r'\s+)(.*?)'
                description = re.search(revealmessageregex,line).group(1)
                values = reqid,r,ssb1,ssb2,serial,ID,description
                if reqid not in viewsdict['metaview']['data']:
                    viewsdict['metaview']['data'][reqid] = {}
                for value,head in zip(values,headings):
                    viewsdict['metaview']['data'][reqid][head] = value    

        def noreqid(data):
            #HANDLE   SSB1    SSB2    ERR_CNT        Serial#     Description\n00de        -       -          0         358149     -\n
            pass

        if 'REQID' in headings:
            withreqid(data)
        else:
            noreqid(data)

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict
        

    def pairdisplay(self,stdout,optviews: list=[]) -> dict:
        
        # THIS IS NOT IN USE

        # stdout: some comments
        # Create list and simultaneously filter out empties
        # Drop numeric characters from back of port placed there based upon location in horcm
        viewsdict, data, headings = self.initload(stdout)
        viewsdict['metaview']['stats']['devicecount'] = 0

        for line in data:
            sline = line.split()
            if len(sline) != len(headings): raise("header and data length mismatch")
            sline[0] = re.sub(r'\d+$','',sline[0])
            if sline[0] not in viewsdict['metaview']['data']:
                viewsdict['metaview']['data'][sline[0]] = {}
            for item,head in zip(sline,headings):
                viewsdict['metaview']['data'][sline[0]][head] = item
                # Also, concatenate port type to a list
            viewsdict['metaview']['stats']['devicecount'] += 1

        viewsdict['defaultview'] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_default")(viewsdict['metaview'])
        if viewsdict['header']: viewsdict['list'].insert(0,viewsdict['header'])
        for view in optviews:
            viewsdict[view] = getattr(self.setview,inspect.currentframe().f_code.co_name+"_"+view)(viewsdict['metaview'],view)

        return viewsdict