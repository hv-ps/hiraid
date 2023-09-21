# Work in progress
hiraid is a Python raidcom wrapper for communicating with Hitachi enterprise storage arrays.
raidcom output is parsed to json and also stored beneath storageobject.views.

The primary purpose of this library is to underpin the Hitachi Vantara opensource ansible project: https://github.com/hv-ps/Hitachi.Raidcom.Ansible

### Install Latest
> pip3 install git+https://github.com/hv-ps/hiraid.git

### Quick start
> from hiraid.raidcom import Raidcom  
> storage_serial = 53511  
> horcm_instance = 0  
> storage = Raidcom(storage_serial,horcm_instance)  
> ports = storage.getport()  
> print(json.dumps(ports.view,indent=4))  
> print(ports.data)  
> print(json.dumps(ports.stats))  

### Index your host groups, luns and associated ldevs
> storage.getpool(key='basic')  
> ports = storage.getport().view.keys()  
> hostgroups = storage.concurrent_gethostgrps(ports=ports)  
> allhsds = [f"{port}-{gid}" for port in hostgroups.view for gid in hostgroups.view[port]['_GIDS']]  
> storage.concurrent_getportlogins(ports=ports)  
> storage.concurrent_gethbawwns(portgids=allhsds)  
> storage.concurrent_getluns(portgids=allhsds)  
> ldevlist = set([ self.raidcom.views['_ports'][port]['_GIDS'][gid]['_LUNS'][lun]['LDEV'] for port in self.raidcom.views['_ports'] for gid in self.raidcom.views['_ports'][port].get('_GIDS',{}) for lun in self.raidcom.views['_ports'][port]['_GIDS'][gid].get('_LUNS',{}) ])  
> storage.concurrent_getldevs(ldevlist)  
> file = f"/var/tmp/{storage.serial}__{datetime.now().strftime('%d-%m-%Y_%H.%M.%S')}.json"  
> with open(file,'w') as w:  
>   w.write(json.dumps(storage.views,indent=4))

### raidqry
> rq = storage.raidqry()  
> rq = storage.raidqry(datafilter={'Serial#':'350147'})  
> rq = storage.raidqry(datafilter={'callable':lambda a : int(a['Cache(MB)']) > 50000})  
> print(rq.data)  
> print(rq.view)  
> print(rq.cmd)  
> print(rq.returncode)  
> print(rq.stdout)  
> print(rq.stderr)

### getldev
> l = storage.getldev(ldev_id=20000)  
> l = storage.getldev(ldev_id=20000-21000,datafilter={'LDEV_NAMING':'HAVING_THIS_LABEL'})  
> l = storage.getldev(ldev_id=20000-21000,datafilter={'callable':lambda a : float(a.get(Used_Block(GB)',0)) > 960000})  

> for ldev in l.data:  
>  print(ldev['LDEV'])  

### getport
> p = storage.getport()  
> p = storage.getport(datafilter={'callable':lambda a : a['TYPE'] == 'FIBRE' and 'TAR' in a['ATTR']})  

### gethostgrp
> h = storage.gethostgrp(port="cl1-a")  
> h = storage.gethostgrp(port="cl1-a",datafilter={'HMD':''VMWARE_EX'})  
> h = storage.gethostgrp(port="cl1-a",datafilter={'callable':lambda a : 'TEST' in a['GROUP_NAME']})  

### gethostgrp_key_detail
> h = storage.gethostgrp_key_detail(port="cl1-a")  
> h = storage.gethostgrp_key_detail(port="cl1-a",datafilter={'HMD':''VMWARE_EX'})  
> h = storage.gethostgrp_key_detail(port="cl1-a",datafilter={'callable':lambda a : 'TEST' in a['GROUP_NAME']})  

### getlun
> l = storage.getlun(port="cl1-a-1")  
> l = storage.getlun(port="cl1-a-1",datafilter={'LDEV':['12001','12002']})  
> l = storage.getlun(port="cl1-e-1",datafilter={'callable':lambda a : int(a['LUN']) > 10})  
> l = storage.getlun(port="cl1-e-1",datafilter={'callable':lambda a : int(a['LDEV']) > 12000})  

### getpool
> p = storage.getpool()  

### getcommandstatus
### resetcommandstatus
### lockresource
### unlockresource
### raidqry  
### getresource
### getresourcebyname
### getldevlist
### gethbawwn
### getportlogin
### getcopygrp
### getpath
### getparitygrp
### getlicense
### getsnapshot
### getsnapshotgroup
### addsnapshotgroup
### createsnapshot
### unmapsnapshotsvol
### resyncsnapshotmu
### snapshotevtwait
### snapshotgroupevtwait
### deletesnapshotmu
### addldev
### extendldev
### deleteldev
### addresource
### deleteresource
### addhostgrpresource
### deletehostgrpresourceid
### addhostgrpresourceid
### addldevresource
### deleteldevresourceid
### deleteldevresource
### addhostgrp
### deletehostgrp
### resethostgrp
### addlun
### deletelun
### unmapldev
### mapldev
### modifyldevname
### modifyldevcapacitysaving
### modifyhostgrp
### adddevicegrp
### addcopygrp
### addhbawwn
### addwwnnickname
### setwwnnickname
### gethostgrptcscan
### raidscanremote
### raidscanmu
### getrcu
### gethostgrprgid
### concurrent_gethostgrps
### concurrent_gethbawwns
### concurrent_getluns
### concurrent_getldevs
### concurrent_getportlogins
### concurrent_raidscanremote
### concurrent_addluns
### concurrent_addldevs

