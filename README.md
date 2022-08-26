# Work in progress: hiraid 1.0.10
hiraid is a Python raidcom wrapper for communicating with Hitachi enterprise storage arrays.
raidcom output is parsed to json and also stored beneath storageobject.views.

The primary purpose of this library is to underpin the Hitachi Vantara opensource ansible project: https://github.com/hv-ps/Hitachi.Raidcom.Ansible

### Install
> pip3 install git+https://github.com/hv-ps/hiraid.git

### Quick start
> from hiraid.raidcom import Raidcom  
> storage_serial = 53511  
> horcm_instance = 0
> storage = Raidcom(storage_serial,0)  
> ports = storage.getport()  
> print(json.dumps(ports.view,indent=4))  
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

