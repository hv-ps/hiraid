# Work in progress: hiraid 1.0.00
hiraid is a Python raidcom wrapper for communicating with Hitachi enterprise storage arrays.

raidcom output is parsed and stored into a logical structure beneath storageobject.views.

Some useful storage admin functions can be found in entry point script 'radmin' ( radmin -h )

## Futures roadmap
Add cmrest capability

### Install
> pip3 install git+https://github.com/hv-ps/hiraid.git

In order for this to work you will need to create a personal access token and authorise the personal access token for use with SAML single sign-on: 

https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/creating-a-personal-access-token 
https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/authorizing-a-personal-access-token-for-use-with-saml-single-sign-on

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

### Index your host groups, luns and associated ldevs using the entry point script
# radmin index -I0 -s 53511