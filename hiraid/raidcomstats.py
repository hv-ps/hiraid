#from .storage_utils import StorageCapacity
from .historutils.historutils import Storcapunits

class Raidcomstats():
    def __init__(self,raidcom,log):
        self.log = log
        self.raidcom = raidcom
        self.views = self.raidcom.views
        self.stats = self.raidcom.stats
        self.updateview = self.raidcom.updateview

    def portcounters(self) -> None:
        portcounters = { 'portcounters':{'portcount':len(self.views.get('_ports',{}))} }
        self.raidcom.updateview(self.stats,portcounters)

    def hostgroupcounters(self) -> None:
        hgcounters = { 'ports':{}, 'hostGroupsTotal':0 }
        for port in self.raidcom.views.get('_ports',{}):
            hgcounters['ports'][port] = { 'hostgroups':len(self.views['_ports'][port].get('_GIDS',{}).keys()) }
            hgcounters['hostGroupsTotal'] += hgcounters['ports'][port]['hostgroups']
        self.raidcom.updateview(self.raidcom.stats,{'portcounters':hgcounters})

    def ldevcounts(self) -> None:

        def ldev_sum(ldev_view,stats_root):
            for ldevid in ldev_view:
                stats_root['VOL_Capacity(BLK)'] += int(ldev_view[ldevid].get('VOL_Capacity(BLK)',0))
                stats_root['Used_Block(BLK)'] += int(ldev_view[ldevid].get('Used_Block(BLK)',0))
            vol_capacity = Storcapunits(stats_root['VOL_Capacity(BLK)'],'blk')
            used_capacity = Storcapunits(stats_root['Used_Block(BLK)'],'blk')
            for denom in ['MB','GB','TB','PB']:
                stats_root[f'VOL_Capacity({denom})'] = getattr(vol_capacity,denom)
                stats_root[f'Used_Block({denom})'] = getattr(used_capacity,denom)

        self.stats['ldevcounters'] = {}
        self.stats['ldevcounters']['_ldevs'] = {'VOL_Capacity(BLK)':0, 'Used_Block(BLK)':0, 'ldevcount':len(self.views.get('_ldevs',{})) }
        stats_root = self.stats['ldevcounters']['_ldevs']
        ldev_view = self.views.get('_ldevs',{})
        ldev_sum(ldev_view,stats_root)

        for ldevlist in self.views.get('_ldevlist',{}):
            self.stats['ldevcounters'][ldevlist] = {'VOL_Capacity(BLK)':0, 'Used_Block(BLK)':0, 'ldevcount':len(self.views['_ldevlist'][ldevlist])}
            stats_root = self.stats['ldevcounters'][ldevlist]
            ldev_view = self.views['_ldevlist'][ldevlist]
            ldev_sum(ldev_view,stats_root)

    def portlogincounters(self) -> None:
        portlogins = { 'ports':{}, 'portLoginsTotal':0 }
        wwns = set()
        for port in self.views.get('_ports',{}):
            portlogins['ports'][port] = { 'portlogins':len(self.views['_ports'][port].get('_PORT_LOGINS',[])) }
            wwns.update(self.views['_ports'][port].get('_PORT_LOGINS',[]))
            portlogins['portLoginsTotal'] += portlogins['ports'][port]['portlogins']
        portlogins['uniquePortLoginsTotal'] = len(wwns)
        self.updateview(self.stats,{'portcounters':portlogins})

    def hbawwncounters(self) -> None:
        hbawwncount = { 'ports':{}, 'hbaWwnTotal':0 }
        wwns = set()
        for port in self.views.get('_ports',{}):
            hbawwncount['ports'][port] = { 'hbaWwnCount': 0 }
            for gid in self.views['_ports'][port].get('_GIDS',{}):
                hbawwncount['ports'][port]['hbaWwnCount'] += len(self.views['_ports'][port]['_GIDS'][gid].get('_WWNS',[]))
                wwns.update([wwn.lower() for wwn in self.views['_ports'][port]['_GIDS'][gid].get('_WWNS',{}).keys()])
        hbawwncount['hbaWwnTotal'] += len(wwns)
        self.updateview(self.stats,{'portcounters':hbawwncount})
    
    def luncounters(self) -> None:
        luncounters = { 'ports':{}, 'lunsTotal':0 }
        for port in self.views.get('_ports',{}):
            luncounters['ports'][port] = { 'lunCount': 0 }
            for gid in self.views['_ports'][port].get('_GIDS',{}):
                luncounters['ports'][port]['lunCount'] += len(self.views['_ports'][port]['_GIDS'][gid].get('_LUNS',{}).keys())
            luncounters['lunsTotal'] += luncounters['ports'][port]['lunCount']
        self.updateview(self.stats,{'portcounters': luncounters})

    def poolcounters(self) -> None:
        counters = { 'pools': len(self.views['_pools']) }        
        for poolid in self.views['_pools']:
            pt = self.views['_pools'][poolid].get('PT')
            if pt:
                counters['pool_types'] = counters.get('pool_types',{})
                counters['pool_types'][pt] = counters['pool_types'].get(pt,{ 'count':0, 'Available(MB)':0, 'Capacity(MB)':0 })
                counters['pool_types'][pt]['count'] += 1
                #counters['pool_types'][pt]['Available(MB)'] += int(self.views['_pools'][poolid]['Available(MB)'])
                #counters['pool_types'][pt]['Capacity(MB)'] += int(self.views['_pools'][poolid]['Capacity(MB)'])
            if self.views['_pools'][poolid].get('Available(MB)'):
                for cap in ['Available(MB)','Capacity(MB)']:
                    counters[cap] = counters.get(cap,0)
                    counters[cap] += int(self.views['_pools'][poolid][cap])
        self.updateview(self.stats,{'poolcounters': counters})        
