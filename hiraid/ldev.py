

import logging

class ldev():
    '''Shortcuts'''
    def __init__(self,raidcom,view,log=logging):
        self.raidcom = raidcom
        self.ldev_id = view['LDEV']
        self.virtual_ldev_id = view.get('VIR_LDEV',self.ldev_id)

    def unmap(self,):
        self.raidcom.unmapldev(ldev_id=self.ldev_id,virtual_ldev_id=self.virtual_ldev_id)

    