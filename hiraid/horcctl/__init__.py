
import logging
from ..execute_cci import execute as cci_execute
from .horcctl_parser import Horcctl_parser

class Horcctl():
    def __init__(self,instance,path="/usr/bin/",log=logging,raise_err=True):
        self.instance = instance
        self.path = path
        self.log = log
        self.parser = Horcctl_parser(self,log=self.log)
        self.raise_err = raise_err

    def showControlDeviceOfHorcm(self, unitid: int, acceptable_returns:list=[0], **kwargs) -> object:
        cmd = f'{self.path}horcctl -D -I{self.instance} -u {unitid}'
        cmdreturn = cci_execute(cmd,log=self.log,acceptable_returns=acceptable_returns,raise_err=self.raise_err)
        if cmdreturn.returncode in acceptable_returns:
            self.parser.showControlDeviceOfHorcm(cmdreturn,unitid)
        
        return cmdreturn

