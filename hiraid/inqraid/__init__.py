import os
import logging
from ..execute_cci import execute as cci_execute
from .inqraid_parser import Inqraid_parser

class Inqraid():
    view = []
    def __init__(self,path=('/HORCM/usr/bin/','C:\\HORCM\etc\\')[os.name=='nt'],log=logging,raise_err=True):
        self.path = path
        self.log = log
        self.parser = Inqraid_parser(log=self.log)
        self.raise_err = raise_err

    def inqraidCli(self, acceptable_returns:list=[0], **kwargs) -> object:
        cmd = f'ls /dev/sd* | {self.path}inqraid -CLI'
        cmdreturn = cci_execute(cmd,log=self.log,acceptable_returns=acceptable_returns,raise_err=self.raise_err)
        if cmdreturn.returncode in acceptable_returns:
            self.parser.inqraidCli(cmdreturn)
        self.__class__.view.append(cmdreturn)
        return cmdreturn

