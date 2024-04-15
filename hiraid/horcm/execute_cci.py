    
from ..cmdview import Cmdview
import subprocess
import logging
import json
import ast
from hicciexceptions.cci_exceptions import *
from hicciexceptions.cci_exceptions import cci_exceptions_table
import re

def exception_string(cmdreturn):
    return json.dumps(ast.literal_eval(str(vars(cmdreturn))))

def return_cci_exception(cmdreturn):
    try:
        errorcode = re.match(r".*\[(.*?)\].*",cmdreturn.stderr,re.DOTALL).group(1)
        if cci_exceptions_table.get(errorcode,{}).get('return_value',99999) == cmdreturn.returncode:
            cmdreturn.cci_error = errorcode
            return cci_exceptions_table[errorcode]['Exception']
    except:
        cmdreturn.cci_error = 'Unknown'
        return Exception

def execute(cmd,log=logging,undocmds=[],acceptable_returns=[0],raise_err=True,**kwargs) -> object:
    cmdreturn = Cmdview(cmd=cmd)
    cmdreturn.expectedreturn = acceptable_returns
    log.info(f"Executing: {cmd}")
    log.debug(f"Acceptable return codes {acceptable_returns}")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
    cmdreturn.stdout, cmdreturn.stderr = proc.communicate()
    cmdreturn.returncode = proc.returncode
    cmdreturn.executed = True
    
    if proc.returncode and proc.returncode not in acceptable_returns:
        log.error("Return > "+str(proc.returncode))
        log.error("Stdout > "+cmdreturn.stdout)
        log.error("Stderr > "+cmdreturn.stderr)
        if raise_err and cmdreturn.returncode not in acceptable_returns:
            raise return_cci_exception(cmdreturn)(exception_string(cmdreturn))
    
    for undocmd in undocmds: 
        echo = f'echo "Executing: {undocmd}"'
        undocmds.insert(0,undocmd)
        undocmds.insert(0,echo)
        cmdreturn.undocmds.insert(0,undocmd)
    
    return cmdreturn
