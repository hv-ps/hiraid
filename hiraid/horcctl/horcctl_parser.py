import logging 

class Horcctl_parser():

    def __init__(self,horcctl,log=logging):
        self.log = log
        self.horcctl = horcctl

    def initload(self,cmdreturn,header='',keys=[]):

        cmdreturn.rawdata = [row.strip() for row in list(filter(None,cmdreturn.stdout.split('\n')))]
        cmdreturn.headers = []
        if not cmdreturn.rawdata:
            return
        else:
            cmdreturn.header = cmdreturn.rawdata.pop(0)
            cmdreturn.headers = cmdreturn.header.split()

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

    def showControlDeviceOfHorcm(self, cmdreturn: object, unitid: int):
        # Current control device = \\.\IPCMD-172.16.167.13-31001
        cmdreturn.rawdata = [row.strip() for row in list(filter(None,cmdreturn.stdout.split('\n')))]
        cmdreturn.data = [{"unitid":unitid, "current_control_device": row.split('=')[1].strip(), "instance": self.horcctl.instance } for row in cmdreturn.rawdata]
        
        def createview(cmdreturn):
            #print(cmdreturn.data)
            for datadict in cmdreturn.data:
                cmdreturn.view[datadict['instance']] = datadict
        
        createview(cmdreturn)
        return cmdreturn


