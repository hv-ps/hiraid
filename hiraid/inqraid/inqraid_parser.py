import logging 

class Inqraid_parser():

    def __init__(self,log=logging):
        self.log = log

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

    def inqraidCli(self, cmdreturn: object, datafilter: dict={}, **kwargs ) -> object:
        self.initload(cmdreturn)
        
        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                device_file = datadict['DEVICE_FILE']
                cmdreturn.view[device_file] = datadict

        prefilter = []
        for line in cmdreturn.rawdata:
            row = line.split(maxsplit=9)
            prefilter.append(dict(zip(cmdreturn.headers, row)))
        
        cmdreturn.data = list(filter(lambda r: self.applyfilter(r,datafilter),prefilter))
        createview(cmdreturn)
        return cmdreturn


