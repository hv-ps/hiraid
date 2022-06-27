import re

class StorageMaths:
    def __init__(self,capacity,denominator,decimals=2,format=None):
        self.capacity = int(capacity)
        self.denominator = denominator.upper()
        self.decimals = decimals
        self.format = format
  
    def setattrs(self):
        for attr in self.bit_table.keys():
            if isinstance(self.format,type):
                setattr(self,attr,self.format(getattr(self,attr)))
            elif callable(self.format):
                setattr(self,attr,self.format(getattr(self,attr),attr))
            elif self.format:
                setattr(self,attr,getattr(self,self.format)(getattr(self,attr),attr))

    def string(self,value,attr=None):
        return f"{value}"

    def word(self,value,attr=None):
        return f"{value} {attr}"

    
class StorageCapacity(StorageMaths):
    '''
    denominators and return attributes:\n
    bits, bytes, KB, MB, GB, TB, PB, blks\n
    format:\n
    set basic type: int, float, str
    int, float, string\n
    OR pass function(value,attr)\n
    OR call local function using string name\n
    '''
    def __init__(self,capacity,denominator,decimals=2,format=None):
        super().__init__(capacity,denominator,decimals=decimals,format=format)
        self.bit_table = {'BITS':1,'BYTES':8*(pow(1024,0)),'KB':8*(pow(1024,1)),'MB':8*(pow(1024,2)),'GB': 8*(pow(1024,3)),'TB': 8*(pow(1024,4)),'PB': 8*(pow(1024,5)),'BLK':8*(512)}
        self.BITS = int(self.capacity * self.bit_table[self.denominator])
        self.BYTES = int(self.BITS / self.bit_table['BYTES'])
        self.BLK = int(self.BITS / self.bit_table['BLK'])
        self.KB = round(self.BITS / self.bit_table['KB'],self.decimals)
        self.MB = round(self.BITS / self.bit_table['MB'],self.decimals)
        self.GB = round(self.BITS / self.bit_table['GB'],self.decimals)
        self.TB = round(self.BITS / self.bit_table['TB'],self.decimals)
        self.PB = round(self.BITS / self.bit_table['PB'],self.decimals)
        self.setattrs()

class Ldevid():
    '''
    Return ldevid in all possible formats\n
    culdev = Ldevid(1000).culdev\n
    decimal = Ldevid(1000).decimal
    '''
    def __init__(self,ldevid):
        self.ldevid = ldevid
        self.pattern = re.compile('\w{2}:\w{2}')
        self.convert()

    def convert(self):
        if self.pattern.match(str(self.ldevid)):
            self.culdev = self.ldevid
            self.decimal = int(self.ldevid.replace(':',''),16)
        else:
            self.decimal = self.ldevid
            self.culdev = format(int(self.ldevid), '02x')
            while len(self.culdev) < 4:
                self.culdev = "0" + self.culdev
            self.culdev = self.culdev[:2] + ":" + self.culdev[2:]
  