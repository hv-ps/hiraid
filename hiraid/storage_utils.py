import re
    
class StorageCapacity():
    '''
    capacity\n
    denominator [ 'bits' | 'bytes' | 'KB' | 'MB' | 'GB' | 'TB' | 'PB' | 'BLK' ]\n
    decimals\n\n
    e.g\n
    >>> cap = StorageCapacity(300,'blk')\n
    >>> vars(cap)\n
    {'BITS': 1228800, 'BYTES': 153600, 'BLK': 300, 'KB': 150.0, 'MB': 0.15, 'GB': 0.0, 'TB': 0.0, 'PB': 0.0}\n
    >>> cap.MB\n
    0.15\n
    '''
    bit_table = {'BITS':1,'BYTES':8*(pow(1024,0)),'KB':8*(pow(1024,1)),'MB':8*(pow(1024,2)),'GB': 8*(pow(1024,3)),'TB': 8*(pow(1024,4)),'PB': 8*(pow(1024,5)),'BLK':8*(512)}

    def __init__(self,capacity: int,denominator: str,decimals: int=2) -> object:
        self.BITS = int(int(capacity) * self.bit_table[denominator.upper()])
        self.BYTES = int(self.BITS / self.bit_table['BYTES'])
        self.BLK = int(self.BITS / self.bit_table['BLK'])
        self.KB = round(self.BITS / self.bit_table['KB'],decimals)
        self.MB = round(self.BITS / self.bit_table['MB'],decimals)
        self.GB = round(self.BITS / self.bit_table['GB'],decimals)
        self.TB = round(self.BITS / self.bit_table['TB'],decimals)
        self.PB = round(self.BITS / self.bit_table['PB'],decimals)


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
  