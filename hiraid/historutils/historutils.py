#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Hitachi Vantara, Inc. All rights reserved.
# Author: Darren Chambers <@Darren-Chambers>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

import re
import argparse

version = "1.0.0"

class Storcapunits():
    '''
    capacity: Specify the capacity number to convert\n
    unit: Specify the unit of the capacity [ 'bits' | 'bytes' | 'KB' | 'MB' | 'GB' | 'TB' | 'PB' | 'BLK' ]\n
    decimals: Optionally enter the number of decimal places, default is 2\n\n
    e.g\n
    >>> cap = StorageCapacity(300,'blk')\n
    >>> vars(cap)\n
    {'BITS': 1228800, 'BYTES': 153600, 'BLK': 300, 'KB': 150.0, 'MB': 0.15, 'GB': 0.0, 'TB': 0.0, 'PB': 0.0}\n
    >>> cap.MB\n
    0.15\n
    '''
    bit_table = {'BITS':1,'BYTES':8*(pow(1024,0)),'KB':8*(pow(1024,1)),'MB':8*(pow(1024,2)),'GB': 8*(pow(1024,3)),'TB': 8*(pow(1024,4)),'PB': 8*(pow(1024,5)),'BLK':8*(512)}

    def __init__(self,capacity: int,unit: str,decimals: str=2, **kwargs) -> object:
        self.BITS = int(int(capacity) * self.bit_table[unit.upper()])
        self.BYTES = int(self.BITS / self.bit_table['BYTES'])
        self.BLK = int(self.BITS / self.bit_table['BLK'])
        self.KB = round(self.BITS / self.bit_table['KB'],int(decimals))
        self.MB = round(self.BITS / self.bit_table['MB'],int(decimals))
        self.GB = round(self.BITS / self.bit_table['GB'],int(decimals))
        self.TB = round(self.BITS / self.bit_table['TB'],int(decimals))
        self.PB = round(self.BITS / self.bit_table['PB'],int(decimals))


class Ldevid():
    '''
    Return ldevid in formats:\n
    culdev = Ldevid('1000').culdev\n
    decimal = Ldevid('1000').decimal\n
    hexldev = Ldevid('1000').hexldev\n
    >>> vars(Ldevid('1000').hexldev)\n
    '0x3e8'
    '''
    def __init__(self,ldev_id: str,**kwargs):
        self.ldev_id = ldev_id
        self.convert()

    def tohex(self):
        self.culdev = format(int(self.decimal), '02x')
        self.hexldev = f"0x{self.culdev}"
        while len(self.culdev) < 4:
            self.culdev = "0" + self.culdev
        self.culdev = self.culdev[:2] + ":" + self.culdev[2:]
    
    def convert(self):
        if re.match('\w{2}:\w{2}',str(self.ldev_id)):
            self.decimal = int(self.ldev_id.replace(':',''),16)
            self.tohex()
        elif re.match(r'0x[0-9A-F]+',str(self.ldev_id), re.I):
            self.decimal = int(self.ldev_id[2:],16)
            self.tohex()
        elif re.match(r'^\d{1,5}$',str(self.ldev_id)):
            self.decimal = int(self.ldev_id)
            self.tohex()
        else:
            raise Exception(f"Unknown ldev_id format '{self.ldev_id}'")

class Hitachi_id():

    ident = { "014":65536,"015":65536,"016":65536,"017":65536,"018":65536,"013":200000,"012":400000,"022":465536 }
    disallowed_port_letters = ['I','O']
    def __init__(self):
        pass

    def naacheck(self,naa: str) -> None:
        if not re.match(r'^naa\.[0-9A-F]{32}$',naa,re.I):
            raise Exception(f"Malformed naa: {naa}")

    def wwidcheck(self,wwid: str) -> None:
        if not re.match(r'^[0-9A-F]{33}$',wwid,re.I):
            raise Exception(f"Malformed naa: {wwid}")

    def wwncheck(self,wwn: str) -> None:
        if not re.match(r'^[0-9A-F]{16}$',wwn,re.I):
            raise Exception(f"Malformed naa: {wwn}")

    def wwndecode(self,wwn: str) -> None:
        self.wwncheck(wwn)
        identity = wwn[7:10]
        hexserial = wwn[10:14]
        serial = int(hexserial,16)+self.ident.get(identity,0)
        porthex = [re.sub(r'^0x','',hex(x)).lower() for x in range(16)]
        letters = [chr(i) for i in range(65, 83) if chr(i) not in self.disallowed_port_letters]
        portlookup = dict(zip(porthex,letters))
        cl = re.sub(r'^0x','',hex(int(str(wwn[-2]),16)+1))
        port = "CL{}-{}".format(cl,portlookup[str(wwn[-1].lower())])
        self.serial = serial
        self.port = port

    def virtual_storage_device(self,device: str,idslicestart: int,serialslicestart: int,sliceend: int) -> None:
        identity = device[idslicestart:serialslicestart]
        hexserial = device[serialslicestart:sliceend]
        self.serial = int(hexserial,16)+self.ident.get(identity,0)

    def storageLdevIdFromCanonicalName(self,naa: str) -> None:
        self.naacheck(naa)
        id = naa[-4:]
        self.culdev = id[:2]+':'+id[2:]
        self.hexldev = '0x'+id.lstrip('0')
        self.decimal = int(str(id),16)
    
    def esxi_nmp_virtual_storage_device_from_naa(self,naa: str) -> None:
        self.naacheck(naa)
        self.virtual_storage_device(naa,11,14,18)
        
    def linux_mp_virtual_storage_device_from_wwid(self,wwid: str) -> None:
        self.wwidcheck(wwid)
        self.virtual_storage_device(wwid,8,11,15)

class HiStorageFromVMwareNaa(Hitachi_id):
    '''
    naa = HiStorageFromVMwareNaa('naa.60060e8007c3e3000030c3e300000af1')\n
    Return object attributes:\n
    naa.decimal\n
    naa.hexldev\n
    naa.culdev\n
    naa.serial\n
    naa.naa\n
    >>> vars(HiStorageFromVMwareNaa('naa.60060e8007c3e3000030c3e300000af1'))\n
    {'naa': 'naa.60060e8007c3e3000030c3e300000af1', 'culdev': '0a:f1', 'hexldev': '0x0af1', 'decimal': 2801, 'serial': 50147}\n
    '''
    def __init__(self,naa,**kwargs):
        self.naa = naa
        self.storageLdevIdFromCanonicalName(naa)
        self.esxi_nmp_virtual_storage_device_from_naa(naa)

class HiStorageFromLinuxWwid(Hitachi_id):
    '''
    wwid = HiStorageFromLinuxWwid('360060e8007c3e3000030c3e30000027f')\n
    Return object attributes:\n
    wwid.serial\n
    wwid.wwid\n
    >>> vars(HiStorageFromLinuxWwid('360060e8007c3e3000030c3e30000027f'))\n
    {'wwid': '360060e8007c3e3000030c3e30000027f', 'serial': 50147}\n
    '''
    def __init__(self,wwid: str,**kwargs):
        self.wwid = wwid
        self.linux_mp_virtual_storage_device_from_wwid(wwid)

class HiWwnDecode(Hitachi_id):
    '''
    wwn = HiWwnDecode('50060e8007c3e370')\n
    Return object attributes:\n
    wwn.serial\n
    wwn.wwid\n
    wwn.port\n
    >>> vars(HiStorageFromLinuxWwid('50060e8007c3e370'))\n
    {'wwn': '50060e8007c3e370', 'serial': 50147, 'port': 'CL8-A'}\n
    '''
    def __init__(self,wwn: str,**kwargs):
        self.wwn = wwn
        self.wwndecode(wwn)


def main():
    parser = argparse.ArgumentParser(description=f"Some useful storage conversion utilities")
    parser.add_argument('--version', action='version', version=f"%(prog)s {version}")
    subparsers = parser.add_subparsers(help="Subcommands", dest="subcommand", required=True)
    
    parser_convertcap = subparsers.add_parser('caps', help="Convert storage capacity to popular units: blk, bits, bytes, kb, mb, gb, tb, pb")
    parser_convertcap.add_argument("-c", "--capacity", help="Specify the capacity you wish to convert", required=True)
    parser_convertcap.add_argument("-u", "--unit", choices=['blk','bits','bytes','kb','mb','gb','tb','pb'], help="Specify input unit", required=True)
    parser_convertcap.add_argument("-d", "--decimals", type=int, default=2, help="Specify decimal places", required=False)
    parser_convertcap.set_defaults(func=Storcapunits)

    parser_convertldevid = subparsers.add_parser('ldev', help="Return Hitachi ldev_id format { 'hexldev': '0x0', 'culdev': '00:00', 'decimal': 0 }")
    parser_convertldevid.add_argument("-l", "--ldev_id", help="Specify Hitachi storage ldev_id in hex, cu:ldev or decimal format", required=True)
    parser_convertldevid.set_defaults(func=Ldevid)

    parser_vmwarenaa = subparsers.add_parser('vmwarenaa', help="Return Hitachi storage information from VMware naa {'naa': 'naa.60060e8007c3e3000030c3e300000af1', 'culdev': '0a:f1', 'hexldev': '0x0af1', 'decimal': 2801, 'serial': 50147}")
    parser_vmwarenaa.add_argument("-n", "--naa", help="Specify vmware naa identifier e.g. naa.60060e8007c3e3000030c3e300000af1", required=True)
    parser_vmwarenaa.set_defaults(func=HiStorageFromVMwareNaa)

    parser_linuxwwid = subparsers.add_parser('linuxwwid', help="Return Hitachi storage information from Linux WWID {'wwid': '360060e8007c3e3000030c3e30000027f', 'serial': 50147}")
    parser_linuxwwid.add_argument("-w", "--wwid", help="Specify Linux multipath WWID e.g. 360060e8007c3e3000030c3e30000027f", required=True)
    parser_linuxwwid.set_defaults(func=HiStorageFromLinuxWwid)
    
    parser_wwndecode = subparsers.add_parser('wwndecode', help="Return Hitachi storage information Hitachi port wwn {'wwn': '50060e8007c3e370', 'serial': 50147, 'port': 'CL8-A'}")
    parser_wwndecode.add_argument("-w", "--wwn", help="Specify Hitachi storage wwn to decode e.g. 50060e8007c3e370", required=True)
    parser_wwndecode.set_defaults(func=HiWwnDecode)    

    args = parser.parse_args()
    print(vars(args.func(**vars(args))))

if __name__ == "__main__":
    main()
