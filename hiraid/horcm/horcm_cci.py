
import os
import re
import json
import time
import copy
import logging
import subprocess
from glob import glob
from string import Template
from datetime import datetime
from ..historutils.historutils import Storcapunits as storagecaps
from ..cmdview import Cmdview



try:
    from .horcm_template import default_template
except:
    from horcm_template import default_template

class cd:
    '''Context manager for changing to and returning from the current working directory'''
    def __init__(self, newPath):
        self.newPath = os.path.expanduser(newPath)
    
    def __enter__(self):
        self.savedPath = os.getcwd()
        os.chdir(self.newPath)
        
    def __exit__(self, etype, value, traceback):
        os.chdir(self.savedPath)

class Cci():
    '''
    Create new horcm files using the next available horcm partner instance numbers.\n
    log: python logger\n
    base_service_port: if no instance is passed into create_horcms, free horcm inst numbers are located and horcm.service = base_service_port + located instance. Default is 11000, therefore if instance 1 is available horcm.service = 11001.\n
    horcm_dir: Location of working horcm directory.\n
    start: Starting instance number to search from. default = 0\n
    end: Ending instance number to search up to. default = 500\n
    local_inst: 'odd' | 'even' - Specify if you prefer the local horcm instance to be an even number ( default ) or an odd number.\n
    path: horcm binary path default = '/usr/bin'\n
    cciextension: '.sh' ( default ) | '.exe' ( windows )\n
    horcm_template_file: Use an alternate file as your horcm template rather than using the default_template.\n
    Add this horcm and see it break!! /etc/horcm21_tmp.conf
    '''
    def __init__(self,log=logging,base_service_port: int=11000,horcm_dir: str='/etc',start: int=0,end: int=500,local_inst: str='even',path: str='/usr/bin/',cciextension: str='.sh',horcm_template_file: str=None,):
        
        self.log = log
        self.horcm_template_file = horcm_template_file
        self.horcm_dir = horcm_dir
        self.base_service_port=base_service_port
        self.start = start
        self.end = end
        self.find_free_horcm_partners(self.start,self.end)
        self.local_inst = local_inst
        self.poll = -1
        self.ip_address = "localhost"
        self.remote_ip_address = "localhost"
        self.timeout = 3000
        self.path = path
        self.cciextension = cciextension
        self.undocmds = []

    def now(self,format='%d-%m-%Y_%H.%M.%S'):
        return datetime.now().strftime(format)
    
    def raidqry(self, inst: int):
        cmd = '{}raidqry -l -I{}'.format(self.path,inst)
        stdout, stderr, cmdreturn = self.execute(cmd)
        cmdreturn = Cmdview("raidqry")
        cmdreturn.rawdata = stdout
        self.parse_raidqry(cmdreturn)
        return cmdreturn


    def parse_raidqry(self,cmdreturn):
        
        rawdata = [row.strip() for row in list(filter(None,cmdreturn.rawdata.split('\n')))]
        header = rawdata.pop(0)
        cmdreturn.headers = header.split()

        def createview(cmdreturn):
            for datadict in cmdreturn.data:
                serial = datadict['Serial#']
                cmdreturn.view[serial] = datadict

        for line in rawdata:
            row = line.split()
            cmdreturn.data.append(dict(zip(cmdreturn.headers, row)))

        createview(cmdreturn)
        return cmdreturn

    def XXXXparse_pairdisplay(self,pairdisplay: list) -> dict:
        '''
        Returns dictionary of parsed pairdisplay:
        { Group: { PairVol: { L/R: { heading:data } } } }
        '''
        headings = pairdisplay.pop(0).split()
        view = { 'pairs': {} }
        for line in pairdisplay:
            sline = line.split()
            if len(sline) != len(headings): raise("header and data length mismatch")
            data = {head:item for item,head in zip(sline,headings)}
            view['pairs'][data['Group']] = view['pairs'].get(data['Group'],{})
            view['pairs'][data['Group']][data['PairVol']] = view['pairs'][data['Group']].get(data['PairVol'],{})
            view['pairs'][data['Group']][data['PairVol']][data['L/R']] = data

        return view



    def return_used_horcm_insts(self):
        with cd(self.horcm_dir) as horcm_dir:
            horcm_files = glob('horcm[0-9]*.conf')
        self.used_insts = sorted([ int(horcm.strip().replace('horcm','').replace('.conf','')) for horcm in horcm_files])
        return self.used_insts

    def find_free_horcm_partners(self,start: int=0, end: int=500, local_inst: str='even') -> list:
        '''
        start: Specify horcm instance range start
        end: Specify horcm instance range end
        local_inst: odd | even. Default is even giving (0,1),(2,3),(4,5) odd gives (1,2),(3,4),(5,6)
        '''
        if ((start % 2) != 0 and local_inst == 'even') or ((start % 2) == 0 and local_inst == 'odd'): start += 1
        horcm_range = [(inst,inst+1) for inst in range(start, end, 2)]
        self.return_used_horcm_insts()
        self.free = [horcm_partners for horcm_partners in horcm_range if horcm_partners[0] not in self.used_insts and horcm_partners[1] not in self.used_insts]
        #self.log.info(f"Found free horcm partner instances: {self.free}")
        self.log.info(f"Found {len(self.free)} horcm partner instances")
        return self.free

    def show_free_horcms(self):
        self.log.info(self.free)
        
    def check_service_port(self,service):
        pass

    def check_instance(self,instance):
        pass

    def checks(self,horcm_dict: dict):
        allowed_keys = ('local','remote')
        mutual_keys = ('service','instance')

        def checkinput():
            
            for key in allowed_keys:
                if key not in horcm_dict:
                    raise Exception(f"{key} is not present in horcm dict.")
            for key in horcm_dict.keys():
                if key not in allowed_keys:
                    raise Exception(f"Unknown horcm locality '{key}', possible values are {allowed_keys}")

        self.log.info(f"Horcm_dict: {horcm_dict}")
        checkinput()


    def create_horcms(self, horcm_dict: dict):
        '''
        horcm_dict {
            "local" : { "service": 11000,\n"instance": 0,\n"HORCM_CMD": ["\\.\CMD-350147:/dev/sd"]\n, "HORCM_LDEVG":["copy_grp\tdevice_grp\tserial"]\n },\n
            "remote": { "service": 11001,\n"instance": 1,\n"HORCM_CMD": ["\\.\CMD-358149:/dev/sd"]\n, "HORCM_LDEVG":["copy_grp\tdevice_grp\tserial"]\n }\n
            }\n
        If instance is not specified, the next available instances are created.\n
        If service is not specified ( udp port ), one is generated from the base_service_port and next free horcm. Pass both of these together or neither if you care that the numbers relate to one another.\n

        Returns: horcm_dict along with instance and service numbers
        '''
        self.checks(horcm_dict)
        if horcm_dict['local'].get('instance') is None:
            horcm_dict['local']['instance'] = self.free[0][0]

        if horcm_dict['remote'].get('instance') is None:
            horcm_dict['remote']['instance'] = self.free[0][1]

        if horcm_dict['local'].get('service') is None:
            horcm_dict['local']['service'] = int(self.base_service_port)+int(self.free[0][0])

        if horcm_dict['remote'].get('service') is None:
            horcm_dict['remote']['service'] = int(self.base_service_port)+int(self.free[0][1])

        if horcm_dict['local'].get('service_dest_port') is None:
            horcm_dict['local']['service_dest_port'] = horcm_dict['remote']['service']

        if horcm_dict['remote'].get('service_dest_port') is None:
            horcm_dict['remote']['service_dest_port'] = horcm_dict['local']['service']

        for horcm_locality in horcm_dict:
            self.create_horcm(horcm_dict[horcm_locality])
        
        return horcm_dict

    def create_minimal_horcm(self,horcm_detail: dict):
        horcm_instance = horcm_detail['instance']
        detail = {
            'date': self.now(),
            'instance': horcm_detail['instance'],
            'HORCM_CMD': '\n'.join(horcm_detail['HORCM_CMD'])
        }
        try:
            from .horcm_template import minimal_template
        except:
            from horcm_template import minimal_template
        horcm_template = Template(minimal_template)
        
        horcm_content = horcm_template.substitute(detail)
        horcm_file = f"{self.horcm_dir}{os.sep}horcm{horcm_instance}.conf"
        self.backupfile(horcm_file)
        self.writehorcmfile(horcm_file,horcm_content)

    def create_horcm(self,horcm_detail: dict):
        horcm_instance = horcm_detail['instance']
        horcm_ldev_type = ('HORCM_LDEV','HORCM_LDEVG')['HORCM_LDEVG' in horcm_detail]
        remote_ip_address = (self.remote_ip_address,horcm_detail.get('remote_ip_address'))['remote_ip_address' in horcm_detail]
        HORCM_INST_groups = {horcm_grp.split()[0] for horcm_grp in horcm_detail[horcm_ldev_type]}
        HORCM_INST_LIST = [f"{horcm_grp}\t{remote_ip_address}\t{horcm_detail['service_dest_port']}" for horcm_grp in HORCM_INST_groups]
        HORCM_INST = '\n'.join(HORCM_INST_LIST)

        detail = {
            'ip_address':(self.ip_address,horcm_detail.get('ip_address'))[horcm_detail.get('ip_address') is not None],
            'service': horcm_detail['service'],
            'HORCM_CMD': '\n'.join(horcm_detail['HORCM_CMD']),
            'HORCM_LDEV_TYPE': horcm_ldev_type,
            'HORCM_LDEV': '\n'.join(horcm_detail[horcm_ldev_type]),
            'poll': (self.poll,horcm_detail.get('poll'))[horcm_detail.get('poll') is not None],
            'timeout': (self.timeout,horcm_detail.get('timeout'))[horcm_detail.get('timeout') is not None],
            'HORCM_INST': HORCM_INST,
            'site': horcm_detail['site'],
            'date': horcm_detail['date'],
            'instance': horcm_instance
        }

        try:
            horcm_template = Template(open(self.horcm_template_file).read())
        except:
            horcm_template = Template(default_template)

        horcm_content = horcm_template.substitute(detail)
        horcm_file = f"{self.horcm_dir}{os.sep}horcm{horcm_instance}.conf"
        self.backupfile(horcm_file)
        self.writehorcmfile(horcm_file,horcm_content)
    
    def writehorcmfile(self,horcmfile,content):
        try:
            self.log.info('Writing horcm file {}'.format(horcmfile))
            file = open(horcmfile,"w")
            file.write(content)
        except Exception as e:
            raise Exception('Unable to {}, error \'{}\''.format('writehorcmfile',str(e)))

    def backupfile(self,fqfile,prepend='',append=''):
        ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
        separator = ('/','\\')[os.name=='nt']
        fqfilebackup = '{}{}{}{}{}.{}{}'.format(('/','')[os.name=='nt'],separator.join(fqfile.split(separator)[(1,0)[os.name=='nt']:-1]),separator,prepend,fqfile.split(separator)[-1],ts,append)
        try:
            os.rename(fqfile,fqfilebackup)
            self.log.info('Backed up file {} to {}'.format(fqfile,fqfilebackup))
        except FileNotFoundError:
            self.log.warn('File does not exist \'{}\', backup not required'.format(fqfile))
        except Exception as e:
            raise Exception('Unable to backup files \'{}\''.format(e))

    def horcmshutdown(self,inst):
        self.log.info(f'Shutdown horcm instance {inst}')
        cmd = f'{self.path}horcmshutdown{self.cciextension} {inst}'
        return self.nexecute(cmd)
    
    def horcmstart(self,inst):
        self.log.info(f'Start horcm instance {inst}')
        cmd = f'{self.path}horcmstart{self.cciextension} {inst}'
        return self.nexecute(cmd)
    
    def removehorcmfile(self,inst):
        self.log.info(f'Remove horcm file {self.horcm_dir}{os.sep}horcm{inst}.conf')
        os.remove(f'{self.horcm_dir}{os.sep}horcm{inst}.conf')

    def nexecute(self,cmd,undocmds=[],acceptable_returns=[0],**kwargs) -> object:

        cmdreturn = Cmdview(cmd=cmd)
        cmdreturn.expectedreturn = acceptable_returns

        self.log.info(f"Executing: {cmd}")
        self.log.debug(f"Acceptable return codes {acceptable_returns}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        cmdreturn.stdout, cmdreturn.stderr = proc.communicate()
        cmdreturn.returncode = proc.returncode
        cmdreturn.executed = True
        
        if proc.returncode and proc.returncode not in acceptable_returns:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+cmdreturn.stdout)
            self.log.error("Stderr > "+cmdreturn.stderr)
            message = {'return':proc.returncode,'stdout':cmdreturn.stdout, 'stderr':cmdreturn.stderr }
            raise Exception(f"Unable to execute Command '{cmd}'. Command dump > {message}")
        
        for undocmd in undocmds: 
            echo = f'echo "Executing: {undocmd}"'
            self.undocmds.insert(0,undocmd)
            self.undocmds.insert(0,echo)
            cmdreturn.undocmds.insert(0,undocmd)
        
        return cmdreturn
    '''
    def execute(self,cmd,expectedreturn=0):
        self.log.info(f"Executing: {cmd}")
        self.log.debug(f"Expecting return code {expectedreturn}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = proc.communicate()
        self.log.info(f"Return Code: {proc.returncode}")
  
        if proc.returncode and proc.returncode != expectedreturn and expectedreturn is not None:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+stdout.strip())
            self.log.error("Stderr > "+stderr.strip())
            message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
            raise Exception('Unable to execute Command "{}". Command dump > {}'.format(cmd,message))
    
        return stdout, stderr, proc.returncode
    '''

    def restart_horcm_inst(self,inst):
        
        self.log.info('Restarting horcm instance {}'.format(inst))
        cmd = '{}horcmshutdown{} {}'.format(self.path,self.cciextension,inst)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = proc.communicate()
        if proc.returncode:
            if re.search(r'Can\'t be attached to HORC manager',stderr):
                self.log.warn('OK - Looks like horcm inst {} is not running.'.format(inst))
            else:
                self.log.error("Return > "+str(proc.returncode))
                self.log.error("Stdout > "+stdout)
                self.log.error("Stderr > "+stderr)
                message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
                raise Exception('Unable to shutdown horcm inst: {}. Command dump > {}'.format(cmd,message))
                
        # Now start the instance
        time.sleep(2)
        cmd = '{}horcmstart{} {}'.format(self.path,self.cciextension,inst)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = proc.communicate()
        if proc.returncode:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+stdout)
            self.log.error("Stderr > "+stderr)
            message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
            raise Exception('Unable to start horcm inst: {}. Command dump > {}'.format(cmd,message))

    def pairdisplay_include_capacities(self,pairdisplay_dict: dict,volume_capacities: dict) -> dict:
        #'totalgb': 0, 'totalrepgb':0
        if volume_capacities.keys():
            pairdisplay_dict['totalgb'] = 0
            pairdisplay_dict['totalrepgb'] = 0
            for group in pairdisplay_dict['pairs']:
                for pairvol in pairdisplay_dict['pairs'][group]:                    
                    ldev_id = str(pairdisplay_dict['pairs'][group][pairvol]['L']['LDEV#'])
                    percent = (0,pairdisplay_dict['pairs'][group][pairvol]['L']['%'])[pairdisplay_dict['pairs'][group][pairvol]['L']['%'] != "-"]
                    serial = int(pairdisplay_dict['pairs'][group][pairvol]['L']['Seq#'])

                    #try:
                    pairdisplay_dict['pairs'][group][pairvol]['R']['GB'] = "-"
                    pairdisplay_dict['pairs'][group][pairvol]['R']['REPGB'] = "-"
                    pairdisplay_dict['pairs'][group][pairvol]['L']['GB'] = round(storagecaps(int(volume_capacities[serial][ldev_id]),'blk').GB,2)
                    pairdisplay_dict['pairs'][group][pairvol]['L']['REPGB'] = round(storagecaps((int(volume_capacities[serial][ldev_id])/100)*int(percent),'blk').GB,2)
                    pairdisplay_dict['totalgb'] += round(storagecaps(int(volume_capacities[serial][ldev_id]),'blk').GB,2)
                    pairdisplay_dict['totalrepgb'] += round(storagecaps((int(volume_capacities[serial][ldev_id])/100)*int(percent),'blk').GB,2)
                    #except Exception as e:
                    #    self.log.error(f"Capacity issue :{e}")

            if pairdisplay_dict['totalgb']:
                pairdisplay_dict['totalgb'] = round(pairdisplay_dict['totalgb'],2)
                try:
                    pairdisplay_dict['totalrepgb'] = round(pairdisplay_dict['totalrepgb'],2)
                except:
                    pass
                
    def paircreate(self, inst: int, group: str, mode='', quorum='', jp='', js='', fence='', copy_pace=15):
        undocmd = []
        modifier = ''
        if re.search(r'\d',str(quorum)):
            modifier = '-jq {}'.format(quorum)
            undocmd.insert(0,'{}pairsplit -g {} -I{}{}'.format(self.path,group,mode,inst))
            undocmd.insert(0,'{}pairsplit -g {} -I{}{} -S'.format(self.path,group,mode,inst))

        if re.search(r'\d',str(jp)) and re.search(r'\d',str(js)):
            modifier = '-jp {} -js {}'.format(jp,js)
            undocmd.insert(0,'{}pairsplit -g {} -I{}{} -S'.format(self.path,group,mode,inst))

        cmd = '{}paircreate -g {} -vl {} -f {} -c {} -I{}{}'.format(self.path,group,modifier,fence,copy_pace,mode,inst)
        
        stdout, stderr, cmdreturn = self.execute(cmd)
        return { 'stdout':stdout, 'stderr':stderr, 'cmdreturn':cmdreturn }

    def pairdisplayx(self,inst: int,group: str,mode='',opts='',header=True,volume_capacities: dict={},print_pairdisplay=True) -> dict:
        '''
        group: Horcm_group
        mode: None|TC|SI
        opts: e.g. -fe ( -fce is always applied )
        header: True ( default ) | False Return pairdisplay list with header or not
        volume_capacities: { 'serial': {'ldev_id_decimal':'capacity_blks' } }
        print_pairdisplay: True ( default ) | False
        '''
        pairdisplayout = None
        cmd = '{}pairdisplay -g {} -I{}{} {} -fce -CLI'.format(self.path,group,mode,inst,opts)
        stdout, stderr, cmdreturn = self.execute(cmd)
        pairdisplayout = [row.strip() for row in list(filter(None,stdout.split('\n')))]
        pairdisplaydata = self.parse_pairdisplay(pairdisplayout)
        self.pairdisplay_include_capacities(pairdisplaydata,volume_capacities)
        pairdisplayx = self.print_pairdisplay(pairdisplaydata,print_pairdisplay=print_pairdisplay)

        return { 'stdout':stdout, 'stderr':stderr, 'cmdreturn':cmdreturn, 'pairdisplay':(pairdisplayout[1:],pairdisplayout)[header], 'pairdisplayx':(pairdisplayx[1:],pairdisplayx)[header], 'pairdisplaydata':pairdisplaydata }

    def print_pairdisplay(self,pairdisplay_dict: dict,print_pairdisplay: bool=True) -> list:
        col_widths = []
        rows = []
        for g in pairdisplay_dict['pairs']:
            for d in pairdisplay_dict['pairs'][g]:
                for p in pairdisplay_dict['pairs'][g][d]:
                    header = tuple(pairdisplay_dict['pairs'][g][d][p].keys())
                    rows.append(tuple(pairdisplay_dict['pairs'][g][d][p].values()))

        footer = [''] * len(header)
        if pairdisplay_dict.get('totalgb'):
            footer[-1] = pairdisplay_dict['totalrepgb']
            footer[-2] = pairdisplay_dict['totalgb']

        rows.insert(0,header)
        rows.append(footer)
        for column in zip(*rows):
            col_widths.append(max([len(str(v)) for v in column]))
        formats = " ".join(["{:<" + str(l) + "}" for l in col_widths])
        
        for row in rows:
            self.log.debug(formats.format(*row))
        for row in rows:
            if print_pairdisplay: print(formats.format(*row))
            
        return rows

    def parse_pairdisplay(self,pairdisplay: list) -> dict:
        '''
        Returns dictionary of parsed pairdisplay:
        { Group: { PairVol: { L/R: { heading:data } } } }
        '''
        headings = pairdisplay.pop(0).split()
        view = { 'pairs': {} }
        for line in pairdisplay:
            sline = line.split()
            if len(sline) != len(headings): raise("header and data length mismatch")
            data = {head:item for item,head in zip(sline,headings)}
            view['pairs'][data['Group']] = view['pairs'].get(data['Group'],{})
            view['pairs'][data['Group']][data['PairVol']] = view['pairs'][data['Group']].get(data['PairVol'],{})
            view['pairs'][data['Group']][data['PairVol']][data['L/R']] = data

        return view
        

    def pairkeychk(self,inst: int, group: str, mode: str='', **keywargs):
        returns = 0
        messages = []
        pairdisplaydata = self.pairdisplayx(inst=inst,group=group,mode=mode)['pairdisplaydata']
        for group in pairdisplaydata['pairs']:
            for pairvol in pairdisplaydata['pairs'][group]:
                try:
                    for key in keywargs:
                        for LR in ('L','R'):
                            if keywargs[key] != pairdisplaydata['pairs'][group][pairvol][LR][key]:
                                returns = 1
                                messages.append(f"Group: '{group}' PairVol: '{pairvol}' required key: '{key}' value: '{keywargs[key]}' does not match pairdisplay ({LR}) pair key value '{pairdisplaydata['pairs'][group][pairvol][LR][key]}'")
                except KeyError as e:
                    returns = 2
                    messages.append(f"KeyError: {e}")
                    
        return { 'return':returns, 'messages':messages }
                        

    def pairmonitor(self, inst: int, groups: str, mode: str='', considered_complete_percent: int=100, acceptable_states: list=[], interval_seconds: int=40, timeout_seconds=8000,volume_capacities: dict={}):
        '''
        Normally pairevtwait and pairvolchk would be tools of choice for checking pair status but there are situations where the pairs can be in differing states.
        Take GAD-on-GAD migration for example. Some pairs have to be in COPY but some of the migrating volumes might be in PAIR because they are not in GAD at source.
        acceptable_states: list=['PAIR','COPY']
        '''
        completed = False
        iterations = int(timeout_seconds / interval_seconds)
        iterated = 0

        while not completed and iterated < iterations:
            all_complete = True
            if iterated and not completed: time.sleep(interval_seconds)
            for group in groups:
                pairdisplay = self.pairdisplayx(inst=inst,group=group,mode=mode,volume_capacities=volume_capacities)
                for group in pairdisplay['pairdisplaydata']['pairs']:
                    for pairvol in pairdisplay['pairdisplaydata']['pairs'][group]:
                        
                        data = pairdisplay['pairdisplaydata']['pairs'][group][pairvol]['L']
                        local_percentage_pass = False
                        accept_state = False
                        percent = (0,pairdisplay['pairdisplaydata']['pairs'][group][pairvol]['L']['%'])[data['%'] != "-"]
                        if int(percent) >= int(considered_complete_percent):
                            pairdisplay['pairdisplaydata']['pairs'][data['Group']][data['PairVol']][data['L/R']]['percent_completed'] = True
                        else:
                            all_complete = False
                            
                        if len(acceptable_states):    
                            for local_remote in pairdisplay['pairdisplaydata']['pairs'][group][pairvol]:
                                if pairdisplay['pairdisplaydata']['pairs'][group][pairvol][local_remote]['Status'] in acceptable_states:
                                    pairdisplay['pairdisplaydata']['pairs'][data['Group']][data['PairVol']][data['L/R']]['acceptable_state'] = True
                                else:
                                    all_complete = False                         
            completed = all_complete
            print(f"Iteration: {iterated+1} of {iterations} completed {completed}\n")
            self.log.info(f"Iteration: {iterated+1} of {iterations}")
            iterated += 1
        
        return completed

    def pairvolchk(self, inst: int, group: str, expectedreturn: int, device: str=None, opts: str='') -> dict:
        '''
        inst: horcm_inst
        group: horcm group
        expectedreturn: Check for this return from pairvolchk, usually 23 P-VOL pair or 33 S-VOL pair
        device: Optionally pass an individual device
        opts: Pass options such as -c to check remote end
        '''
        check_device = ''
        if device:
            check_device = f'-d {device}'
        cmd = '{}pairvolchk -g {} {} -I{} -ss {}'.format(self.path,group,check_device,inst,opts)
        stdout, stderr, cmdreturn = self.execute(cmd,expectedreturn=expectedreturn)
        return  { 'stdout':stdout, 'stderr':stderr, 'cmdreturn':cmdreturn }
    
    def pairsplit(self, inst: int, group: str, device: str=None, opts: str='') -> dict:
        '''
        inst: horcm_inst
        group: horcm group
        device: Optionally pass an individual device
        opts: Pass pairsplit options
        '''
        opt_device = ''
        if device:
            opt_device = f'-d {device}'
        cmd = '{}pairsplit -g {} {} -I{} {}'.format(self.path,group,opt_device,inst,opts)
        stdout, stderr, cmdreturn = self.execute(cmd)
        return { 'stdout':stdout, 'stderr':stderr, 'cmdreturn':cmdreturn }
    
    def pairresync(self, inst: int, group: str, device: str=None, opts: str='', pace: int=15, mode: str='') -> dict:
        '''
        inst: horcm_inst
        group: horcm group
        device: Optionally pass an individual device
        opts: Pass pairresync options
        '''
        opt_device = ''
        if device:
            opt_device = f'-d {device}'
        cmd = '{}pairresync -g {} {} -c {} -I{}{} {}'.format(self.path,group,opt_device,pace,mode,inst,opts)
        stdout, stderr, cmdreturn = self.execute(cmd)
        return { 'stdout':stdout, 'stderr':stderr, 'cmdreturn':cmdreturn }
    
    def pairtakeover(self, inst: int, group: str, device: str=None, timeout: int=60, mode: str='') -> dict:
        '''
        inst: horcm_inst
        group: horcm group
        device: Optionally pass an individual device
        opts: Pass pairtakeover options
        '''
        opt_device = ''
        if device:
            opt_device = f'-d {device}'
        cmd = '{}horctakeover -g {} {} -t {} -I{}{}'.format(self.path,group,opt_device,timeout,mode,inst)
        stdout, stderr, cmdreturn = self.execute(cmd, expectedreturn=1)
        return { 'stdout':stdout, 'stderr':stderr, 'cmdreturn':cmdreturn }
    
    def raidvchkdsp(self,inst: int,group: str,device: str='',operation: str='-v gflag',mode: str='',opts: str='') -> dict:
        '''
        group: Horcm_group
        device: Optionally pass an individual device
        operation: only gflag support as parsers only capability
        opts: e.g. -fx to display ldev id in hex
        '''
        cmd = '{}raidvchkdsp -g {} {} {} -I{}{} {}'.format(self.path,group,device,operation,mode,inst,opts)
        stdout, stderr, cmdreturn = self.execute(cmd)
        raidvchkdsplayout = [row.strip() for row in list(filter(None,stdout.split('\n')))]
        raidvchkdspdata = self.parse_raidvchkdsp(raidvchkdsplayout)
        return { 'stdout':stdout, 'stderr':stderr, 'cmdreturn':cmdreturn, 'raidvchkdspdata':raidvchkdspdata }
    
    def parse_raidvchkdsp(self,raidvchkdsp: list) -> dict:
        '''
        TODO THIS IS BROKEN
        Group	PairVol   Port#  TID  LU  Seq# LDEV# GI-C-R-W-S  PI-C-R-W-S  R-Time
        HUR	HUR1     CL1-A-11  0    0 612239   102  E E E E E   E E E E E       0
        Returns dictionary of parsed raidvchkdsp:
        { Group: { PairVol: { Port#: { heading:data } } } }
        '''
        headings = raidvchkdsp.pop(0).split()
        stringExistsAtPosition = headings.index('GI-C-R-W-S')
        if (stringExistsAtPosition != -1):
            #'GI-C-R-W-S' and we need to split this
            headings[stringExistsAtPosition] = 'GI'
            headings.insert(stringExistsAtPosition+1,'GS')
            headings.insert(stringExistsAtPosition+1,'GW')
            headings.insert(stringExistsAtPosition+1,'GR')
            headings.insert(stringExistsAtPosition+1,'GC')
        stringExistsAtPosition = headings.index('PI-C-R-W-S')
        if (stringExistsAtPosition != -1):
            #'PI-C-R-W-S' and we need to split this
            headings[stringExistsAtPosition] = 'PI'
            headings.insert(stringExistsAtPosition+1,'PS')
            headings.insert(stringExistsAtPosition+1,'PW')
            headings.insert(stringExistsAtPosition+1,'PR')
            headings.insert(stringExistsAtPosition+1,'PC')
        view = { 'pairs': {} }
        for line in raidvchkdsp:
            sline = line.split()
            if len(sline) != len(headings): raise("header and data length mismatch")
            data = {head:item for item,head in zip(sline,headings)}
            view['pairs'][data['Group']] = view['pairs'].get(data['Group'],{})
            view['pairs'][data['Group']][data['PairVol']] = view['pairs'][data['Group']].get(data['PairVol'],{})
            #view['pairs'][data['Group']][data['PairVol']][data['Port#']] = data
            view['pairs'][data['Group']][data['PairVol']] = data
        return view
    
    def pairevtwaitexec(self,cmd):
        self.log.info('Executing: {}'.format(cmd))
        proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return proc
    
    def execute(self,cmd,expectedreturn=0):
        self.log.info(f"Executing: {cmd}")
        self.log.debug(f"Expecting return code {expectedreturn}")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout, stderr = proc.communicate()
        self.log.info(f"Return Code: {proc.returncode}")
  
        if proc.returncode and proc.returncode != expectedreturn and expectedreturn is not None:
            self.log.error("Return > "+str(proc.returncode))
            self.log.error("Stdout > "+stdout.strip())
            self.log.error("Stderr > "+stderr.strip())
            message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
            raise Exception('Unable to execute Command "{}". Command dump > {}'.format(cmd,message))
    
        return stdout, stderr, proc.returncode

if __name__ == "__main__":

    horcm_manager = Cci()
    now = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')

    manual_horcm_dict = {
        "local" : { "service":11018, "instance":18, "HORCM_CMD": [r"\\.\CMD-350147:/dev/sd"], "HORCM_LDEV":["group\tdevice\tserial\tldevid"], "site":"local", "date":now },
        "remote": { "service":11019, "instance":19, "HORCM_CMD": [r"\\.\CMD-358149:/dev/sd",r"\\.\CMD-358149:/dev/sd"], "HORCM_LDEV":["group\tdevice\tserial\tldevid"], "site":"remote", "date":now }
    }

    horcm_dict = {
        "local" : { "HORCM_CMD": [r"\\.\CMD-350147:/dev/sd"], "HORCM_LDEVG":["copy_grp\tdevice_grp\tserial"], "site":"local", "date":now },
        "remote": { "HORCM_CMD": [r"\\.\CMD-358149:/dev/sd",r"\\.\CMD-358149:/dev/sd"], "HORCM_LDEVG":["copy_grp\tdevice_grp\tserial"], "site":"remote", "date":now }
    }

    horcm_manager.create_horcms(horcm_dict)
