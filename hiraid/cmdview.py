
from . import timer

class Cmdview():
    def __init__(self,cmd):
        self.cmd = cmd
        self.executed = False
        self.expectedreturn = 0
        self.returncode = None
        self.stdout = None
        self.stderr = None
        self.view = {}
        self.altview = {}
        self.data = []
        self.undocmds = []
        self.undodefs = []
        self.stats = {}
        self.actions = {}
        self.start = timer.now()

class CmdviewConcurrent():
    def __init__(self,returncodes=[],stdout=[],stderr=[]):
        self.returncodes = returncodes
        self.returncode = 0
        self.stdout = stdout
        self.stderr = stderr
        self.view = {}
        self.data = []
        self.cmds = []
        self.undocmds = []
        self.undodefs = []
        self.stats = {}
        self.start = timer.now()

