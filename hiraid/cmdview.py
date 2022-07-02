
class XCmdview():
    def __init__(self,cmd,returncode,stdout,stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.view = {}
        self.data = []
        self.cmd = ""
        self.undocmds = []
        self.undodefs = []

class Cmdview():
    def __init__(self,cmd):
        self.cmd = cmd
        self.executed = False
        self.expectedreturn = 0
        self.returncode = None
        self.stdout = None
        self.stderr = None
        self.view = {}
        self.data = []
        self.undocmds = []
        self.undodefs = []

class CmdviewConcurrent():
    def __init__(self,returncode=[],stdout=[],stderr=[]):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.view = {}
        self.data = []
        self.cmds = []

