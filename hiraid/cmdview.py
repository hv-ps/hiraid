
class Cmdview():
    def __init__(self,returncode,stdout,stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.view = {}

class CmdviewConcurrent():
    def __init__(self,returncode=[],stdout=[],stderr=[]):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.view = {}

