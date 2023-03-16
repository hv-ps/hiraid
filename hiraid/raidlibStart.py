

# Raidlib quickstart
from hvmodules import hvutil
from hvmodules.raidlib import Storage as Storage
log = hvutil.configlog('myscript','/tmp/logs','log.txt')
source = Storage('350147',log,useapi='raidcom')
source.raidcom(10)


source.raidscan('cl1-a-80')


print("HELLO")
print(source.views['_raidscanmu'])

