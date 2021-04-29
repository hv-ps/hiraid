#!/usr/bin/python3.6
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.00
# -----------------------------------------------------------------------------------------------------------------------------------
#
# License Terms
# -------------
# Unless stated otherwise, Hitachi Vantara Limited and/or its group companies is/are the owner or the licensee
# of all intellectual property rights in this script. This work is protected by copyright laws and treaties around
# the world. This script is solely for use by Hitachi Vantara Limited and/or its group companies in the provision
# of services to you by Hitachi Vantara Limited and/or its group companies and, as a condition of your receiving
# such services, you expressly agree not to use, reproduce, duplicate, copy, sell, resell or exploit for any purposes,
# commercial or otherwise, this script or any portion of this script. All of Hitachi Vantara Limited and/or its
# group companies rights are reserved.
#
# -----------------------------------------------------------------------------------------------------------------------------------
# Changes:
#
# 10/12/2020    v1.1.00     Initial Release - DEC
#
# -----------------------------------------------------------------------------------------------------------------------------------

import subprocess
import time
#from .raidcomparser import Raidcomparser
import inspect
import os
import json
import re
import paramiko

class RedhatMultipathd():

    def __init__(self,hostobj,ssh_host,ssh_username,ssh_password):

        from .redhatmultipathdparser import RedhatMultipathdParser

        self.username = ssh_username
        self.password = ssh_password
        self.host = ssh_host
        self.log = hostobj.log
        self.parser = RedhatMultipathdParser(hostobj)
        self.hostobj = hostobj

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ssh_host, username=ssh_username, password=ssh_password, timeout=10)

        self.client = client


    #def exec_command(self,command="ls -ltr /dev/disk/by-path/fc*", jobid="None"):
    def Xexecute(self,cmd,undocmds=[],expectedreturn=0,jobid="None"):
        """Executes a command over a established SSH connectio.
        :param ssh_machine: IP of the machine to which SSH connection to be established.
        :param ssh_username: User Name of the machine to which SSH connection to be established..
        :param ssh_password: Password of the machine to which SSH connection to be established..
        returns status of the command executed and Output of the command.
        """
        command = "sudo -S -p '' {}".format(cmd)
        self.log.info("Job[%s]: Executing: %s" % (jobid, command))
        stdin, stdout, stderr = self.client.exec_command(command=command)
        stdin.write(self.password + "\n")
        stdin.flush()
        stdoutput = ''
        for line in stdout:
            stdoutput += line
        #stdoutput = [line for line in stdout]
        #stdoutput = stdout.readlines()
        stderroutput = [line for line in stderr]
        for output in stdoutput:
            self.log.info("Job[%s]: %s" % (jobid, output.strip()))
        # Check exit code.
        self.log.debug("Job[%s]:stdout: %s" % (jobid, stdoutput))
        self.log.debug("Job[%s]:stderror: %s" % (jobid, stderroutput))
        self.log.info("Job[%s]:Command status: %s" % (jobid, stdout.channel.recv_exit_status()))
        if not stdout.channel.recv_exit_status():
            self.log.info("Job[%s]: Command executed." % jobid)
            #conn.close()
            if not stdoutput:
                stdoutput = True
            return True, stdoutput
        else:
            self.log.error("Job[%s]: Command failed." % jobid)
            for output in stderroutput:
                self.log.error("Job[%s]: %s" % (jobid, output))
            #conn.close()
            return False, stderroutput

    def execute(self,cmd,undocmds=[],expectedreturn=0):
        command = "sudo -S -p '' {}".format(cmd)
        self.log.info('Executing: {}'.format(command))
        self.log.debug('Expecting return code {}'.format(expectedreturn))
        #proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        #proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        #stdout, stderr = proc.communicate()
        stdin, pstdout, pstderr = self.client.exec_command(command=cmd)
        stdin.write(self.password + "\n")
        stdin.flush()

        stdout = ''.join(pstdout.readlines())
        stderr = ''.join(pstderr.readlines())
        returncode = pstdout.channel.recv_exit_status()

        if returncode and returncode != expectedreturn:
            self.log.error("Return > "+str(returncode))
            self.log.error("Stdout > "+stdout)
            self.log.error("Stderr > "+stderr)
            message = {'return':returncode,'stdout':stdout, 'stderr':stderr }
            raise Exception('Unable to execute Command "{}". Command dump > {}'.format(cmd,message))

        return {'return':returncode,'stdout':stdout, 'stderr':stderr }
        
        # Get the name of the calling function
        #parse = getattr(raidcomparser,inspect.currentframe().f_back.f_code.co_name)(stdout)
        
        
        #if proc.returncode and proc.returncode != expectedreturn:
        #    self.log.error("Return > "+str(proc.returncode))
        #    self.log.error("Stdout > "+stdout)
        #    self.log.error("Stderr > "+stderr)
        #    message = {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }
        #    raise Exception('Unable to execute Command "{}". Command dump > {}'.format(cmd,message))

        #if len(undocmds):
        #    for undocmd in undocmds: 
        #        echo = 'echo "Executing: {}"'.format(undocmd)
        #        self.undocmds.insert(0,undocmd)
        #        self.undocmds.insert(0,echo)

        #if self.cmdoutput:
        #    self.log.info(stdout)

        #return {'return':proc.returncode,'stdout':stdout, 'stderr':stderr }

    def multipathll(self,optviews: list=[]) -> dict:
        cmd = 'multipath -ll'
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.multipathll(cmdreturn['stdout'],optviews)
        return cmdreturn

    def devdiskbypath(self,optviews: list=[]) -> dict:
        cmd = 'ls -ltr /dev/disk/by-path/fc*'
        cmdreturn = self.execute(cmd)
        cmdreturn['views'] = self.parser.devdiskbypath(cmdreturn['stdout'],optviews)
        return cmdreturn

    def getmultipathing(self,optviews: list=[]) -> dict:

        cmdreturn = { "devdiskbypathview":self.devdiskbypath()['views']['metaview']['data'], "multipathllview":self.multipathll()['views']['metaview']['data'] }
        cmdreturn['views'] = self.parser.getmultipathing(cmdreturn,optviews)
        return cmdreturn


