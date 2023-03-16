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
# 14/01/2020  v1.1.00   Initial Release
#
# -----------------------------------------------------------------------------------------------------------------------------------

import logging
import os
import argparse, sys
from datetime import datetime
import json

class NewArgParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

def man(parser,gadmigrationconftemplate,gadmigrationhoststemplate):
  parser.print_help()
  
  try:
    print("\n\nGAD migration configuration template description")
    with open(gadmigrationconftemplate) as json_file:
      jsonout = json.load(json_file)
      print(json.dumps(jsonout,indent=4))
    print("\n\nGAD host configuration template description")
    with open(gadmigrationhoststemplate) as json_file:
      jsonout = json.load(json_file)
      print(json.dumps(jsonout,indent=4))
  except Exception as e:
    print(e)
  parser.exit()

def createdir(directory):
  if not os.path.exists(directory):
    os.makedirs(directory)

def configlog(scriptname,logdir,logname,basedir=os.getcwd()):

  try:
    separator = ('/','\\')[os.name == 'nt']
    cwd = basedir
    #createdir('{}{}{}'.format(cwd,separator,logdir))
    createdir('{}'.format(logdir))
    #logfile = '{}{}{}{}{}'.format(cwd, separator, logdir, separator, logname)
    logfile = '{}{}{}'.format(logdir, separator, logname)
    logger = logging.getLogger(scriptname)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # Add handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

  except Exception as e:
    raise Exception('Unable to configure logger > {}'.format(str(e)))
    
  return logger

