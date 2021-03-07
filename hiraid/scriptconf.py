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
# 14/01/2020    v1.1.00     Initial Release
#
# -----------------------------------------------------------------------------------------------------------------------------------

import json.encoder
import os
from datetime import datetime

class Scriptconf:

    basedir = os.getcwd()
    ts = datetime.now().strftime('%d-%m-%Y_%H.%M.%S')
    separator = ('/','\\')[os.name == 'nt']
    cciextension =  ('.sh','.exe')[os.name == 'nt']
    horcmbinpath = ('/usr/bin/','C:\\HORCM\\etc\\')[os.name == 'nt']
    horcmdir = ('/etc/','C:\\Windows\\')[os.name == 'nt']
    configdir = '{}{}{}'.format(basedir,separator,'etc')
    migrationinputdir = '{}{}{}'.format(basedir,separator,'input')
    gadmigrationconftemplatefile = 'gadmigrationconftemplate.json'
    gadmigrationconffile = 'gadmigrationconf.json'
    gadhostsfilename = 'gadhosts.json'
    gadhoststemplatefilename = 'gadhoststemplate.json'
    gadmigrationconftemplate = '{}{}{}'.format(configdir,separator,gadmigrationconftemplatefile)
    gadhoststemplate = '{}{}{}'.format(configdir,separator,gadhoststemplatefilename)
    jsonintemplatefilename = 'jsonintemplate.json'
    jsonintemplate = '{}{}{}'.format(configdir,separator,jsonintemplatefilename)
    migrationoutputdir = '{}{}{}'.format(basedir,separator,'migrations')
    horcmtemplatefilename = 'horcmtemplate.conf'
    horcmtemplatefile = '{}{}{}'.format(configdir,separator,horcmtemplatefilename)
    # english, japanese, french - feel free to add language class to messaging.py
    language = 'english'
    logbasedir = basedir
    logdir = ('/var/log/hiraid','C:\\log\\hiraid')[os.name == 'nt']
    smilies = True
    targeted_rollback = True

    

