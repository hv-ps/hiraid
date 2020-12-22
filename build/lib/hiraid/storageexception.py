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

import traceback
class StorageException(Exception):

    def __init__(self, message, storages, log, storage: object=None, migration: object=None ):
        '''
        message = output message
        storages = storage class
        log = logging object
        storage = storage object raising exception
        '''
        super(StorageException, self).__init__(message)
        self.storages = storages
        self.log = log
        self.log.error(message)
        self.message = traceback.format_exc()

        # Load the message onto the json task
        if storage:
            storage.writemessagetotaskref(self.message)
            
        self.log.info("Dumping jsonin to log")
        self.storages.instances[0].dumpjsonin()
        
        for instance in self.storages.lockedstorage:
            self.log.info('Unlock storage {}'.format(instance.serial))
            instance.unlockresource()
            instance.writeundofile()

        self.log.error("-- End on Error --")
    
    def __str__(self):
        if not self.message:
            return message
        return self.message
        