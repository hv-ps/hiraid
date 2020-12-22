#!/usr/bin/python
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
# 14/01/2020    v1.1.00     Initial Release - Storage migration capabilites matrix
#
# -----------------------------------------------------------------------------------------------------------------------------------

class Storagecapabilities:

    microcode_capabilities = {
                                "R8"        :{ "80-06-75": ["GAD+UR", "EXPAND GAD DP-VOLS"]},
                                "VSP G5100" :{ "90-03-01": ["GAD+UR", "TI PAIR DEFRAG", "SENT RECEIVED OPTICAL SIGNAL","INFORMATION COLLECTION PROCESSING","QoS"] },
                                "VSP G5500" :{ "90-03-01": ["GAD+UR", "TI PAIR DEFRAG", "SENT RECEIVED OPTICAL SIGNAL","INFORMATION COLLECTION PROCESSING","QoS"] }
                                }
    migration_path = {
                                "R7"        : "ndm",
                                "R8"        : "gad"
    }
    migration_type_requirements = {
                                "gad"       :{ "ldevs": {'VOL_ATTR':{"HORC": { "Fence":{"ASYNC":"GAD+UR","NEVER":"GAD+TC"}}}}}
                    }