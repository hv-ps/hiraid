#!/usr/bin/python
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.01
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
# 17/05/2022    v1.1.01     Added support for VSP G
#
# 25/05/2023    v1.1.02     Added G800 ( M800H )
#
# 26/05/2023    v1.1.03     Added G200, G400, G600
#
# -----------------------------------------------------------------------------------------------------------------------------------

class Storagecapabilities:

    default_limitations = {
        "maxldevid": 65279,
        "minldevcapblk": 96000
    }

    limitations = {
        "M700":     { "maxldevid": 16383, "type": "G800" },
        "M800H":    { "maxldevid": 16383, "type": "G800" },
        "M8H":      { "maxldevid": 16383, "type": "G800" },
        "M8S":      { "maxldevid": 2048,  "type": "G200" },
        "M800S":    { "maxldevid": 2048,  "type": "G200" },
        "M800M":    { "maxldevid": 4096,  "type": "G400" },
        "M8M":      { "maxldevid": 4096,  "type": "G400" }
    }

