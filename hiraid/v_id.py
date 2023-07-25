#!/usr/bin/python
# -----------------------------------------------------------------------------------------------------------------------------------
# Version v1.1.03
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
# 17/11/2022    v1.1.01     Updated model list
#
# 09/01/2023    v1.1.02     Updated model list
#
# 23/06/2023    v1.1.03     Updated model list
#
# -----------------------------------------------------------------------------------------------------------------------------------

class VId:
    version = "v1.1.03"
    models = {
                "R7"        :{ "v_id":"R7",     "type":"R700",      "model":["VSP"] },
                #"-"         :{ "v_id":"R7",     "type":"R700",      "model":["VSP"] },
                "M700"      :{ "v_id":"M700",   "type":"M700",      "model":["HUSVM"] },
                "R8"        :{ "v_id":"R8",     "type":"R800",      "model":["VSP G1000"] },
                "M8S"       :{ "v_id":"M8S",    "type":"M800S",     "model":["VSP G200"] },
                "M8M"       :{ "v_id":"M8M",    "type":"M800M",     "model":["VSP G400","VSP G600"] },
                "M8H"       :{ "v_id":"M8H",    "type":"M800H",     "model":["VSP G800"] },
                "M850S1"    :{ "v_id":"M850S1", "type":"M850S1",    "model":["VSP G350"] },
                "M850S2"    :{ "v_id":"M850S2", "type":"M850S2",    "model":["VSP G370"] },
                "M850M3"    :{ "v_id":"M850M3", "type":"M850M3",    "model":["VSP G700"] },
                "M850M3F"   :{ "v_id":"M850M3F","type":"M850M3F",   "model":["VSP F700"] },
                "M850HF"    :{ "v_id":"M850HF", "type":"M850HF",    "model":["VSP F900"] },
                "M850S1F"   :{ "v_id":"M850S1F","type":"M850S1F",   "model":["VSP F350"] },
                "M850S2F"   :{ "v_id":"M850S2F","type":"M850S2F",   "model":["VSP F370"] },
                "M850H"     :{ "v_id":"M850H",  "type":"M850H",     "model":["VSP G900"] },
                "R9F"       :{ "v_id":"R9F",    "type":"R900F",     "model":["VSP G5100","VSP G5500"] },
                "RH10HG"    :{ "v_id":"RH10HG", "type":"R900F",     "model":["VSP G5200"] },
                "RH10HF"    :{ "v_id":"RH10HF", "type":"R900F",     "model":["VSP G5600"] },
                "RH10MHF"   :{ "v_id":"RH10MHF","type":"E1090",     "model":["VSP E1090"] },
                "R9G"       :{ "v_id":"R9G",    "type":"R900G",     "model":["VSP G5500H"] },
                "M9S"       :{ "v_id":"M9S",    "type":"E590",      "model":["VSP E590"] },
                "VSP"       :{ "v_id":"R7",     "type":"R700",      "model":["VSP"] },
                "HUSVM"     :{ "v_id":"M700",   "type":"M700",      "model":["HUSVM"] },
                "VSP G1000" :{ "v_id":"R8",     "type":"R800",      "model":["VSP G1000"] },
                "VSP G200"  :{ "v_id":"M8S",    "type":"M800S",     "model":["VSP G200"] },
                "VSP G400"  :{ "v_id":"M8M",    "type":"M800M",     "model":["VSP G400"] },
                "VSP G600"  :{ "v_id":"M8M",    "type":"M800M",     "model":["VSP G600"] },
                "VSP G800"  :{ "v_id":"M8H",    "type":"M800H",     "model":["VSP G800"] },
                "VSP G350"  :{ "v_id":"M850S1", "type":"M850S1",    "model":["VSP G350"] },
                "VSP G370"  :{ "v_id":"M850S2", "type":"M850S2",    "model":["VSP G370"] },
                "VSP G700"  :{ "v_id":"M850M3", "type":"M850M3",    "model":["VSP G700"] },
                "VSP G900"  :{ "v_id":"M850H",  "type":"M850H",     "model":["VSP G900"] },
                "VSP G5100" :{ "v_id":"R9F",    "type":"R900F",     "model":["VSP G5100"] },
                "VSP G5500" :{ "v_id":"R9F",    "type":"R900F",     "model":["VSP G5500"] },
                "VSP G5200" :{ "v_id":"RH10HG", "type":"R900F",     "model":["VSP G5200"] },
                "VSP G5600" :{ "v_id":"RH10HF", "type":"R900F",     "model":["VSP G5600"] },
                "VSP G5500H":{ "v_id":"R9G",    "type":"R900G",     "model":["VSP G5500H"] },
                "VSP E590"  :{ "v_id":"M9S",    "type":"E590",      "model":["VSP E590"] },
                "VSP E1090" :{ "v_id":"RH10MHF","type":"E1090",     "model":["VSP E1090"] },
                "VSP E790"  :{ "v_id":"M9M",    "type":"E790",      "model":["VSP E790"] },
                "M9M"       :{ "v_id":"M9M",    "type":"E790",      "model":["VSP E790"] },
                "M7"        :{ "v_id":"M7",     "type":"M700",      "model":["HUSVM"] },
                "R9F"       :{ "v_id":"R9F",    "type":"R900F",     "model":["VSP G5600"] }
            }
    micro_ver = {
                "70"        :{ "v_id":"R7",     "type":"R700",      "model":["VSP"] },
                "73"        :{ "v_id":"M700",   "type":"M700",      "model":["HUS VM"] }
            }
