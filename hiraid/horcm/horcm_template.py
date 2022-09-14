default_template = '''
# Site: $site
# Date: $date
# Inst: $instance

HORCM_MON
#ip_address     service     poll(10ms)      timeout(10ms)
$ip_address     $service    $poll           $timeout

HORCM_CMD
#dev_name       dev_name        dev_name
$HORCM_CMD

$HORCM_LDEV_TYPE
$HORCM_LDEV

HORCM_INST
#dev_group              ip_address      service
$HORCM_INST
'''
