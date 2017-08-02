#!/usr/bin/env python
# -*- coding:utf-8 -*-
import plistlib
import boto.ec2
import os



a = b'<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n<plist version="1.0">\n<dict>\n<key>pings</key>\n<array></array>\n<key>metrics</key>\n<dict>\n  <key>dialogId</key><string>itemNotAvailable</string>\n  <key>message</key><string>The item you&#39;ve requested</string>\n  <key>actionUrl</key><string>itunes.apple.com/WebObjects/MZStore.woa/wa/viewSoftware?id=699529454&amp;cc=us&amp;urlDesc=</string>\n</dict>\n<key>failureType</key><string></string>\n<key>customerTitleMessage</key><string>Item Not Available</string>\n<key>customerMessage</key><string>The item you&#39;ve requested is not currently available in the U.S. store.</string>\n<key>m-allowed</key><false/>\n<key>dialog</key>\n<dict><key>m-allowed</key><false/>\n<key>message</key><string>Item Not Available</string>\n<key>explanation</key><string>The item you&#39;ve requested is not currently available in the U.S. store.</string>\n<key>defaultButton</key><string>ok</string>\n<key>okButtonString</key><string>OK</string>\n<key>initialCheckboxValue</key><true/></dict>\n</dict>\n</plist>\n'
b = {"task_id": "1110673493", "type_error_msg": "b'<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\\n<plist version=\"1.0\">\\n<dict>\\n<key>pings</key>\\n<array></array>\\n<key>metrics</key>\\n<dict>\\n  <key>dialogId</key><string>itemNotAvailable</string>\\n  <key>message</key><string>\\xe6\\x82\\xa8\\xe8\\xa6\\x81\\xe7\\x9a\\x84\\xe4\\xba\\xa7\\xe5\\x93\\x81\\xe7\\x9b\\xae\\xe5\\x89\\x8d\\xe5\\x9c\\xa8\\xe4\\xb8\\xad\\xe5\\x9b\\xbd\\xe5\\x95\\x86\\xe5\\xba\\x97\\xe4\\xb8\\x8d\\xe6\\x8f\\x90\\xe4\\xbe\\x9b</string>\\n  <key>actionUrl</key><string>itunes.apple.com/WebObjects/MZStore.woa/wa/viewSoftware?id=1110673493&amp;cc=cn&amp;urlDesc=</string>\\n</dict>\\n<key>failureType</key><string></string>\\n<key>customerTitleMessage</key><string>\\xe9\\xa1\\xb9\\xe7\\x9b\\xae\\xe4\\xb8\\x8d\\xe5\\x8f\\xaf\\xe7\\x94\\xa8</string>\\n<key>customerMessage</key><string>\\xe6\\x82\\xa8\\xe8\\xa6\\x81\\xe7\\x9a\\x84\\xe4\\xba\\xa7\\xe5\\x93\\x81\\xe7\\x9b\\xae\\xe5\\x89\\x8d\\xe5\\x9c\\xa8\\xe4\\xb8\\xad\\xe5\\x9b\\xbd\\xe5\\x95\\x86\\xe5\\xba\\x97\\xe4\\xb8\\x8d\\xe6\\x8f\\x90\\xe4\\xbe\\x9b</string>\\n<key>m-allowed</key><false/>\\n<key>dialog</key>\\n<dict><key>m-allowed</key><false/>\\n<key>message</key><string>\\xe9\\xa1\\xb9\\xe7\\x9b\\xae\\xe4\\xb8\\x8d\\xe5\\x8f\\xaf\\xe7\\x94\\xa8</string>\\n<key>explanation</key><string>\\xe6\\x82\\xa8\\xe8\\xa6\\x81\\xe7\\x9a\\x84\\xe4\\xba\\xa7\\xe5\\x93\\x81\\xe7\\x9b\\xae\\xe5\\x89\\x8d\\xe5\\x9c\\xa8\\xe4\\xb8\\xad\\xe5\\x9b\\xbd\\xe5\\x95\\x86\\xe5\\xba\\x97\\xe4\\xb8\\x8d\\xe6\\x8f\\x90\\xe4\\xbe\\x9b</string>\\n<key>defaultButton</key><string>ok</string>\\n<key>okButtonString</key><string>\\xe5\\xa5\\xbd</string>\\n<key>initialCheckboxValue</key><true/></dict>\\n</dict>\\n</plist>\\n' is not JSON serializable"}
c = b'<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\\n<plist version=\"1.0\">\\n<dict>\\n<key>pings</key>\\n<array></array>\\n<key>metrics</key>\\n<dict>\\n  <key>dialogId</key><string>itemNotAvailable</string>\\n  <key>message</key><string>\\xe6\\x82\\xa8\\xe8\\xa6\\x81\\xe7\\x9a\\x84\\xe4\\xba\\xa7\\xe5\\x93\\x81\\xe7\\x9b\\xae\\xe5\\x89\\x8d\\xe5\\x9c\\xa8\\xe4\\xb8\\xad\\xe5\\x9b\\xbd\\xe5\\x95\\x86\\xe5\\xba\\x97\\xe4\\xb8\\x8d\\xe6\\x8f\\x90\\xe4\\xbe\\x9b</string>\\n  <key>actionUrl</key><string>itunes.apple.com/WebObjects/MZStore.woa/wa/viewSoftware?id=1110673493&amp;cc=cn&amp;urlDesc=</string>\\n</dict>\\n<key>failureType</key><string></string>\\n<key>customerTitleMessage</key><string>\\xe9\\xa1\\xb9\\xe7\\x9b\\xae\\xe4\\xb8\\x8d\\xe5\\x8f\\xaf\\xe7\\x94\\xa8</string>\\n<key>customerMessage</key><string>\\xe6\\x82\\xa8\\xe8\\xa6\\x81\\xe7\\x9a\\x84\\xe4\\xba\\xa7\\xe5\\x93\\x81\\xe7\\x9b\\xae\\xe5\\x89\\x8d\\xe5\\x9c\\xa8\\xe4\\xb8\\xad\\xe5\\x9b\\xbd\\xe5\\x95\\x86\\xe5\\xba\\x97\\xe4\\xb8\\x8d\\xe6\\x8f\\x90\\xe4\\xbe\\x9b</string>\\n<key>m-allowed</key><false/>\\n<key>dialog</key>\\n<dict><key>m-allowed</key><false/>\\n<key>message</key><string>\\xe9\\xa1\\xb9\\xe7\\x9b\\xae\\xe4\\xb8\\x8d\\xe5\\x8f\\xaf\\xe7\\x94\\xa8</string>\\n<key>explanation</key><string>\\xe6\\x82\\xa8\\xe8\\xa6\\x81\\xe7\\x9a\\x84\\xe4\\xba\\xa7\\xe5\\x93\\x81\\xe7\\x9b\\xae\\xe5\\x89\\x8d\\xe5\\x9c\\xa8\\xe4\\xb8\\xad\\xe5\\x9b\\xbd\\xe5\\x95\\x86\\xe5\\xba\\x97\\xe4\\xb8\\x8d\\xe6\\x8f\\x90\\xe4\\xbe\\x9b</string>\\n<key>defaultButton</key><string>ok</string>\\n<key>okButtonString</key><string>\\xe5\\xa5\\xbd</string>\\n<key>initialCheckboxValue</key><true/></dict>\\n</dict>\\n</plist>\\n'
d = b'\xe6\x82\xa8\xe8\xa6\x81\xe7\x9a\x84\xe4\xba\xa7\xe5\x93\x81\xe7\x9b\xae\xe5\x89\x8d\xe5\x9c\xa8\xe4\xb8\xad\xe5\x9b\xbd\xe5\x95\x86\xe5\xba\x97\xe4\xb8\x8d\xe6\x8f\x90\xe4\xbe\x9b'

# print(plistlib.loads(c))
print(type(c), c.decode('utf8'))
# print(c.decode('utf8').encode('utf8'))
print(type(d), d.decode("utf8"))


ec2 = boto.ec2.connect_to_region('cn-north-1')


def get_latest_ip(instance_id):
    latest_ip = None
    reservations = ec2.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    for i in instances:
        attr = i.__dict__
        if attr.get("id") == instance_id:
            latest_ip = attr.get('ip_address')
            break
    return latest_ip


def get_allocation_id(ip):
    allocation_id = None
    resp = ec2.get_all_addresses()
    for addr in resp:
        if addr.public_ip == ip:
            allocation_id = addr.allocation_id
            break
    return allocation_id


def change_ip(instance_id):
    latest_ip = get_latest_ip(instance_id)
    if not latest_ip:
        raise RuntimeError('Invalid ec2 instance_id')
    allocation_id = get_allocation_id(latest_ip)
    new_ip = str(ec2.allocate_address()).split(':', 1)[-1]
    ec2.associate_address(instance_id, new_ip)
    ec2.release_address(allocation_id=allocation_id)


if __name__ == '__main__':
    pass
    # instance_id = 'i-04aeaf6ed18555703'
    # change_ip(instance_id)
    # print(type(b))
    # a = os.popen("wget -q -O - http://169.254.169.254/latest/meta-data/instance-id")
    # instance_id = a.read()
    # print(instance_id)