#!/usr/bin/env python
# -*- coding:utf-8 -*-
import boto3
import boto.ec2
from subprocess import check_output
import logging
import json
import codecs
import time
import os
from collections import Counter

log_server = logging.getLogger('server')

reader = codecs.getdecoder('utf-8')


class PublicVar:
    object_path = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))


class EC2Console:
    def __init__(self):
        self.client = boto3.client("ec2", region_name='cn-north-1')
        self.log = log_server
        self.ec2 = boto3.resource('ec2')
        self.ec2_boto = boto.ec2.connect_to_region('cn-north-1')
    
    def get_instance_ids(self, tag_name):
        reservations = self.ec2_boto.get_all_instances()
        instances = [i for r in reservations for i in r.instances]
        ret_ips = []
        ids = []
        for i in instances:
            attr = i.__dict__
            if str(attr.get("tags", {}).get("Name")).startswith(tag_name) and str(attr.get('_state', '')).startswith('running'):
                latest_ip = attr.get('ip_address')
                id = attr.get('id')
                ret_ips.append(latest_ip)
                ids.append(id)
        return list(filter(lambda x: x, ids)), list(filter(lambda x: x, ret_ips))
    
    # todo 某些角色可能不需要硬盘，可以节约一些
    def create_ec2(self, instances_num):
        response = self.client.run_instances(
            ImageId='ami-c5a275a8',
            MinCount=int(instances_num),
            MaxCount=int(instances_num),
            KeyName='for_spider',
            SecurityGroupIds=['sg-d30f11b1', 'sg-46273524', 'sg-fed2c09c'],
            InstanceType='t2.micro',
            SubnetId='subnet-b61b33c2',
            Placement={
                'AvailabilityZone': 'cn-north-1b'
            },
            IamInstanceProfile={
                'Name': 'ec2_s3+RDS'
            },
            BlockDeviceMappings=[
                {
                    'VirtualName': '0',
                    'DeviceName': '/dev/sdc',
                    'Ebs': {
                        'VolumeSize': 100,
                        'DeleteOnTermination': True,
                        'VolumeType': 'gp2'
                    }
                }
            ],
            Monitoring={'Enabled': True},
            DisableApiTermination=False,
            InstanceInitiatedShutdownBehavior='stop'
        )
        instance_list = self.extract_ec2_info(response)
        return instance_list
    
    def _name_instance(self, instance_id, tag):
        check_output(
            'aws ec2 create-tags --resources %s --tags Key=Name,Value=%s' % (
                instance_id, tag),
            shell=True)
    
    def name_instance(self, instance_list, task_group, startswith=0):
        """为实例起系列名"""
        instance_count = len(instance_list) + startswith
        instance_tag_list = []
        for index, instance in enumerate(instance_list):
            i = instance.get("instance_id")
            self._name_instance(i, '%s%s' % (task_group, range(startswith, instance_count)[index]))
            instance_tag_list.append([i, '%s%s' % (task_group, range(startswith, instance_count)[index])])
            self.log.info('{} {} {}'.format(index, task_group + str(index), i))
        return instance_tag_list
    
    def check_instance_status(self, cmd_ret):
        """检查所有启动的实例状态, 如果该实例已经准备好了则返回True, 否则返回False"""
        cmd_ret_dict = json.loads(reader(cmd_ret)[0])
        InstanceStatuses_list = cmd_ret_dict.get('InstanceStatuses', [])
        flag = False
        for iss in InstanceStatuses_list:
            InstanceStatus = iss.get('InstanceStatus', {}).get('Status', '')
            SystemStatus = iss.get('SystemStatus', {}).get('Status', '')
            if InstanceStatus == 'ok' and SystemStatus == 'ok':
                flag = True
        return flag
    
    def check_instances_status(self, instance_tag_list, instance_list, check_num=10):
        """如果实例没有准备好下面的代码会一直阻塞程序的运行, 如果有实例超时(5分钟)未启动，则重新启动一个实例取代它
        并且会原地修改instance_tag_list和instance_list"""
        instance_status_check_list = [False] * len(instance_tag_list)
        check_counter = Counter()
        while True:
            self.log.info('Waiting for %s Instances Launch Complete ...' % len(list(filter(lambda x: not x, instance_status_check_list))))
            for i, bool in enumerate(instance_status_check_list):
                if not bool:
                    self.log.info("Waiting for %s" % instance_tag_list[i])
            for index, instance in enumerate(instance_list):
                i = instance.get("instance_id")
                ret_json = check_output('aws ec2 describe-instance-status --instance-id %s' % i, shell=True)
                instance_status = self.check_instance_status(ret_json)
                instance_status_check_list[index] = instance_status
                check_counter[i] += 1
                if check_counter[i] >= check_num:
                    # 终止这台实例，并且重新启动新的实例
                    self.rerun_instance(i, index, instance_tag_list, instance_list)
            if all(instance_status_check_list):
                break
            time.sleep(30)
        time.sleep(30)
    
    def get_tag(self, instance_id):
        resp_json = check_output('aws ec2 describe-tags --filters "Name=resource-id,Values={}"'.format(instance_id), shell=True)
        cmd_ret_dict = json.loads(reader(resp_json)[0])
        src_tag = cmd_ret_dict.get('Tags', [{}])[0].get('Value', '')
        return src_tag
    
    def get_role_ip(self, task_name):
        instance_id_list, instance_ip_list = self.get_instance_ids(task_name)
        spider_clients_public_ip_list = []
        consumer_clients_public_ip_list = []
        producer_clients_public_ip_list = []
        for role, ip in zip([self.get_tag(instance_id) for instance_id in instance_id_list], instance_ip_list):
            if 'fetcher' in role:
                spider_clients_public_ip_list.append(ip)
            elif 'consumer' in role:
                consumer_clients_public_ip_list.append(ip)
            elif 'producer' in role:
                producer_clients_public_ip_list.append(ip)
        
        spider_clients_public_ip_list = sorted(spider_clients_public_ip_list)
        consumer_clients_public_ip_list = sorted(consumer_clients_public_ip_list)
        producer_clients_public_ip_list = sorted(producer_clients_public_ip_list)
        return spider_clients_public_ip_list, consumer_clients_public_ip_list, producer_clients_public_ip_list
    
    def rerun_instance(self, instance_id, index, instance_tag_list, instance_list):
        self.stop_instance([instance_id])
        new_instance_id = self.create_ec2(1)[0]
        src_tag = self.get_tag(instance_id)
        new_tag = src_tag + '_backup'
        self._name_instance(new_instance_id.get('instance_id', ''), new_tag)
        assert instance_tag_list[index][-1] == src_tag
        instance_tag_list[index] = [new_instance_id.get('instance_id'), new_tag]
        assert instance_list[index].get('instance_id', '') == instance_id
        instance_list[index] = new_instance_id
        self.log.warning('rerun a new instance: {} replace {}'.format(new_tag, src_tag))
    
    def extract_ec2_info(self, response):
        instance_list = []
        for index_num, item in enumerate(response.get("Instances")):
            instance_id = item['InstanceId']
            private_ip = item['PrivateIpAddress']
            
            self.log.info('instance_id : %s' % instance_id)
            while True:
                public_ip = self.instance_public_ip(instance_id)
                if public_ip:
                    break
            self.log.info('public_ip : %s' % public_ip)
            temp_dict = {'instance_id': instance_id, 'private_ip': private_ip, 'public_ip': public_ip}
            self.log.info('temp_dict_type : %s' % type(temp_dict))
            self.log.info('temp_dict : %s' % temp_dict)
            instance_list.append(temp_dict)
        self.log.info('instance_list : %s' % instance_list)
        return instance_list
    
    def stop_instance(self, instance_id_list):
        response = self.client.terminate_instances(InstanceIds=instance_id_list)
        return response
    
    def instance_public_ip(self, instances_id):
        a = self.ec2.Instance(instances_id)
        instance_public_ip = a.public_ip_address
        return instance_public_ip


if __name__ == '__main__':
    pass
