#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
import math
import os

import arrow

from app_comment_spider.aws_console import EC2Console, PublicVar

object_path = PublicVar.object_path

set_log = logging.getLogger('server')
set_log.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] %(filename)s [line:%(lineno)d] %(levelname)s-%(name)s: %(message)s")

cmd = logging.StreamHandler()
cmd.setLevel(logging.DEBUG)
cmd.setFormatter(formatter)
set_log.addHandler(cmd)


# fw = logging.FileHandler('/PATH/TO/LOG_FILE')
# fw.setLevel(logging.INFO)
# fw.setFormatter(formatter)
# set_log.addHandler(fw)

def current_time():
    start_time_utc = arrow.utcnow()
    start_time_local = start_time_utc.to('Asia/Shanghai')
    # time_str = local.format('YYYYMMDD_HHmmss')
    start_time_str = start_time_local.format('YYYYMMDD_HHmmss')
    return start_time_str


def chunks(l, n):
    """Yield successive n-sized chunks from l. n: 要按多少行切一个文件"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def bucket(l, n):
    """n:要生成多少个文件"""
    chunk_num = math.ceil(len(l) / n)
    print(type(l), len(l))
    print('***', chunk_num)
    return chunks(l, chunk_num)


def stop_all_instances():
    aws_console = EC2Console()
    ids, ips = aws_console.get_instance_ids(tag_name=task_group)
    aws_console.stop_instance(ids)


def self_upload_result_to_s3():
    # Self Unload spider Result File to S3:
    aws_console = EC2Console()
    ids, ips = aws_console.get_instance_ids(tag_name=task_group)
    ips_str = ','.join(ips)
    set_log.info('ips_str: {}'.format(ips_str))
    os.system('fab -i {key_file} -f {fab_path} -H %s -u ubuntu {fab_func}:{start_time},{country},{equ}'.format(key_file=key_file, fab_path=fab_path, fab_func='upload_result_to_s3', country=country, start_time=start_time, equ=equ) % ips_str)
    # os.system('fab -i {key_file} -f {fab_path} -H %s -u ubuntu {fab_func}'.format(key_file=key_file, fab_path=fab_path, fab_func='reset_result_file') % ips_str)


def select_spider_instances_info():
    aws_console = EC2Console()
    ids, ips = aws_console.get_instance_ids(tag_name=task_group)
    ips_str = ','.join(ips)
    while True:
        cmd = input("Input Command : ")
        cmd = cmd.strip()
        if not len(cmd):
            continue
        elif cmd == 'quit':
            break
        os.system("fab -i {key_file} -f {fab_path} -H %s -u ubuntu {fab_func}:{var_1}".format(key_file=key_file, fab_path=fab_path, fab_func='select_spider_result', var_1=str(cmd)) % ips_str)


def select_task_complete_instances_ips():
    aws_console = EC2Console()
    ids, ips = aws_console.get_instance_ids(tag_name=task_group)
    temp_list = []
    cmd = "'ps -aux | grep py'"
    for index_num, ip_str in enumerate(ips):
        result = os.popen("fab -i {key_file} -f {fab_path} -H %s -u ubuntu {fab_func}:{var_1}".format(key_file=key_file, fab_path=fab_path, fab_func='select_spider_result', var_1=cmd) % ip_str)
        result_list = result.read().split('\n')
        for value in result_list:
            if 'app_comment_frequency.py' in value:
                print(value.split()[0].replace('[', '').replace(']', ''))
                temp_list.append(value.split()[0].replace('[', '').replace(']', ''))
    print(temp_list)
    return temp_list


def main():
    # Create and Run Instances:
    aws_console = EC2Console()
    instances_info_list = aws_console.create_ec2(instances_num=instances_count)
    set_log.info("instances_info_list : {}".format(instances_info_list))
    # instances_info_list : [{'public_ip': '54.223.189.7', 'instance_id': 'i-07247e5fcf03b2953', 'private_ip': '172.31.31.161'}]
    # instances_info_list = [{'public_ip': '54.223.189.7', 'instance_id': 'i-07247e5fcf03b2953', 'private_ip': '172.31.31.161'}]
    
    # Create Name of Instances:
    instance_tag_list = aws_console.name_instance(instance_list=instances_info_list, task_group=task_group, startswith=0)
    set_log.info("instance_tag_list: {}".format(instance_tag_list))
    # instance_tag_list: [['i-07247e5fcf03b2953', 'test_lookup_lj_0']]
    # instance_tag_list = [['i-07247e5fcf03b2953', 'test_lookup_lj_0']]
    
    # Get Instances name: test_lookup_lj_0
    # set_log.info(aws_console.get_tag(instance_tag_list[0][0]))
    
    # Check Instance Status
    aws_console.check_instances_status(instance_tag_list=instance_tag_list, instance_list=instances_info_list, check_num=15)
    set_log.info('Instances is OK !!!')
    
    # Get Instances ids and ips: ids: ['i-07247e5fcf03b2953'] , ips: ['54.223.189.7']
    set_log.info("Get Instances ids and ips")
    ids, ips = aws_console.get_instance_ids(tag_name=task_group)
    set_log.info("ids: {ids} , ips: {ips}".format(ids=ids, ips=ips))
    
    # Get Source File and Split It:
    # split_count 这个参数是跟开启实例的数量相同的
    split_count = len(ips)
    set_log.info('spilt_count: {}'.format(split_count))
    # os.system('aws s3 cp s3://datawarehouse.lbadvisor.com/aso/tmp/{source_file} /mnt/aso_data/source_files/'.format(source_file=source_file))  # 源文件不能是bz2格式的，只能是普通txt格式
    with open('/mnt/aso_data/source_files/{source_file}'.format(source_file=source_file)) as rf:
        temp_list_1 = [line for line in rf]
        set_log.info(len(temp_list_1))
        for index_num, line_list in enumerate(bucket(temp_list_1, split_count)):
            with open('/mnt/aso_data/source_files/{}_source_file.txt'.format(index_num), 'w') as wf:
                for i in line_list:
                    wf.write(i)
    
    # Upload spider file
    for file_num, ip_str in enumerate(ips):
        os.system('fab -i {key_file} -f {fab_path} -H %s -u ubuntu {fab_func}:{file_num}'.format(key_file=key_file, fab_path=fab_path, fab_func='upload_spider_file', file_num=file_num) % ip_str)
    
    # Run spider file
    ips_str = ','.join(ips)
    set_log.info('ips_str: {}'.format(ips_str))
    os.system(
        'fab -i {key_file} -f {fab_path} -H %s -u ubuntu {fab_func}:{input_file},{output_file},{interval},{country},{equ},{start_time},{task_name}'.format(key_file=key_file, fab_path=fab_path, fab_func='run_spider', input_file=input_file,
                                                                                                                                                           output_file=output_file, interval=interval, country=country, equ=equ,
                                                                                                                                                           start_time=start_time, task_name=task_name) % ips_str)


if __name__ == '__main__':
    pass
    task_flag_dict = {
        'get_app_comment_count': 'app_comment_count',
    }
    
    # 开启的实例数量，app id的文件
    instances_count = 50
    source_file = 'task_5032_appid_cn.txt'
    
    # fabric的参数
    fab_path = object_path + '/fabfile.py'
    key_file = '/mnt/for_spider.pem'
    
    # 爬虫需要的参数
    input_file = '/mnt/source_file/temp_source_file.txt'
    output_file = '/mnt/result_file/result_file.json'
    interval = 2
    country = 'us'
    equ = 'iphone'
    start_time = current_time()
    task_name = 'get_app_comment_count'  # 根据任务名称来区别要启动的任务,任务名称(gat_app_comment_count, gat_app_comment_info)
    
    # 实例的组名
    task_group = '{task_flag}_{country}_'.format(country=country, task_flag=task_flag_dict[task_name])
    set_log.info('task_flag : {}'.format(task_flag_dict[task_name]))
    
    # main()
    # stop_all_instances()
    # select_spider_instances_info()
    # select_task_complete_instances_ips()
    # self_upload_result_to_s3()
