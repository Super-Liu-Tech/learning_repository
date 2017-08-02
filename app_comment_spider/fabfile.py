#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
import traceback

from fabric.api import run, put, parallel, sudo

from app_comment_spider.aws_console import PublicVar

object_path = PublicVar.object_path

log_fab = logging.getLogger('server')

TestMod = 'test/'


def upload_spider_file(file_num):
    try:
        log_fab.info('Begin mount and init disk !')
        sudo('mkfs.ext4 /dev/xvdc')
        sudo('mount /dev/xvdc /mnt')
        sudo('mkdir -p /mnt/{spider_dir,log,source_file,result_file}')
        sudo('chown -R ubuntu:ubuntu /mnt/')
        log_fab.info('Disk init Done !')
        log_fab.info('Begin upload spider file !')
        put('{}/app_comment_frequency.py'.format(object_path), '/mnt/spider_dir/', use_sudo=True)
        put('/mnt/aso_data/source_files/{file_num}_source_file.txt'.format(file_num=file_num), '/mnt/source_file/', use_sudo=True)
        # exclude='.git'
        # rsync_project(local_dir='{object_path}/test_lookup_frequency.py'.format(object_path=object_path), remote_dir='/mnt/spider_dir/')
        # rsync_project(local_dir='/mnt/aso_data/source_files/{file_num}_source_file.txt'.format(file_num=file_num), remote_dir='/mnt/source_file/')
        sudo('mv /mnt/source_file/{file_num}_source_file.txt /mnt/source_file/temp_source_file.txt'.format(file_num=file_num))
        sudo('pip3 install arrow -i https://pypi.tuna.tsinghua.edu.cn/simple/')
        sudo('pip3 install retrying -i https://pypi.tuna.tsinghua.edu.cn/simple/')
        log_fab.info('Upload spider file Complete !')
    except:
        log_fab.warning(traceback.format_exc())


@parallel
def run_spider(input_file, output_file, interval, country, equ, start_time, task_name):
    try:
        run('nohup python3.5 /mnt/spider_dir/app_comment_frequency.py {input_file} {output_file} {interval} {country} {equ} {start_time} {task_name} > /mnt/log/spider.log 2>&1 &'.format(input_file=input_file, output_file=output_file,
                                                                                                                                                                                         interval=interval,
                                                                                                                                                                                         country=country, equ=equ, task_name=task_name,
                                                                                                                                                                                         start_time=start_time), pty=False)
    except:
        log_fab.warning(traceback.format_exc())


@parallel
def upload_result_to_s3(start_time, country, equ):
    instance_local_ip = "`ifconfig | grep 'inet addr' | head -n 1 | awk -F':' '{print $2}' | awk -F' ' '{print $1}' | sed 's/\./_/g'`"
    sudo("mv /mnt/result_file/* /mnt/result_file/{start_time}_{local_ip}_result.json".format(local_ip=instance_local_ip, start_time=start_time))
    sudo("bzip2 /mnt/result_file/*")
    sudo("aws s3 cp /mnt/result_file/{start_time}_{local_ip}_result.json.bz2 s3://datawarehouse.lbadvisor.com/aso/{TestMod}app_comment/{equ}/{country}/{start_time}/".format(local_ip=instance_local_ip, start_time=start_time, country=country,
                                                                                                                                                                             equ=equ, TestMod=TestMod))
    
    sudo('touch /mnt/aso.app_comment.{equ}.{country}.{start_time}.{start_time}_{local_ip}_result'.format(local_ip=instance_local_ip, start_time=start_time, country=country, equ=equ))
    sudo(
        'aws s3 cp /mnt/aso.app_comment.{equ}.{country}.{start_time}.{start_time}_{local_ip}_result s3://datawarehouse.lbadvisor.com/aso/{TestMod}spider_status_msg/'.format(local_ip=instance_local_ip, start_time=start_time, country=country,
                                                                                                                                                                             TestMod=TestMod, equ=equ))


@parallel
def reset_result_file():
    instance_local_ip = "`ifconfig | grep 'inet addr' | head -n 1 | awk -F':' '{print $2}' | awk -F' ' '{print $1}' | sed 's/\./_/g'`"
    sudo('echo {local_ip}'.format(local_ip=instance_local_ip))
    # sudo('bzip2 -d /mnt/result_file/*')
    sudo('mv /mnt/result_file/* /mnt/result_file/result_file.json')


# @parallel
def select_spider_result(cmd):
    sudo(cmd)


if __name__ == '__main__':
    pass
    print(object_path)
