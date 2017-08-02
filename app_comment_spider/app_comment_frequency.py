# ! /usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import os
import time
from multiprocessing import Queue
import sys
import sched
from functools import wraps
import six.moves.queue as queue
import traceback

import requests
import arrow

import boto.ec2
import boto3

ec2 = boto.ec2.connect_to_region('cn-north-1')

args = sys.argv[1:]

q_in = Queue(100)
q_out = Queue(100)

__version__ = '0.0.1'

TestMod = 'test/'


def suicide():
    client = boto3.client("ec2", region_name='cn-north-1')
    a = os.popen("wget -q -O - http://169.254.169.254/latest/meta-data/instance-id")
    instance_id = a.read()
    response = client.terminate_instances(InstanceIds=instance_id)
    return response


def time_out_error(exception):
    print("*" * 50)
    print("""
        Exception Information : %s
        ReConnect !!!
        """ % exception)
    print("*" * 50)
    return True


# 周期运行装饰器
def period_task(period):
    def wraped_f(f):
        @wraps(f)
        def sched_fun(*args, **kwargs):
            s = sched.scheduler(time.time, time.sleep)
            
            def do_something(sc):
                f(*args, **kwargs)
                s.enter(period, 1, do_something, (sc,))
            
            s.enter(period, 1, do_something, (s,))
            s.run()
        
        return sched_fun
    
    return wraped_f


def reviews(appid):
    """
    评分统计 https://aso100.com/app/comment/appid/284882215/country/us
    """
    # url = 'https://itunes.apple.com/WebObjects/MZStore.woa/wa/customerReviews?id=%s&displayable-kind=11'
    # r = requests.get(url % appid, headers=headers)
    url = 'https://itunes.apple.com/WebObjects/MZStore.woa/wa/customerReviews?id={app_id}&displayable-kind=11'
    t = time.time()
    try:
        r = requests.get(url.format(app_id=appid), headers=header_dict.get(country))
        result = r.json()
        
        result['task'] = task + '_' + country
        result['fetch_time'] = t
        result['task_kw'] = appid
        result['time_now'] = t
        result['spider_version'] = __version__
    
    except:
        result = {'task': task + '_' + country, "fetch_time": t, "task_kw": appid, 'time_now': t, 'spider_version': __version__, "err_msg": "Result is Error !"}
    return result


@period_task(int(args[2]))
def lookup_run():
    try:
        task = q_in.get(timeout=0.1)
        if task['appid'] == 'None001':
            q_out.put(json.dumps(task))
            raise TypeError
        resp = reviews(**task)
        q_out.put(json.dumps(resp))
        print(resp.keys())
    except queue.Empty:
        pass
    except TimeoutError:
        pass


def run_in_thread(func, *args, **kwargs):
    """Run function in thread, return a Thread object"""
    from threading import Thread
    thread = Thread(target=func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread


def get_current_time(*args, **kwargs):
    utc = arrow.utcnow()
    local = utc.to('Asia/Shanghai')
    return str(local)


def producer(input_file):
    with open(input_file, "r") as fr:
        for item in fr:
            task = {"appid": item.strip()}
            q_in.put(task)
        end_dict = {'appid': 'None001'}
        q_in.put(end_dict)


def consumer(output_file):
    with open(output_file, 'w') as fw:
        while True:
            try:
                resp = q_out.get(timeout=0.1)
                fw.write(resp + '\n')
            except queue.Empty:
                continue


if __name__ == '__main__':
    # todo 这里增加了User-Agent，是用来解决苹果服务器判断程序是爬虫而拒绝访问，还有进行测试是否有效
    header_dict = {'cn': {"X-Apple-Store-Front": "143465-19,29", "User-Agent": "User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30"},
                   'us': {"X-Apple-Store-Front": "143441-1,29", "User-Agent": "User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30"}
                   }
    
    task_flag_dict = {'get_app_comment_count': 'app_comment_count'}
    
    # 输入文件，输出文件，间隔秒数, 国家, 设备(iphone, ipad), 开始时间, 任务名称(gat_app_comment_count, gat_app_comment_info)
    # /mnt/source_file/temp_source_file.txt /mnt/result_file/result_file.json 2 us iphone 20170424_135628 get_app_comment_count
    
    input_file = args[0]
    output_file = args[1]
    # interval = args[2]
    country = args[3]
    equ = args[4]
    start_time = args[5]
    task = args[6]
    
    # input_file = '/mnt/aso_data/source_files/test_appid_us.txt'
    # output_file = '/mnt/temp_files/comment_temp_file.json'
    # country = 'us'
    # equ = 'iphone'
    # start_time = '20170425_150101'
    # task = 'get_app_comment_info'
    
    task_flag = task_flag_dict[task]
    
    try:
        print("spider_start_time: ", start_time)
        run_in_thread(consumer, output_file)
        run_in_thread(producer, input_file)
        lookup_run()
        
    except TypeError:
        local_ip = "`ifconfig | grep 'inet addr' | head -n 1 | awk -F':' '{print $2}' | awk -F' ' '{print $1}' | sed 's/\./_/g'`"
        os.system("mv /mnt/result_file/* /mnt/result_file/{start_time}_{local_ip}_result.json".format(local_ip=local_ip, start_time=start_time))
        os.system("bzip2 /mnt/result_file/*")
        os.system("aws s3 cp /mnt/result_file/{start_time}_{local_ip}_result.json.bz2 s3://datawarehouse.lbadvisor.com/aso/{TestMod}{task_flag}/{equ}/{country}/{start_time}/".format(local_ip=local_ip, start_time=start_time, country=country,
                                                                                                                                                                                      equ=equ, task_flag=task_flag, TestMod=TestMod))

        os.system('touch /mnt/aso.{TestMod}{task_flag}.{equ}.{country}.{start_time}.{start_time}_{local_ip}_result'.format(local_ip=local_ip, start_time=start_time, country=country, equ=equ, task_flag=task_flag,
                                                                                                                           TestMod=TestMod.replace('/', '.')))
        os.system(
            'aws s3 cp /mnt/aso.{TestMod}{task_flag}.{equ}.{country}.{start_time}.{start_time}_{local_ip}_result s3://datawarehouse.lbadvisor.com/aso/%sspider_status_msg/'.format(local_ip=local_ip, start_time=start_time, country=country,
                                                                                                                                                                                   equ=equ, task_flag=task_flag,
                                                                                                                                                                                   TestMod=TestMod.replace('/', '.')) % TestMod)

        suicide()
    
    except:
        print(traceback.format_exc())
        print(traceback.print_exc())
        local_ip = "`ifconfig | grep 'inet addr' | head -n 1 | awk -F':' '{print $2}' | awk -F' ' '{print $1}' | sed 's/\./_/g'`"
        os.system("mv /mnt/result_file/* /mnt/result_file/{start_time}_{local_ip}_result.json".format(local_ip=local_ip, start_time=start_time))
        os.system("bzip2 /mnt/result_file/*")
        os.system("aws s3 cp /mnt/result_file/{start_time}_{local_ip}_result.json.bz2 s3://datawarehouse.lbadvisor.com/aso/{TestMod}{task_flag}/{equ}/{country}/{start_time}/".format(local_ip=local_ip, start_time=start_time, country=country,
                                                                                                                                                                                      equ=equ, task_flag=task_flag, TestMod=TestMod))

        os.system('touch /mnt/aso.{TestMod}{task_flag}.{equ}.{country}.{start_time}.{start_time}_{local_ip}_result'.format(local_ip=local_ip, start_time=start_time, country=country, equ=equ, task_flag=task_flag,
                                                                                                                           TestMod=TestMod.replace('/', '.')))
        os.system(
            'aws s3 cp /mnt/aso.{TestMod}{task_flag}.{equ}.{country}.{start_time}.{start_time}_{local_ip}_result s3://datawarehouse.lbadvisor.com/aso/%sspider_status_msg/'.format(local_ip=local_ip, start_time=start_time, country=country,
                                                                                                                                                                                   equ=equ, task_flag=task_flag,
                                                                                                                                                                                   TestMod=TestMod.replace('/', '.')) % TestMod)
