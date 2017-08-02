#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
import json
import traceback
import fileinput
import bz2
import sys
import requests
import copy
import csv
import codecs
from openpyxl import Workbook


def handling_01():
    __version__ = '0.0.1'
    country = 'us'
    task = 'get_app_comment_count_{country}'.format(country=country)
    
    input_file = '/mnt/temp_files/handling_data/all_file.json'
    output_file = '/mnt/temp_files/handling_data/output_file.json'
    t = time.time()
    
    rf = open(input_file)
    wf = open(output_file, 'w')
    
    num = 0
    for index_num, line in enumerate(rf):
        try:
            result = json.loads(line)
            app_id = result['adamId']
            result['task'] = task
            result['fetch_time'] = t
            result['task_kw'] = app_id
            result['time_now'] = t
            result['spider_version'] = __version__
            # print(index_num, result)
            wf.write(json.dumps(result) + '\n')
        except:
            num += 1
            print(traceback.format_exc())
            result = {'task': task, "fetch_time": t, "task_kw": result['app_id'], 'time_now': t, 'spider_version': __version__, "err_msg": "Result is Error !"}
            print(num, result)
            wf.write(json.dumps(result) + '\n')
    rf.close()


def handling_02():
    input_file = '/mnt/temp_files/handling_data/second_result.json'
    output_file = '/mnt/aso_data/source_files/true_second_app_id.txt'
    
    # rf = fileinput.input(input_file, openhook=bz2.BZ2File)
    # wf = open(output_file, 'w')
    # for line in rf:
    #     result = json.loads(line.decode('utf8'))
    #     app_id = result['task_kw']
    #     wf.write(str(app_id) + '\n')
    #     print(fileinput.lineno(), app_id)
    #
    # rf.close()
    # wf.close()
    rf = open(input_file)
    wf = open(output_file, 'w')
    for line in rf:
        result = json.loads(line)
        wf.write(result['task_kw'] + '\n')
        print(type(result), result)
    
    rf.close()
    wf.close()


def handling_03():
    input_file_1 = '/mnt/aso_data/source_files/second_app_id_us.txt'
    input_file_2 = '/mnt/aso_data/source_files/true_second_app_id_us.txt'
    output_file = '/mnt/aso_data/source_files/third_app_id_us.txt'
    
    rf_1 = open(input_file_1)
    rf_2 = open(input_file_2)
    wf = open(output_file, 'w')
    
    a_set = set([i for i in rf_1])
    b_set = set([i for i in rf_2])
    
    c_set = a_set - b_set
    c_list = list(c_set)
    
    # print(type(c_list), c_list[:100])
    for i in c_list:
        wf.write(i)
    
    rf_1.close()
    rf_2.close()
    wf.close()


def handling_04():
    input_file = '/mnt/temp_files/handling_data/all_result_file.json'
    # input_file = '/mnt/temp_files/handling_data/third_result.json'
    
    rf = open(input_file)
    wf_1 = open('/mnt/aso_data/source_files/lt_2000_appid.txt', 'w')
    wf_2 = open('/mnt/aso_data/source_files/lt_10000_appid.txt', 'w')
    wf_3 = open('/mnt/aso_data/source_files/lt_32000_appid.txt', 'w')
    wf_4 = open('/mnt/aso_data/source_files/gt_32000_appid.txt', 'w')
    
    result_0 = 0
    result_1 = 0
    result_2 = 0
    result_3 = 0
    result_4 = 0
    max_list = []
    for line in rf:
        # print(json.loads(line).keys())
        result = json.loads(line)
        # print(result.get("task_kw"), result.get("totalNumberOfReviews"))
        # break
        comment_count = int(result.get("totalNumberOfReviews", -1))
        if comment_count == -1:
            result_0 += 1
            continue
        app_id = str(result.get("task_kw"))
        
        if 0 < comment_count < 2000:
            result_1 += 1
            wf_1.write(app_id + '\n')
        if 2000 < comment_count < 10000:
            result_2 += 1
            wf_2.write(app_id + '\n')
        if 10000 < comment_count < 32000:
            result_3 += 1
            wf_3.write(app_id + '\n')
        if 32000 < comment_count:
            result_4 += 1
            wf_4.write(app_id + '\n')
            # max_list.append([app_id, comment_count])
            # print(app_id, comment_count)
    # print(sorted(max_list, key=lambda x: x[-1], reverse=True)[:10])
    print('result_0 :', result_0, 'result_1 :', result_1, 'result_2 :', result_2, 'result_3 :', result_3, 'result_4 :', result_4)

    # [['389801252', 1402126], ['488628250', 1123207], ['284882215', 891300], ['321916506', 584627], ['429047995', 565659], [491113310, 497601], [282935706, 461762], ['553834731', 438363], [420009108, 394329], ['573511269', 373952]]


def reviews_row(appid, url):
    """
    评分详情 https://aso100.com/app/commentList/appid/284882215/country/us
    """
    header_dict = {'cn': {"X-Apple-Store-Front": "143465-19,29", "User-Agent": "User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30"},
                   'us': {"X-Apple-Store-Front": "143441-1,29", "User-Agent": "User-Agent:Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.1 Safari/603.1.30"}
                   }
    # url = 'https://itunes.apple.com/WebObjects/MZStore.woa/wa/userReviewsRow?id=%s&displayable-kind=11&startIndex=%s&endIndex=%s&sort=4&appVersion=all'
    # r = requests.get(url % (appid, start_index, end_index), headers=headers)
    # totalNumberOfReviews = reviews(appid) 47922
    # json.dump(result, sys.stdout, indent=4, ensure_ascii=False)
    
    # url = 'https://itunes.apple.com/WebObjects/MZStore.woa/wa/userReviewsRow?id={app_id}&displayable-kind=11&startIndex={start_index}&endIndex={end_index}&sort=4&appVersion=all'
    
    try:
        r = requests.get(url.format(app_id=appid, start_index=start_index, end_index=end_index), headers=header_dict.get(country))
        result = r.json()
        
        # result['task'] = task + '_' + country
        # result['fetch_time'] = t
        # result['task_kw'] = appid
        # result['time_now'] = t
        # result['spider_version'] = __version__
    
    except:
        result = {'task': task, "fetch_time": t, "task_kw": appid, 'time_now': t, 'spider_version': __version__, "err_msg": "Result is Error !"}
        print(result)
    return result


def current_version_result(appid, url):
    result = reviews_row(appid, url)
    with open('/mnt/temp_files/current_version_{app_id}_comment_info.csv'.format(app_id=appid), 'w', encoding='utf-8') as wf:
        key_list = result['userReviewList'][0].keys()
        spamwriter = csv.writer(wf, dialect='excel',  quoting=csv.QUOTE_ALL)
        spamwriter.writerow(key_list)
        for t_dict in result['userReviewList']:
            value_list = t_dict.values()
            spamwriter.writerow(value_list)
        

def all_version_result(appid, url):
    result = reviews_row(appid, url)
    with codecs.open('/mnt/temp_files/all_version_{app_id}_comment_info.csv'.format(app_id=appid), 'w', encoding='utf-8') as wf:
        key_list = list(result['userReviewList'][0].keys())
        key_list.pop(key_list.index('reportConcernExplanation'))
        key_list.remove('reportConcernReasons')
        spamwriter = csv.writer(wf, dialect='excel',  quoting=csv.QUOTE_ALL)
        spamwriter.writerow(key_list)
        for t_dict in result['userReviewList']:
            del t_dict['reportConcernExplanation']
            del t_dict['reportConcernReasons']
            value_list = t_dict.values()
            spamwriter.writerow(value_list)
        
    
def create_excle_file(appid, url, version):
    wb = Workbook()
    ws = wb.active
    result = reviews_row(appid, url)
    key_list = list(result['userReviewList'][0].keys())
    key_list.pop(key_list.index('reportConcernExplanation'))
    key_list.remove('reportConcernReasons')
    ws.append(key_list)
    try:
        for t_dict in result['userReviewList']:
            del t_dict['reportConcernExplanation']
            del t_dict['reportConcernReasons']
            t_list = list(t_dict.values())
            for i in t_list:
                if isinstance(i, dict):
                    print('##', type(i), i)
                    t_list[t_list.index(i)] = i.get('body')
                    # print('**', type(t_list[t_list.index(i)]), t_list[t_list.index(i)])
            # print(t_list)
            ws.append(t_list)
        wb.save("/mnt/temp_files/{version}_version_{app_id}_comment_info.xlsx".format(app_id=appid, version=version))
    except:
        # print("*****", t_list)
        print(traceback.print_exc())
        pass
    
if __name__ == '__main__':
    pass
    start_index = 0
    end_index = 999
    t = time.time()
    country = 'cn'
    task = 'test_task'
    __version__ = '0.0.1'
    # 当前版本app的评论详情，{'sort': 1, 'name': '最有帮助'}, {'sort': 2, 'name': '最高评价'}, {'sort': 3, 'name': '最低评价'}, {'sort': 4, 'name': '最新发表'}
    current_url = 'https://itunes.apple.com/WebObjects/MZStore.woa/wa/userReviewsRow?appVersion=current&id={app_id}&displayable-kind=11&startIndex={start_index}&endIndex={end_index}&sort=1'
    # 所有版本app的评论详情，{'sort': 1, 'name': '最有帮助'}, {'sort': 2, 'name': '最高评价'}, {'sort': 3, 'name': '最低评价'}, {'sort': 4, 'name': '最新发表'}
    all_version_url = 'https://itunes.apple.com/WebObjects/MZStore.woa/wa/userReviewsRow?id={app_id}&displayable-kind=11&startIndex={start_index}&endIndex={end_index}&sort=1'

    app_id_list = ['989673964', '1126393139', '586871187', '458318329', '1069297969', '688137161']
    for app_id in app_id_list:
        create_excle_file(app_id, current_url, 'current')
        create_excle_file(app_id, all_version_url, 'all')
    
    
    
    
    
    
    