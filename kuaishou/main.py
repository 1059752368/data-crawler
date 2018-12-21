#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function

import requests
import pickle
import grpc
import datetime, time
import json
import chardet
import eff_upload_pb2 as eff
import eff_upload_pb2_grpc as eff_grpc

def get_config(user_id, client_id):
    # with grpc.insecure_channel('127.0.0.1:8086') as channel:
    with grpc.insecure_channel('172.16.6.48:8086') as channel:
        client = eff_grpc.effUploadStub(channel)
        resp = client.GetConfig(eff.config_capture_request(
            platform='kuaishou',
            user_info=eff.config_capture_request.info(
                operator_id=client_id,
                agent_id=client_id,
                advertiser_ids=[user_id]
            ),
            period=30))
        user_id2date = {}
        for item in resp.capture_list:
            user_id2date[item.advertiser_id] = item.dates
        return user_id2date

def post_data(req):
    with grpc.insecure_channel('172.16.6.48:8086') as channel:
        client = eff_grpc.effUploadStub(channel)
        while True:
            resp = client.Upload(req)
            if len(resp.errs) > 0:
                if resp.errs[0].Code == 540:
                    print('queue full, wait for 10s')
                    time.sleep(10)
                    continue
            break

header = {
    'Origin': 'https://ad.e.kuaishou.com',
    'Referer': 'https://ad.e.kuaishou.com/',
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15'
}

def date2ts(date):
    ts = time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple())
    return ts * 1000

def ts_add_day(ts, day):
    return ts + day * 24 * 3600 * 1000

def login(username, password):
    import traceback
    from selenium import webdriver
    # from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
    # from selenium.webdriver.support.ui import WebDriverWait
    # from selenium.common.exceptions import TimeoutException

    # driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', 
    #     desired_capabilities=DesiredCapabilities.CHROME)
    driver = webdriver.Chrome('/usr/bin/chromedriver')#'/Users/hongxiliang/Downloads/chromedriver'

    cookies = {}
    try:
        driver.set_page_load_timeout(40)
        driver.implicitly_wait(20) 
        driver.get('https://ad.e.kuaishou.com')
        
        driver.find_element_by_class_name('phone').find_element_by_tag_name('input').send_keys(username)
        driver.find_element_by_class_name('password').find_element_by_tag_name('input').send_keys(password)
        driver.find_element_by_class_name('login-item').find_element_by_class_name('foot').click()

        #implicitly wait for page load complete
        driver.get('https://ad.e.kuaishou.com/#/account/account-flow')
        time.sleep(20)
        cookies = driver.get_cookies()
        print(cookies)
        global header
        header['User-Agent'] = driver.execute_script("return navigator.userAgent;") 
    except:
        traceback.print_exc()
    finally:
        driver.quit()

    c = requests.cookies.cookiejar_from_dict({})
    for item in cookies:
        c.set(item['name'], item['value'])
    return c

def logout(cookies):
    r = requests.post(
        'https://id.kuaishou.com/pass/kuaishou/login/logout',
        data={'sid': 'kuaishou.ad.dsp'},
        cookies = cookies,
        headers = header
    )
    body = r.json()
    print(body)

def user_info(cookies):
    r = requests.post(
        'https://ad.e.kuaishou.com/rest/dsp/owner/info',
        params={'kuaishou.ad.dsp_ph': cookies.get('kuaishou.ad.dsp_ph')},
        cookies = cookies,
        headers = header
    )
    body = r.json()
    return body['user'], body['agentUserId'] # object, int
    # {
    #     "verified": false,
    #     "following": false,
    #     "headurls": [
    #         {
    #             "cdn": "ali2.a.yximgs.com",
    #             "url": "http://ali2.a.yximgs.com/uhead/AB/2018/11/01/11/BMjAxODExMDExMTE5NTNfMTEyNzU2MDA2MF8yX2hkODEyXzg2MA==_s.jpg",
    #             "urlPattern": "http://aliimg.a.yximgs.com/uhead/AB/2018/11/01/11/BMjAxODExMDExMTE5NTNfMTEyNzU2MDA2MF8yX2hkODEyXzg2MA==_s.jpg@0e_0o_0l_{h}h_{w}w_85q.src"
    #         },
    #         {
    #             "cdn": "js2.a.yximgs.com",
    #             "url": "http://js2.a.yximgs.com/uhead/AB/2018/11/01/11/BMjAxODExMDExMTE5NTNfMTEyNzU2MDA2MF8yX2hkODEyXzg2MA==_s.jpg",
    #             "urlPattern": "http://js2.a.yximgs.com/uhead/AB/2018/11/01/11/BMjAxODExMDExMTE5NTNfMTEyNzU2MDA2MF8yX2hkODEyXzg2MA==_s.jpg@base@tag%3DimgScale%26r%3D0%26q%3D85%26w%3D{w}%26h%3D{h}%26rotate"
    #         }
    #     ],
    #     "user_id": 1127560060,
    #     "user_sex": "F",
    #     "user_text": "爱玩，爱看，爱推荐",
    #     "headurl": "http://ali2.a.yximgs.com/uhead/AB/2018/11/01/11/BMjAxODExMDExMTE5NTNfMTEyNzU2MDA2MF8yX2hkODEyXzg2MA==_s.jpg",
    #     "user_name": "网易－明日之后"
    # }
    
def campaign_list(cookies):
    campaigns = []
    page = 1
    while True:
        r = requests.post(
            'https://ad.e.kuaishou.com/rest/dsp/control-panel/campaigns',
            params={'kuaishou.ad.dsp_ph': cookies.get('kuaishou.ad.dsp_ph')},
            json={
                'campaignId': 'null',
                'unitId': 'null',
                'pageInfo': {"currentPage":page,"pageSize":10,"totalCount":0},
                "reportStartDay": 1542816000000,
                "reportEndDay": 1542902399999
                },
            cookies = cookies,
            headers = header)
        body = r.json()
        for campaign in body['campaigns']:
            campaigns.append(campaign)
        total = body['pageInfo']['totalCount']
        n_per_page = body['pageInfo']['pageSize']
        if page * n_per_page < total:
            page += 1
        else:
            break
    return campaigns
    # [{
    #     "comment":0,
    #     "impression":7,
    #     "updateTime":1541062440003,
    #     "clickConversionRatio":0,
    #     "downloadCompletedRatio":0,
    #     "downloadInstalled":0,
    #     "unfollow":0,
    #     "share":0,
    #     "play3sCost":0,
    #     "formCount":0,
    #     "cancelLike":0,
    #     "play3sActionRatio":0,
    #     "likes":0,
    #     "playedEnd":0,
    #     "actionRatio":0,
    #     "follow":0,
    #     "click":0,
    #     "downloadConversionRatio":0,
    #     "formCost":0,
    #     "playedFiveSeconds":0,
    #     "conversion":0,
    #     "totalCharge":0,
    #     "campaignId":2078731,
    #     "negative":0,
    #     "createTime":1541062440003,
    #     "formActionRatio":0,
    #     "clickCost":0,
    #     "campaignName":"明日之后-CPC",
    #     "click1kCost":0,
    #     "dayBudget":24000000,
    #     "conversionCost":0,
    #     "downloadCompleted":0,
    #     "actionCost":0,
    #     "actionbarClick":0,
    #     "playedThreeSeconds":0,
    #     "impression1kCost":0,
    #     "viewStatus":1,
    #     "downloadStarted":0,
    #     "downloadStartedCost":0,
    #     "report":0,
    #     "viewStatusReason":"广告计划已暂停",
    #     "clickRatio":0,
    #     "impressionBudget":0,
    #     "putStatus":2,
    #     "type":2,
    #     "photoClick":0,
    #     "downloadStartedRatio":0,
    #     "downloadCompletedCost":0,
    #     "photoClickRatio":0,
    #     "play3sRatio":0,
    #     "block":0
    # }]

def ad_list(cookies):
    ads = []
    page = 1
    while True:
        r = requests.post(
            'https://ad.e.kuaishou.com/rest/dsp/control-panel/units',
            params={'kuaishou.ad.dsp_ph': cookies.get('kuaishou.ad.dsp_ph')},
            json={
                'campaignId': 'null',
                'unitId': 'null',
                'pageInfo': {"currentPage":page,"pageSize":10,"totalCount":0},
                "reportStartDay": 1542816000000,
                "reportEndDay": 1542902399999
                },
            cookies = cookies,
            headers = header)
        body = r.json()
        for ad in body['units']:
            ads.append(ad)
        total = body['pageInfo']['totalCount']
        n_per_page = body['pageInfo']['pageSize']
        if page * n_per_page < total:
            page += 1
        else:
            break
    return ads
    # [{
    #     "viewStatus":1,
    #     "viewStatusReason":"广告计划已暂停",
    #     "formCount":0,
    #     "conversion":90,
    #     "totalCharge":889318,
    #     "impression":35309,
    #     "click":5101,
    #     "playedThreeSeconds":3298,
    #     "playedFiveSeconds":2959,
    #     "photoClick":4812,
    #     "actionbarClick":95,
    #     "likes":63,
    #     "negative":33,
    #     "playedEnd":418,
    #     "cancelLike":7,
    #     "comment":3,
    #     "follow":12,
    #     "share":0,
    #     "report":0,
    #     "unfollow":0,
    #     "block":1,
    #     "downloadCompleted":0,
    #     "downloadStarted":0,
    #     "downloadInstalled":0,
    #     "clickRatio":0.1444674162394857,
    #     "photoClickRatio":0.1362825341980798,
    #     "play3sRatio":0.6465398941384042,
    #     "actionRatio":0.01862379925504803,
    #     "play3sActionRatio":0.028805336567616736,
    #     "downloadStartedRatio":0,
    #     "downloadCompletedRatio":0,
    #     "downloadConversionRatio":0,
    #     "clickConversionRatio":0.01764359929425603,
    #     "formActionRatio":0,
    #     "formCost":0,
    #     "impression1kCost":25.18672293183041,
    #     "click1kCost":174.34189374632425,
    #     "play3sCost":0.26965372953305033,
    #     "clickCost":0.17434189374632425,
    #     "actionCost":9.361242105263157,
    #     "conversionCost":9.881311111111112,
    #     "downloadStartedCost":0,
    #     "downloadCompletedCost":0,
    #     "unitId":2317157,
    #     "campaignId":2085165,
    #     "campaignName":"明日之后-OCPA",
    #     "putStatus":1,
    #     "beginTime":1541750678754,
    #     "endTime":null,
    #     "bid":0,
    #     "bidType":6,
    #     "cpaBid":14000,
    #     "ocpcStage":1,
    #     "ocpxActionType":180,
    #     "dayBudget":0,
    #     "impressionBudget":0,
    #     "level":0,
    #     "p3trLevel":0,
    #     "ltrLevel":0,
    #     "htrLevel":0,
    #     "target":{
    #         "id":852,
    #         "gender":"M",
    #         "md5":"8868e8d98f77e4718aff7bcf9d96b09b",
    #         "age":[
    #             {
    #                 "min":18,
    #                 "max":23
    #             },
    #             {
    #                 "min":24,
    #                 "max":30
    #             },
    #             {
    #                 "min":31,
    #                 "max":40
    #             },
    #             {
    #                 "min":41,
    #                 "max":49
    #             },
    #             {
    #                 "min":12,
    #                 "max":17
    #             }
    #         ],
    #         "platform":{
    #             "ios":{
    #                 "min":6,
    #                 "max":null
    #             }
    #         },
    #         "region":Array[369],
    #         "language":Array[0],
    #         "deviceBrand":Array[0],
    #         "devicePrice":Array[0],
    #         "page":Array[1],
    #         "network":Array[1],
    #         "interest":Array[0],
    #         "audience":Array[0],
    #         "paidAudience":Array[0],
    #         "fansStar":Array[0],
    #         "interestVideo":Array[0],
    #         "businessInterest":Array[0],
    #         "packageName":Array[0],
    #         "population":Array[0]
    #     },
    #     "unitDiverseData":{
    #         "appName":"明日之后",
    #         "appPackageName":"",
    #         "deviceOsType":2
    #     },
    #     "platform":{
    #         "ios":{
    #             "min":6,
    #             "max":null
    #         }
    #     },
    #     "unitSchedule":[

    #     ],
    #     "campaignType":2,
    #     "h5App":0,
    #     "appId":59204,
    #     "showMode":2,
    #     "speed":1,
    #     "webUri":"https://itunes.apple.com/cn/app/id1435567000",
    #     "appIconUrl":"",
    #     "uri":"https://itunes.apple.com/cn/app/id1435567000",
    #     "schemaUri":"",
    #     "useCdn":false,
    #     "unitName":"1109-商店视频16"
    # }]

def creative_list(cookies):
    creatives = []
    page = 1
    while True:
        r = requests.post(
            'https://ad.e.kuaishou.com/rest/dsp/control-panel/creatives',
            params={'kuaishou.ad.dsp_ph': cookies.get('kuaishou.ad.dsp_ph')},
            json={
                'campaignId': 'null',
                'unitId': 'null',
                'pageInfo': {"currentPage":page,"pageSize":10,"totalCount":0},
                "reportStartDay": 1542816000000,
                "reportEndDay": 1542902399999
                },
            cookies = cookies,
            headers = header)
        body = r.json()
        for creative in body['creatives']:
            photo_id = creative.get('photoId', '')
            if photo_id != '':
                photo_info = get_creative_photo(cookies, photo_id)
                creative['photoInfo'] = photo_info
            creatives.append(creative)
        total = body['pageInfo']['totalCount']
        n_per_page = body['pageInfo']['pageSize']
        if page * n_per_page < total:
            page += 1
        else:
            break
    return creatives
    # {
    #     "viewStatus": 1,
    #     "viewStatusReason": "广告计划已暂停",
    #     "formCount": 0,
    #     "conversion": 90,
    #     "totalCharge": 889318,
    #     "impression": 35309,
    #     "click": 5101,
    #     "playedThreeSeconds": 3298,
    #     "playedFiveSeconds": 2959,
    #     "photoClick": 4812,
    #     "actionbarClick": 95,
    #     "likes": 63,
    #     "negative": 33,
    #     "playedEnd": 418,
    #     "cancelLike": 7,
    #     "comment": 3,
    #     "follow": 12,
    #     "share": 0,
    #     "report": 0,
    #     "unfollow": 0,
    #     "block": 1,
    #     "downloadCompleted": 0,
    #     "downloadStarted": 0,
    #     "downloadInstalled": 0,
    #     "clickRatio": 0.1444674162394857,
    #     "photoClickRatio": 0.1362825341980798,
    #     "play3sRatio": 0.6465398941384042,
    #     "actionRatio": 0.01862379925504803,
    #     "play3sActionRatio": 0.028805336567616736,
    #     "downloadStartedRatio": 0,
    #     "downloadCompletedRatio": 0,
    #     "downloadConversionRatio": 0,
    #     "clickConversionRatio": 0.01764359929425603,
    #     "formActionRatio": 0,
    #     "formCost": 0,
    #     "impression1kCost": 25.18672293183041,
    #     "click1kCost": 174.34189374632425,
    #     "play3sCost": 0.26965372953305033,
    #     "clickCost": 0.17434189374632425,
    #     "actionCost": 9.361242105263157,
    #     "conversionCost": 9.881311111111112,
    #     "downloadStartedCost": 0,
    #     "downloadCompletedCost": 0,
    #     "creativeId": 2569614,
    #     "campaignId": 2085165,
    #     "unitId": 2317157,
    #     "unitName": "1109-商店视频16",
    #     "campaignName": "明日之后-OCPA",
    #     "putStatus": 1,
    #     "creativeDisplayInfo": {
    #         "actionBar": "立即下载",
    #         "description": "网易首款末世生存手游《明日之后》重磅来袭!饥饿、病毒、感染者[衰]阻碍着你生存的脚步,在这里,你需要采集、狩猎、烹饪填饱肚子,制作药品、武器保全自己,你做好生存的准备了吗?"
    #     },
    #     "campaignType": 2,
    #     "level": 0,
    #     "p3trLevel": 0,
    #     "ltrLevel": 0,
    #     "htrLevel": 0,
    #     "category": "",
    #     "clickUrl": "https://affiliate.youmi.net/ios/v1/recv?s=a8f004bd3bTr7TUs2CQTFy0DZtYDqrd7sm4&idfa=__IDFA__&callback_url=__CALLBACK__&ip=__IP__",
    #     "coverWidth": 720,
    #     "coverHeight": 960,
    #     "coverUrl": "http://static.yximgs.com/udata/pkg/cover_compose_d57a8a1d39044a0099103aee6470d8f6.jpg",
    #     "impressionUrl": "",
    #     "photoId": "5200250204559572191",
    #     "screenshots": "",
    #     "creativeTags": [],
    #     "appDetailType": 0,
    #     "actionbarClickUrl": "",
    #     "creativeName": "1109-商店视频16",
    #     "material1": 0,
    #     "material2": 0
    # }

def get_creative_photo(cookies, photo_id):
    r = requests.post(
        'https://ad.e.kuaishou.com/rest/dsp/photo/info',
        params={'kuaishou.ad.dsp_ph': cookies.get('kuaishou.ad.dsp_ph')},
        json={'photoId': str(photo_id)},
        cookies = cookies,
        headers = header)
    body = r.json()
    return body.get('photoInfo', {})

def get_data(cookies, start_date, end_date):
    datas = []
    page = 1

    start_ts, end_ts = date2ts(start_date), ts_add_day(date2ts(end_date), 1)-1
    while True:
        r = requests.post(
            'https://ad.e.kuaishou.com/rest/dsp/report/effect/detailedReport',
            params={'kuaishou.ad.dsp_ph': cookies.get('kuaishou.ad.dsp_ph')},
            json={
                "viewType": 4,
                "startTime": start_ts,
                "endTime": end_ts,
                "groupType": 1,
                "campaignType": -1,
                "idSet": [],
                "sortingColumn": "",
                "order": 0,
                'pageInfo': {"currentPage":page,"pageSize":20,"totalCount":0},
                },
            cookies = cookies,
            headers = header)
        body = r.json()
        for data in body['resultList']:
            datas.append({
                'cost': float(data['totalCharge'])/1000.0,
                'view_count': data['impression'],
                'click_count': data['click'],
                'click_rate': data['clickRatio'],
                'cpc': data['click1kCost'],
                'cpm': data['impression1kCost'],
                'checkout_rate': data['actionRatio'],
                'like_count': data['likes'],
                'comment_count': data['comment'],
                'follow_count': data['follow']
            })
        total = body['pageInfo']['totalCount']
        n_per_page = body['pageInfo']['pageSize']
        if page * n_per_page < total:
            page += 1
        else:
            break
    return datas
    # {
    #     "impression": 3, #曝光数
    #     "photoClick": 0,
    #     "click": 0, #点击数
    #     "actionbarClick": 0, #行为数
    #     "formCount": 0,
    #     "downloadStarted": 0,
    #     "downloadCompleted": 0,
    #     "downloadInstalled": 0,
    #     "conversion": 0,
    #     "playedThreeSeconds": 0,
    #     "playedFiveSeconds": 0,
    #     "playedEnd": 0,
    #     "likes": 0, 
    #     "cancelLike": 0,
    #     "comment": 0,
    #     "follow": 0,
    #     "share": 0,
    #     "report": 0,
    #     "negative": 0,
    #     "unfollow": 0,
    #     "block": 0,
    #     "totalCharge": 0, #花费
    #     "reportHour": 0,
    #     "campaignId": 2078731,
    #     "campaignName": "明日之后-CPC",
    #     "unitId": 2298308,
    #     "unitName": "1011-商店视频06",
    #     "creativeId": 2543171,
    #     "creativeName": "1101-商店视频06",
    #     "photoInfo": null,
    #     "play3sActionRatio": 0,
    #     "click1kCost": 0,
    #     "conversionCost": 0,
    #     "impression1kCost": 0, #平均千次曝光花费
    #     "actionRatio": 0, #行为率
    #     "downloadStartedCost": 0,
    #     "downloadStartedRatio": 0,
    #     "downloadCompletedCost": 0,
    #     "downloadCompletedRatio": 0,
    #     "downloadConversionRatio": 0,
    #     "clickConversionRatio": 0,
    #     "formActionRatio": 0,
    #     "formCost": 0,
    #     "photoClickRatio": 0, 
    #     "play3sRatio": 0,
    #     "actionCost": 0, #平均行为单价
    #     "clickCost": 0, #平均点击单价
    #     "clickRatio": 0, #点击率
    #     "play3sCost": 0,
    #     "reportDate": "2018-11-26",
    #     "reportDateHour": "2018-11-26 00:00"
    # }

def app_list(cookies):
    apps = []
    r = requests.post(
        'https://ad.e.kuaishou.com/rest/dsp/control-panel/app/list',
        params={'kuaishou.ad.dsp_ph': cookies.get('kuaishou.ad.dsp_ph')},
        cookies = cookies,
        headers = header)
    body = r.json()
    for app in body['apps']:
        apps.append(app)
    return apps
    # {
    #     "appId": 59204,
    #     "accountId": 10906,
    #     "appVersion": "明日之后-IOS",
    #     "appName": "明日之后",
    #     "packageName": "",
    #     "sysPackageName": null,
    #     "packageSize": 0,
    #     "url": "https://itunes.apple.com/cn/app/id1435567000",
    #     "appIconUrl": "",
    #     "h5App": false,
    #     "platform": "ios",
    #     "virusScore": -2,
    #     "updateTime": 1541062092437,
    #     "date": 1541001600000,
    #     "appScore": 37
    # }

def run():
    import pymysql
    import traceback

    conn = pymysql.connect(host='172.16.6.48', user='ymserver', passwd='123321', db='cs_0930')
    cur = conn.cursor()
    cur.execute('select username, password from platform_account where platform="kuaishou"')
    rows = cur.fetchall()

    for row in rows:
        try:
            cookie = login(row[0], row[1])
            fetch(cookie)
        except:
            traceback.print_exc()
        finally:
            logout(cookie)
        break

def fetch(cookie):
    # cookie = cookies()
    # print(cookie)
    users = user_info(cookie)
    campaigns = campaign_list(cookie)
    ads = ad_list(cookie)
    creatives = creative_list(cookie)
    apps = app_list(cookie)

    # print('users')
    # print(users)
    # print('campaigns')
    # print(campaigns)
    # print('ads')
    # print(ads)
    # print('creatives')
    # print(creatives)
    # print('apps')
    # print(apps)

    # save(users, campaigns, ads, creatives, apps)
    # users, ads, campaigns, creatives, apps = load()
    print(len(users), len(campaigns), len(ads), len(creatives), len(apps))

    agent_id = str(users[1])
    user_id = str(users[0]['user_id'])
    cfg = get_config(user_id, agent_id)

    cid2info = {}
    for campaign in campaigns:
        cid2info[campaign['campaignId']] = campaign
    appid2info = {}
    for app in apps:
        appid2info[app['appId']] = app
    adid2info = {}
    for ad in ads:
        ad['app'] = appid2info.get(ad['appId'], {})
        adid2info[ad['unitId']] = ad
    crid2info = {}
    for creative in creatives:
        crid2info[creative['creativeId']] = creative


    user = eff.upload_request.ad_account(
        advertiser = eff.upload_request.ad_account.ad_advertiser(
            id = user_id,
            name = users[0]['user_name']
        ),
        agent = eff.upload_request.ad_account.ad_agent(
            id = agent_id,
            name = ''
        ),
        operator = eff.upload_request.ad_account.ad_operator(
            id = agent_id,
            name = ''
        )
    )

    for date in cfg[user_id]:
        creative_datas = []
        datas = get_data(cookie, date, date)

        print(datas)
        for data in datas:
            if data.get('reportDate', '') != date:
                continue
            ds = []
            # rearrange
            for key in data:
                val = data[key]
                if val is None:
                    continue
                if type(val).__name__ != 'int' and type(val).__name__ != 'float':
                    print(key, val)
                    continue
                    # print(chardet.detect(val))
                ds.append(eff.upload_request.ads.data(
                    key = key,
                    val = str(val),
                    dtype = type(val).__name__
                ))
            c = cid2info[data['campaignId']]
            ad = adid2info[data['unitId']]
            cr = crid2info[data['creativeId']]

            photo_info = cr['photoInfo']
            slogan = photo_info['caption']
            video_url = photo_info['mainMvUrls'][0]['url']
            image_url = photo_info['coverUrls'][0]['url']

            creative_datas.append(eff.upload_request.ads(
                    campaign = eff.upload_request.ads.ad_campaign(
                        id = str(c['campaignId']),
                        name = c['campaignName'],
                        original_data = json.dumps(c)
                    ),
                    ad = eff.upload_request.ads.ad_advertise(
                        id = str(ad['unitId']),
                        name = ad['unitName'],
                        original_data = json.dumps(ad)
                    ),
                    creative = eff.upload_request.ads.ad_creative(
                        id = str(cr['creativeId']),
                        name = cr['creativeName'],
                        slogan = slogan,
                        images = image_url,
                        videos = video_url,
                        original_data = json.dumps(cr)
                    ),
                    datas = ds
                ))
        req = eff.upload_request(
            date = date,
            platform = 'kuaishou',
            user_info = user,
            creatives = creative_datas
        )
        post_data(req)


if __name__ == '__main__':
    run()
