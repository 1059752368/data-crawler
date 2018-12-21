#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function

import pickle
import requests
import grpc
import json
import time
import pymysql
# from pb import eff_upload_pb2 as eff
# from pb import eff_upload_pb2_grpc as eff_grpc
import eff_upload_pb2 as eff
import eff_upload_pb2_grpc as eff_grpc

def get_config(user_id, client_id):
    with grpc.insecure_channel('172.16.6.48:8086') as channel:
        client = eff_grpc.effUploadStub(channel)
        resp = client.GetConfig(eff.config_capture_request(
            platform='weilianbo',
            user_info=eff.config_capture_request.info(
                operator_id=client_id,
                agent_id=client_id,
                advertiser_ids=[user_id]
            ),
            period=30))
        user_id2date, user_id2creatives = {}, {}
        for item in resp.capture_list:
            user_id2date[item.advertiser_id] = item.dates
            user_id2creatives[item.advertiser_id] = item.creatives
        return user_id2date, user_id2creatives

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


headers = {
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'https://ad.uniscrm.cn/res/static/dsp/index.html',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
}

def login(username, password):
    r = requests.post(
        url = 'https://ad.uniscrm.cn/login',
        headers = headers,
        data = {'u': username, 'p': password}
    )
    body = r.json()
    # {u'dsp_user': {u'updateTime': 1540742400000, u'money': 3932347759, u'uniscrmAccount': None, u'linkman': u'\u5434\u5c0f\u59d0', u'authority': 1, u'totalIn': 0, u'brn': u'', u'id': 303, u'certificate': u'', u'psw': u'', u'customerUserList': None, u'role': 20, u'financialPsw': u'', u'company': u'\u5e7f\u5dde\u7f51\u6613\u8ba1\u7b97\u673a\u7cfb\u7edf\u6709\u9650\u516c\u53f8', u'status': 1, u'noticeType': 0, u'memo': u'', u'phone': u'13580557992', u'createTime': 1540798291000, u'totalOut': 0, u'name': u'mingrizhihou', u'license': u'', u'frozen': 0, u'bcc': u'', u'agentUserList': None, u'todayCost': 0, u'email': u'466851283@qq.com'}, u'code': 1, u'platforms': [1]}
    if body['code'] != 1:
        raise Exception('login failed')
    print(body['dsp_user'])
    return r.cookies, body['dsp_user']

def logout(cookies):
    r = requests.get(
        url = 'https://ad.uniscrm.cn/logout',
        headers = headers
    )
    print(r.json())

def child_account_list(cookies):
    r = requests.get(
        url = 'https://ad.uniscrm.cn/agent/customer',
        headers = headers,
        cookies = cookies
    )
    body = r.json()
    accounts = filter(lambda x: x['totalOut']>0, body['msg'])
    return list(map(lambda x: x['id'], accounts))

def switch_to_child_account(cookies, account_id):
    r = requests.post(
        url = 'https://ad.uniscrm.cn/agent/switch/customer',
        params = {'customerUserId': account_id},
        headers = headers,
        cookies = cookies
    )
    body = r.json()
    return body['dsp_user']

def switch_back_parent_account(cookies):
    requests.post(
        url = 'https://ad.uniscrm.cn/user/switch/agent',
        headers = headers,
        cookies = cookies
    )
    # body = r.json()
    # parent user info
    # return body['dsp_user']

def ad_client_id(cookies):
    r = requests.get(
        url='https://ad.uniscrm.cn/my/cust/listall', 
        params={'platform': 0},
        headers=headers,
        cookies=cookies
    )
    body = r.json()
    # clientId: "clt1kg03x85"
    # clientName: "广州网易计算机系统有限公司"
    return body[0]
    

def campaign_list(cookies):
    page, number = 1, 10
    cid2info = {}
    while True:
        r = requests.get(
            url='https://ad.uniscrm.cn/my/adgroup/get', 
            params={'page': page, 'number': number},
            headers=headers,
            cookies=cookies
        )
        body = r.json()
        for campaign in body['msg']:
            cid2info[campaign['id']] = {
                'id': str(campaign['id']),
                'name': campaign['name'],
                'advertiser_id': campaign['userId'], #int
                #ads
            }
        total = body['total']
        if page * number < total:
            page += 1
        else:
            break
    return cid2info

def ad_list(cookies):
    page, number = 1, 10
    adid2info = {}
    while True:
        r = requests.get(
            url= 'https://ad.uniscrm.cn/my/task/user/last', 
            params={'version':1, 'page': page, 'pageSize': number},
            headers=headers,
            cookies=cookies
        )
        body = r.json()
        for ad in body['list']:
            ad['id'] = ad['creativeId']
            ad['name'] = ad['title']
            adid2info[ad['creativeId']] = ad
        total = body['total']
        if page * number < total:
            page += 1
        else:
            break
    return adid2info

def creative_list(cookies):
    page, number = 1, 10
    crids = set()
    while True:
        r = requests.get(
            url= 'https://ad.uniscrm.cn/my/adinfo/user/get', 
            params={'version':1, 'page': page, 'pageSize': number},
            headers=headers,
            cookies=cookies
        )
        body = r.json()
        for creative in body['list']:
            crids.add(creative['creativeId'])
        total = body['total']
        if page * number < total:
            page += 1
        else:
            break
    crid2info = {}
    for crid in crids:
        crid2info[crid] = creative_detail(cookies, crid)
    return crid2info

def creative_detail(cookies, creative_id):
    r = requests.get(
        url= 'https://ad.uniscrm.cn/my/adinfo/msg', 
        params={'crtId': creative_id},
        headers=headers,
        cookies=cookies
    )
    body = r.json()
    if body['code'] != 1:
        print('error get msg of creative', creative_id)
        return {}
    creative = body['msg']
    creative['id'] = creative['creativeId']
    creative['name'] = creative['title']
    creative['cid'] = creative['groupId']
    return creative

def get_data(cookies, date):
    page, number = 1, 20
    id2info = {}
    while True:
        r = requests.get(
            url= 'https://ad.uniscrm.cn/my/datacenter/ad',
            params={'adTitle': '', 'groupName': '', 'start': date, 'end': date, 'page': page, 'pageSize': number},
            headers=headers,
            cookies=cookies
        )
        body = r.json()
        if not body.__contains__('rows'):
            print(r, body)
            return {}
        if len(body['rows']) == 0:
            break
        for data in body['rows']:
            d = {
                # 'id': data['creativeId'],
                'view_count': data['impNum'], # 曝光量
                'clk_count': data['clickNum'], # 互动数
                'repost_count': 0, # 转发数
                'comment_count': 0,  # 评论数
                'like_count': 0,  # 赞次数
                'follow_count': 0,  # 关注数
                'url_clk_count': 0,  # 短链点击
                'page_clk_count': 0,  # 正文点击
                'img_clk_count': 0, # 图片点击数
                'vid_clk_count': 0,  # 视频点击数
                'card_clk_count': 0,  # card图文点击
                'card_button_clk_count': 0, # card图片按钮点击
                'img_slip_count': 0, # 图片横滑数
                'img_tag_clk_count': 0, # 图片标签点击数
                'interact_bid_count': data['bidNum'], # 竞价次数
                'interact_win_count': data['impNum'], # 胜出数
                'cpm': data['cpmCost']/100.0, # 曝光成本(千次)
                'interact_cost': data['interactCost'], #单次互动成本
                'cost': data['totalPay']/100000.0, # 消耗
            }
            if body.__contains__('stats'):
                d['repost_count'] = data['stats']['repost'] # 转发数
                d['comment_count'] = data['stats']['comment']  # 评论数
                d['like_count'] = data['stats']['like']  # 赞次数
                d['follow_count'] = data['stats']['follow']  # 关注数
                d['url_clk_count'] = data['stats']['urlClick']  # 短链点击
                d['page_clk_count'] = data['stats']['pageClick']  # 正文点击
                d['img_clk_count'] = data['stats']['pciClick'] # 图片点击数
                d['vid_clk_count'] = data['stats']['videoClick']  # 视频点击数
                d['card_clk_count'] = data['stats']['cardClick']  # card图文点击
                d['card_button_clk_count'] = data['stats']['buttonClick'] # card图片按钮点击
                d['img_slip_count'] = data['stats']['gridImgSlip'] # 图片横滑数
                d['img_tag_clk_count'] = data['stats']['gridImgTagClick'] # 图片标签点击数
            id2info[data['creativeId']] = d

        total = body['total']
        if page < total:
            page += 1
        else:
            break
    return id2info

def cookiesInfodb(account_id):
    use_date = str (time.strftime ('"%Y-%m-%d"', time.localtime (time.time ())))
    status_change_date = str (time.strftime ('"%Y-%m-%d %H:%M:%S"', time.localtime (time.time ())))
    aid = account_id
    # print(date)
    # print(str_cookie)

    # 数据库
    db = pymysql.connect ("localhost", "root", "youmi", "COOKIES_USE_INFO")
    cursor = db.cursor ()

    sql = " insert into cookies_info values ('%d',%s,1,%s)" % (int(aid), use_date,status_change_date)

    cursor.execute (sql)
    cursor.close ()
    db.commit ()
    db.close ()

def reStart(account_id):
    date = str (time.strftime ('"%Y-%m-%d"', time.localtime (time.time ())))
    aid= account_id

    db = pymysql.connect ("localhost", "root", "youmi", "COOKIES_USE_INFO")
    cursor = db.cursor ()

    sql = " select status from cookies_info where account_id='%d' and use_date = %s and status=1" % (int(aid), date)

    cursor.execute (sql)
    data = cursor.fetchone ()

    cursor.close ()
    db.commit ()
    db.close ()
    return data


def run():
    import traceback

    cookies, _user = login('youmi' ,'youmi123')
    #with open (r'/home/youmi/cookies.txt', 'wb') as f :
    #    pickle.dump (cookies, f)

    account_ids = child_account_list(cookies)
    print(account_ids)


    i = 1
    try:
        for account_id in account_ids:

            data = reStart(account_id)
            print(data)
            if data!=None:
                switch_back_parent_account (cookies)
                i += 1
            else:
                print('fetching account ', i, '/', len(account_ids))
                child = switch_to_child_account(cookies, account_id)
                print(child)
                fetch_child_account(cookies, child)

                cookiesInfodb(account_id)

                switch_back_parent_account (cookies)
                i += 1

    except:
        traceback.print_exc()


    finally:
        logout(cookies)

def fetch_child_account(cookies, user):
    # cookies, user = login('mingrizhihou' ,'mrzh123')
    uid = str(user['id'])
    client = ad_client_id(cookies)
    client_id, client_name = client['clientId'], client['clientName']
    ad_campaigns = campaign_list(cookies)
    ad_advertises = ad_list(cookies)
    ad_creatives = creative_list(cookies)

    assert len(ad_campaigns) != 0
    assert len(ad_advertises) != 0
    assert len(ad_creatives) != 0

    # print('campaign')
    # print(json.dumps(ad_campaigns))
    # print('ad')
    # print(json.dumps(ad_advertises))
    # print('creative')
    # print(json.dumps(ad_creatives))

    user_info = eff.upload_request.ad_account(
        advertiser = eff.upload_request.ad_account.ad_advertiser(
            id = uid,
            name = user['company']
        ),
        agent = eff.upload_request.ad_account.ad_agent(
            id = client_id,
            name = client_name
        ),
        operator = eff.upload_request.ad_account.ad_operator(
            id = client_id,
            name = client_name
        )
    )

    #get config
    id2dates, id2creatives = get_config(uid, client_id)
    print(id2dates[uid])
    print(id2creatives[uid])
    if len(id2dates) == 0:
        return

    #get data
    for date in id2dates[uid]:
        creative_id2data = get_data(cookies, date)
        creative_datas = []
        for creative_id in ad_creatives:
            brief = True if creative_id in id2creatives[uid] else False
            if brief:
                print('skip', creative_id)
            else:
                print('fetch', creative_id)

            creative = ad_creatives[creative_id]
            name = creative['name']
            #debug info
            print(creative_id, name)

            ad = ad_advertises.get(creative_id, {'id': '-1', 'name': ''}) 
            cid = creative['cid']
            c = ad_campaigns.get(cid, {'id': '-1', 'name': ''})

            d = creative_id2data.get(creative_id)

            # type == 2
            if not brief:
                if creative['type'] == 2: #普通微博
                    weibo_url = creative['adUrl'].strip()
                    info = parse_weibo(weibo_url)
                    creative['landingpageUrl'] = info['target']
                else: #品速视频, 品牌大Card
                    info = {
                        'images': creative['adUrl'].strip(),
                        'videos':'',
                        'text': ''
                    }


            datas = []
            if d is None:
                datas.append(eff.upload_request.ads.data(
                    key = '-',
                    val = '-',
                    dtype = '-'
                ))
            else:
                for key in d:
                    datas.append(eff.upload_request.ads.data(
                        key = key,
                        val = str(d[key]),
                        dtype = type(d[key]).__name__
                    ))
            creative_datas.append(eff.upload_request.ads(
                    campaign = eff.upload_request.ads.ad_campaign(
                        id = c['id'],
                        name = '' if brief else c['name']
                    ),
                    ad = eff.upload_request.ads.ad_advertise(
                        id = ad['id'],
                        name = '' if brief else ad['name']
                    ),
                    creative = eff.upload_request.ads.ad_creative(
                        id = creative_id,
                        name = '' if brief else name,
                        slogan = '' if brief else info['text'],
                        images = '' if brief else ','.join(info['images']),
                        videos = '' if brief else '^'.join(info['videos']),
                        original_data = '' if brief else json.dumps(creative),
                    ),
                    datas = datas
                ))
        req = eff.upload_request(
            date = date,
            platform = 'weilianbo',
            user_info = user_info,
            creatives = creative_datas
        )
        print(date)
        # print(req)
        post_data(req)

weibo_url2info = {}
def cache(func):
    def wrapper(weibo_url):
        global weibo_url2info
        info = weibo_url2info.get(weibo_url)
        if info is None:
            info = func(weibo_url)
            weibo_url2info[weibo_url] = info
        return info
    return wrapper

@cache
def parse_weibo(weibo_url):
    import traceback
    from selenium import webdriver
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.common.exceptions import TimeoutException
    import urllib.parse as up

    driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', 
        desired_capabilities=DesiredCapabilities.CHROME)
    #return values
    target_url, body_text, img_urls, vid_urls = '', '',  set(), set()
    for loop_cnt in range(3):
        try:
            driver.set_page_load_timeout(40)
            # driver.implicitly_wait(20) 
            driver.get(weibo_url)
            
            #素材
            detail = WebDriverWait(driver, 30).until(lambda x: x.find_element_by_class_name("WB_detail"))
            wb_media_wrap = detail.find_elements_by_class_name('WB_media_wrap')
            if len(wb_media_wrap) != 1:
                print('error weibo page, url =', weibo_url)
                continue

            e = wb_media_wrap[0]
            action_datas = e.find_elements_by_xpath('.//li[@action-data]')
            if len(action_datas) > 0:
                for action_data in action_datas:
                    action_type = action_data.get_attribute('action-type')
                    # 普通微博（单视频）
                    if action_type == 'feed_list_third_rend':
                        d = up.parse_qs(action_data.get_attribute('action-data'))
                        video_src = d.get('video_src')
                        if not video_src is None:
                            url = video_src[0]
                            if not url.startswith('https'):
                                url = 'https:' + url
                            vid_urls.add(url)
                        cover_img = d.get('cover_img')
                        if not cover_img is None:
                            url = cover_img[0]
                            if not url.startswith('https'):
                                url = 'https:' + url
                            img_urls.add(url)
                    # 九宫格缩略图+视频
                    elif action_type == 'fl_pics':
                        d = up.parse_qs(action_data.get_attribute('action-data'))
                        gif_url = d.get('gif_url')
                        if gif_url is None:
                            continue
                        url = gif_url[0]
                        if not url.startswith('https'):
                            url = 'https:' + url
                        vid_urls.add(url)
                    # 普通微博（单图)
                    elif action_type == 'feed_list_media_img':
                        imgs = action_data.find_elements_by_tag_name('img')
                        for img in imgs:
                            img_urls.add(img.get_attribute('src'))
                    else:
                        print('unknown weibo type, url =', weibo_url)
            else:
                wb_feed_spec = e.find_elements_by_xpath('.//div[@action-data]')
                # 大card
                if len(wb_feed_spec) == 1 and wb_feed_spec[0].get_attribute('action-type') == 'fl_jumpurl':
                    imgs = wb_feed_spec[0].find_elements_by_tag_name('img')
                    for img in imgs:
                        url = img.get_attribute('src')
                        if not url.startswith('https'):
                            url = 'https:' + url
                        img_urls.add(url)

            # 正文
            text = WebDriverWait(driver, 30).until(lambda x: x.find_element_by_class_name("WB_text"))
            body_text = text.text
            links = text.find_elements_by_tag_name('a')
            for link in links:
                action_type = link.get_attribute('action-type')
                if action_type is None or action_type != 'feed_list_url':
                    continue
                else:
                    redirect_link = link.get_attribute('href')
                    driver.get(redirect_link)
                    target_url = driver.current_url
                    driver.close()
                    break

        except TimeoutException:
            print('timeout when getting', weibo_url)
            # traceback.print_exc()
            continue
        except:
            print('error', weibo_url)
            traceback.print_exc()
        break
            
    driver.quit()
    return {
        'target': target_url,
        'text': body_text,
        'images': list(img_urls),
        'videos': list(vid_urls)
    }

if __name__ =='__main__':
    # print(parse_weibo("http://weibo.com/1673433935/Fh3IpBV5d"))
    run()