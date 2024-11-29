import requests
import time


def get_millisecond():
    """
    :return: 获取精确毫秒时间戳,13位
    """
    millis = int(round(time.time() * 1000))
    return millis

def get_randomId():
    url = "https://www.north-info.cn/northinfo/oa/api/wechat/authorize?code=0b3QDX100xoCcT1eWf000E4acR2QDX1K"
    headers = {
    "Host": "www.north-info.cn",
    "Connection": "keep-alive",
    "content-type": "application/json",
    "Accept-Encoding": "gzip,compress,br,deflate",
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.53(0x18003531) NetType/WIFI Language/zh_CN",
    "Referer": "https://servicewechat.com/wxf053b0a0e3a94d52/5/page-frame.html"
}

    r = requests.get(url=url, headers=headers)
    return r.json()

def get_token():
    url = "https://www.north-info.cn/northinfo/oa/api/wechat/sys/auth/sendVerfiyCode?phoneNumber=15566273486"
    headers = {
        "Host": "www.north-info.cn",
        "Connection": "keep-alive",
        "content-type": "application/json",
        "Accept-Encoding": "gzip,compress,br,deflate",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.53(0x18003531) NetType/WIFI Language/zh_CN",
        "Referer": "https://servicewechat.com/wxf053b0a0e3a94d52/5/page-frame.html"
    }
    r = requests.get(url=url, headers=headers)
    return r.json()


def get_code(token):
    url = f"https://www.north-info.cn/northinfo/oa/api/wechat/clock/doClock?clockType=3&clockTime={get_millisecond()}&clockStatus=3&lat=41.72100531684028&lng=123.44808485243055"

    # 1309101741307859524
    # 1309094993167651316
    headers = {
        "Host": "www.north-info.cn",
        "Connection": "keep-alive",
        "tokenid": token,
        "content-type": "application/json",
        "Accept-Encoding": "gzip,compress,br,deflate",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.53(0x18003531) NetType/WIFI Language/zh_CN",
        "Referer": "https://servicewechat.com/wxf053b0a0e3a94d52/5/page-frame.html"
    }

    r = requests.get(url=url, headers=headers)
    return r

def start_code(token):
    url = f"https://www.north-info.cn/northinfo/oa/api/wechat/clock/doClock?clockType=1&clockTime={get_millisecond()}&clockStatus=1&lat=41.72100531684028&lng=123.44808485243055"
    headers = {
        "Host": "www.north-info.cn",
        "Connection": "keep-alive",
        "tokenid": token,
        "content-type": "application/json",
        "Accept-Encoding": "gzip,compress,br,deflate",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.53(0x18003531) NetType/WIFI Language/zh_CN",
        "Referer": "https://servicewechat.com/wxf053b0a0e3a94d52/5/page-frame.html"
    }

    r = requests.get(url=url, headers=headers)
    return r
