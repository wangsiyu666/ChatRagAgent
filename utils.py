import os
import base64
import json
import asyncio
import hashlib
import re
import aiohttp
from pydantic import BaseModel, Field
from typing import (
    Union,
    Tuple,
    Dict,
    List,
    Callable,
    Generator,
    Any,
    Awaitable
)
from html2text import HTML2Text
from langchain.docstore.document import Document
from langchain_core.embeddings import Embeddings
from langchain.prompts.chat import ChatMessagePromptTemplate
from concurrent.futures import ThreadPoolExecutor, as_completed
from langchain_openai.chat_models import ChatOpenAI
from langchain_openai.llms import OpenAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
import requests
import time


def get_millisecond():
    """
    :return: 获取精确毫秒时间戳,13位
    """
    millis = int(round(time.time() * 1000))
    return millis


# def get_code(token):
#     url = f"https://www.north-info.cn/northinfo/oa/api/wechat/clock/doClock?clockType=3&clockTime={get_millisecond()}&clockStatus=3&lat=41.72100531684028&lng=123.44808485243055"
#
#     # 1309101741307859524
#     # 1309094993167651316
#     headers = {
#         "Host": "www.north-info.cn",
#         "Connection": "keep-alive",
#         "tokenid": token,
#         "content-type": "application/json",
#         "Accept-Encoding": "gzip,compress,br,deflate",
#         "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.53(0x18003531) NetType/WIFI Language/zh_CN",
#         "Referer": "https://servicewechat.com/wxf053b0a0e3a94d52/5/page-frame.html"
#     }
#
#     r = requests.get(url=url, headers=headers)
#     return r

def validate_file():
    if not os.path.exists("Upload"):
        os.makedirs("Upload")
    for file_name in os.listdir("Upload"):
        if file_name.endswith('.csv'):
            os.remove("Upload/" + file_name)


def get_binary_file_downloader_html(bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    f.close()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{os.path.basename(bin_file)}">点击下载 {file_label}</a>'
    return href


def get_auth_token():
    url = "http://172.24.244.28:31950/admin-api/system/auth/login"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Content-Length": "210",
        "Content-Type": "application/json;charset=UTF-8",
        "Host": "172.24.244.28:31950",
        "Origin": "http://172.24.244.28:31950",
        "Referer": "http://172.24.244.28:31950/admin-ui/login?redirect=%2F",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
    }
    params = {
        "username": "xiexinlai",
        "password": "RlJ9UVdQWfqOVIKwOesNCfit+T9umFE4yvYeeXtvAGPoQ8Wnl/sDj/dQC5qoqvqA4hWCnYp5/fd/i8wJ+gyo6zEVxeUu6vaic9jmoxQH5epk6rBBxgb3ta4KEhdQS7UxhOOCH9hR7pcHNWg/heKoCXU4Yp8xr2wi2RAD5K003iE="
    }
    r = requests.post(url=url, headers=headers, data=json.dumps(params))
    return r.json()['data']['accessToken']


def north_info(token):
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


from typing import (
    Optional,
    Literal,
    Dict,
    List
)
from urllib.parse import urlparse
from memoization import cached, CachingAlgorithmFlag


class MsgType:
    TEXT = 1
    IMAGE = 2
    AUDIO = 3
    VIDEO = 4


def get_config_platform() -> Dict[str, Dict]:
    MODEL_PLATFORMS: List = [
        {
            "platform_name": "xinference",
            "platform_type": "xinference",
            "api_base_url": "http://127.0.0.1:9997/v1",
            "api_key": "EMPTY",
            "api_concurrencies": 5,
            "auto_detect_model": True,
            "llm_models": [],
            "embed_models": [],
            "rerank_models": [],
        }
    ]
    return {m["platform_name"]: m for m in MODEL_PLATFORMS}


def get_base_url(url):
    parsed_url = urlparse(url)
    base_url = '{uri.schema}://{uri.netloc}'.format(uri=parsed_url)
    return base_url.rstrip('/')


@cached(max_size=10, ttl=60, algorithm=CachingAlgorithmFlag.LRU)
def detect_xf_models(xf_url: str) -> Dict[str, List[str]]:
    xf_model_type_maps = {
        "llm_models": lambda xf_models: [k for k, v in xf_models.items()
                                         if "LLM" == v["model_type"]
                                         and "vision" not in v["model_ability"]],
        "embed_models": lambda xf_models: [k for k, v in xf_models.items()
                                           if "embedding" == v["model_type"]],
        "rerank_models": lambda xf_models: [k for k, v in xf_models.items()
                                            if "rerank" == v["model_type"]],
    }
    models = {}
    try:
        from xinference_client import RESTfulClient as Client
        xf_client = Client(xf_url)
        xf_models = xf_client.list_models()
        for m_type, filter in xf_model_type_maps.items():
            models[m_type] = filter(xf_models)
    except Exception as e:
        print("Xinference Client")

    return models


def get_config_models(
        model_name: str = None,
        model_type: Optional[Literal[
            "llm", "embed", "rerank"
        ]] = None,
        platform_name: str = "xinference"
) -> Dict[str, Dict]:
    result = {}
    if model_type is None:
        model_types = [
            "llm_models",
            "embed_models",
            "rerank_models",
        ]
    else:
        model_types = [f"{model_type}_models"]
    conf = list(get_config_platform().values())[0]
    if conf["auto_detect_model"]:
        xf_url = get_base_url(conf['api_base_url'])
        xf_models = detect_xf_models(xf_url)
        for m_type in model_types:
            conf[m_type] = xf_models.get(m_type, [])

    for m_type in model_types:
        models = conf.get(m_type, [])
        for m_name in models:
            if model_name is None or model_name == m_name:
                result[m_name] = {
                    "platform_name": conf.get("platform_name"),
                    "platform_type": conf.get("platform_type"),
                    "model_type": m_type.split("_")[0],
                    "api_base_url": conf.get("api_base_url"),
                    "api_key": conf.get("api_key"),
                    "api_proxy": conf.get("api_proxy"),
                }
    return result


def get_default_llm():
    available_llms = list(get_config_models(model_type='llm').keys())
    return available_llms[0]


class History(BaseModel):
    role: str = Field(...)
    content: str = Field(...)

    def to_msg_tuple(self):
        return "ai" if self.role == "assistant" else "human", self.content

    def to_msg_template(self, is_raw=True) -> ChatMessagePromptTemplate:
        role_maps = {
            "ai": "assistant",
            "human": "user"
        }
        role = role_maps.get(self.role, self.role)
        if is_raw:
            content = "{% raw %}" + self.content + "{% endraw %}"
        else:
            content = self.content

        return ChatMessagePromptTemplate.from_template(
            content,
            "jinja2",
            role=role
        )

    @classmethod
    def from_data(cls, h: Union[List, Tuple, Dict]) -> "History":
        if isinstance(h, (list, tuple)) and len(h) >= 2:
            h = cls(role=h[0], content=h[1])
        elif isinstance(h, dict):
            h = cls(**h)
        else:
            h = cls(role=h.type, content=h.content)
        return h


def api_address(is_public: bool = False) -> str:
    server = {
        "host": "0.0.0.0",
        "port": 7861,
        "public_host": "127.0.0.1",
        "public_port": 7861
    }
    if is_public:
        host = server.get("public_host", "127.0.0.1")
        port = server.get("public_port", "7861")
    else:
        host = server.get("host", "127.0.0.1")
        port = server.get("port", "7861")
        if host == "0.0.0.0":
            host = " 127.0.01"
    return f"http://{host}:{port}"


def get_default_embedding():
    available_embeddings = list(get_config_models(model_type="embed").keys())
    DEFAULT_EMBEDDING_MODEL = "custom-bge-large-zh-v1.5"
    if DEFAULT_EMBEDDING_MODEL in available_embeddings:
        return DEFAULT_EMBEDDING_MODEL
    else:
        print("default embedding model not found")
    return available_embeddings[0]


def get_model_info(
        model_name: str = None,
        platform_name: str = None,
        multiple: bool = False
) -> Dict:
    result = get_config_models(model_name=model_name, platform_name=platform_name)
    if len(result) > 0:
        if multiple:
            return result
        else:
            return list(result.values())[0]
    else:
        return {}


def get_Embeddings(
        embed_model: str = None,
        local_wrap: bool = False,
) -> Embeddings:
    from copilotkit.knowledge_base.localai_embeddings import LocalAIEmbeddings

    embed_model = embed_model or get_default_embedding()
    model_info = get_model_info(model_name=embed_model)
    params = dict(model=embed_model)

    if local_wrap:
        pass
    else:
        params.update(
            openai_api_base=model_info.get("api_base_url"),
            openai_api_key=model_info.get("api_key"),
            openai_proxy=model_info.get("api_proxy")
        )
    return LocalAIEmbeddings(**params)


def check_embed_model(embed_model: str = None) -> Tuple[bool, str]:
    embed_model = embed_model or get_default_embedding()
    embeddings = get_Embeddings(embed_model=embed_model)
    try:
        embeddings.embed_query("this is similarity_image test")
        return True, ""
    except Exception as e:
        return False, "failed to access embed model"


def run_in_thread_pool(
        func: Callable,
        params: List[Dict] = []
) -> Generator:
    tasks = []
    with ThreadPoolExecutor() as pool:
        for kwargs in params:
            tasks.append(pool.submit(func, **kwargs))

        for obj in as_completed(tasks):
            try:
                yield obj.result()
            except Exception as e:
                print(e)


class BaseResponse(BaseModel):
    code: int = Field(200)
    msg: str = Field("success")
    data: Any = Field(None)

    class Config:
        json_schema_extra = {
            "example": {
                "code": 200,
                "msg": "success"
            }
        }


class ListResponse(BaseResponse):
    data: List[Any] = Field(...)

    class Config:
        json_schema_extra = {
            "example": {
                "code": 200,
                "msg": "success",
                "data": ["doc1.docx", "doc2.pdf", "doc3.txt"]
            }
        }


async def wrap_done(fn: Awaitable, event: asyncio.Event):
    try:
        await fn
    except Exception as e:
        msg = f"Caught exception: {e}"
        print(msg)
    finally:
        event.set()


def get_ChatOpenAI(
        model_name: str = "custom-Qwen2.5-chat",
        temperature: float = 0.7,
        max_tokens: int = None,
        streaming: bool = True,
        callbacks: List[Callable] = [],
        verbose: bool = True,
        local_wrap: bool = False,
        **kwargs: Any
) -> ChatOpenAI:
    model_info = get_model_info(model_name)
    params = dict(
        streaming=streaming,
        verbose=verbose,
        callbacks=callbacks,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs
    )
    for k in list(params):
        if params[k] is None:
            params.pop(k)

    try:
        if local_wrap:
            params.update(
                openai_api_base=f"http://127.0.0.1:7861/v1",
                openai_api_key="EMPTY",
            )
        else:
            params.update(
                openai_api_base=model_info.get("api_base_url"),
                openai_api_key=model_info.get("api_key"),
                openai_proxy=model_info.get("api_proxy")
            )
        model = ChatOpenAI(**params)
    except Exception as e:
        print(e)
        model = None
    return model


def get_OpenAI(
        model_name: str,
        temperature: float,
        max_tokens: int = 1024,
        streaming: bool = True,
        echo: bool = True,
        callbacks: List[Callable] = [],
        verbose: bool = True,
        local_wrap: bool = False,
        **kwargs: Any,
) -> OpenAI:
    model_info = get_model_info(model_name)
    params = dict(
        streaming=streaming,
        verbose=verbose,
        callbacks=callbacks,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        echo=echo,
        **kwargs,
    )
    try:
        if local_wrap:
            params.update(
                openai_api_base=f"http://127.0.0.1:7861/v1",
                openai_api_key="EMPTY",
            )
        else:
            params.update(
                openai_api_base=model_info.get("api_base_url"),
                openai_api_key=model_info.get("api_key"),
                openai_proxy=model_info.get("api_proxy")
            )
        model = OpenAI(**params)
    except Exception as e:
        print(f"Error create OpenAI for model {model_name}")
        model = None
    return model









