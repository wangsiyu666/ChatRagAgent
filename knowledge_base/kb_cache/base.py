import threading
from collections import OrderedDict
from contextlib import contextmanager
from typing import (
    Any,
    List,
    Tuple,
    Union,
    Generator
)
from langchain.vectorstores.faiss import FAISS


class ThreadSafeObject:
    def __init__(
            self,
            key: Union[str, Tuple],
            obj: Any = None,
            pool: "CachePool" = None
    ):
        self._obj = obj
        self._key = key
        self._pool = pool
        # 实例 嵌套锁：解决在同一个线程中多次请求同一资源。只要在同一个线程内就可以重复使用
        self._lock = threading.RLock()
        # Event创建一个事件管理标志默认False
        # event.wait线程阻塞， timeout超时后停止阻塞
        # event.set将Event设置为True，调用wait方法的所有线程将被唤醒
        # event.clear将Event设置为False，调用wait方法的所有线程将被阻塞
        # event.isSet判断Event的标志是否为True
        self._loaded = threading.Event()

    def __repr__(self) -> str:
        cls = type(self).__name__
        return f"<{cls}: key: {self.key}, obj: {self._obj}>"

    @property
    def key(self):
        return self._key

    @contextmanager
    def acquire(self, owner: str = "", msg: str = "") -> Generator[None, None, FAISS]:
        # get_native_id 用于返回内核分配的当前线程的本机线程ID， 该非负数可用于标记此特定线程
        owner = owner or f"thread {threading.get_native_id()}"
        try:
            # 当你加锁后， 意味着同一时间有且仅有一个线程在执行这段代码
            # acquire 获取锁，为获取就会阻塞
            self._lock.acquire()
            if self._pool is not None:
                self._pool._cache.move_to_end(self.key)
            print(f"{owner} 开始操作: {self.key}.{msg}")
            yield self._obj
        finally:
            print(f"{owner} 结束操作: {self.key}. {msg}")
            # 释放锁
            self._lock.release()

    def start_loading(self):
        self._loaded.clear()

    def finish_loading(self):
        self._loaded.set()

    def wait_for_loading(self):
        self._loaded.wait()

    @property
    def obj(self):
        return self._obj

    @obj.setter
    def obj(self, val: Any):
        self._obj = val


class CachePool:
    def __init__(
            self,
            cache_num: int = -1
    ):
        self._cache_num = cache_num
        self._cache = OrderedDict()
        self.atomic = threading.RLock()

    def keys(self) -> List[str]:
        return list(self._cache.keys())

    def _check_count(self):
        if isinstance(self._cache_num, int) and self._cache_num > 0:
            while len(self._cache) > self._cache_num:
                # 删除字典最后一个键值
                self._cache.popitem(last=False)

    def get(self, key: str) -> ThreadSafeObject:
        if cache := self._cache.get(key):
            cache.wait_for_loading()
            return cache

    def set(self, key: str, obj: ThreadSafeObject) -> ThreadSafeObject:
        self._cache[key] = obj
        self._check_count()

    def pop(self, key: str = None) -> ThreadSafeObject:
        if key is None:
            return self._cache.popitem(last=False)
        else:
            return self._cache.pop(key, None)

    def acquire(self, key: Union[str, Tuple], owner: str = "", msg: str = ""):
        cache = self.get(key)
        if cache is None:
            raise RuntimeError(f"请求的资源 {key} 不存在")
        elif isinstance(cache, ThreadSafeObject):
            self._cache.move_to_end(key)
            return cache.acquire(owner=owner, msg=msg)
        else:
            return cache








