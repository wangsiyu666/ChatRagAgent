import os
from langchain.docstore.in_memory import InMemoryDocstore
from langchain.schema import Document
from copilotkit.knowledge_base.kb_cache.base import *
from copilotkit.knowledge_base.utils import get_vs_path
from copilotkit.utils import get_Embeddings, get_default_embedding


def _new_ds_search(self, search: str) -> Union[str, Document]:
    if search not in self._dict:
        return f"ID {search} not found"
    else:
        doc = self._dict[search]
        if isinstance(doc, Document):
            doc.metadata["id"] = search
        return doc


InMemoryDocstore.search = _new_ds_search


class ThreadSafeFaiss(ThreadSafeObject):
    def __repr__(self) -> str:
        cls = type().__name__
        return f"<{cls}: key: {self.key}, obj: {self._obj}, docs_count: {self.docs_count()}>"

    def docs_count(self) -> int:
        return len(self._obj.docstore._dict)

    def save(self, path: str, create_path: bool = True):
        with self.acquire():
            if not os.path.isdir(path) and create_path:
                os.makedirs(path)
            ret = self._obj.save_local(path)
            print(f"已将向量库 {self.key} 保存到磁盘")
        return ret

    def clear(self):
        ret = []
        with self.acquire():
            ids = list(self._obj.docstore._dict.keys())
            if ids:
                ret = self._obj.delete(ids)
                assert len(self._obj.docstore._dict) == 0
            print(f"已将向量库 {self.key} 清空")
        return ret


class _FaissPool(CachePool):
    def new_vector_store(
            self,
            kb_name: str,
            embed_model: str = get_default_embedding(),
    ) -> FAISS:
        embeddings = get_Embeddings(embed_model=embed_model)
        doc = Document(page_content="init", metadata={})
        vector_store = FAISS.from_documents([doc], embeddings, normalize_L2=True)
        ids = list(vector_store.docstore._dict.keys())
        vector_store.delete(ids)
        return vector_store

    def new_temp_vector_store(
            self,
            embed_model: str = get_default_embedding(),
    ) -> FAISS:
        embeddings = get_Embeddings(embed_model=embed_model)
        doc = Document(page_content="init", metadata={})
        vector_store = FAISS.from_documents([doc], embeddings, normalize_L2=True)
        ids = list(vector_store.docstore._dict.keys())
        vector_store.delete(ids)
        return vector_store

    def save_vector_store(self, kb_name: str, path: str = None):
        if cache := self.get(kb_name):
            return cache.save(path)

    def unload_vector_store(self, kb_name: str):
        if cache := self.get(kb_name):
            self.pop(kb_name)
            print(f"成功释放向量库: {kb_name}")


class KBFaissPool(_FaissPool):
    def load_vector_store(
            self,
            kb_name: str,
            vector_name: str = None,
            create: bool = True,
            embed_model: str = get_default_embedding(),
    ) -> ThreadSafeFaiss:
        self.atomic.acquire()
        locked = True
        vector_name = vector_name or embed_model.replace(":", "_")
        cache = self.get((kb_name, vector_name))
        try:
            if cache is None:
                item = ThreadSafeFaiss((kb_name, vector_name), pool=self)
                self.set((kb_name, vector_name), item)
                with item.acquire(msg="初始化"):
                    self.atomic.release()
                    locked = False
                    print(
                        f"loading vector store in '{kb_name}/vector_store/{vector_name}' from disk."
                    )
                    vs_path = get_vs_path(kb_name, vector_name)

                    if os.path.isfile(os.path.join(vs_path, "index.faiss")):
                        embeddings = get_Embeddings(embed_model=embed_model)
                        vector_store = FAISS.load_local(
                            vs_path,
                            embeddings,
                            normaliz_L2=True,
                            allow_dangerous_deserialization=True,
                        )
                    elif create:
                        if not os.path.exists(vs_path):
                            os.makedirs(vs_path)
                        vector_store = self.new_vector_store(
                            kb_name=kb_name, embed_model=embed_model
                        )
                        vector_store.save_local(vs_path)
                    else:
                        raise RuntimeError(f"knowledge base {kb_name} not exists")
                    item.obj = vector_store
                    item.finish_loading()
            else:
                self.atomic.release()
                locked = False
        except Exception as e:
            if locked:
                self.atomic.release()
            print(e)
            raise RuntimeError(f"向量库 {kb_name} 加载失败")
        return self.get((kb_name, vector_name))


class MemoFaissPool(_FaissPool):

    def load_vector_store(
            self,
            kb_name: str,
            embed_model: str = get_default_embedding(),
    ) -> ThreadSafeFaiss:
        self.atomic.acquire()
        cache = self.get(kb_name)
        if cache is None:
            item = ThreadSafeFaiss(kb_name, pool=self)
            self.set(kb_name, item)
            with item.acquire(msg="初始化"):
                self.atomic.release()
                print(f"loading vector store in '{kb_name}' to memory")

                vector_store = self.new_vector_store(embed_model=embed_model)
                item.obj = vector_store
                item.finish_loading()
        else:
            self.atomic.release()
        return self.get(kb_name)

kb_faiss_pool = KBFaissPool(cache_num=1)
memo_faiss_pool = MemoFaissPool(cache_num=10)
