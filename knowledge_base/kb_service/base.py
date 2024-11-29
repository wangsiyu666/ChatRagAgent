import operator
from typing import (
    Union,
    Tuple,
    List,
    Dict
)
from abc import ABC, abstractmethod
import os
from pathlib import Path
from langchain.docstore.document import Document
from copilotkit.utils import get_default_embedding
from copilotkit.utils import check_embed_model as _check_embed_model
from copilotkit.db.repository.knowledge_base_respository import (
    add_kb_to_db,
    delete_kb_from_db,
    kb_exists,
    list_kbs_from_db,
    load_kb_from_db
)
from copilotkit.db.repository.knowledge_file_respository import (
    delete_file_from_db,
    add_file_to_db,
    file_exists_in_db,
    list_files_from_db,
    count_files_from_db,
    list_docs_from_db,
    get_file_detail,
    delete_files_from_db
)
from copilotkit.knowledge_base.utils import (
    KnowledgeFile,
    get_doc_path,
    get_kb_path,
    list_files_from_folder,
    list_kbs_from_folder
)


class SupportedVSType:
    FAISS = "faiss"
    MILVUS = "milvus"
    DEFAULT = "default"
    ZILLIZ = "zilliz"
    CHROMADB = "chromadb"


class DocumentWithVSId(Document):
    id: str = None
    score: float = 3.0


class KBService(ABC):
    def __init__(
            self,
            knowledge_base_name: str,
            kb_info: str = None,
            embed_model: str = get_default_embedding(),
    ):
        self.kb_name = knowledge_base_name
        self.kb_info = kb_info
        self.embed_model = embed_model
        self.kb_path = get_kb_path(self.kb_name)
        self.doc_path = get_doc_path(self.kb_name)
        self.do_init()

    def __repr__(self):
        return f"{self.kb_name} @ {self.embed_model}"

    def save_vector_store(self):
        pass

    def check_embed_model(self) -> Tuple[bool, str]:
        return _check_embed_model(self.embed_model)

    async def create_kb(self):
        if not os.path.exists(self.doc_path):
            os.mkdir(self.doc_path)

        status = await add_kb_to_db(
            kb_name=self.kb_name,
            kb_info=self.kb_info,
            vs_type=self.vs_type(),
            embed_model=self.embed_model,
            user_id="admin"
        )
        if status:
            self.do_create_kb()
        return status

    async def clear_vs(self):
        self.do_clear_vs()
        status = await delete_files_from_db(self.kb_name)

    async def drop_kb(self):
        self.do_drop_kb()
        status = await delete_kb_from_db(self.kb_name)
        return status

    async def delete_doc(
            self,
            kb_file: KnowledgeFile,
            delete_content: bool = False,
            **kwargs
    ):
        self.do_delete_doc(kb_file, **kwargs)
        status = await delete_file_from_db(kb_file)
        if delete_content and os.path.exists(kb_file.filepath):
            os.remove(kb_file.filepath)
        return status

    async def add_doc(self, kb_file: KnowledgeFile, docs: List[Document] = [], **kwargs):
        if not self.check_embed_model()[0]:
            return False

        if docs:
            custom_docs = True

        else:
            docs = kb_file.file2docs()
            custom_docs = False

        if docs:
            for doc in docs:
                try:
                    doc.metadata.setdefault("source", kb_file.filename)
                    source = doc.metadata.get("source", "")
                    if os.path.isabs(source):
                        rel_path = Path(source).relative_to(self.doc_path)
                        doc.metadata["source"] = str(rel_path.as_posix().strip("/"))
                except Exception as e:
                    print(
                        f"cannot convert absolute path ({source}) to relative path {e}"
                    )
            doc_infos = self.do_add_doc(docs, **kwargs)
            status = await add_file_to_db(
                kb_file,
                custom_docs=custom_docs,
                docs_count=len(docs),
                doc_infos=doc_infos,
            )
        else:
            status = False
        return status

    async def update_info(self, kb_info: str):
        self.kb_info = kb_info
        status = await add_kb_to_db(
            kb_name=self.kb_name,
            kb_info=self.kb_info,
            vs_type=self.vs_type(),
            embed_model=self.embed_model,
            user_id="admin"
        )
        return status

    async def update_doc(self, kb_file: KnowledgeFile, docs: List[Document] = [], **kwargs):
        if not self.check_embed_model()[0]:
            return False

        if os.path.exists(kb_file.filepath):
            await self.delete_doc(kb_file, **kwargs)
            return await self.add_doc(kb_file, docs=docs, **kwargs)

    async def exist_doc(self, file_name: str):
        return await file_exists_in_db(
            KnowledgeFile(knowledge_base_name=self.kb_name, file_name=file_name)
        )

    async def list_files(self):
        return await list_files_from_db(self.kb_name)

    def count_files(self):
        return count_files_from_db(self.kb_name)

    async def search_docs(
            self,
            query: str,
            top_k: int = 1,
            score_threshold: float = 0.8
    ) -> List[Document]:
        if not self.check_embed_model()[0]:
            return []
        docs = self.do_search(query, top_k, score_threshold)
        return docs

    def get_doc_by_ids(self, ids: List[str]) -> List[Document]:
        return []

    def del_doc_by_ids(self, ids: List[str]) -> bool:
        raise NotImplementedError

    def update_doc_by_ids(self, docs: Dict[str, Document]) -> bool:
        if not self.check_embed_model()[0]:
            return False
        self.del_doc_by_ids(list(docs.keys()))
        pending_docs = []
        ids = []
        for _id, doc in docs.items():
            if not doc or not doc.page_content.strip():
                continue
            ids.append(_id)
            pending_docs.append(doc)
        self.do_add_doc(docs=pending_docs, ids=ids)
        return True

    async def list_docs(
            self,
            file_name: str = None,
            metadata: Dict = {}
    ) -> List[DocumentWithVSId]:
        doc_infos = await list_docs_from_db(
            kb_name=self.kb_name,
            file_name=file_name,
            metadata=metadata
        )
        docs = []
        for x in doc_infos:
            doc_info = self.get_doc_by_ids(x["id"])[0]
            if doc_info is not None:
                doc_with_id = DocumentWithVSId(**doc_info.dict(), id=x["id"])
                docs.append(doc_with_id)
            else:
                pass
        return docs

    def get_relative_score_path(self, filepath: str):
        relative_path = filepath
        if os.path.isabs(relative_path):
            try:
                relative_path = Path(filepath).relative_to(self.doc_path)
            except Exception as e:
                print(
                    f"cannot convert absolute path {relative_path}"
                )
        relative_path = str(relative_path.as_posix().strip("/"))
        return relative_path

    @abstractmethod
    def do_create_kb(self):
        pass

    @abstractmethod
    def vs_type(self):
        pass

    @classmethod
    def list_kbs(cls):
        return list_kbs_from_db

    def exists(self, kb_name: str = None):
        kn_name = kb_name or self.kb_name
        return kb_exists(kb_name)

    @abstractmethod
    def do_init(self):
        pass

    @abstractmethod
    def do_clear_vs(self):
        pass

    @abstractmethod
    def do_drop_kb(self):
        pass

    @abstractmethod
    def do_delete_doc(self, kb_file: KnowledgeFile):
        pass

    @abstractmethod
    def do_add_doc(
            self,
            docs: List[Document],
            **kwargs,
    ) -> List[Dict]:
        pass

    @abstractmethod
    def do_search(
            self,
            query: str,
            top_k: int,
            score_threshold: float,
    ) -> List[Tuple[Document, float]]:
        pass


class KBServiceFactory:

    @staticmethod
    def get_service(
            kb_name: str,
            vector_store_type: Union[str, SupportedVSType],
            embed_model: str = get_default_embedding(),
            kb_info: str = None,
    ) -> KBService:
        if isinstance(vector_store_type, str):
            vector_store_type = getattr(SupportedVSType, vector_store_type.upper())
        params = {
            "knowledge_base_name": kb_name,
            "embed_model": embed_model,
            "kb_info": kb_info,
        }
        if SupportedVSType.FAISS == vector_store_type:
            from copilotkit.knowledge_base.kb_service.faiss_kb_service import (
                FaissKBService
            )
            return FaissKBService(**params)
        elif SupportedVSType.MILVUS == vector_store_type:
            from copilotkit.knowledge_base.kb_service.milvus_kb_service import (
                MilvusKBService
            )

    @staticmethod
    async def get_service_by_name(kb_name: str) -> KBService:
        _, vs_type, embed_model = await load_kb_from_db(kb_name)
        if _ is None:
            return None
        return KBServiceFactory.get_service(kb_name, vs_type, embed_model)

    @staticmethod
    def get_default():
        return KBServiceFactory.get_service("default", SupportedVSType.DEFAULT)


def get_kb_details() -> List[Dict]:
    kbs_in_folder = list_kbs_from_folder()
    kbs_in_db = KBService.list_kbs()

    result = {}
    for kb in kbs_in_folder:
        result[kb] = {
            "kb_name": kb,
            "vs_type": "",
            "kb_info": "",
            "embed_model": "",
            "file_count": 0,
            "create_time": None,
            "in_folder": True,
            "in_db": False
        }
    for kb in kbs_in_db:

        kb_detail = get_kb_details(kb)
        if kb_detail:
            kb_detail["in_db"] = True
            if kb in result:
                result[kb].update(kb_detail)
            else:
                kb_detail["in_folder"] = False
                result[kb] = kb_detail
    data = []
    for i, v in enumerate(result.values()):
        v["No"] = i + 1
        data.append(v)
    return data


async def get_kb_file_details(kb_name: str) -> List[Dict]:
    kb = await KBServiceFactory.get_service_by_name(kb_name)
    if kb is None:
        return []

    files_in_folder = list_files_from_folder(kb_name)
    files_in_db = await kb.list_files()
    result = {}

    for doc in files_in_folder:
        result[doc] = {
            "kb_name": kb_name,
            "file_name": doc,
            "file_ext": os.path.splitext(doc)[-1],
            "file_version": 0,
            "document_loader": "",
            "docs_count": 0,
            "text_splitter": "",
            "create_time": None,
            "in_folder": True,
            "in_db": False
        }
    lower_names = {x.lower(): x for x in result}
    for doc in files_in_db:
        doc_detail = await get_file_detail(kb_name, doc)
        if doc_detail:
            doc_detail["in_db"] = True
            if doc.lower() in lower_names:
                result[lower_names[doc.lower()]].update(doc_detail)
            else:
                doc_detail["in_folder"] = False
                result[doc] = doc_detail
    data = []
    for i, v in enumerate(result.values()):
        v["No"] = i + 1
        data.append(v)
    return data


def score_threshold_process(score_threshold, k, docs):
    if score_threshold is not None:
        cmp = (
            operator.le
        )
        docs = [
            (doc, similarity)
            for doc, similarity in docs
            if cmp(score_threshold, similarity)
        ]
    return docs[:k]

