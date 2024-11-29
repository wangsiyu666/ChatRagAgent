from copilotkit.knowledge_base.kb_service.base import (
    KBService,
    SupportedVSType,
    score_threshold_process
)

from langchain.vectorstores.milvus import Milvus
from langchain.docstore.document import Document
from typing import (
    List,
    Dict
)
from copilotkit.utils import get_Embeddings
from copilotkit.knowledge_base.utils import KnowledgeFile
from copilotkit.db.repository.knowledge_file_respository import (
    list_file_num_docs_id_by_kb_name_and_file_name
)


class MilvusKBService(KBService):
    milvus: Milvus


    @staticmethod
    def get_collection(milvus_name):
        from pymilvus import Collection
        return Collection(milvus_name)

    def get_doc_by_ids(self, ids: List[str]) -> List[Document]:
        result = []
        if self.milvus.col:
            data_list = self.milvus.col.query(
                expr=f"pk in {[int(_id) for _id in ids]}", output_fields=["*"]
            )
            for data in data_list:
                text = data.pop("text")
                result.append(Document(page_content=text, metadata=data))
        return result

    def del_doc_by_ids(self, ids: List[str]) -> bool:
        self.milvus.col.delete(expr=f"pk in {ids}")


    @staticmethod
    def search(milvus_name, content, limit=3):
        search_params = {
            "metric_type": "L2",
            "params": {"nprobe": 10}
        }
        c = MilvusKBService.get_collection(milvus_name)
        return c.search(content, "embeddings", search_params, limit=limit, output_fields=["content"])

    def do_create_kb(self):
        pass


    def vs_type(self) -> str:
        return SupportedVSType.MILVUS


    def _load_milvus(self):
        self.milvus = Milvus(
            embedding_function=get_Embeddings(self.embed_model),
            collection_name=self.kb_name,
            connection_args={
                "host": "192.168.102.7",
                "port": "19530",
                "user": "",
                "password": "",
                "secure": False
            },
            index_params={
                "metric_type": "IP",
                "index_type": "FLAT"
            },
            search_params={
                "metric_type": "IP",
                "params": {"nprobe": 10}
            },
            text_field="Question",
            vector_field="embedding_Q",
            auto_id=True
        )

    def do_init(self):
        self._load_milvus()

    def do_drop_kb(self):
        if self.milvus.col:
            self.milvus.col.release()
            self.milvus.col.drop()

    async def do_search(
            self,
            query: str,
            top_k: int,
            score_threshold: float,
    ):
        self._load_milvus()
        embed_func = get_Embeddings(self.embed_model)
        embeddings = embed_func.embed_query(query)

        docs = self.milvus.similarity_search_with_score_by_vector(embeddings, top_k)
        return score_threshold_process(score_threshold, top_k, docs)

    async def do_add_doc(
            self,
            docs: List[Document],
            **kwargs,
    ) -> List[Dict]:
        for doc in docs:
            for k, v in doc.metadata.items():
                doc.metadata[k] = str(v)

            for field in self.milvus.fields:
                doc.metadata.setdefault(field, "")

            doc.metadata.pop(self.milvus._text_field, None)
            doc.metadata.pop(self.milvus._vector_field, None)

        ids = self.milvus.add_documents(docs)
        doc_infos = [{"id": id, "metadata": doc.metadata} for id, doc in zip(ids, docs)]
        return doc_infos

    async def do_delete_doc(self, kb_file: KnowledgeFile):
        id_list = list_file_num_docs_id_by_kb_name_and_file_name(
            kb_file.kb_name, kb_file.filename
        )

        if self.milvus.col:
            self.milvus.col.delete(expr=f"pk in {id_list}")

    def do_clear_vs(self):
        if self.milvus.col:
            self.do_drop_kb()
            self.do_init()

