import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from typing import Any, Optional, Sequence
from langchain.callbacks.manager import Callbacks
from langchain.retrievers.document_compressors.base import BaseDocumentCompressor
from langchain_core.documents import Document
from pydantic import Field, PrivateAttr
from sentence_transformers import CrossEncoder


class LangchainReranker(BaseDocumentCompressor):

    model_name_or_path: str = Field()
    _model: Any = PrivateAttr()
    top_n: int = Field()
    device: str = Field()
    max_length: int = Field()
    batch_size: int = Field()
    num_workers: int = Field()

    def __init__(
            self,
            model_name_or_path: str,
            top_n: int = 3,
            device: str = "cpu",
            max_length: int = 1024,
            batch_size: int = 32,
            num_workers: int = 0,
    ):
        super().__init__(
            top_n=top_n,
            model_name_or_path=model_name_or_path,
            device=device,
            max_length=max_length,
            batch_size=batch_size,
            num_workers=num_workers
        )

    def compress_documents(
        self,
        documents: Sequence[Document],
        query: str,
        callbacks: Optional[Callbacks] = None,
    ) -> Sequence[Document]:
        if len(documents) == 0:
            return []
        doc_list = list(documents)
        _docs = [d.page_content for d in doc_list]
        sentence_pairs = [[query, _doc] for _doc in _docs]
        model = CrossEncoder(
            model_name=self.model_name_or_path,
            max_length=self.max_length,
            device=self.device
        )
        results = model.predict(
            sentences=sentence_pairs,
            batch_size=self.batch_size,
            num_workers=self.num_workers,
            convert_to_tensor=True,
        )

        top_k = self.top_n if self.top_n < len(results) else len(results)

        values, indices = results.topk(top_k)
        final_results = []
        for value, index in zip(values, indices):
            doc = doc_list[index]
            doc.metadata["relevance_score"] = value
            final_results.append(doc)
        return final_results

