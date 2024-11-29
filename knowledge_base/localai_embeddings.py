import warnings

from langchain_core.embeddings import Embeddings
from langchain.pydantic_v1 import BaseModel, Field, root_validator
from typing import (
    Any,
    Optional,
    Union,
    Literal,
    Set,
    Sequence,
    Tuple,
    Dict,
    List
)
from copilotkit.api_server.utils import run_in_thread_pool
from langchain_core.utils import get_from_dict_or_env, get_pydantic_field_names
from langchain_community.utils.openai import is_openai_v1


class LocalAIEmbeddings(BaseModel, Embeddings):
    client: Any = Field(default=None, exclude=True)
    async_client: Any = Field(default=None, exclude=True)

    model: str = "text-embedding-ada-002"
    deployment: str = model
    openai_api_version: Optional[str] = None
    openai_api_base: Optional[str] = Field(default=None, alias="base_url")
    openai_proxy: Optional[str] = None
    embedding_ctx_length: int = 8191
    openai_api_key: Optional[str] = Field(default=None, alias="api_key")
    openai_organization: Optional[str] = Field(default=None, alias="organization")
    allowed_special: Union[Literal["all"], Set[str]] = set()
    disallowed_special: Union[Literal["all"], Set[str], Sequence[str]] = "all"
    chunk_size: int = 1000
    max_retries: int = 3
    request_timeout: Union[float, Tuple[float, float], Any, None] = Field(
        default=None, alias="timeout"
    )
    headers: Any = None
    show_progress_bar: bool = False
    model_kwargs: Dict[str, Any] = Field(default_factory=dict)


    class Config:
        allow_population_by_field_name = True


    @root_validator(pre=True)
    def build_extras(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        all_required_field_names = get_pydantic_field_names(cls)
        extra = values.get("model_kwargs", {})
        for field_name in list(values):
            if field_name in extra:
                raise ValueError(f"Found {field_name} supplied twice")
            if field_name not in all_required_field_names:
                warnings.warn(
                    f"warning!!"
                )
                extra[field_name] = values.pop(field_name)
        invalid_model_kwargs = all_required_field_names.intersection(extra.keys())
        if invalid_model_kwargs:
            raise ValueError(
                f"Parameters {invalid_model_kwargs} should be specified explicitly. "
                f"Instead they were passed in as part of `model_kwargs` parameter."
            )
        values["model_kwargs"] = extra
        return values

    @root_validator()
    def validate_environment(cls, values: Dict) -> Dict:
        values["openai_api_key"] = get_from_dict_or_env(
            values,
            "openai_api_key",
            "OPENAI_API_KEY"
        )
        values["openai_api_base"] = get_from_dict_or_env(
            values,
            "openai_api_base",
            "OPENAI_API_BASE",
            default="",
        )
        values["openai_proxy"] = get_from_dict_or_env(
            values,
            "openai_proxy",
            "OPENAI_PROXY",
            default="",
        )

        default_api_version = ""
        values["openai_api_version"] = get_from_dict_or_env(
            values,
            "openai_api_version",
            "OPENAI_API_VERSION",
            default=default_api_version
        )
        values["openai_organization"] = get_from_dict_or_env(
            values,
            "openai_organization",
            "OPENAI_ORGANIZATION",
            default="",
        )
        try:
            import openai

            if is_openai_v1():
                client_params = {
                    "api_key": values["openai_api_key"],
                    "organization": values["openai_organization"],
                    "base_url": values["openai_api_base"],
                    "timeout": values["request_timeout"],
                    "max_retries": values["max_retries"],
                }

                if not values.get("client"):
                    values["client"] = openai.OpenAI(**client_params).embeddings
                if not values.get("async_client"):
                    values["async_client"] = openai.AsyncOpenAI(
                        **client_params
                    ).embeddings
            elif not values.get("client"):
                values["client"] = openai.Embedding
            else:
                pass
        except ImportError:
            raise ImportError(
                "Could not import openai python package. "
                "Please install it with `pip install openai`."
            )
        return values

    @property
    def _invocation_params(self) -> Dict:
        openai_args = {
            "model": self.model,
            "timeout": self.request_timeout,
            "extra_headers": self.headers,
            **self.model_kwargs
        }
        return openai_args

    def _embedding_func(self, text: str, *, engine: str) -> List[float]:
        return (
            embed(
                self,
                input=[text],
                **self._invocation_params,
            )
            .data[0]
            .embedding
        )

    async def _aembedding_func(self, text: str, *, engine: str) -> List[float]:
        return (
            (
                await async_embed(
                    self,
                    input=[text],
                    **self._invocation_params,
                )
            )
            .data[0]
            .embedding
        )

    def embed_documents(self, texts: List[str], chunk_size: Optional[int] = 0) -> List[List[float]]:
        def task(seq, text):
            return (seq, self._embedding_func(text, engine=self.deployment))

        params = [{"seq": i, "text": text} for i, text in enumerate(texts)]
        result = list(run_in_thread_pool(func=task, params=params))
        result = sorted(result, key=lambda x: x[0])
        return [x[1] for x in result]

    async def aembed_documents(self, texts: List[str], chunk_size: Optional[int] = 0) -> List[List[float]]:
        embeddings = []
        for text in texts:
            response = await self._aembedding_func(text, engine=self.deployment)
            embeddings.append(response)
        return embeddings

    def embed_query(self, text: str) -> List[float]:
        embedding = self._embedding_func(text, engine=self.deployment)
        return embedding

    async def aembed_query(self, text: str) -> List[float]:
        embedding = await self._aembedding_func(text, engine=self.deployment)
        return embedding


def embed(embeddings: LocalAIEmbeddings, **kwargs: Any) -> Any:
    def _embed(**kwargs) -> Any:
        response = embeddings.client.create(**kwargs)
        return response
    return _embed(**kwargs)

async def async_embed(embeddings: LocalAIEmbeddings, **kwargs: Any) -> Any:
    async def _async_embed(**kwargs: Any) -> Any:
        response = await embeddings.async_client.create(**kwargs)
        return response
    return await _async_embed(**kwargs)