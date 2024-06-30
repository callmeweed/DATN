import datetime
from typing import Any, List, Mapping, Optional
from langchain.chat_models import PromptLayerChatOpenAI
from langchain.schema import BaseMessage, ChatResult
from langchain.callbacks.manager import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
import os

api_base = os.environ.get("API_BASE")
api_version = os.environ.get("API_VERSION")
api_type = os.environ.get("API_TYPE")


class PromptLayerCustom(PromptLayerChatOpenAI):
    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any
    ) -> ChatResult:
        """Call ChatOpenAI generate and then call PromptLayer API to log the request."""
        from promptlayer.utils import get_api_key, promptlayer_api_request

        request_start_time = datetime.datetime.now().timestamp()
        generated_responses = super()._generate(
            messages,
            stop,
            run_manager,
            api_base=api_base,
            api_type=api_type,
            api_version=api_version,
            **self.model_kwargs
        )
        request_end_time = datetime.datetime.now().timestamp()
        message_dicts, params = super()._create_message_dicts(messages, stop)
        for i, generation in enumerate(generated_responses.generations):
            response_dict, params = super()._create_message_dicts(
                [generation.message], stop
            )
#             print(response_dict)
            params = {**params, **kwargs}
            pl_request_id = promptlayer_api_request(
                "langchain.PromptLayerChatOpenAI",
                "langchain",
                message_dicts,
                params,
                self.pl_tags,
                response_dict[0],
                request_start_time,
                request_end_time,
                get_api_key(),
                return_pl_id=self.return_pl_id,
            )
            if self.return_pl_id:
                if generation.generation_info is None or not isinstance(
                    generation.generation_info, dict
                ):
                    generation.generation_info = {}
                generation.generation_info["pl_request_id"] = pl_request_id
        return generated_responses

    async def _agenerate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
        **kwargs: Any
    ) -> ChatResult:
        """Call ChatOpenAI agenerate and then call PromptLayer to log."""
        from promptlayer.utils import get_api_key, promptlayer_api_request_async

        request_start_time = datetime.datetime.now().timestamp()
        generated_responses = await super()._agenerate(
            messages,
            stop,
            run_manager,
            api_base=api_base,
            api_type=api_type,
            api_version=api_version,
            **self.model_kwargs
        )
        request_end_time = datetime.datetime.now().timestamp()
        message_dicts, params = super()._create_message_dicts(messages, stop)
        for i, generation in enumerate(generated_responses.generations):
            response_dict, params = super()._create_message_dicts(
                [generation.message], stop
            )
            params = {**params, **kwargs}
            pl_request_id = await promptlayer_api_request_async(
                "langchain.PromptLayerChatOpenAI.async",
                "langchain",
                message_dicts,
                params,
                ["openai-chat"],
                response_dict[0],
                request_start_time,
                request_end_time,
                get_api_key(),
                return_pl_id=False,
            )
        return generated_responses
