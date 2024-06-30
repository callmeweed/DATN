from datetime import timedelta
from common import config
from pydantic import BaseModel
from typing import List
from typing import Type
from storage.storage_base import StorageBase


class StreamCfg(BaseModel):
    signal_name: str
    same_table_name: bool = False
    timestep: timedelta
    timestamp_field: str = config.SYSTEM_TIMESTAMP_COL
    version: str
    to_create: bool = True
    symbol_field: str = config.SYSTEM_SYMBOL_COL
    stream_fields: List[tuple] = []
    have_scd: bool = False
    storage_backend: Type[StorageBase]
