import logging
from abc import abstractmethod

from common.utils import generated_identified_name
from streams.stream_cfg import StreamCfg
from typing import Union
from common import config


def format_timedelta(t_delta, fmt):
    d = {"days": t_delta.days}
    d["hours"], rem = divmod(t_delta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)


class DataStreamBase:

    def __init__(self, signal_name, timestep, version, stream_record_fields, have_scd, timestamp_field='timestamp',
                 symbol_field='symbol'):
        self.signal_name = signal_name
        self.timestep = timestep
        self.version = version
        self.timestamp_field = timestamp_field
        self.symbol_field = symbol_field
        self.stream_record_fields = stream_record_fields
        self.have_scd = have_scd
        self.name_fields = set([item[0] for item in stream_record_fields])

        # assert self.timestamp_field != '__indexed_timestamp'    # reserved name
        # assert self.symbol_field != '__symbol'    # reserved name
        # assert self.symbol_field == config.SYSTEM_SYMBOL_COL or self.symbol_field in self.name_fields
        # assert self.timestamp_field == config.SYSTEM_TIMESTAMP_COL or self.timestamp_field in self.name_fields

    @classmethod
    def from_config(cls, cfg: Union[dict, StreamCfg]):
        assert isinstance(cfg, (dict, StreamCfg))
        
        if isinstance(cfg, dict):
            stream_cfg = StreamCfg(**cfg)
        else:
            stream_cfg = cfg
        return cls(
            signal_name=stream_cfg.signal_name,
            timestep=stream_cfg.timestep,
            version=stream_cfg.version,
            timestamp_field=stream_cfg.timestamp_field,
            symbol_field=stream_cfg.symbol_field,
            have_scd=stream_cfg.have_scd,
            stream_record_fields=stream_cfg.stream_fields
        ).set_backend(stream_cfg.storage_backend(stream_cfg))

    @abstractmethod
    def get_unique_id(self):
        raise NotImplementedError("Should implement this!")

    def set_backend(self, backend):
        self.backend = backend
        return self

    def append(self, record, commit_every=1000):
        logging.info(f"Commit: {commit_every}")
        if config.SYSTEM_TIMESTAMP_COL not in record.columns:
            record[config.SYSTEM_TIMESTAMP_COL] = record[self.timestamp_field]
        if config.SYSTEM_SYMBOL_COL not in record.columns:
            record[config.SYSTEM_SYMBOL_COL] = record[self.symbol_field]

        if len(record.index) > 0:
            self.backend.append(record, commit_every, have_scd=self.have_scd)

    def get_record(self, indexed_timestamp,symbol_column, target_symbols, filter_query):
        return self.backend.get_record(indexed_timestamp,symbol_column, target_symbols, filter_query)
    
    def get_record_v2(self, indexed_timestamp,symbol_column, timestamp_column, target_symbols, filter_query):
        return self.backend.get_record_v2(indexed_timestamp,symbol_column, timestamp_column, target_symbols, filter_query)

    def get_record_range(self, included_min_timestamp, included_max_timestamp,symbol_column, target_symbols, filter_query):
        return self.backend.get_record_range(included_min_timestamp, included_max_timestamp,symbol_column, target_symbols, filter_query)
    
    def get_record_range_v2(self, included_min_timestamp, included_max_timestamp,symbol_column, timestamp_column, target_symbols, filter_query):
        return self.backend.get_record_range_v2(included_min_timestamp, included_max_timestamp,symbol_column, timestamp_column, target_symbols, filter_query)

    def get_record_filter_query(self, filter_query):
        return self.backend.get_record_filter_query(filter_query)

    def get_distinct_symbol(self,symbol_column, table_name):
        return self.backend.get_distinct_symbol(symbol_column, table_name)

    @property
    def latest_timestamp(self):
        return self.backend.get_latest_timestamp()

    @property
    def next_timestamp(self):
        return self.latest_timestamp + self.timestep

    def __str__(self) -> str:
        msg = "Signal: {}, timestep: {}, verison: {}"
        return msg.format(self.signal_name, self.timestep, self.version)

    @property
    def identified_name(self):
        return generated_identified_name(self.signal_name, self.timestep, self.version)

    @classmethod
    def generated_identified_name(cls, signal_name, timestep, version):
        return "{}_{}_{}".format(signal_name, format_timedelta(timestep, '{days}d{hours}h{minutes}m'), version)

    # @classmethod
    # def load(cls, backend_cls: StorageBase, signal_name, timestep, version):
    # stream_info = backend_cls.find(cls.generated_identified_name(signal_name, timestep, version))
    # if stream_info is None:
    #    return None
    # else:
    #    pass