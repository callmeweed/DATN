import importlib
import inspect
import logging

from common.template_loader import TemplateLoader
from storage.connection_utils import PSQLHook
from storage.database_storage import DatabaseStorage
from streams.stream_cfg import StreamCfg
from common.utils import format_timedelta, generated_identified_name



def identified_table_name(stream_cfg: StreamCfg):
    if stream_cfg.same_table_name:
        return stream_cfg.signal_name

    return generated_identified_name(stream_cfg.signal_name,
                                     stream_cfg.timestep,
                                     stream_cfg.version
                                     )
    # return '{}_{}_{}'.format(
    #    stream_cfg[cfg][source_name].signal_name,
    #    format_timedelta(stream_cfg[cfg][source_name].timestep, '{days}d{hours}h{minutes}m'),
    #    stream_cfg[cfg][source_name].version
    # )


def get_list_table_need_create(miner_cfgs):
    current_table_miners = []
    db_storage = PSQLHook()
    list_table_in_db = db_storage.get_all_tables_name()
    for stream_name in miner_cfgs:
        logging.info(f"{miner_cfgs[stream_name]['output'].signal_name}, {miner_cfgs[stream_name]['output'].to_create}")
        if not miner_cfgs[stream_name]['output'].to_create:
            continue

        table_name = identified_table_name(miner_cfgs[stream_name]['output'])
        current_table_miners.append(table_name)
        if miner_cfgs[stream_name]['output'].have_scd:
            current_table_miners.append(f"{table_name}_scd")

    list_table_need_create = [item for item in current_table_miners if item not in list_table_in_db]
    return list_table_need_create


class Generator:
    def __init__(self, storage, module_name='miners') -> None:
        self.storage = storage
        self.module = importlib.import_module(module_name)

    def get_all_miner_cfg(self, module):
        miner_cfgs = {}
        for name, cls_ in inspect.getmembers(module):
            if inspect.isclass(cls_):
                if not hasattr(cls_, 'output_cfg'):
                    continue
                if isinstance(cls_.output_cfg, dict):
                    cls_.output_cfg = StreamCfg(**cls_.output_cfg)
                if cls_.output_cfg.storage_backend != DatabaseStorage:
                    # Only auto table creation for DatabaseStorage
                    logging.warning('Only auto table creation for DatabaseStorage')
                    continue

                identified_name = identified_table_name(cls_.output_cfg)
                cfg = {}
                cfg['output'] = cls_.output_cfg
                cfg['input'] = cls_.input_cfg
                cfg['class_name'] = name
                miner_cfgs[identified_name] = cfg
        return miner_cfgs

    def create_streams(self, miner_cfgs):
        """
        Create the underlying table in the storage with the miner_cfgs 
        """
        list_table_create = get_list_table_need_create(miner_cfgs)
        logging.info("List table need create: {}".format(list_table_create))
        for table_name in list_table_create:
            # get all columns in table
            if not '_scd' in table_name:
                colums = miner_cfgs[table_name]['output'].stream_fields
                self.create_table(table_name, colums)
            else:
                colums = miner_cfgs[table_name.replace('_scd', '')]['output'].stream_fields
                colums.append(('unique_key', 'text'))
                colums.append(('valid_from', 'timestamp'))
                colums.append(('valid_to', 'timestamp'))
                self.create_table(table_name, colums, ['unique_key'])

    def create_table(self, table_name, colums, primary_list=None):
        """
        Call Database Storage to create table

        :param table_name:
        :param colums: list column and data types
        :param primary_list: list primary key
        :return:
        """
        db_storage = PSQLHook()
        db_storage.create_table(table_name, colums, primary_list=primary_list)
        return
