import importlib
import inspect
import logging

from common.template_loader import TemplateLoader
from storage.connection_utils import PSQLHook
from streams.data_stream_base import DataStreamBase
from storage.storage_base import StorageBase
from storage.database_storage import DatabaseStorage
from streams.stream_cfg import StreamCfg
from common.utils import format_timedelta, generated_identified_name


# def format_timedelta(t_delta, fmt):
#    d = {"days": t_delta.days}
#    d["hours"], rem = divmod(t_delta.seconds, 3600)
#    d["minutes"], d["seconds"] = divmod(rem, 60)
#    return fmt.format(**d)


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

    def generate_task(self, miner_cfg):
        miners = miner_cfg['class_name']
        loader = TemplateLoader('template/dag_template')
        args = {'miners': miners}
        sql = loader.render("task_code.tpl", **args)
        return sql + "\n"

    def generate_logic(self, miner_cfg):
        miners = miner_cfg['class_name']
        target_symbols = "target_symbols=['HPG', 'VNM']"
        loader = TemplateLoader('template/dag_template')
        args = {'miners': miners, 'target_symbols': target_symbols}
        sql = loader.render("logic_code.tpl", **args)
        return sql + "\n"

    def get_distinct_timestep(self, miner_cfgs):
        dist_timestep = set()
        for cfg in miner_cfgs:
            dist_timestep.add(miner_cfgs[cfg]['output']['timestep'])

        return dist_timestep

    def generate_depends(self, miner_cfgs, timestep):
        depends = set()
        miner_cfgs = {k: v for k, v in miner_cfgs.items() if v['output']['timestep'] == timestep}
        for out in miner_cfgs:
            for inp in miner_cfgs:
                if miner_cfgs[out]['output'] in miner_cfgs[inp]['input']:
                    depends.add(f"{miner_cfgs[out]['class_name'].lower()} >> {miner_cfgs[inp]['class_name'].lower()}")
                else:
                    depends.add(f"{miner_cfgs[out]['class_name'].lower()}")

        depends_str = "\n    ".join(depends)
        return depends_str

    def generate_dag(self, miner_cfgs):
        """
        Generate dag file and save to dag folder. Consider jinja2 templating.
        """
        # get distinct timestep from config
        dist_timestep = self.get_distinct_timestep(miner_cfgs)
        # for each timestep
        for item_timestep in dist_timestep:
            str_gen_logic = ""
            str_gen_task = ""
            depends_code = self.generate_depends(miner_cfgs, item_timestep)

            # for miner để lấy ra các timestep giống nhau và gán vào logics + task string
            for cfg in miner_cfgs:
                if miner_cfgs[cfg]['output']['timestep'] == item_timestep:
                    str_gen_logic += self.generate_logic(miner_cfgs[cfg])
                    str_gen_task += self.generate_task(miner_cfgs[cfg])
            args_dag = {
                "dag_name": format_timedelta(item_timestep, '{days}d{hours}h{minutes}m'),
                "logic_code": str_gen_logic,
                "task_code": str_gen_task,
                "schedule_interval": f"timedelta(seconds={item_timestep.total_seconds()})",
                "depends_code": depends_code
            }
            # print(args_dag)
            loader = TemplateLoader('template/dag_template')
            sql = loader.render("dag_code.tpl", **args_dag)
            file_name = f"{format_timedelta(item_timestep, '{days}d{hours}h{minutes}m')}.py"
            with open(f"../dags/{file_name}", "w") as f:
                f.write(sql)
            print(f"Create dag {file_name} successfully")
            # params input gen logic: miners, target_symbols
            # params input gen task: miners

        #  generate tung dag file theo tung timestep
        #    gen logic, gen task, gen cac thong tin con lai, gen dag
        #  save tung dag file vao tung folder

        pass
