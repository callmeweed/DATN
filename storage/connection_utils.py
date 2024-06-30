import hashlib
import logging
import os
import traceback
from enum import Enum
from typing import List

import numpy as np
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2.extras
from pandas import DataFrame

from common import config
from common.template_loader import TemplateLoader


class ConnectionId(Enum):
    POSTGRES_LOCAL = 'TIMESCALEDB_LOCAL'
    POSTGRES_PRODUCTION = 'TIMESCALEDB_PRD'
    ENCAP_TIMESCALEDB_PRD = 'ENCAP_TIMESCALEDB_PRD'


def get_connection_name_by_env():
    if os.getenv('ENVIRONMENT') == 'PRD':
        return ConnectionId.POSTGRES_PRODUCTION.value
    return ConnectionId.POSTGRES_LOCAL.value


def create_unique_key(row, columns):
    string_to_hash = ''.join(map(str, row[columns]))

    unique_key = hashlib.md5(string_to_hash.encode()).hexdigest()

    return unique_key

class PSQLHook(PostgresHook):
    SYSTEM_TIMESTAMP_COL = 'indexed_timestamp_'
    SYSTEM_SYMBOL_COL = 'symbol_'
    SYSTEM_COLS = [(SYSTEM_TIMESTAMP_COL, 'timestamp'), (SYSTEM_SYMBOL_COL, 'text')]

    def __init__(self):
        print('Starting connection')

        self.connection_name = get_connection_name_by_env()
        super().__init__(postgres_conn_id=self.connection_name)
        self.conn = self.get_conn()

    def get_conn(self):
        if self.conn is None or (self.conn and self.conn.closed):
            self.conn = PostgresHook(postgres_conn_id=self.connection_name).get_conn()
        return self.conn

    def __del__(self):
        print('Deleting connection')
        self.conn.close()

    def execute_postgres_query(self, long_queries: str) -> None:
        queries = [q.strip() for q in long_queries.split(';')]
        try:
            cursor = self.get_conn().cursor()
            for query in queries:
                if len(query) > 3:
                    logging.info("Execute query :\n" + query)
                    cursor.execute(query)
                    self.conn.commit()
        except Exception as e:
            logging.error(f"Failed query: {query}")
            logging.error(e)
            raise e

    def execute_query_params(self, long_queries: str, params: list) -> None:
        queries = [q.strip() for q in long_queries.split(';')]
        try:
            cursor = self.get_conn().cursor()
            for query in queries:
                if len(query) > 3:
                    cursor.execute(query, params)
                    self.conn.commit()
        except Exception as e:
            logging.error(f"Failed query: {query}")
            logging.error(e)
            traceback.print_exc()

    def execute_query_result_dataframe(self, query: str, parameters=None):
        try:
            # cursor = self.get_conn().cursor()
            logging.info("Execute query :\n" + query)
            # cursor.execute(query)
            result = self.get_pandas_df(query, parameters)
            # self.conn.commit()
            return result

        except Exception as e:
            logging.error(f"Failed query: {query}")
            logging.error(e)
            raise e

    def get_all_tables_name(self, schema='public'):
        try:
            cursor = self.get_conn().cursor()
            logging.info("Get all tables name in schema:" + schema)
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")
            result = cursor.fetchall()
            self.conn.commit()
            res = []
            for row in result:
                res.append(row[0])
            return res
        except Exception as e:
            logging.error(e)
            raise e

    def get_all_columns_name_from_table(self, table_name, schema='public'):
        try:
            cursor = self.get_conn().cursor()
            logging.info("Get all columns in table:" + table_name)
            cursor.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table_name}'")
            result = cursor.fetchall()
            self.conn.commit()
            res = []
            for row in result:
                res.append(row[0])
            if len(res) == 0:
                raise Exception(f"Table {table_name} not found")
            return res
        except Exception as e:
            logging.error(e)
            raise e

    def create_table(self, table_name, columns_list, extend_column=True, primary_list=None, require_primary=True):
        loader = TemplateLoader()
        if extend_column:
            column_extend = self.SYSTEM_COLS
        else:
            column_extend = [(self.SYSTEM_TIMESTAMP_COL, 'timestamp')]

        columns_list.extend(column_extend)
        if primary_list is None and require_primary:
            primary_list = [self.SYSTEM_TIMESTAMP_COL, self.SYSTEM_SYMBOL_COL]
        args = {'table_name': table_name, 'columns_list': columns_list, 'primary_list': primary_list}
        sql = loader.render("create_table.tpl", **args)
        self.execute_postgres_query(sql)
        return

    def get_record(self, table_name, indexed_timestamp, symbol_column, target_symbols=None, filter_query=None):
        loader = TemplateLoader()
        args = {'table_name': table_name, 'indexed_timestamp': indexed_timestamp, "symbol_column": symbol_column,
                'target_symbols': target_symbols, 'filter_query': filter_query}
        sql = loader.render("get_record.tpl", **args)
        return self.execute_query_result_dataframe(sql)

    def get_record_range(self, table_name, included_min_timestamp, included_max_timestamp,
                         symbol_column, target_symbols=None, filter_query=None, ):
        loader = TemplateLoader()
        args = {'table_name': table_name, 'included_min_timestamp': included_min_timestamp,
                'included_max_timestamp': included_max_timestamp, "symbol_column": symbol_column,
                'target_symbols': target_symbols, 'filter_query': filter_query}
        sql = loader.render("get_record_range.tpl", **args)
        return self.execute_query_result_dataframe(sql)

    def get_record_range_v2(
        self,
        table_name: str,
        included_min_timestamp: str,
        included_max_timestamp: str,
        symbol_column: str = SYSTEM_SYMBOL_COL,
        timestamp_column: str = SYSTEM_TIMESTAMP_COL,
        target_symbols: List = None,
        filter_query: str = None
    ) -> DataFrame:
        """
        Get record from redshift table

        :param table_name: table name
        :param included_min_timestamp: included min timestamp with format YYYY-MM-DD
        :param included_max_timestamp: included max timestamp with format YYYY-MM-DD
        :param symbol_column: symbol column name
        :param timestamp_column: timestamp column name
        :param target_symbols: target symbols
        :param filter_query: filter query
        :return: DataFrame
        """
        loader = TemplateLoader()
        args = {
            "table_name": table_name,
            "included_min_timestamp": included_min_timestamp,
            "included_max_timestamp": included_max_timestamp,
            "symbol_column": symbol_column,
            "timestamp_column": timestamp_column,
            "target_symbols": target_symbols,
            "filter_query": filter_query
        }
        sql = loader.render("get_record_range_v2.tpl", **args)
        return self.execute_query_result_dataframe(sql)

    def get_distinct_symbol(self, symbol_column, table_name):
        loader = TemplateLoader()
        arg = {
            "symbol_column": symbol_column, "table_name": table_name
        }
        sql = loader.render("get_distinct_symbol.tpl", **arg)
        return self.execute_query_result_dataframe(sql)

    def get_distinct_symbol_exclude_future(self, symbol_column, table_name):
        loader = TemplateLoader()
        arg = {
            "symbol_column": symbol_column, "table_name": table_name
        }
        sql = loader.render("get_distinct_symbol_exclude_future.tpl", **arg)
        return self.execute_query_result_dataframe(sql)

    def append(
            self,
            table_name,
            df_record,
            replace=True,
            replace_index=None,
            commit_every=1000,
            have_scd=False,
            scd_field=None
    ):
        if replace_index is None:
            replace_index = [config.SYSTEM_SYMBOL_COL, config.SYSTEM_TIMESTAMP_COL]

        logging.info(f"Commit CONNECTION : {commit_every}")
        logging.info(f"Replace : {replace}")
        correct_cols = self.get_all_columns_name_from_table(table_name)
        logging.info(f"correct_cols {correct_cols} ")
        logging.info(replace_index)
        df_record = df_record[correct_cols]
        try:
            self.insert_rows(
                table_name,
                rows=set(df_record.itertuples(index=False)) if replace == True else df_record.itertuples(index=False),
                replace=replace,
                replace_index=replace_index,
                target_fields=correct_cols,
                commit_every=commit_every
            )
            logging.info(f"Table {table_name} insert successfully")
        except Exception as e:
            logging.error(f"Failed append : " + table_name)
            logging.error(e)
            raise e

        if have_scd:
            if scd_field is None:
                scd_field = replace_index
            scd_field.append('last_updated')
            self.update_scd_table(table_name, scd_field)

    def update_scd_table(self, source_table: str, scd_field: list[str]) -> None:
        scd_table_name = f"{source_table}_scd"

        logging.info("Start get latest record from scd table")
        sql_get_latest_record = f"SELECT * FROM {scd_table_name} WHERE valid_to IS NULL"
        df_scd_table = self.execute_query_result_dataframe(sql_get_latest_record)

        logging.info(f"SCD table have {len(df_scd_table)} records")
        logging.info("Start get current record from source table")
        df_current_table = self.execute_query_result_dataframe(f"SELECT * FROM {source_table}")

        logging.info(f"Source table have {len(df_current_table)} records")
        logging.info(f"scd field {scd_field}")
        df_current_table['unique_key'] = df_current_table.apply(create_unique_key, axis=1, columns=scd_field)

        logging.info("Start update scd table field valid_to")
        # First update scd table field end date.
        unique_scd_table = df_scd_table['unique_key'].loc[~df_scd_table['unique_key'].isin(df_current_table['unique_key'])]

        end_df = df_scd_table[df_scd_table['unique_key'].isin(unique_scd_table)]
        end_df['valid_to'] = pd.to_datetime('today')

        try:
            self.insert_rows(
                scd_table_name,
                rows=set(end_df.itertuples(index=False)),
                replace=True,
                replace_index='unique_key',
                target_fields=end_df.columns.tolist(),
                commit_every=10000
            )
            logging.info(f"Table {scd_table_name} insert successfully")
        except Exception as e:
            logging.error(f"Failed append : " + scd_table_name)
            logging.error(e)
            raise e
        logging.info("Start update scd table field valid_from")
        # Second, append new record from current table to scd table.
        unique_current_table = df_current_table['unique_key'].loc[
            ~df_current_table['unique_key'].isin(df_scd_table['unique_key'])]

        new_df = df_current_table[df_current_table['unique_key'].isin(unique_current_table)]

        new_df['valid_from'] = pd.to_datetime('today')
        new_df['valid_to'] = None
        new_df = new_df.replace({np.nan: None})
        logging.info(f"Column new df list {new_df.columns}")
        try:
            self.insert_rows(
                scd_table_name,
                rows=set(new_df.itertuples(index=False)),
                replace=True,
                replace_index='unique_key',
                target_fields=new_df.columns.tolist(),
                commit_every=10000
            )
            logging.info(f"Table {scd_table_name} insert successfully")
        except Exception as e:
            logging.error(f"Failed append : " + scd_table_name)
            logging.error(e)
            raise e

    def filter_columns_in_data(self, table_name, df_record) -> list:
        correct_cols = self.get_all_columns_name_from_table(table_name)
        filtered_column_name = []
        logging.info(f"correct_cols {correct_cols} ")
        logging.info(f"columns {df_record.columns}")
        for col in correct_cols:
            if col in df_record.columns:
                filtered_column_name.append(col)
        return filtered_column_name

    def append_without_redundant_columns(self, table_name, df_record, replace=True,
                                         replace_index=[config.SYSTEM_SYMBOL_COL, config.SYSTEM_TIMESTAMP_COL],
                                         commit_every=1000):
        filtered_column_name = self.filter_columns_in_data(table_name, df_record)
        logging.info(f'Filtered column names {filtered_column_name}')
        df_record = df_record[filtered_column_name]
        list_record = df_record.itertuples(index=False)
        set_record = set(list_record)

        try:
            self.insert_rows(
                table_name,
                set_record,
                replace=replace,
                replace_index=replace_index,
                target_fields=filtered_column_name,
                commit_every=commit_every
            )
            logging.info(f"Table {table_name} insert successfully")
        except Exception as e:
            logging.error(f"Failed append : " + table_name)
            logging.error(e)
            raise e

    def get_latest_timestamp(self, table_name):
        loader = TemplateLoader()
        args = {'table_name': table_name}
        sql = loader.render("get_latest_timestamp.tpl", **args)
        df_output = self.execute_query_result_dataframe(sql)
        return df_output['max'].iloc[0]

    def delete_from_table(self, table_name, filter_query):
        loader = TemplateLoader()
        args = {'table_name': table_name, 'filter_query': filter_query}
        sql = loader.render("delete_from_table.tpl", **args)
        logging.info(f"Delete from table {table_name} with filter {filter_query}")
        self.execute_postgres_query(sql)
        return

    def truncate_table(self, table_name):
        loader = TemplateLoader()
        args = {'table_name': table_name}
        sql = loader.render("truncate_table.tpl", **args)
        logging.info(f"Truncate table {table_name}: {sql}")
        try:
            self.execute_postgres_query(sql)
        except Exception as e:
            logging.error(f"Failed truncate table : " + table_name)
            logging.error(e)
            raise e
        return

    def update_rows(self, sql_query, data):
        try:
            connection = self.get_conn()
            cursor = connection.cursor()
            logging.info("Execute query :\n" + sql_query)
            psycopg2.extras.execute_batch(cursor, sql_query, data)
            connection.commit()
        except Exception as e:
            logging.error(e)
            raise e

    def get_record_filter_query(
        self,
        table_name: str,
        filter_query: str = None
    ) -> DataFrame:
        """
        Get record from redshift table

        :param table_name: table name
        :param filter_query: filter query
        :return: DataFrame
        """
        loader = TemplateLoader()
        args = {
            "table_name": table_name,
            "filter_query": filter_query
        }
        sql = loader.render("get_record_filter_query.tpl", **args)
        return self.execute_query_result_dataframe(sql)


class EncapPSQLHook(PostgresHook):

    def __init__(self):
        print('Starting connection')
        self.connection_name = ConnectionId.ENCAP_TIMESCALEDB_PRD.value
        super().__init__(postgres_conn_id=self.connection_name)
        self.conn = self.get_conn()

    def get_record(self, table_name, indexed_timestamp, symbol_column, target_symbols=None, filter_query=None):
        loader = TemplateLoader()
        args = {'table_name': table_name, 'time': indexed_timestamp, "symbol_column": symbol_column,
                'target_symbols': target_symbols, 'filter_query': filter_query}
        sql = loader.render("encap_get_record.tpl", **args)
        return self.execute_query_result_dataframe(sql)

    def get_record_range(self, table_name, included_min_timestamp, included_max_timestamp,
                         symbol_column, target_symbols=None, filter_query=None, ):
        loader = TemplateLoader()
        args = {'table_name': table_name, 'included_min_timestamp': included_min_timestamp,
                'included_max_timestamp': included_max_timestamp, "symbol_column": symbol_column,
                'target_symbols': target_symbols, 'filter_query': filter_query}
        sql = loader.render("encap_get_record_range.tpl", **args)
        return self.execute_query_result_dataframe(sql)

    def execute_query_params(self, long_queries: str, params: list) -> None:
        queries = [q.strip() for q in long_queries.split(';')]
        try:
            cursor = self.get_conn().cursor()
            for query in queries:
                if len(query) > 3:
                    cursor.execute(query, params)
                    self.conn.commit()
        except Exception as e:
            logging.error(f"Failed query: {query}")
            logging.error(e)
            traceback.print_exc()

    def execute_query_result_dataframe(self, query: str, parameters=None):
        try:
            # cursor = self.get_conn().cursor()
            logging.info("Execute query :\n" + query)
            # cursor.execute(query)
            result = self.get_pandas_df(query, parameters)
            # self.conn.commit()
            return result

        except Exception as e:
            logging.error(f"Failed query: {query}")
            logging.error(e)
            raise e

    def get_distinct_symbol_exclude_future(self, symbol_column, table_name):
        loader = TemplateLoader()
        arg = {
            "symbol_column": symbol_column, "table_name": table_name
        }
        sql = loader.render("get_distinct_symbol_exclude_future.tpl", **arg)
        return self.execute_query_result_dataframe(sql)
