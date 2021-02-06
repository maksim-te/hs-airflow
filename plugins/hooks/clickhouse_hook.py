from airflow.hooks.http_hook import HttpHook
from clickhouse_driver import Client


class ClickHouseHook(HttpHook):

    def __init__(self, clickhouse_conn_id):
        self.clickhouse_conn_id = clickhouse_conn_id

    def get_client(self, **kwargs):
        compression = False
        if 'compression' in kwargs:
            compression = kwargs.pop('compression')

        database = 'default'
        if 'database' in kwargs:
            database = kwargs.pop('database')

        secure = False
        if 'secure' in kwargs:
            secure = kwargs.pop('secure')

        verify = False
        if 'verify' in kwargs:
            verify = kwargs.pop('verify')

        conn = self.get_connection(self.clickhouse_conn_id)
        client = Client(**{
            'host': conn.host,
            'port': conn.port,
            'user': conn.login,
            'password': conn.password,
            'database': database,
            'client_name': 'clickhouse_plugin',
            'compression': compression,
            'secure': secure,
            'verify': verify,
            'settings': kwargs,
        })
        return client

    def insert_data(self, database, table, values, **kwargs):
        """

        :param database: str database in clickhouse
        :param table: str table's name in database
        :param values: list of dictionaries, dict.keys are equal to field names in table
        :param kwargs:
        :return: int number inserted rows
        """
        if not values:
            return 0
        columns = values[0].keys()
        print(columns)
        client = self.get_client(**kwargs)
        query_tmpl = 'INSERT INTO {database}.{table} VALUES'
        query = query_tmpl.format(**{
            'database': database,
            'table': table
        })
        print(values)
        result = client.execute(query, values)
        return result

    def select_data(self,
                query='',
                **kwargs):
        """

        :param query: str query text
        :param kwargs:
        :return: list of dictionaries, dict.keys are equal to field names in select
        """
        client = self.get_client(**kwargs)
        data = []
        if not query:
            return data
        query_result = client.execute(query, with_column_types=True)

        for row_data in query_result[0]:
            data_dict = {}
            for value, column_metadata in zip(row_data, query_result[1]):
                data_dict[column_metadata[0]] = value
            data.append(data_dict)
        return data

    def execute_query_without_data(self,
                query='',
                **kwargs):
        """

        :param query: str query text
        :param kwargs:
        :return: bool True if query is OK
        """
        client = self.get_client(**kwargs)
        if not query:
            return False
        client.execute(query)
        return True

    def get_conn(self, headers=None, **kwargs):
        return self.get_client(**kwargs)