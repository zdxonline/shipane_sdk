# -*- coding: utf-8 -*-

import sqlite3
import datetime
from shipane_sdk.jobs.basic_job import BasicJob


class UpdateInfoJob(BasicJob):
    def __init__(self, client, client_aliases=None, name=None, **kwargs):
        super(UpdateInfoJob, self).__init__(name, kwargs.get('schedule', None), kwargs.get('enabled', False))
        self._client = client
        self._client_aliases = client_aliases
        self._db_path=kwargs.get('db_path',None)
        self._logger.info(self._db_path)

    def __call__(self):
        for client_alias in self._client_aliases:
            try:
                client = self._client_aliases[client_alias]
                # init db
                conn = sqlite3.connect(self._db_path)
                cursor = conn.cursor()

                today = datetime.datetime.now().strftime('%Y-%m-%d')
                time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                def getInsertSQL(table_name, columns, values):
                    sql_key = ''  # 数据库行字段
                    sql_value = ''  # 数据库值
                    for i, key in enumerate(columns):  # 生成insert插入语句
                        sql_key = sql_key + ' ' + '"' + key + '"' + ','
                        if isinstance(values[i], int):
                            sql_value = (sql_value + str(values[i]) + ',')
                        elif isinstance(values[i], float):
                            sql_value = (sql_value + str('%.3f' % values[i]) + ',')
                        else:
                            sql_value = (sql_value + '"' + values[i] + '"' + ',')
                    return "INSERT INTO %s (%s) VALUES (%s)" % (table_name, sql_key[:-1], sql_value[:-1])

                # update accounts start
                positions = self._client.get_positions(client)
                accounts = positions['sub_accounts']

                column_names = accounts.columns.values.tolist()
                values = accounts.values.tolist()

                deleteSQL = 'delete from balance WHERE 日期 = %s' % '"' + today + '"'
                self._logger.debug(deleteSQL)
                cursor.execute(deleteSQL)

                column_names.append('日期')
                column_names.append('更新时间')

                for value in values:
                    value.append(today)
                    value.append(time_now)
                    insertSQL = getInsertSQL('balance', column_names, value)
                    self._logger.debug(insertSQL)
                    cursor.execute(insertSQL)
                # update accounts end


                # update positions start
                positions = positions['positions']

                column_names = positions.columns.values.tolist()
                values = positions.values.tolist()

                column_names.append('日期')
                column_names.append('更新时间')

                deleteSQL = 'delete from position WHERE 日期 = %s' % ('"' + today + '"')
                cursor.execute(deleteSQL)

                for value in values:
                    value.append(today)
                    value.append(time_now)
                    insertSQL = getInsertSQL('position', column_names, value)
                    cursor.execute(insertSQL)
                # update positions end

                # update orders start
                orders = self._client.get_orders(client)

                column_names = orders.columns.values.tolist()
                values = orders.values.tolist()

                column_names.append('日期')
                column_names.append('更新时间')

                deleteSQL = 'delete from orders WHERE 日期 = %s' % ('"' + today + '"')
                cursor.execute(deleteSQL)

                for value in values:
                    value.append(today)
                    value.append(time_now)
                    insertSQL = getInsertSQL('orders', column_names, value)
                    cursor.execute(insertSQL)

                # update orders enda


                conn.commit()
                conn.close()

            except:
                self._logger.exception('客户端[%s]更新资产失败', client_alias)
