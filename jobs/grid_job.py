# -*- coding: utf-8 -*-

import sqlite3
import easyquotation
import time
from urllib3 import request
from shipane_sdk.jobs.basic_job import BasicJob

STOCKS = ['159915']
MSG_SERVER_KER = 'SCU27710T4231a38048a591265edaa5db7ec33bdb5b1b3a12e8040'


class GridJob(BasicJob):
    def __init__(self, client, client_aliases=None, name=None, **kwargs):
        super(GridJob, self).__init__(name, kwargs.get('schedule', None), kwargs.get('enabled', False))
        self._client = client
        self._client_aliases = client_aliases
        self._db_path = kwargs.get('db_path', None)
        self._logger.info(self._db_path)
        self._can_run = True

    def __call__(self):
        if not self._can_run:
            self._logger('something is wrong ,please check it first')
            return
        for client_alias in self._client_aliases:
            try:
                client = self._client_aliases[client_alias]
                # init db
                conn = sqlite3.connect(self._db_path)
                cursor = conn.cursor()

                quotation = easyquotation.use('sina')

                for stock_id in STOCKS:

                    orders = self._client.get_orders(client)

                    get_grids_sql = 'select price,hold,contract_number from grid_%s order by id' % stock_id
                    grids = cursor.execute(get_grids_sql).fetchall()

                    current_price = quotation.real(stock_id)[stock_id]['now']
                    self._logger.info('{} current price is {}'.format(stock_id, current_price))

                    # 更新已成的交易
                    for index, row in orders.iterrows():
                        if not ('已成' in row['备注']):
                            continue
                        contract_number = row['合同编号']

                        # 更新卖出单，把持有置0，并且把合同编号置0，表明这个合同已完成
                        if '卖出' in row['操作']:
                            update_sql = 'update grid_%s set hold=%s,contract_number=0 where contract_number=%s' % (
                                stock_id, 0, contract_number)
                            self._logger.info(update_sql)
                            cursor.execute(update_sql)

                        # 更新买入单，把持有置成成交数量，并把合同编号置0，表明这个合同已完成
                        if '买入' in row['操作']:
                            update_sql = 'update grid_%s set hold=%s,contract_number=0 where contract_number=%s' % (
                                stock_id, row['成交数量'], contract_number)
                            self._logger.info(update_sql)
                            cursor.execute(update_sql)

                    for i, grid in enumerate(grids):
                        if current_price < float(grid[0]):  # 一直找到小于等于当前价格的格子
                            continue

                        if int(grid[2]) > 0:  # 如何合同编号不为0，则表明该格子在交易中，直接返回
                            self._logger.info('%s is on trading' % grid[0])
                            break

                        if int(grid[1]) > 0:  # 如果持有量大于0，说明可以卖出
                            sell_args = {"symbol": stock_id, "priceType": 0, "price": grid[0], "amount": 100}
                            self._logger.info('sell ', sell_args)
                            self._client.sell(**sell_args)
                            for _ in range(20):  # 最多等20秒返回新的委托列表
                                new_orders = self._client.get_orders().sort_values(by='委托时间')
                                if (len(new_orders) > len(orders)):
                                    contract_number = new_orders.loc[len(new_orders) - 1, '合同编号']
                                    update_sql = 'update grid_%s set contract_number=%s where price=%s' % (
                                        stock_id, contract_number, grid[0])
                                    self._logger.info(update_sql)
                                    cursor.execute(update_sql)
                                    break
                                time.sleep(1)
                        else:  # 否则应该买入
                            buy_args = {"symbol": stock_id, "priceType": 0, "price": grid[0], "amount": 100}
                            client.buy(**buy_args)
                            for _ in range(20):
                                new_orders = self._client.get_orders().sort_values(by='委托时间')
                                if (len(new_orders) > len(orders)):
                                    contract_number = new_orders.loc[len(new_orders) - 1, '合同编号']
                                    update_sql = 'update grid_%s set contract_number=%s where price=%s' % (
                                        stock_id, contract_number, grid[0])
                                    self._logger.info(update_sql)
                                    cursor.execute(update_sql)
                                    break
                                time.sleep(1)
                        break  # 后续的格子不再处理
                conn.commit()
                conn.close()

            except:
                self._can_run = False
                self._logger.exception('客户端[%s]网格任务失败', client_alias)
