import os
import sqlite3
import time
import easyquotation
from shipane_sdk.client import Client

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_PATH, 'db', 'Trader.db')

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

client_args={'key':'86925090'}
client = Client(**client_args)

orders = client.get_orders().sort_values(by='委托时间')

grids = cursor.execute('select price,hold,contract_number from grid_159915 order by id').fetchall()

quotation = easyquotation.use('sina')
current_price=quotation.real('159915')['159915']['now']

current_price=1.39

# 更新已成的交易
for index, row in orders.iterrows():
    if not ('已成' in row['备注']):
        continue
    contract_number = row['合同编号']
    # 更新卖出单，把持有置0，并且把合同编号置0，表明这个合同已完成
    if '卖出' in row['操作']:
        update_sql = 'update grid_159915 set hold=%s,contract_number=0 where contract_number=%s' % (0, contract_number)
        print(update_sql)
        cursor.execute(update_sql)

    #更新买入单，把持有置成成交数量，并把合同编号置0，表明这个合同已完成
    if '买入' in row['操作']:
        update_sql = 'update grid_159915 set hold=%s,contract_number=0 where contract_number=%s' % (
            row['成交数量'], contract_number)
        cursor.execute(update_sql)

for i, grid in enumerate(grids):
    if current_price < float(grid[0]):  # 一直找到小于等于当前价格的格子
        continue

    if int(grid[2]) > 0:  # 如何合同编号不为0，则表明该格子在交易中，直接返回
        print('%s is on trading' % grid[0])
        break

    if int(grid[1]) > 0:  # 如果持有量大于0，说明可以卖出
        print('sell by price ',grid[0])
        sell_args={"symbol" : "159915","priceType" : 0,"price" :grid[0],"amount" : 100}
        client.sell(**sell_args)
        for _ in range(20):  # 最多等20秒返回新的委托列表
            new_orders = client.get_orders().sort_values(by='委托时间')
            if (len(new_orders) > len(orders)):
                contract_number = new_orders.loc[len(new_orders) - 1, '合同编号']
                update_sql = 'update grid_159915 set contract_number=%s where price=%s' % (contract_number, grid[0])
                print(update_sql)
                cursor.execute(update_sql)
                break
            time.sleep(1)
    else:  # 否则应该买入
        print('buy by price',grid[0])
        buy_args = {"symbol": "159915", "priceType": 0, "price": grid[0], "amount": 100}
        client.buy(**buy_args)
        for _ in range(20):
            new_orders = client.get_orders().sort_values(by='委托时间')
            if (len(new_orders) > len(orders)):
                contract_number = new_orders.loc[len(new_orders) - 1, '合同编号']
                update_sql = 'update grid_159915 set contract_number=%s where price=%s' % (contract_number, grid[0])
                print(update_sql)
                cursor.execute(update_sql)
                break
            time.sleep(1)
    break  # 后续的格子不再处理
conn.commit()
conn.close()
