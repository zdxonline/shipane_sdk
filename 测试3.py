

import easyquotation
quotation = easyquotation.use('sina')
stock_id='159915'
current_price = quotation.real(stock_id)[stock_id]['now']
print(current_price)