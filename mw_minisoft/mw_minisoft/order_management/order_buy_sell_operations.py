from datetime import date, datetime
from os import path

import pandas as pd
from trade_logger.logger import cus_logger

from mw_minisoft.constants.file_constants import AUTO_INPUTS_FILE
from mw_minisoft.instruments_operations.instrument_read_write_operations import read_instrument_tokens


def position_enter(exchange, expiry_date, instrument_name, kite_session, price, quantity, ticks_ind_running_qt,
                   user_info_id, order_side, direction):
    env = pd.read_csv(AUTO_INPUTS_FILE).iloc[0].env
    instrument_details = read_instrument_tokens(instrument_name, price, order_side, exchange, expiry_date)
    trading_symbol = instrument_details['Expiry date'].values[0]
    if (env == 'prod') and ('entry' in str(direction)):
        data = {"symbol": instrument_details['Expiry date'].values[0], "ohlcv_flag": "1"}
        price_last = (kite_session.depth(data)['d'][instrument_details['Expiry date'].values[0]]['ask'])[4]['price']
        cus_logger.info('Entering into new position :- instrument_token: %s ,User( %s) ,  Order Type : buy order , '
                        'ticks_ind_running_qt: %s , price: %s ', trading_symbol, user_info_id, ticks_ind_running_qt,
                        price_last)
        data = {"symbol": trading_symbol, "qty": quantity, "type": 2, "side": 1, "productType": "INTRADAY",
                "limitPrice": 0, "stopPrice": 0, "validity": "DAY", "disclosedQty": 0, "offlineOrder": "False",
                "stopLoss": 0, "takeProfit": 0
                }
        order_info = kite_session.place_order(data)
        cus_logger.info('Instrument Order Detail', order_info['message'])

    position_write(direction, instrument_details, instrument_name, kite_session, quantity, trading_symbol)


def holding_position_exit(holding, instrument_token, instrument_trading_symbol, kite_session, quantity, order_side,
                          user_info_id, direction):
    env = pd.read_csv(AUTO_INPUTS_FILE).iloc[0].env
    if (env == 'prod') and ('exit' in str(direction)):
        data = {"symbol": holding['Expiry date'], "ohlcv_flag": "1"}
        price_last = kite_session.depth(data)['d'][holding['Expiry date']]['ask'][4]['price']
        # running_quant(instrument_trading_symbol, holding.buy_price, price_last, kite_session.api_key)
        cus_logger.info('Existing From previous %s, Position :- instrument_token: %s, User(%s), quantity: %s , '
                        'price_last: %s ', order_side, instrument_token, user_info_id, quantity, price_last)
        data = {"symbol": holding['Expiry date'], "qty": holding.netQty, "type": 2, "side": 1,
                "productType": "INTRADAY",
                "limitPrice": 0, "stopPrice": 0, "validity": "DAY", "disclosedQty": 0, "offlineOrder": "False",
                "stopLoss": 0, "takeProfit": 0
                }
        order_info = kite_session.place_order(data)
        cus_logger.info('Instrument Order Detail', order_info['message'])


def position_write(direction, instrument_details, instrument_name, kite_session, quantity, trading_symbol):
    file_name = 'resources/positions/' + instrument_name.replace(':', '_') + '_positions.csv'
    positions = pd.DataFrame()
    if 'entry' in str(direction):
        if path.exists(file_name):
            # file exits -> add this record
            # if file not exists -> create & add this record
            positions_file = pd.read_csv(file_name)
            data = {"symbol": instrument_details['Expiry date'].values[0], "ohlcv_flag": "1"}
            price_last = (kite_session.depth(data)['d'][instrument_details['Expiry date'].values[0]]['ask'])[4]['price']
            data = {'entry_time': str(date.today()), "symbol": trading_symbol, "qty": quantity,
                    'entry_price': price_last,
                    "type": 2, "side": 1, 'direction': direction
                    }
            positions = positions.append(data, ignore_index=True)
            positions_file = (positions_file.append(positions.tail(1), ignore_index=True)).to_csv(file_name,
                                                                                                  index=False)
        else:
            data = {"symbol": instrument_details['Expiry date'].values[0], "ohlcv_flag": "1"}
            price_last = (kite_session.depth(data)['d'][instrument_details['Expiry date'].values[0]]['ask'])[4]['price']
            data = {'entry_time': str(datetime.now()), "symbol": trading_symbol, "qty": quantity,
                    'entry_price': price_last, "type": 2, "side": 1, 'direction': direction}

            positions = positions.append(data, ignore_index=True)
            positions.to_csv(file_name, index=False)

    elif 'exit' in str(direction):
        # if file exists -> take last record -> find new value buy value -> update same record -> append
        # if file does not exit -> leave the record
        if path.exists(file_name):
            positions_file = pd.read_csv(file_name)
            positions_file_1 = positions_file.tail(1)
            data = {"symbol": positions_file_1.symbol[0], "ohlcv_flag": "1"}
            price_last = (kite_session.depth(data)['d'][positions_file_1.symbol[0]]['ask'])[4][
                'price']
            data = {'exit_time': date.today(), "symbol": positions_file_1.symbol[0], "qty": quantity,
                    'exit_price': price_last,
                    "type": 2, "side": 1, 'direction': direction
                    }
            positions_file = positions_file.append(data, ignore_index=True)
            positions.to_csv(file_name, index=False)
