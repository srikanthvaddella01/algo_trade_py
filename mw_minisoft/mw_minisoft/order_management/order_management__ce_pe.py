from datetime import datetime
from os import path

from session_builder.retrive_request_token import generate_user_session

from mw_minisoft.order_management.order_buy_sell_operations import *
from mw_minisoft.order_management.supporting_operations import *
from mw_minisoft.persistence_operations.account_management import *

cus_logger.setLevel(10)


def place_regular_orders(auto_inputs):
    """
    This code will determine whether an existing indicator order file is available and, if so, will create a user
    order file and place a new order for each and every user.
    """
    ticks_indicator_df = ticks_indi()
    for ind_record_position, ind_record in ticks_indicator_df.iterrows():
        ind_order_file = create_indicator_order_file(ind_record.instrument_trading_symbol.replace(':', '_'),
                                                     auto_inputs['data_interval'][0])
        if path.exists(ind_order_file):
            resource_record = pd.read_csv(ind_order_file).tail(1)
            user_info_df = pd.DataFrame(read_user_info())
            for idx_user_info, user_info in user_info_df.iterrows():
                try:
                    place_regular_order_based_file_exists(auto_inputs, resource_record, ind_record, user_info)
                except Exception as all_errors:
                    cus_logger.error("User (%s) Entering into new order Instrument position (%s) had been failed - "
                                     "error message %s", user_info.user_id, ind_record.instrument_name, all_errors)
        else:
            cus_logger.info('fresh order is not available')


def place_regular_order_based_file_exists(auto_inputs, resource_record, ticks_user, user_info):
    """
    This code will determine whether an existing user indicator order file is available and, if available will append
    nor create new file then place order
    """
    ind_user_order_file = create_indicator_user_order_file(user_info['user_id'],
                                                           ticks_user.instrument_trading_symbol.replace(':', '_'),
                                                           auto_inputs['data_interval'][0])
    if path.exists(ind_user_order_file):
        previous_record_count = pd.read_csv(ind_user_order_file).tail(1)
        old_record = pd.to_datetime(previous_record_count.iloc[0]['date'])
        previous_record = pd.to_datetime(resource_record.iloc[0].date)

        diff = (old_record - previous_record).total_seconds() / 60
        if diff != 0:
            previous_record_count = pd.read_csv(ind_user_order_file)
            (previous_record_count.append(resource_record)).to_csv(ind_user_order_file, index=False)
            cus_logger.info('initiating the process to initiate the position order')
            prepare_instrument_order(resource_record, ticks_user, user_info)
    else:
        resource_record.to_csv(ind_user_order_file, index=False)
        cus_logger.info('initiating the process to initiate the new position order')
        prepare_instrument_order(resource_record, ticks_user, user_info)


def prepare_instrument_order(resource_record, ticks_user, user_info):
    order_dict = {'exchange': ticks_user['instrument_exchange'],
                  'instrument_token': ticks_user['instrument_token'],
                  'quantity': ticks_user[user_info['user_id']],
                  'order_side': resource_record.super_trend_direction_7_3.values[0],
                  'client_id': user_info['user_id'],
                  'kite_session': generate_user_session(user_info),
                  'instrument_trading_symbol': ticks_user['instrument_trading_symbol'],
                  'nfo': ticks_user['instrument_segment'],
                  'price': resource_record.iloc[0].future_price,
                  'expiry_date': ticks_user['instrument_expiry_date'],
                  'product_type': ticks_user['product_type'],
                  'instrument_name': ticks_user['instrument_name'],
                  'direction': resource_record.iloc[0].direction}

    place_instrument_order(order_dict['exchange'], order_dict['instrument_token'], order_dict['quantity'],
                           order_dict['order_side'], order_dict['client_id'], order_dict['kite_session'],
                           order_dict['instrument_trading_symbol'], order_dict['nfo'], order_dict['price'],
                           order_dict['expiry_date'], order_dict['product_type'], order_dict['instrument_name'],
                           order_dict['direction'])


def place_instrument_order(exchange, instrument_token, quantity, order_side, client_id, kite_session,
                           instrument_trading_symbol, nfo, price, expiry_date, product_type, instrument_name,
                           direction):
    """
    This code will check the user's position and, based on that, will either enter a new position or exit an existing
    position and enter a new one.
    """
    order_id, product, order_type_market, order_type_limit, variety = 'order_not_executed', product_type, 2, 1, 'INTRADAY'
    net_positions = pd.DataFrame(kite_session.positions()['netPositions'])
    #net_positions.to_csv('net_positions.csv')
    holding = pd.DataFrame()
    cus_logger.info('checking existing position is available or not')
    if net_positions.shape[0] > 0 and net_positions[net_positions['qty'] != 0].shape[0] > 0:
        net_positions.rename(columns={'symbol': 'Expiry date'}, inplace=True)
        position_instrument_details = instrument_positions(instrument_name, net_positions, nfo)
        if (position_instrument_details.shape[0] > 0) and ('exit' in str(direction)):
            for idx, holding in position_instrument_details.iterrows():
                exit_positions(exchange, holding, instrument_trading_symbol, instrument_token, kite_session, nfo,
                               order_id, order_side, order_type_market, product, quantity, variety, price, expiry_date,
                               order_type_limit, instrument_name, direction)
        # Enable this block; incase if you're planning for the EXIT AND ENTRY at same time
        elif (position_instrument_details.shape[0] == 0) and ('entry' in str(direction)):
            holding_quantity_zero(exchange, holding, instrument_trading_symbol, instrument_token,
                                  kite_session, nfo, order_id, order_side, order_type_market, product,
                                  quantity, variety, price, expiry_date, order_type_limit, instrument_name, direction)
    else:
        holding_quantity_zero(exchange, holding, instrument_trading_symbol, instrument_token,
                              kite_session, nfo, order_id, order_side, order_type_market, product,
                              quantity, variety, price, expiry_date, order_type_limit, instrument_name, direction)


def holding_quantity_zero(exchange, holding, instrument_trading_symbol, instrument_token, kite_session, nfo, order_id,
                          order_side, order_type_market, product, quantity, variety, price, expiry_date,
                          order_type_limit, instrument_name, direction):
    """
    This peace code would get executed in-case position size is empty
    """

    try:
        ticks_ind = ticks_ind_collect_instrument(instrument_trading_symbol)
        user_info_id = collect_user_id(kite_session)

        if str(ticks_ind.multi_quan.values[0]).upper() == 'TRUE':
            ticks_ind_running_qt = ticks_ind[user_info_id].values[0]
        else:
            ticks_ind_running_qt = ticks_ind.default_quantity.values[0]

        position_enter(exchange, expiry_date, instrument_name, kite_session, price, quantity, ticks_ind_running_qt,
                       user_info_id, order_side, direction)

    except Exception as all_errors:
        cus_logger.error("Entering into new order position had been failed - error message %s", all_errors)


def exit_positions(exchange, holding, instrument_trading_symbol, instrument_token, kite_session, nfo, order_id,
                   order_side, order_type_market, product, quantity, variety, price, expiry_date, order_type_limit,
                   instrument_name, direction):
    """
    would be used to exit the previous positions
    """

    if int(holding['qty']) > 0:
        quantity = abs((int(holding['qty'])))

        if holding['Option type'] == 'CE':
            exit_positive_holding_quantity(exchange, holding, instrument_trading_symbol, instrument_token,
                                           kite_session, nfo, order_id, order_side, order_type_market, product,
                                           quantity, variety, price, expiry_date, order_type_limit,
                                           instrument_trading_symbol, direction)

        elif holding['Option type'] == 'PE':
            exit_negative_holding_quantity(exchange, holding, instrument_trading_symbol, instrument_token,
                                           kite_session, nfo, order_id, order_side, order_type_market, product,
                                           quantity, variety, price, expiry_date, order_type_limit,
                                           instrument_trading_symbol, direction)


def exit_positive_holding_quantity(exchange, holding, instrument_trading_symbol, instrument_token, kite_session,
                                   nfo, order_id, order_side, order_type_market, product, quantity, variety, price,
                                   expiry_date, order_type_limit, instrument_name, direction):
    """
     would be used to exit the previous 'CE' positions
    """
    try:
        if order_side == "up":
            cus_logger.info('Existing Buy position available; however, this new order will not be executed.')

        elif order_side == "down":
            holding_position_exit(holding, instrument_token, instrument_trading_symbol, kite_session, quantity,
                                  direction)

    except Exception as all_errors:
        cus_logger.info('Order execution failed - Error message %s', all_errors)

    return order_id


def exit_negative_holding_quantity(exchange, holding, instrument_trading_symbol, instrument_token, kite_session, nfo,
                                   order_id, order_side, order_type_market, product, quantity, variety, price,
                                   expiry_date, order_type_limit, instrument_name, direction):
    """
     would be used to exit the previous 'PE' positions
    """
    try:
        if order_side == "down":
            cus_logger.info('Existing Buy position available; however, this new order will not be executed.')

        elif order_side == "up":
            user_info_id = collect_user_id(kite_session)
            holding_position_exit(holding, instrument_token, instrument_trading_symbol, kite_session, quantity,
                                  order_side, user_info_id, direction)

    except Exception as all_errors:
        cus_logger.error('Order execution failed - Error message %s', all_errors)

    return order_id
