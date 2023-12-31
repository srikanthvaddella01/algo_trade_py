from datetime import datetime
import pandas as pd

from strategy_builder.strategy_builder_common import *


def open_inside_close_strategy(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    instr_positional_orders = pd.DataFrame()
    primary_entry_condition_ = False
    exit_time_ = exit_entry_time(instrument_name)
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        pre_high = current_candle_dict_info['pre_high']
        pre_low = current_candle_dict_info['pre_low']
        pre_close = current_candle_dict_info['pre_close']
        cur_close = current_candle_dict_info['cur_close']
        first_candle_type_ = current_candle_dict_info['candle_type']

        high_diff_per = abs(round(((pre_high - cur_close) / pre_high) * 100, 2))
        low_diff_per = abs(round(((pre_low - cur_close) / pre_low) * 100, 2))

        green_candle = (instr_positional_orders.shape[0] == 0) and (first_candle_type_ == 'green candle')
        red_candle = (instr_positional_orders.shape[0] == 0) and (first_candle_type_ == 'red candle')

        if current_day_data.index[1] == row_number:
            primary_entry_condition_ = row_record.close < pre_close

        if green_candle and row_record.close < pre_close:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif red_candle and row_record.close < pre_close and low_diff_per < high_diff_per:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif red_candle and primary_entry_condition_ and high_diff_per < low_diff_per:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif (instr_positional_orders.shape[0] == 1) & (current_time < exit_time):
            update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                    row_number, row_record, instr_days_data)

        elif (instr_positional_orders.shape[0] == 1) & (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def open_inside_down_close_outside_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                            instr_days_data):
    profit_loss_day_df_new = pd.DataFrame()
    second_entry_condition_buy = False
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)

    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        cur_low = current_candle_dict_info['cur_low']
        first_candle_type_ = current_candle_dict_info['candle_type']

        previous_record = current_day_data.loc[row_number - 1].super_trend_direction_7_1
        current_record = current_day_data.loc[row_number].super_trend_direction_7_1

        if previous_record != current_record:
            profit_loss_df = {'date_on_str': row_record.date_on_str, 'instrument': 'bank_nifty',
                              'traded_date_time': row_record.date,
                              'direction': current_record, 'price': row_record.close}
            profit_loss_day_df_new = profit_loss_day_df_new.append(profit_loss_df, ignore_index=True)

        shape_ = profit_loss_day_df_new.shape[0]
        if shape_ > 2:
            second = profit_loss_day_df_new.iloc[shape_ - 2].price

        primary_entry_condition_sell = (instr_positional_orders.shape[0] == 0) and (first_candle_type_ == 'red candle')

        if row_number == 1:
            second_entry_condition_buy = (first_candle_type_ == 'red candle') and row_record.low < cur_low

        if primary_entry_condition_sell and second_entry_condition_buy and row_record.close < cur_low:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif primary_entry_condition_sell and current_record == 'up' and shape_ > 2 and row_record.close > second:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif primary_entry_condition_sell and current_record == 'down' and shape_ > 2 and row_record.close < second:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif (instr_positional_orders.shape[0] == 1) & (current_time < exit_time):
            update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                    row_number, row_record, instr_days_data)

        elif (instr_positional_orders.shape[0] == 1) & (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def open_in_side_up_close_out_side(current_day_data, current_candle_dict_info, instrument_name, instr_days_data):
    profit_loss_day_df_new = pd.DataFrame()
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)

    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        cur_low = current_candle_dict_info['cur_low']
        first_candle_type_ = current_candle_dict_info['candle_type']
        cur_high = current_candle_dict_info['cur_high']

        previous_record = current_day_data.loc[row_number - 1].super_trend_direction_7_1
        current_record = current_day_data.loc[row_number].super_trend_direction_7_1

        primary_entry_condition_sell = (instr_positional_orders.shape[0] == 0) and (
                first_candle_type_ == 'green candle')

        if previous_record != current_record:
            profit_loss_df = {'date_on_str': row_record.date_on_str, 'instrument': 'bank_nifty',
                              'traded_date_time': row_record.date,
                              'direction': current_record, 'price': row_record.close}
            profit_loss_day_df_new = profit_loss_day_df_new.append(profit_loss_df, ignore_index=True)

        shape_ = profit_loss_day_df_new.shape[0]

        if primary_entry_condition_sell and row_record.close < cur_low and shape_ > 0 and current_record == 'up':
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif primary_entry_condition_sell and row_record.close > cur_high and shape_ > 0 and current_record == 'down':
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif (instr_positional_orders.shape[0] == 1) & (current_time < exit_time):
            update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                    row_number, row_record, instr_days_data)

        elif (instr_positional_orders.shape[0] == 1) & (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def up_open_out_side_close_out_side_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                             instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)

    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        cur_open = current_candle_dict_info['cur_open']
        current_record = current_day_data.loc[row_number].super_trend_direction_7_1

        diff_super_trend = abs(round(((row_record.close - row_record.super_trend_7_3) / row_record.close) * 100, 2))
        shape_ = instr_positional_orders.shape[0]

        if (shape_ == 0) and (current_record == 'down') and (row_record.open < cur_open) and (diff_super_trend < 1):
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        # with-out stop loss -> 1500 points -> with stop loss -> 80 percent occurrence -> remove and test it

        elif (shape_ == 1) & (current_time < exit_time):
            update_record_before_exit_time_strategy(current_day_data, instr_positional_orders, row_number, row_record,
                                                    instr_days_data)

        elif (shape_ == 1) & (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def down_open_outside_down_close_outside_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                                  instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)
    second_entry_condition_buy = False
    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        current_record = current_day_data.loc[row_number].super_trend_direction_7_1

        shape_ = instr_positional_orders.shape[0]
        cur_low = current_candle_dict_info['cur_low']
        cur_open = current_candle_dict_info['cur_open']
        first_candle_type_ = current_candle_dict_info['candle_type']

        primary_entry_condition_sell = (shape_ == 0) and (first_candle_type_ == 'red candle')

        if current_day_data.index[1] == row_number:
            second_entry_condition_buy = (first_candle_type_ == 'red candle') and row_record.low > cur_low

        if primary_entry_condition_sell and second_entry_condition_buy:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif primary_entry_condition_sell and row_record.close > cur_open and current_record == 'up':
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'up_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif (shape_ == 1) & (current_time < exit_time):
            update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                    row_number, row_record, instr_days_data)

        elif (shape_ == 1) & (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


def down_open_outside_close_in_side_strategy(current_day_data, current_candle_dict_info, instrument_name,
                                             instr_days_data):
    instr_positional_orders = pd.DataFrame()
    exit_time_ = exit_entry_time(instrument_name)

    for row_number, row_record in current_day_data.iloc[1:].iterrows():
        current_time = row_record.date.time()
        exit_time = datetime.strptime(exit_time_, '%H:%M:%S').time()
        shape_ = instr_positional_orders.shape[0]
        pre_close = current_candle_dict_info['pre_close']
        first_candle_type_ = current_candle_dict_info['candle_type']

        diff_super_trend = abs(round(((row_record.close - pre_close) / row_record.close) * 100, 2))
        primary_entry_condition_sell = (shape_ == 0) and (first_candle_type_ == 'green candle')

        if primary_entry_condition_sell and diff_super_trend < 0.30:
            instr_days_data.loc[row_number, 'day_open_strategy'] = 'down_entry'
            row_number_update_ = instr_days_data.loc[row_number]
            instr_positional_orders = instr_positional_orders.append(row_number_update_, ignore_index=True)

        elif (shape_ == 1) & (current_time < exit_time):
            update_record_before_exit_time_strategy(current_day_data, instr_positional_orders,
                                                    row_number, row_record, instr_days_data)

        elif (shape_ == 1) & (current_time >= exit_time):
            instr_positional_orders = update_record_after_exit_time_strategy(current_day_data, instr_positional_orders,
                                                                             row_number, instr_days_data)


