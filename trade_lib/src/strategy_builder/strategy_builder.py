from strategy_builder.day_open_strategy import *
from strategy_builder.hill_base_direction import collect_day_specific_data
from strategy_builder.strategy_formulas import first_candle_type_input_build
from tech_indicator.super_trend_builder import super_trend
from tech_indicator.vwap_indicator import vwap


def strategy_data_builder(instrument_history_data, auto_inputs, instrument_name):
    df_bank_nifty_super_trend_7_3 = super_trend(instrument_history_data.copy(),
                                                auto_inputs['super_trend_period'][0],
                                                auto_inputs['super_trend_multiplier'][0])

    df_bank_nifty_super_trend_7_1 = super_trend(instrument_history_data.copy(), auto_inputs['super_trend_period'][0], 1)
    df_bank_nifty_super_trend_7_3['super_trend_direction_7_1'] = df_bank_nifty_super_trend_7_1[
        'super_trend_direction_7_3']

    df_bank_nifty_super_trend_7_3 = vwap(df_bank_nifty_super_trend_7_3.copy())

    df_bank_nifty_super_trend_7_3['date'] = (pd.to_datetime(df_bank_nifty_super_trend_7_3['date'].copy()))
    df_bank_nifty_super_trend_7_3["date_on"] = df_bank_nifty_super_trend_7_3["date"].copy().dt.date
    df_bank_nifty_super_trend_7_3['date_on_str'] = df_bank_nifty_super_trend_7_3["date"].copy().dt.date.astype(str)

    df_bank_nifty_super_trend_7_3 = df_bank_nifty_super_trend_7_3[['date_on', 'date_on_str', 'date', 'open',
                                                                   'low', 'high', 'close', 'volume',
                                                                   'super_trend_7_3', 'super_trend_direction_7_3',
                                                                   'super_trend_direction_7_1', 'vwap']]

    df_bank_nifty_intraday_data_group_by_days = df_bank_nifty_super_trend_7_3.groupby(['date_on_str'])
    df_bank_nifty_days_data = pd.DataFrame({'days': list(df_bank_nifty_intraday_data_group_by_days.groups.keys())})

    # df_bank_nifty_super_trend_7_3 = strategy_builder_stg_retrace_ment(df_bank_nifty_super_trend_7_3,df_bank_nifty_days_data)
    # df_bank_nifty_super_trend_7_3 = strategy_builder_hill_base_entry(df_bank_nifty_super_trend_7_3,df_bank_nifty_days_data, instrument_name)

    df_bank_nifty_super_trend_7_3 = day_open_strategy(df_bank_nifty_super_trend_7_3, df_bank_nifty_days_data,
                                                      instrument_name)

    return df_bank_nifty_super_trend_7_3


def day_open_strategy(instr_days_data, instr_days, instrument_name):
    instr_days_data['day_open_strategy'] = None

    for instr_days_record_position, instr_days_record in instr_days.iloc[1:].iterrows():
        instr_day_data = collect_day_specific_data(instr_days_data, instr_days_record)
        current_day = instr_days_record.days
        previous_day = instr_days.iloc[instr_days_record_position - 1].days
        current_candle_dict_info = {'date': current_day}
        current_candle_dict_info = first_candle_type_input_build(instr_days_data, current_day, previous_day,
                                                                 current_candle_dict_info)

        if current_candle_dict_info['candle_position'] == 'OPEN_IN_SIDE_CLOSE_IN_SIDE':
            open_inside_close_strategy(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

        if current_candle_dict_info['candle_position'] == 'OPEN_IN_SIDE_DOWN_CLOSE_OUT_SIDE':
            open_inside_down_close_outside_strategy(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

        if current_candle_dict_info['candle_position'] == 'OPEN_IN_SIDE_UP_CLOSE_OUT_SIDE':
            open_in_side_up_close_out_side(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

        if current_candle_dict_info['candle_position'] == 'UP_OPEN_OUT_SIDE_UP_CLOSE_OUT_SIDE':
            up_open_out_side_close_out_side_strategy(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

        if current_candle_dict_info['candle_position'] == 'DOWN_OPEN_OUTSIDE_DOWN_CLOSE_OUT_SIDE':
            down_open_outside_down_close_outside_strategy(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

        if current_candle_dict_info['candle_position'] == 'DOWN_OPEN_OUT_SIDE_CLOSE_IN_SIDE':
            down_open_outside_close_in_side_strategy(instr_day_data, current_candle_dict_info, instrument_name, instr_days_data)

    return instr_days_data
