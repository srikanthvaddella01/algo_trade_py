import os
import sys

from mw_minisoft.historical_feed.historical_feed import *
from mw_minisoft.instruments_operations.instrument_read_write_operations import download_write_instrument_tokens
from mw_minisoft.order_management.generate_strategy_orders import storage_regular_orders
from mw_minisoft.order_management.order_management__ce_pe import *
from mw_minisoft.persistence_operations.account_management import *


def strategy_execution_steps(auto_inputs):
    """
    New instrument data will be added, and technical values will be generated on top of it; recently,
    the order management process will begin.
    """

    model_indicator_data_generator(auto_inputs)



def remove_create_dir():
    """
    All files such as order, user order, and ticks will be deleted.
    """
    folders = [ORDERS_FOLDER_, TICKS_FOLDER_, USER_ORDERS_FOLDER_]
    cus_logger.info('The process of deleting old files has begun.')
    for folder in folders:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            os.remove(file_path)


def execute_strategy_programs():
    """
    A user session token will be generated, and the most recent instruments file will be downloaded and saved to the
    local directory.
    """
    cus_logger.info("strategy execution started")
    auto_inputs = pd.read_csv(AUTO_INPUTS_FILE)
    user_info = pd.read_csv(USER_INPUTS_FILE)
    user_info = user_info[user_info.day != date.today().day]

    if user_info.shape[0] > 0:
        download_each_user_tokens()
        download_write_instrument_tokens()
        remove_create_dir()

    strategy_execution_steps(auto_inputs)
    cus_logger.info("strategy execution completed")


def scheduler_main_program_run(env, minutes, super_trend_period, super_trend_multiplier):
    """
    This function will update the input parameters and launch the main programme.
    """
    try:
        cus_logger.info("%s main program execution started", env)
        update_auto_inputs(env, minutes, super_trend_period, super_trend_multiplier)
        execute_strategy_programs()
        cus_logger.info("%s main program execution ended", env)
        sys.exit()
    except Exception as exception:
        cus_logger.exception(exception)
