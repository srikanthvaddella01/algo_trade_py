USER_ORDERS_FOLDER_ = 'resources/userOrders/'
TICKS_FOLDER_ = 'resources/ticks/'
ORDERS_FOLDER_ = 'resources/orders/'
INSTRUMENTS_DATA_FILE = 'resources/instruments/data.csv'
ACCOUNTS_FOLDER = 'resources/account/'
GS_SHEET_KEYS = 'resources/keys/keys.json'
AUTO_INPUTS_FILE = ACCOUNTS_FOLDER + 'auto_inputs.csv'
USER_INPUTS_FILE = ACCOUNTS_FOLDER + 'user_info.csv'
TICKS_IND_FILE = ACCOUNTS_FOLDER + 'ticks_indi.csv'
FIREFOX_DRIVER_PATH = 'resources/lib/geckodriver.exe'


def create_indicator_order_file(instrument_token, data_interval):
    return ORDERS_FOLDER_ + str(instrument_token) + '_Orders_' + str(data_interval) + '.csv '


def create_indicator_user_order_file(user_id, instrument_token, data_interval):
    return USER_ORDERS_FOLDER_ + str(user_id) + '_' + str(instrument_token) + '_Orders_' + str(data_interval) + '.csv'
