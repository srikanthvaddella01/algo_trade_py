import unittest
from unittest import TestCase
import pandas as pd

from session_builder.kite_backupretrive_request_token import obtain_access_token
from session_builder.retrive_request_token import create_user_session


class Test(TestCase):
    def test_obtain_access_token(self):
        user_records = pd.read_csv('../../data/resources/account/user_info.csv')
        request_token = '10'
        for record_position, user_record in user_records.iterrows():
            request_token = obtain_access_token(user_record)
        self.assertNotEqual(request_token, '10')

    def test_create_access_token(self):
            user_records = pd.read_csv('../../data/resources/account/user_info.csv')
            request_token = '10'
            for record_position, user_record in user_records.iterrows():
                request_token = create_user_session(user_record, 'firefox_driver_path')
            self.assertNotEqual(request_token, '10')


if __name__ == '__main__':
    unittest.main()
