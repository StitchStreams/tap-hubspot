"""
Unit tests at the functions need to run `sync_deals`
"""
import os
import unittest
from tap_hubspot import acquire_access_token_from_refresh_token
from tap_hubspot import CONFIG
from tap_hubspot import gen_request
from tap_hubspot import get_url


class TestDeals(unittest.TestCase):
    """
    This class gets an access token for the tests to use and then tests
    assumptions we have about the tap
    """
    def setUp(self):
        """
        This functions reads in the variables need to get an access token
        """
        CONFIG['redirect_uri'] = os.environ['HUBSPOT_REDIRECT_URI']
        CONFIG['refresh_token'] = os.environ['HUBSPOT_REFRESH_TOKEN']
        CONFIG['client_id'] = os.environ['HUBSPOT_CLIENT_ID']
        CONFIG['client_secret'] = os.environ['HUBSPOT_CLIENT_SECRET']

        acquire_access_token_from_refresh_token()


    def test_can_fetch_hs_date_entered_props(self):
        """
        This test is written on the assumption that `sync_deals()` calls
        `gen_request()` to get records
        """
        state = {}
        url = get_url('deals_all')
        params = {'count': 250,
                  'includeAssociations': False,
                  'properties' : []}
        v3_fields = ['hs_date_entered_appointmentscheduled']

        records = list(
            gen_request(state, 'deals', url, params, 'deals', "hasMore", ["offset"], ["offset"], v3_fields=v3_fields)
        )

        for record in records:
            # The test account has a deal stage called "appointment scheduled"
            value = record.get('properties',{}).get('hs_date_entered_appointmentscheduled')
            error_msg = ('Could not find "hs_date_entered_appointment_scheduled"'
                         'in {}').format(record)
            self.assertIsNotNone(value, msg=error_msg)
