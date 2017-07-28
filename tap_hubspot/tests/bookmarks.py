import unittest
import singer.bookmarks
import singer.messages
import tap_hubspot
import pprint
import os
from tap_hubspot.tests import utils

LOGGER = singer.get_logger()

class Bookmarks(unittest.TestCase):
    def setUp(self):
        utils.verify_environment_vars()
        utils.seed_tap_hubspot_config()
        singer.write_bookmark = utils.our_write_bookmark
        singer.write_state    = utils.our_write_state
        singer.write_record   = utils.our_write_record
        singer.write_schema   = utils.our_write_schema

    def sync_contacts(self):
        STATE = utils.get_clear_state()
        catalog = {'stream_alias' : 'hubspot_contacts'}

        STATE = tap_hubspot.sync_contacts(STATE, catalog)
        #offset has been cleared
        self.assertEqual(utils.caught_state['bookmarks']['contacts']['offset'], {})

        #some bookmark has been recorded in the state
        self.assertNotEqual(utils.caught_state['bookmarks']['contacts']['lastmodifieddate'], None)

        #should sync some contacts
        self.assertGreater(len(utils.caught_records),0)
        self.assertEqual(set(utils.caught_records.keys()), {'contacts'})
        self.assertEqual(utils.caught_pks, {'contacts': ['canonical-vid']})

        utils.caught_records = []
        STATE = tap_hubspot.sync_contacts(STATE, catalog)

        #no new records thanks to bookmark
        self.assertEqual(len(utils.caught_records),0)

    def sync_companies(self):
        STATE = utils.get_clear_state()

        catalog = {'stream_alias' : 'hubspot_companies'}
        STATE = tap_hubspot.sync_companies(STATE, catalog)

        #offset has been cleared
        self.assertEqual(utils.caught_state['bookmarks']['companies']['offset'], {})

        #some bookmark has been recorded in the state
        self.assertNotEqual(utils.caught_state['bookmarks']['companies']['hs_lastmodifieddate'], None)

        #should sync some contacts && some hubspot_contacts_by_company
        self.assertGreater(len(utils.caught_records), 0)
        self.assertEqual(set(utils.caught_records.keys()), {'companies', 'hubspot_contacts_by_company'})

        self.assertEqual(utils.caught_pks,  {'companies': ['companyId'], 'hubspot_contacts_by_company': ['company-id', 'contact-id']})

        utils.caught_records = []
        STATE = tap_hubspot.sync_companies(STATE, catalog)

        #no new records thanks to bookmark
        self.assertEqual(len(utils.caught_records),0)
