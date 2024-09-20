import os
import unittest
from tap_hubspot import acquire_access_token_from_refresh_token
from tap_hubspot import CONFIG
from tap_hubspot import Context
from tap_hubspot import gen_request
from tap_hubspot import get_url
from tap_hubspot import load_schema
from tap_hubspot import merge_responses
from tap_hubspot import process_v3_deals_records
from tap_hubspot import sync_contacts
from unittest import mock

import logging
logging.getLogger().level = logging.INFO


class TestContacts(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    @mock.patch("singer.write_record")
    @mock.patch("tap_hubspot.request")
    @mock.patch("tap_hubspot.gen_request")
    @mock.patch("tap_hubspot.load_schema")
    def test_bookmarking(self, fake_load_schema, fake_gen_request, fake_request, fake_write_record):
        self.maxDiff = None
        state = {
            "bookmarks": {
                "contacts": {
                    "updated_at": "2022-09-26T00:00:00Z"
                }
            }
        }
        state = {
            "bookmarks": {
                "contacts": {
                    "versionTimestamp": "2022-09-26T00:00:00Z"}
                },

        }
        catalog = {"streams": [
            {"tap_stream_id": "contacts",
             "metadata": [],
             "schema": {}}
        ]}
        ctx = Context(catalog)

        fake_load_schema.return_value = {}
        fake_gen_request.return_value = [
            {"vid": 1},
            {"vid": 2},
            {"vid": 3, "versionTimestamp": "2022-09-26T12:00:00Z"},
        ]

        fake_request.return_value.json.return_value.values.return_value = [
            {"vid": 1},
            {"vid": 2},
            {"vid": 3},
        ]

        sync_contacts(state, ctx)
        actual = [x[0][1] for x in fake_write_record.call_args_list]

        expected = [
            {'vid': 1, 'versionTimestamp': None},
            {'vid': 2, 'versionTimestamp': None},
            {'vid': 3, 'versionTimestamp': '2022-09-26T12:00:00.000000Z'}
        ]

        self.assertEqual(expected, actual)
