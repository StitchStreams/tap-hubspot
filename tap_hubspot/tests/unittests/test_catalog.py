import unittest
from tap_hubspot import deselect_unselected_fields

class TestDeselectUnselectedFields(unittest.TestCase):
    def test_deselect_unselected_fields(self):
        catalog = {
            'streams': [
                {
                    'metadata': [
                        {'metadata': {'selected': True}},
                        {'breadcrumb': []},
                        {'breadcrumb': ['properties', 'custom_field_1'], 'metadata': {'selected': False}},
                        {'breadcrumb': ['properties', 'custom_field_2'], 'metadata': {'selected': True}},
                        {'breadcrumb': ['properties', 'custom_field_3'], 'metadata': {}},
                    ]
                },
                {
                    'metadata': [
                        {'metadata': {'selected': False}},
                        {'breadcrumb': ['properties', 'custom_field'], 'metadata': {}},
                    ]
                }
            ]
        }

        expected_catalog = {
            'streams': [
                {
                    'metadata': [
                        {'metadata': {'selected': True}},
                        {'breadcrumb': []},
                        {'breadcrumb': ['properties', 'custom_field_1'], 'metadata': {'selected': False}},
                        {'breadcrumb': ['properties', 'custom_field_2'], 'metadata': {'selected': True}},
                        {'breadcrumb': ['properties', 'custom_field_3'], 'metadata': {'selected': False}},
                    ]
                },
                {
                    'metadata': [
                        {'metadata': {'selected': False}},
                        {'breadcrumb': ['properties', 'custom_field'], 'metadata': {}},
                    ]
                }
            ]
        }

        deselect_unselected_fields(catalog)
        self.assertEqual(catalog, expected_catalog)
