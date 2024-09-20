#!/usr/bin/env python3
import datetime
import pytz
import itertools
import os
import re
import sys
import json
# pylint: disable=import-error,too-many-statements
import attr
import backoff
import requests
import singer
import singer.messages
from singer import metrics
from singer import metadata
from singer import utils
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)

LOGGER = singer.get_logger()
SESSION = requests.Session()

REQUEST_TIMEOUT = 300
class InvalidAuthException(Exception):
    pass

class SourceUnavailableException(Exception):
    pass

class DependencyException(Exception):
    pass

class UriTooLongException(Exception):
    pass

class DataFields:
    offset = 'offset'

class StateFields:
    offset = 'offset'
    this_stream = 'this_stream'

BASE_URL = "https://api.hubapi.com"

CONTACTS_BY_COMPANY = "contacts_by_company"

DEFAULT_CHUNK_SIZE = 1000 * 60 * 60 * 24

V3_PREFIXES = {'hs_date_entered', 'hs_date_exited', 'hs_time_in'}

CONFIG = {
    "access_token": None,
    "token_expires": None,
    "email_chunk_size": DEFAULT_CHUNK_SIZE,
    "subscription_chunk_size": DEFAULT_CHUNK_SIZE,

    # in config.json
    "redirect_uri": None,
    "client_id": None,
    "client_secret": None,
    "refresh_token": None,
    "start_date": None,
    "hapikey": None,
    "include_inactives": None,
}

ENDPOINTS = {
    "contacts_properties":  "/properties/v1/contacts/properties",
    "contacts_all":         "/contacts/v1/lists/all/contacts/all",
    "contacts_recent":      "/contacts/v1/lists/recently_updated/contacts/recent",
    "contacts_detail":      "/contacts/v1/contact/vids/batch/",

    "companies_properties": "/companies/v2/properties",
    "companies_all":        "/companies/v2/companies/paged",
    "companies_recent":     "/companies/v2/companies/recent/modified",
    "companies_detail":     "/companies/v2/companies/{company_id}",
    "contacts_by_company_v3": "/crm/v3/associations/company/contact/batch/read",

    "deals_properties":     "/properties/v1/deals/properties",
    "deals_all":            "/deals/v1/deal/paged",
    "deals_recent":         "/deals/v1/deal/recent/modified",
    "deals_detail":         "/deals/v1/deal/{deal_id}",

    "deals_v3_batch_read":  "/crm/v3/objects/deals/batch/read",
    "deals_v3_properties":  "/crm/v3/properties/deals",

    "deal_pipelines":       "/deals/v1/pipelines",

    "campaigns_all":        "/email/public/v1/campaigns/by-id",
    "campaigns_detail":     "/email/public/v1/campaigns/{campaign_id}",

    "engagements_all":        "/engagements/v1/engagements/paged",

    "subscription_changes": "/email/public/v1/subscriptions/timeline",
    "email_events":         "/email/public/v1/events",
    "contact_lists":        "/contacts/v1/lists",
    "forms":                "/forms/v2/forms",
    "workflows":            "/automation/v3/workflows",
    "owners":               "/crm/v3/owners/",

    "tickets_properties":   "/crm/v3/properties/tickets",
    "tickets":              "/crm/v4/objects/tickets",

    "custom_objects_schema":        "/crm/v3/schemas",
    "custom_objects": "/crm/v3/objects/p_{object_name}"
}

def get_start(state, tap_stream_id, bookmark_key, older_bookmark_key=None):
    """
    If the current bookmark_key is available in the state, then return the bookmark_key value.
    If it is not available then check and return the older_bookmark_key in the state for the existing connection.
    If none of the keys are available in the state for a particular stream, then return start_date.

    We have made this change because of an update in the replication key of the deals stream.
    So, if any existing connections have only older_bookmark_key in the state then tap should utilize that bookmark value.
    Then next time, the tap should use the current bookmark value.
    """
    current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        if older_bookmark_key:
            previous_bookmark = singer.get_bookmark(state, tap_stream_id, older_bookmark_key)
            if previous_bookmark:
                return previous_bookmark

        return CONFIG['start_date']
    return current_bookmark

def get_current_sync_start(state, tap_stream_id):
    current_sync_start_value = singer.get_bookmark(state, tap_stream_id, "current_sync_start")
    if current_sync_start_value is None:
        return current_sync_start_value
    return utils.strptime_to_utc(current_sync_start_value)

def write_current_sync_start(state, tap_stream_id, start):
    value = start
    if start is not None:
        value = utils.strftime(start)
    return singer.write_bookmark(state, tap_stream_id, "current_sync_start", value)

def clean_state(state):
    """ Clear deprecated keys out of state. """
    for stream, bookmark_map in state.get("bookmarks", {}).items():
        if "last_sync_duration" in bookmark_map:
            LOGGER.info("%s - Removing last_sync_duration from state.", stream)
            state["bookmarks"][stream].pop("last_sync_duration", None)

def get_selected_property_fields(catalog, mdata):

    fields = catalog.get("schema").get("properties").keys()
    property_field_names = []
    for field in fields:
        if "property_" in field:
            field_metadata = mdata.get(('properties', field))
            if utils.should_sync_field(field_metadata.get('inclusion'),
                                       field_metadata.get('selected')):
                property_field_names.append(field.split("property_", 1)[1])
    return ",".join(property_field_names)

def get_url(endpoint, **kwargs):
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    return BASE_URL + ENDPOINTS[endpoint].format(**kwargs)


def get_field_type_schema(field_type):
    if field_type == "bool":
        return {"type": ["null", "boolean"]}

    elif field_type == "datetime":
        return {"type": ["null", "string"],
                "format": "date-time"}

    elif field_type == "number":
        # A value like 'N/A' can be returned for this type,
        # so we have to let this be a string sometimes
        return {"type": ["null", "number", "string"]}

    else:
        return {"type": ["null", "string"]}

def get_field_schema(field_type, extras=False):
    if extras:
        return {
            "type": "object",
            "properties": {
                "value": get_field_type_schema(field_type),
                "timestamp": get_field_type_schema("datetime"),
                "source": get_field_type_schema("string"),
                "sourceId": get_field_type_schema("string"),
            }
        }
    else:
        return {
            "type": "object",
            "properties": {
                "value": get_field_type_schema(field_type),
            }
        }

def parse_custom_schema(entity_name, data, is_custom_object=False):
    if is_custom_object:
        return {
            field['name']: get_field_type_schema(field['type'])
            for field in data
        }
    if entity_name == "tickets":
        return {
            field['name']: get_field_type_schema(field['type'])
            for field in data["results"]
        }

    return {
        field['name']: get_field_schema(field['type'], entity_name != 'contacts')
        for field in data
    }


def get_custom_schema(entity_name):
    return parse_custom_schema(entity_name, request(get_url(entity_name + "_properties")).json())

def get_v3_schema(entity_name):
    url = get_url("deals_v3_properties")
    return parse_custom_schema(entity_name, request(url).json()['results'])

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_associated_company_schema():
    associated_company_schema = load_schema("companies")
    #pylint: disable=line-too-long
    associated_company_schema['properties']['company-id'] = associated_company_schema['properties'].pop('companyId')
    associated_company_schema['properties']['portal-id'] = associated_company_schema['properties'].pop('portalId')
    return associated_company_schema

def load_schema(entity_name):
    schema = utils.load_json(get_abs_path('schemas/{}.json'.format(entity_name)))
    if entity_name in ["contacts", "companies", "deals", "tickets"]:
        custom_schema = get_custom_schema(entity_name)

        schema['properties']['properties'] = {
            "type": "object",
            "properties": custom_schema,
        }

        if entity_name in ["deals"]:
            v3_schema = get_v3_schema(entity_name)
            for key, value in v3_schema.items():
                if any(prefix in key for prefix in V3_PREFIXES):
                    custom_schema[key] = value

        # Move properties to top level
        custom_schema_top_level = {'property_{}'.format(k): v for k, v in custom_schema.items()}
        schema['properties'].update(custom_schema_top_level)

        # Exclude properties_versions field for tickets stream. As the versions are not present in
        # the api response.
        if entity_name != "tickets":
            # Make properties_versions selectable and share the same schema.
            versions_schema = utils.load_json(get_abs_path('schemas/versions.json'))
            schema['properties']['properties_versions'] = versions_schema

    if entity_name == "contacts":
        schema['properties']['associated-company'] = load_associated_company_schema()

    return schema

#pylint: disable=invalid-name
def acquire_access_token_from_refresh_token():
    payload = {
        "grant_type": "refresh_token",
        "redirect_uri": CONFIG['redirect_uri'],
        "refresh_token": CONFIG['refresh_token'],
        "client_id": CONFIG['client_id'],
        "client_secret": CONFIG['client_secret'],
    }


    resp = requests.post(BASE_URL + "/oauth/v1/token", data=payload, timeout=get_request_timeout())
    if resp.status_code == 403:
        raise InvalidAuthException(resp.content)

    resp.raise_for_status()
    auth = resp.json()
    CONFIG['access_token'] = auth['access_token']
    CONFIG['refresh_token'] = auth['refresh_token']
    CONFIG['token_expires'] = (
        datetime.datetime.utcnow() +
        datetime.timedelta(seconds=auth['expires_in'] - 600))
    LOGGER.info("Token refreshed. Expires at %s", CONFIG['token_expires'])


def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429

def on_giveup(details):
    if len(details['args']) == 2:
        url, params = details['args']
    else:
        url = details['args']
        params = {}

    raise Exception("Giving up on request after {} tries with url {} and params {}" \
                    .format(details['tries'], url, params))

URL_SOURCE_RE = re.compile(BASE_URL + r'/(\w+)/')

def parse_source_from_url(url):
    match = URL_SOURCE_RE.match(url)
    if match:
        return match.group(1)
    return None

def get_params_and_headers(params):
    """
    This function makes a params object and headers object based on the
    authentication values available. If there is an `hapikey` in the config, we
    need that in `params` and not in the `headers`. Otherwise, we need to get an
    `access_token` to put in the `headers` and not in the `params`
    """
    params = params or {}
    hapikey = CONFIG['hapikey']
    if hapikey is None:
        if CONFIG['token_expires'] is None or CONFIG['token_expires'] < datetime.datetime.utcnow():
            acquire_access_token_from_refresh_token()
        headers = {'Authorization': 'Bearer {}'.format(CONFIG['access_token'])}
    else:
        params['hapikey'] = hapikey
        headers = {}

    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    return params, headers


# backoff for Timeout error is already included in "requests.exceptions.RequestException"
# as it is a parent class of "Timeout" error
@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException,
                       requests.exceptions.HTTPError),
                      max_tries=5,
                      jitter=None,
                      giveup=giveup,
                      on_giveup=on_giveup,
                      interval=10)
def request(url, params=None):

    params, headers = get_params_and_headers(params)

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET %s", req.url)
    with metrics.http_request_timer(parse_source_from_url(url)) as timer:
        resp = SESSION.send(req, timeout=get_request_timeout())
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        if resp.status_code == 403:
            raise SourceUnavailableException(resp.content)
        elif resp.status_code == 414:
            raise UriTooLongException(resp.content)
        resp.raise_for_status()

    return resp
# {"bookmarks" : {"contacts" : { "lastmodifieddate" : "2001-01-01"
#                                "offset" : {"vidOffset": 1234
#                                           "timeOffset": "3434434 }}
#                 "users" : { "timestamp" : "2001-01-01"}}
#  "currently_syncing" : "contacts"
# }
# }

def lift_properties_and_versions(record):
    for key, value in record.get('properties', {}).items():
        computed_key = "property_{}".format(key)
        record[computed_key] = value
        if isinstance(value, dict):
            versions = value.get('versions')
            if versions:
                if not record.get('properties_versions'):
                    record['properties_versions'] = []
                record['properties_versions'] += versions
    return record

# backoff for Timeout error is already included in "requests.exceptions.RequestException"
# as it is a parent class of "Timeout" error
@backoff.on_exception(backoff.constant,
                      (requests.exceptions.RequestException,
                       requests.exceptions.HTTPError),
                      max_tries=5,
                      jitter=None,
                      giveup=giveup,
                      on_giveup=on_giveup,
                      interval=10)
def post_search_endpoint(url, data, params=None):

    params, headers = get_params_and_headers(params)
    headers['content-type'] = "application/json"

    with metrics.http_request_timer(url) as _:
        resp = requests.post(
            url=url,
            json=data,
            params=params,
            timeout=get_request_timeout(),
            headers=headers
        )

        resp.raise_for_status()

    return resp

def merge_responses(v1_data, v3_data):
    for v1_record in v1_data:
        v1_id = v1_record.get('dealId')
        for v3_record in v3_data:
            v3_id = v3_record.get('id')
            if str(v1_id) == v3_id:
                v1_record['properties'] = {**v1_record['properties'],
                                           **v3_record['properties']}

def process_v3_deals_records(v3_data):
    """
    This function:
    1. filters out fields that don't contain 'hs_date_entered_*' and
       'hs_date_exited_*'
    2. changes a key value pair in `properties` to a key paired to an
       object with a key 'value' and the original value
    """
    transformed_v3_data = []
    for record in v3_data:
        new_properties = {field_name : {'value': field_value}
                          for field_name, field_value in record['properties'].items()
                          if any(prefix in field_name for prefix in V3_PREFIXES)}
        transformed_v3_data.append({**record, 'properties' : new_properties})
    return transformed_v3_data

def get_v3_deals(v3_fields, v1_data):
    v1_ids = [{'id': str(record['dealId'])} for record in v1_data]

    # Sending the first v3_field is enough to get them all
    v3_body = {'inputs': v1_ids,
               'properties': [v3_fields[0]],}
    v3_url = get_url('deals_v3_batch_read')
    v3_resp = post_search_endpoint(v3_url, v3_body)
    return v3_resp.json()['results']

#pylint: disable=line-too-long
def gen_request(STATE, tap_stream_id, url, params, path, more_key, offset_keys, offset_targets, v3_fields=None):
    if len(offset_keys) != len(offset_targets):
        raise ValueError("Number of offset_keys must match number of offset_targets")

    if singer.get_offset(STATE, tap_stream_id):
        params.update(singer.get_offset(STATE, tap_stream_id))

    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            data = request(url, params).json()

            if data.get(path) is None:
                raise RuntimeError("Unexpected API response: {} not in {}".format(path, data.keys()))

            if v3_fields:
                v3_data = get_v3_deals(v3_fields, data[path])

                # The shape of v3_data is different than the V1 response,
                # so we transform v3 to look like v1
                transformed_v3_data = process_v3_deals_records(v3_data)
                merge_responses(data[path], transformed_v3_data)

            for row in data[path]:
                counter.increment()
                yield row

            if not data.get(more_key, False):
                break

            STATE = singer.clear_offset(STATE, tap_stream_id)
            for key, target in zip(offset_keys, offset_targets):
                if key in data:
                    params[target] = data[key]
                    STATE = singer.set_offset(STATE, tap_stream_id, target, data[key])

            singer.write_state(STATE)

    STATE = singer.clear_offset(STATE, tap_stream_id)
    singer.write_state(STATE)


def _sync_contact_vids(catalog, vids, schema, bumble_bee, bookmark_values, bookmark_key):
    if len(vids) == 0:
        return

    data = request(get_url("contacts_detail"), params={'vid': vids, 'showListMemberships' : True, "formSubmissionMode" : "all"}).json()
    time_extracted = utils.now()
    mdata = metadata.to_map(catalog.get('metadata'))
    for record in data.values():
        # Explicitly add the bookmark field "versionTimestamp" and its value in the record.
        record[bookmark_key] = bookmark_values.get(record.get("vid"))
        record = bumble_bee.transform(lift_properties_and_versions(record), schema, mdata)
        singer.write_record("contacts", record, catalog.get('stream_alias'), time_extracted=time_extracted)

default_contact_params = {
    'showListMemberships': True,
    'includeVersion': True,
    'count': 100,
}

def sync_contacts(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    bookmark_key = 'versionTimestamp'
    start = utils.strptime_with_tz(get_start(STATE, "contacts", bookmark_key))
    LOGGER.info("sync_contacts from %s", start)

    max_bk_value = start
    schema = load_schema("contacts")

    singer.write_schema("contacts", schema, ["vid"], [bookmark_key], catalog.get('stream_alias'))

    url = get_url("contacts_all")

    vids = []
    # Dict to store replication key value for each contact record
    bookmark_values = {}
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        # To handle records updated between start of the table sync and the end,
        # store the current sync start in the state and not move the bookmark past this value.
        sync_start_time = utils.now()
        for row in gen_request(STATE, 'contacts', url, default_contact_params, 'contacts', 'has-more', ['vid-offset'], ['vidOffset']):
            modified_time = None
            if bookmark_key in row:
                modified_time = utils.strptime_with_tz(
                    _transform_datetime( # pylint: disable=protected-access
                        row[bookmark_key],
                        UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING))

            if not modified_time or modified_time >= start:
                vids.append(row['vid'])

            # Adding replication key value in `bookmark_values` dict
            # Here, key is vid(primary key) and value is replication key value.
            if modified_time:
                bookmark_values[row['vid']] = utils.strftime(modified_time)

            if modified_time and modified_time >= max_bk_value:
                max_bk_value = modified_time

            if len(vids) == 100:
                _sync_contact_vids(catalog, vids, schema, bumble_bee, bookmark_values, bookmark_key)
                vids = []

        _sync_contact_vids(catalog, vids, schema, bumble_bee, bookmark_values, bookmark_key)

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(max_bk_value, sync_start_time)
    STATE = singer.write_bookmark(STATE, 'contacts', bookmark_key, utils.strftime(new_bookmark))
    singer.write_state(STATE)
    return STATE

class ValidationPredFailed(Exception):
    pass

# companies_recent only supports 10,000 results. If there are more than this,
# we'll need to use the companies_all endpoint
def use_recent_companies_endpoint(response):
    return response["total"] < 10000

default_contacts_by_company_params = {'count' : 100}

# NB> to do: support stream aliasing and field selection
def _sync_contacts_by_company_batch_read(STATE, ctx, company_ids):
    # Return state as it is if company ids list is empty
    if len(company_ids) == 0:
        return STATE

    schema = load_schema(CONTACTS_BY_COMPANY)
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    url = get_url("contacts_by_company_v3")

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        with metrics.record_counter(CONTACTS_BY_COMPANY) as counter:
            body = {'inputs': [{'id': company_id} for company_id in company_ids]}
            contacts_to_company_rows = post_search_endpoint(url, body).json()
            for row in contacts_to_company_rows['results']:
                for contact in row['to']:
                    counter.increment()
                    record = {'company-id' : row['from']['id'],
                              'contact-id' : contact['id']}
                    record = bumble_bee.transform(lift_properties_and_versions(record), schema, mdata)
                    singer.write_record("contacts_by_company", record, time_extracted=utils.now())
    STATE = singer.set_offset(STATE, "contacts_by_company", 'offset', company_ids[-1])
    singer.write_state(STATE)
    return STATE

default_company_params = {
    'limit': 250, 'properties': ["createdate", "hs_lastmodifieddate"]
}

def sync_companies(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    bumble_bee = Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING)
    bookmark_key = 'property_hs_lastmodifieddate'
    bookmark_field_in_record = 'hs_lastmodifieddate'

    start = utils.strptime_to_utc(get_start(STATE, "companies", bookmark_key, older_bookmark_key=bookmark_field_in_record))
    LOGGER.info("sync_companies from %s", start)
    schema = load_schema('companies')
    singer.write_schema("companies", schema, ["companyId"], [bookmark_key], catalog.get('stream_alias'))

    # Because this stream doesn't query by `lastUpdated`, it cycles
    # through the data set every time. The issue with this is that there
    # is a race condition by which records may be updated between the
    # start of this table's sync and the end, causing some updates to not
    # be captured, in order to combat this, we must store the current
    # sync's start in the state and not move the bookmark past this value.
    current_sync_start = get_current_sync_start(STATE, "companies") or utils.now()
    STATE = write_current_sync_start(STATE, "companies", current_sync_start)
    singer.write_state(STATE)

    url = get_url("companies_all")
    max_bk_value = start
    if CONTACTS_BY_COMPANY in ctx.selected_stream_ids:
        contacts_by_company_schema = load_schema(CONTACTS_BY_COMPANY)
        singer.write_schema('contacts_by_company', contacts_by_company_schema, ["company-id", "contact-id"])

        # This code handles the interrutped sync. When sync is interrupted,
        # last batch of `contacts_by_company` extraction may get interrupted.
        # So before ressuming, we should check between `companies` and `contacts_by_company`
        # whose offset is lagging behind and set that as an offset value for `companies`.
        # Note, few of the records may get duplicated.
        if singer.get_offset(STATE, 'contacts_by_company', {}).get('offset'):
            companies_offset = singer.get_offset(STATE, 'companies', {}).get('offset')
            contacts_by_company_offset = singer.get_offset(STATE, 'contacts_by_company').get('offset')
            if companies_offset:
                offset = min(companies_offset, contacts_by_company_offset)
            else:
                offset = contacts_by_company_offset

            STATE = singer.set_offset(STATE, 'companies', 'offset', offset)
            singer.write_state(STATE)

    # This list collects the recently modified company ids to extract `contacts_by_company` records in batch
    company_ids = []
    with bumble_bee:
        for row in gen_request(STATE, 'companies', url, default_company_params, 'companies', 'has-more', ['offset'], ['offset']):
            row_properties = row['properties']
            modified_time = None
            if bookmark_field_in_record in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties[bookmark_field_in_record]['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis, datetime.timezone.utc)
            elif 'createdate' in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties['createdate']['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis, datetime.timezone.utc)

            if modified_time and modified_time >= max_bk_value:
                max_bk_value = modified_time

            if not modified_time or modified_time >= start:
                record = request(get_url("companies_detail", company_id=row['companyId'])).json()
                record = bumble_bee.transform(lift_properties_and_versions(record), schema, mdata)
                singer.write_record("companies", record, catalog.get('stream_alias'), time_extracted=utils.now())

            if CONTACTS_BY_COMPANY in ctx.selected_stream_ids:
                # Collect the recently modified company id
                if not modified_time or modified_time >= start:
                    company_ids.append(row['companyId'])

                # Once batch size reaches set limit, extract the `contacts_by_company` for company ids collected
                if len(company_ids) >= default_company_params['limit']:
                    STATE = _sync_contacts_by_company_batch_read(STATE, ctx, company_ids)
                    company_ids = []    # reset the list

    # Extract the records for last remaining company ids
    if CONTACTS_BY_COMPANY in ctx.selected_stream_ids:
        STATE = _sync_contacts_by_company_batch_read(STATE, ctx, company_ids)
        STATE = singer.clear_offset(STATE, "contacts_by_company")

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(max_bk_value, current_sync_start)
    STATE = singer.write_bookmark(STATE, 'companies', bookmark_key, utils.strftime(new_bookmark))
    STATE = write_current_sync_start(STATE, 'companies', None)
    singer.write_state(STATE)
    return STATE

def has_selected_custom_field(mdata):
    top_level_custom_props = [x for x in mdata if len(x) == 2 and 'property_' in x[1]]
    for prop in top_level_custom_props:
        # Return 'True' if the custom field is automatic.
        if (mdata.get(prop, {}).get('selected') is True) or (mdata.get(prop, {}).get('inclusion') == "automatic"):
            return True
    return False

def sync_deals(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    bookmark_key = 'property_hs_lastmodifieddate'
    # The Bookmark field('hs_lastmodifieddate') available in the record is different from
    # the tap's bookmark key(property_hs_lastmodifieddate).
    # `hs_lastmodifieddate` is available in the properties field at the nested level.
    # As `hs_lastmodifieddate` is not available at the 1st level it can not be marked as automatic inclusion.
    # tap includes all nested fields of the properties field as custom fields in the schema by appending the
    # prefix `property_` along with each field.
    # That's why bookmark_key is `property_hs_lastmodifieddate` so that we can mark it as automatic inclusion.

    last_modified_date = 'hs_lastmodifieddate'

    # Tap was used to write bookmark using replication key `hs_lastmodifieddate`.
    # Now, as the replication key gets changed to "property_hs_lastmodifieddate", `get_start` function would return
    # bookmark value of older bookmark key(`hs_lastmodifieddate`) if it is available.
    # So, here `older_bookmark_key` is the previous bookmark key that may be available in the state of
    # the existing connection.

    start = utils.strptime_with_tz(get_start(STATE, "deals", bookmark_key, older_bookmark_key=last_modified_date))
    max_bk_value = start
    LOGGER.info("sync_deals from %s", start)
    params = {'limit': 100,
              'includeAssociations': False,
              'properties' : []}

    schema = load_schema("deals")
    singer.write_schema("deals", schema, ["dealId"], [bookmark_key], catalog.get('stream_alias'))

    # Check if we should  include associations
    for key in mdata.keys():
        if 'associations' in key:
            assoc_mdata = mdata.get(key)
            if (assoc_mdata.get('selected') and assoc_mdata.get('selected') is True):
                params['includeAssociations'] = True

    v3_fields = None
    has_selected_properties = mdata.get(('properties', 'properties'), {}).get('selected')
    if has_selected_properties or has_selected_custom_field(mdata):
        # On 2/12/20, hubspot added a lot of additional properties for
        # deals, and appending all of them to requests ended up leading to
        # 414 (url-too-long) errors. Hubspot recommended we use the
        # `includeAllProperties` and `allpropertiesFetchMode` params
        # instead.
        params['includeAllProperties'] = True
        params['allPropertiesFetchMode'] = 'latest_version'

        # Grab selected `hs_date_entered/exited` fields to call the v3 endpoint with
        v3_fields = [breadcrumb[1].replace('property_', '')
                     for breadcrumb, mdata_map in mdata.items()
                     if breadcrumb
                     and (mdata_map.get('selected') is True or has_selected_properties)
                     and any(prefix in breadcrumb[1] for prefix in V3_PREFIXES)]

    url = get_url('deals_all')

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        # To handle records updated between start of the table sync and the end,
        # store the current sync start in the state and not move the bookmark past this value.
        sync_start_time = utils.now()
        for row in gen_request(STATE, 'deals', url, params, 'deals', "hasMore", ["offset"], ["offset"], v3_fields=v3_fields):
            row_properties = row['properties']
            modified_time = None
            if last_modified_date in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties[last_modified_date]['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis, datetime.timezone.utc)
            elif 'createdate' in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties['createdate']['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis, datetime.timezone.utc)
            if modified_time and modified_time >= max_bk_value:
                max_bk_value = modified_time

            if not modified_time or modified_time >= start:
                record = bumble_bee.transform(lift_properties_and_versions(row), schema, mdata)
                singer.write_record("deals", record, catalog.get('stream_alias'), time_extracted=utils.now())

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(max_bk_value, sync_start_time)
    STATE = singer.write_bookmark(STATE, 'deals', bookmark_key, utils.strftime(new_bookmark))
    singer.write_state(STATE)
    return STATE


def get_v3_records(tap_stream_id, url, params, path, more_key):
    """
    Cursor-based API Pagination : Used in tickets stream implementation
    """
    with metrics.record_counter(tap_stream_id) as counter:
        while True:
            data = request(url, params).json()

            if data.get(path) is None:
                raise RuntimeError(
                    "Unexpected API response: {} not in {}".format(path, data.keys()))

            for row in data[path]:
                counter.increment()
                yield row

            if not data.get(more_key):
                break
            params['after'] = data.get(more_key).get('next').get('after')

def sync_v3_stream(STATE, ctx, stream_id, params, primary_key="id", bookmark_key="updatedAt"):
    """
    Function to sync streams that are using v3 endpoints
    """
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))

    bookmark_value = utils.strptime_with_tz(
        get_start(STATE, stream_id, bookmark_key))
    max_bk_value = bookmark_value
    LOGGER.info(f"Sync {stream_id} from %s", bookmark_value)

    schema = load_schema(stream_id)
    singer.write_schema(stream_id, schema, [primary_key],
                        [bookmark_key], catalog.get('stream_alias'))

    url = get_url(stream_id)

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
        # To handle records updated between start of the table sync and the end,
        # store the current sync start in the state and not move the bookmark past this value.
        sync_start_time = utils.now()
        for row in get_v3_records(stream_id, url, params, 'results', "paging"):
            # Parsing the string formatted date to datetime object
            modified_time = utils.strptime_to_utc(row[bookmark_key])

            # Checking the bookmark value is present on the record and it
            # is greater than or equal to defined previous bookmark value
            if modified_time and modified_time >= bookmark_value:
                # Transforms the data and filters out the selected fields from the catalog
                record = transformer.transform(lift_properties_and_versions(row), schema, mdata)
                singer.write_record(stream_id, record, catalog.get(
                    'stream_alias'), time_extracted=utils.now())
                if modified_time >= max_bk_value:
                    max_bk_value = modified_time

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(max_bk_value, sync_start_time)
    STATE = singer.write_bookmark(STATE, stream_id, bookmark_key, utils.strftime(new_bookmark))
    singer.write_state(STATE)
    return STATE

def sync_tickets(STATE, ctx):
    """
    Function to sync `tickets` stream records
    """
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    stream_id = "tickets"
    params = {'limit': 100,
              'associations': 'contact,company,deals',
              'properties': get_selected_property_fields(catalog, mdata),
              'archived': False
              }
    return sync_v3_stream(STATE, ctx, stream_id, params)

# NB> no suitable bookmark is available: https://developers.hubspot.com/docs/methods/email/get_campaigns_by_id
def sync_campaigns(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    schema = load_schema("campaigns")
    singer.write_schema("campaigns", schema, ["id"], catalog.get('stream_alias'))
    LOGGER.info("sync_campaigns(NO bookmarks)")
    url = get_url("campaigns_all")
    params = {'limit': 500}

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in gen_request(STATE, 'campaigns', url, params, "campaigns", "hasMore", ["offset"], ["offset"]):
            record = request(get_url("campaigns_detail", campaign_id=row['id'])).json()
            record = bumble_bee.transform(lift_properties_and_versions(record), schema, mdata)
            singer.write_record("campaigns", record, catalog.get('stream_alias'), time_extracted=utils.now())

    return STATE


def sync_entity_chunked(STATE, catalog, entity_name, key_properties, path):
    schema = load_schema(entity_name)
    bookmark_key = 'startTimestamp'

    singer.write_schema(entity_name, schema, key_properties, [bookmark_key], catalog.get('stream_alias'))

    start = get_start(STATE, entity_name, bookmark_key)
    LOGGER.info("sync_%s from %s", entity_name, start)

    now = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    now_ts = int(now.timestamp() * 1000)

    start_ts = int(utils.strptime_with_tz(start).timestamp() * 1000)
    url = get_url(entity_name)

    mdata = metadata.to_map(catalog.get('metadata'))

    if entity_name == 'email_events':
        window_size = int(CONFIG['email_chunk_size'])
    elif entity_name == 'subscription_changes':
        window_size = int(CONFIG['subscription_chunk_size'])

    with metrics.record_counter(entity_name) as counter:
        while start_ts < now_ts:
            end_ts = start_ts + window_size
            params = {
                'startTimestamp': start_ts,
                'endTimestamp': end_ts,
                'limit': 1000,
            }
            with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
                while True:
                    our_offset = singer.get_offset(STATE, entity_name)
                    if bool(our_offset) and our_offset.get('offset') is not None:
                        params[StateFields.offset] = our_offset.get('offset')

                    data = request(url, params).json()
                    time_extracted = utils.now()

                    if data.get(path) is None:
                        raise RuntimeError("Unexpected API response: {} not in {}".format(path, data.keys()))

                    for row in data[path]:
                        counter.increment()
                        record = bumble_bee.transform(lift_properties_and_versions(row), schema, mdata)
                        singer.write_record(entity_name,
                                            record,
                                            catalog.get('stream_alias'),
                                            time_extracted=time_extracted)
                    if data.get('hasMore'):
                        STATE = singer.set_offset(STATE, entity_name, 'offset', data['offset'])
                        singer.write_state(STATE)
                    else:
                        STATE = singer.clear_offset(STATE, entity_name)
                        singer.write_state(STATE)
                        break
            STATE = singer.write_bookmark(STATE, entity_name, 'startTimestamp', utils.strftime(datetime.datetime.fromtimestamp((start_ts / 1000), datetime.timezone.utc)))  # pylint: disable=line-too-long
            singer.write_state(STATE)
            start_ts = end_ts

    STATE = singer.clear_offset(STATE, entity_name)
    singer.write_state(STATE)
    return STATE

def sync_subscription_changes(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    STATE = sync_entity_chunked(STATE, catalog, "subscription_changes", ["timestamp", "portalId", "recipient"],
                                "timeline")
    return STATE

def sync_email_events(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    STATE = sync_entity_chunked(STATE, catalog, "email_events", ["id"], "events")
    return STATE

def sync_contact_lists(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    schema = load_schema("contact_lists")
    bookmark_key = 'updatedAt'
    singer.write_schema("contact_lists", schema, ["listId"], [bookmark_key], catalog.get('stream_alias'))

    start = get_start(STATE, "contact_lists", bookmark_key)
    max_bk_value = start

    LOGGER.info("sync_contact_lists from %s", start)

    url = get_url("contact_lists")
    params = {'count': 250}
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        # To handle records updated between start of the table sync and the end,
        # store the current sync start in the state and not move the bookmark past this value.
        sync_start_time = utils.now()
        for row in gen_request(STATE, 'contact_lists', url, params, "lists", "has-more", ["offset"], ["offset"]):
            record = bumble_bee.transform(lift_properties_and_versions(row), schema, mdata)

            if record[bookmark_key] >= start:
                singer.write_record("contact_lists", record, catalog.get('stream_alias'), time_extracted=utils.now())
            if record[bookmark_key] >= max_bk_value:
                max_bk_value = record[bookmark_key]

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(utils.strptime_to_utc(max_bk_value), sync_start_time)
    STATE = singer.write_bookmark(STATE, 'contact_lists', bookmark_key, utils.strftime(new_bookmark))
    singer.write_state(STATE)

    return STATE

def sync_forms(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    schema = load_schema("forms")
    bookmark_key = 'updatedAt'

    singer.write_schema("forms", schema, ["guid"], [bookmark_key], catalog.get('stream_alias'))
    start = get_start(STATE, "forms", bookmark_key)
    max_bk_value = start

    LOGGER.info("sync_forms from %s", start)

    data = request(get_url("forms")).json()
    time_extracted = utils.now()

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        # To handle records updated between start of the table sync and the end,
        # store the current sync start in the state and not move the bookmark past this value.
        sync_start_time = utils.now()
        for row in data:
            record = bumble_bee.transform(lift_properties_and_versions(row), schema, mdata)

            if record[bookmark_key] >= start:
                singer.write_record("forms", record, catalog.get('stream_alias'), time_extracted=time_extracted)
            if record[bookmark_key] >= max_bk_value:
                max_bk_value = record[bookmark_key]

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(utils.strptime_to_utc(max_bk_value), sync_start_time)
    STATE = singer.write_bookmark(STATE, 'forms', bookmark_key, utils.strftime(new_bookmark))
    singer.write_state(STATE)

    return STATE

def sync_workflows(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    schema = load_schema("workflows")
    bookmark_key = 'updatedAt'
    singer.write_schema("workflows", schema, ["id"], [bookmark_key], catalog.get('stream_alias'))
    start = get_start(STATE, "workflows", bookmark_key)
    max_bk_value = start

    STATE = singer.write_bookmark(STATE, 'workflows', bookmark_key, max_bk_value)
    singer.write_state(STATE)

    LOGGER.info("sync_workflows from %s", start)

    data = request(get_url("workflows")).json()
    time_extracted = utils.now()

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        # To handle records updated between start of the table sync and the end,
        # store the current sync start in the state and not move the bookmark past this value.
        sync_start_time = utils.now()
        for row in data['workflows']:
            record = bumble_bee.transform(lift_properties_and_versions(row), schema, mdata)
            if record[bookmark_key] >= start:
                singer.write_record("workflows", record, catalog.get('stream_alias'), time_extracted=time_extracted)
            if record[bookmark_key] >= max_bk_value:
                max_bk_value = record[bookmark_key]

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(utils.strptime_to_utc(max_bk_value), sync_start_time)
    STATE = singer.write_bookmark(STATE, 'workflows', bookmark_key, utils.strftime(new_bookmark))
    singer.write_state(STATE)
    return STATE

def sync_owners(STATE, ctx):
    """
    Function to sync `owners` stream records
    """
    stream_id = "owners"
    params = {'limit': 500}
    return sync_v3_stream(STATE, ctx, stream_id, params)

def sync_engagements(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    schema = load_schema("engagements")
    bookmark_key = 'lastUpdated'
    singer.write_schema("engagements", schema, ["engagement_id"], [bookmark_key], catalog.get('stream_alias'))
    start = get_start(STATE, "engagements", bookmark_key)

    # Because this stream doesn't query by `lastUpdated`, it cycles
    # through the data set every time. The issue with this is that there
    # is a race condition by which records may be updated between the
    # start of this table's sync and the end, causing some updates to not
    # be captured, in order to combat this, we must store the current
    # sync's start in the state and not move the bookmark past this value.
    current_sync_start = get_current_sync_start(STATE, "engagements") or utils.now()
    STATE = write_current_sync_start(STATE, "engagements", current_sync_start)
    singer.write_state(STATE)

    max_bk_value = start
    LOGGER.info("sync_engagements from %s", start)

    STATE = singer.write_bookmark(STATE, 'engagements', bookmark_key, start)
    singer.write_state(STATE)

    url = get_url("engagements_all")
    params = {'limit': int(CONFIG.get('engagements_page_size') or 190)}
    top_level_key = "results"
    engagements = gen_request(STATE, 'engagements', url, params, top_level_key, "hasMore", ["offset"], ["offset"])

    time_extracted = utils.now()

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for engagement in engagements:
            record = bumble_bee.transform(lift_properties_and_versions(engagement), schema, mdata)
            if record['engagement'][bookmark_key] >= start:
                # hoist PK and bookmark field to top-level record
                record['engagement_id'] = record['engagement']['id']
                record[bookmark_key] = record['engagement'][bookmark_key]
                singer.write_record("engagements", record, catalog.get('stream_alias'), time_extracted=time_extracted)
                if record['engagement'][bookmark_key] >= max_bk_value:
                    max_bk_value = record['engagement'][bookmark_key]

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(utils.strptime_to_utc(max_bk_value), current_sync_start)
    STATE = singer.write_bookmark(STATE, 'engagements', bookmark_key, utils.strftime(new_bookmark))
    STATE = write_current_sync_start(STATE, 'engagements', None)
    singer.write_state(STATE)
    return STATE

def sync_deal_pipelines(STATE, ctx):
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    schema = load_schema('deal_pipelines')
    singer.write_schema('deal_pipelines', schema, ['pipelineId'], catalog.get('stream_alias'))
    LOGGER.info('sync_deal_pipelines')
    data = request(get_url('deal_pipelines')).json()
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in data:
            record = bumble_bee.transform(lift_properties_and_versions(row), schema, mdata)
            singer.write_record("deal_pipelines", record, catalog.get('stream_alias'), time_extracted=utils.now())
    singer.write_state(STATE)
    return STATE

def gen_request_custom_objects(tap_stream_id, url, params, path, more_key):
    """
    Cursor-based API Pagination : Used in custom_objects stream implementation
    """
    try:
        with metrics.record_counter(tap_stream_id) as counter:
            while True:
                data = request(url, params).json()

                if data.get(path) is None:
                    raise RuntimeError(
                        "Unexpected API response: {} not in {}".format(path, data.keys()))

                for row in data[path]:
                    counter.increment()
                    yield row

                if not data.get(more_key):
                    break
                params['after'] = data.get(more_key, {}).get('next', {}).get('after', None)
                if params['after'] is None:
                    break
    except SourceUnavailableException as ex:
        warning_message = str(ex).replace(CONFIG['access_token'], 10 * '*')
        LOGGER.warning(warning_message)
        return []

def sync_custom_objects(stream_id, primary_key, bookmark_key, catalog, STATE, params, is_custom_object=False):
    """
    Synchronize records from a data source
    """
    mdata = metadata.to_map(catalog.get('metadata'))
    if is_custom_object:
        url = get_url("custom_objects", object_name=catalog["table_name"])
    else:
        url = get_url(stream_id)
    max_bk_value = bookmark_value = utils.strptime_with_tz(
        get_start(STATE, stream_id, bookmark_key))

    LOGGER.info(f"Sync record for {stream_id} from {bookmark_value}")
    schema = catalog.get('schema')
    singer.write_schema(stream_id, schema, [primary_key],
                        [bookmark_key], catalog.get('stream_alias'))

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
        # To handle records updated between start of the table sync and the end,
        # store the current sync start in the state and not move the bookmark past this value.
        sync_start_time = utils.now()
        for row in gen_request_custom_objects(stream_id, url, params, 'results', "paging"):
            # parsing the string formatted date to datetime object
            modified_time = utils.strptime_to_utc(row[bookmark_key])

            # Checking the bookmark value is present on the record and it
            # is greater than or equal to defined previous bookmark value
            if modified_time and modified_time >= bookmark_value:
                # transforms the data and filters out the selected fields from the catalog
                record = transformer.transform(lift_properties_and_versions(row), schema, mdata)
                singer.write_record(stream_id, record, catalog.get(
                    'stream'), time_extracted=utils.now())
            if modified_time and modified_time >= max_bk_value:
                max_bk_value = modified_time

    # Don't bookmark past the start of this sync to account for updated records during the sync.
    new_bookmark = min(max_bk_value, sync_start_time)
    STATE = singer.write_bookmark(STATE, stream_id, bookmark_key, utils.strftime(new_bookmark))
    singer.write_state(STATE)
    return STATE


def sync_custom_object_records(STATE, ctx, stream_id):
    """
    Function to sync records for each `custom_object` stream
    """
    catalog = ctx.get_catalog_from_id(singer.get_currently_syncing(STATE))
    mdata = metadata.to_map(catalog.get('metadata'))
    primary_key = "id"
    bookmark_key = "updatedAt"

    params = {'limit': 100,
              'associations': 'emails,meetings,notes,tasks,calls,conversations,contacts,companies,deals,tickets',
              'properties': get_selected_property_fields(catalog, mdata),
              'archived': False
              }
    return sync_custom_objects(stream_id, primary_key, bookmark_key, catalog, STATE, params, is_custom_object=True)


@attr.s
class Stream:
    tap_stream_id = attr.ib()
    sync = attr.ib()
    key_properties = attr.ib()
    replication_key = attr.ib()
    replication_method = attr.ib()

STREAMS = [
    # Do these first as they are incremental
    Stream('subscription_changes', sync_subscription_changes, ['timestamp', 'portalId', 'recipient'], 'startTimestamp', 'INCREMENTAL'),
    Stream('email_events', sync_email_events, ['id'], 'startTimestamp', 'INCREMENTAL'),
    Stream('contacts', sync_contacts, ["vid"], 'versionTimestamp', 'INCREMENTAL'),
    Stream('deals', sync_deals, ["dealId"], 'property_hs_lastmodifieddate', 'INCREMENTAL'),
    Stream('companies', sync_companies, ["companyId"], 'property_hs_lastmodifieddate', 'INCREMENTAL'),
    Stream('tickets', sync_tickets, ['id'], 'updatedAt', 'INCREMENTAL'),
    Stream('owners', sync_owners, ["id"], 'updatedAt', 'INCREMENTAL'),
    Stream('forms', sync_forms, ['guid'], 'updatedAt', 'INCREMENTAL'),
    Stream('workflows', sync_workflows, ['id'], 'updatedAt', 'INCREMENTAL'),
    Stream('contact_lists', sync_contact_lists, ["listId"], 'updatedAt', 'INCREMENTAL'),
    Stream('engagements', sync_engagements, ["engagement_id"], 'lastUpdated', 'INCREMENTAL'),

    # Do these last as they are full table
    Stream('campaigns', sync_campaigns, ["id"], None, 'FULL_TABLE'),
    Stream('deal_pipelines', sync_deal_pipelines, ['pipelineId'], None, 'FULL_TABLE')
]

# pylint: disable=inconsistent-return-statements
def generate_custom_streams(mode, catalog=None):
    """
    - In DISCOVER mode, fetch the custom schema from the API endpoint and set the schema for the custom objects.
    - In SYNC mode, extend STREAMS for the custom objects.

    Args:
        mode (str): The mode indicating whether to DISCOVER or SYNC custom streams.
        catalog (dict): The catalog containing stream information.

    Returns:
        List[dict] or List[str]: Returns list of custom streams (contains dictionary) in DISCOVER mode and a list of custom object names in SYNC mode.
    """
    custom_objects_schema_url = get_url("custom_objects_schema")
    standard_streams = [stream.tap_stream_id for stream in STREAMS]
    if mode == "DISCOVER":
        custom_streams = []
        # Load Hubspot's shared schemas
        refs = load_shared_schema_refs()
        for custom_object in gen_request_custom_objects("custom_objects_schema", custom_objects_schema_url, {}, 'results', "paging"):
            custom_object_name = custom_object["name"]
            stream_id = f'custom_object_{custom_object_name}' if custom_object_name in standard_streams else custom_object_name
            schema = utils.load_json(get_abs_path('schemas/shared/custom_objects.json'))
            custom_schema = parse_custom_schema(stream_id, custom_object["properties"], is_custom_object=True)
            schema["properties"]["properties"] = {
                "type": "object",
                "properties": custom_schema,
            }

            # Move properties to top level
            custom_schema_top_level = {'property_{}'.format(k): v for k, v in custom_schema.items()}
            schema['properties'].update(custom_schema_top_level)

            final_schema = singer.resolve_schema_references(schema, refs)
            custom_streams.append({"custom_object_name": custom_object_name,
                                   "stream": Stream(stream_id, sync_custom_object_records, ['id'], 'updatedAt', 'INCREMENTAL'),
                                   "schema": final_schema})

        return custom_streams

    elif mode == "SYNC":
        rename_stream = lambda stream: f'custom_object_{stream}' if stream in standard_streams else stream
        custom_objects = [rename_stream(custom_object["name"]) for custom_object in gen_request_custom_objects("custom_objects_schema", custom_objects_schema_url, {}, 'results', "paging")]
        if len(custom_objects) > 0:
            for stream in catalog["streams"]:
                if stream["tap_stream_id"] in custom_objects:
                    STREAMS.append(Stream(stream["tap_stream_id"], sync_custom_object_records, ['id'], 'updatedAt', 'INCREMENTAL'))
        return custom_objects

def load_shared_schema_refs():
    shared_schemas_path = get_abs_path('schemas/shared')

    shared_file_names = [f for f in os.listdir(shared_schemas_path)
                         if os.path.isfile(os.path.join(shared_schemas_path, f))]

    shared_schema_refs = {}
    for shared_file in shared_file_names:
        with open(os.path.join(shared_schemas_path, shared_file)) as data_file:
            shared_schema_refs[shared_file] = json.load(data_file)

    return shared_schema_refs

def get_streams_to_sync(streams, state):
    target_stream = singer.get_currently_syncing(state)
    result = streams
    if target_stream:
        skipped = list(itertools.takewhile(
            lambda x: x.tap_stream_id != target_stream, streams))
        rest = list(itertools.dropwhile(
            lambda x: x.tap_stream_id != target_stream, streams))
        result = rest + skipped # Move skipped streams to end
    if not result:
        raise Exception('Unknown stream {} in state'.format(target_stream))
    return result

def get_selected_streams(remaining_streams, ctx):
    selected_streams = []
    for stream in remaining_streams:
        if stream.tap_stream_id in ctx.selected_stream_ids:
            selected_streams.append(stream)
    return selected_streams

def do_sync(STATE, catalog):
    custom_objects = generate_custom_streams(mode="SYNC", catalog=catalog)
    # Clear out keys that are no longer used
    clean_state(STATE)

    ctx = Context(catalog)
    validate_dependencies(ctx)

    remaining_streams = get_streams_to_sync(STREAMS, STATE)
    selected_streams = get_selected_streams(remaining_streams, ctx)
    LOGGER.info('Starting sync. Will sync these streams: %s',
                [stream.tap_stream_id for stream in selected_streams])
    for stream in selected_streams:
        LOGGER.info('Syncing %s', stream.tap_stream_id)
        STATE = singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            if stream.tap_stream_id in custom_objects:
                STATE = stream.sync(STATE, ctx, stream.tap_stream_id)
            else:
                STATE = stream.sync(STATE, ctx) # pylint: disable=not-callable
        except SourceUnavailableException as ex:
            error_message = str(ex).replace(CONFIG['access_token'], 10 * '*')
            LOGGER.error(error_message)
        except UriTooLongException as ex:
            LOGGER.fatal(f"For stream - {stream.tap_stream_id}, please select fewer fields. "
                         f"The current selection exceeds Hubspot's maximum character allowance.")
            raise ex
    STATE = singer.set_currently_syncing(STATE, None)
    singer.write_state(STATE)
    LOGGER.info("Sync completed")

class Context:
    def __init__(self, catalog):
        self.selected_stream_ids = set()

        for stream in catalog.get('streams'):
            mdata = metadata.to_map(stream['metadata'])
            if metadata.get(mdata, (), 'selected'):
                self.selected_stream_ids.add(stream['tap_stream_id'])

        self.catalog = catalog

    def get_catalog_from_id(self, tap_stream_id):
        return [c for c in self.catalog.get('streams') if c.get('stream') == tap_stream_id][0]

# stream a is dependent on stream STREAM_DEPENDENCIES[a]
STREAM_DEPENDENCIES = {
    CONTACTS_BY_COMPANY: 'companies'
}

def validate_dependencies(ctx):
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")

    for k, v in STREAM_DEPENDENCIES.items():
        if k in ctx.selected_stream_ids and v not in ctx.selected_stream_ids:
            errs.append(msg_tmpl.format(k, v))
    if errs:
        raise DependencyException(" ".join(errs))

def get_metadata(stream, schema):
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    if stream.replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])

    for field_name in schema['properties'].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    # The engagements stream has nested data that we synthesize; The engagement field needs to be automatic
    if stream.tap_stream_id == "engagements":
        mdata = metadata.write(mdata, ('properties', 'engagement'), 'inclusion', 'automatic')
        mdata = metadata.write(mdata, ('properties', 'lastUpdated'), 'inclusion', 'automatic')
    return metadata.to_list(mdata)

def load_discovered_schema(stream):
    schema = load_schema(stream.tap_stream_id)
    mdata = get_metadata(stream, schema)

    return schema, mdata

def discover_schemas():
    result = {'streams': []}
    for stream in STREAMS:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        try:
            schema, mdata = load_discovered_schema(stream)
            result['streams'].append({'stream': stream.tap_stream_id,
                                      'tap_stream_id': stream.tap_stream_id,
                                      'schema': schema,
                                      'metadata': mdata})
        except SourceUnavailableException as ex:
            # Skip the discovery mode on the streams were the required scopes are missing
            warning_message = str(ex).replace(CONFIG['access_token'], 10 * '*')
            LOGGER.warning(warning_message)

    for custom_stream in generate_custom_streams(mode="DISCOVER"):
        LOGGER.info('Loading schema for Custom Object - %s', custom_stream["stream"].tap_stream_id)
        result['streams'].append({'stream': custom_stream["stream"].tap_stream_id,
                                  'tap_stream_id': custom_stream["stream"].tap_stream_id,
                                  "table_name": custom_stream["custom_object_name"],
                                  'schema': custom_stream["schema"],
                                  'metadata': get_metadata(custom_stream["stream"], custom_stream["schema"])})

    # Load the contacts_by_company schema
    LOGGER.info('Loading schema for contacts_by_company')
    contacts_by_company = Stream('contacts_by_company', _sync_contacts_by_company_batch_read, ['company-id', 'contact-id'], None, 'FULL_TABLE')
    schema, mdata = load_discovered_schema(contacts_by_company)

    result['streams'].append({'stream': CONTACTS_BY_COMPANY,
                              'tap_stream_id': CONTACTS_BY_COMPANY,
                              'schema': schema,
                              'metadata': mdata})

    return result

def do_discover():
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)

def get_request_timeout():
    # Get `request_timeout` value from config.
    config_request_timeout = CONFIG.get('request_timeout')
    # if config request_timeout is other than 0, "0" or "" then use request_timeout
    if config_request_timeout and float(config_request_timeout):
        request_timeout = float(config_request_timeout)
    else:
        # If value is 0, "0", "" or not passed then it set default to 300 seconds.
        request_timeout = REQUEST_TIMEOUT
    return request_timeout

def main_impl():
    args = utils.parse_args(
        ["redirect_uri",
         "client_id",
         "client_secret",
         "refresh_token",
         "start_date"])

    CONFIG.update(args.config)
    STATE = {}

    if args.state:
        STATE.update(args.state)

    if args.discover:
        do_discover()
    elif args.properties:
        do_sync(STATE, args.properties)
    else:
        LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == '__main__':
    main()
