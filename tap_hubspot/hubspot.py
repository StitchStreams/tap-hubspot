import sys
import requests
from ratelimit import limits
import ratelimit
import singer
import backoff
from datetime import datetime, timezone
from typing import Dict, List, Optional
from tap_hubspot.util import record_nodash
from dateutil import parser
import urllib

LOGGER = singer.get_logger()
hs_calculated_form_submissions = []
event_contact_ids = []
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class Hubspot:
    BASE_URL = "https://api.hubapi.com"
    CONTACT_DEFINITION_IDS = {"companyId": 1}

    def __init__(
        self,
        config: Dict,
        tap_stream_id: str,
        start_date: datetime,
        end_date: datetime,
        limit=250,
    ):
        self.SESSION = requests.Session()
        self.limit = limit
        self.access_token = None
        self.config = config
        self.refresh_access_token()
        self.tap_stream_id = tap_stream_id
        self.start_date = start_date
        self.end_date = end_date

    def streams(self, properties: List):
        if self.tap_stream_id == "companies":
            yield from self.get_companies(properties)
        elif self.tap_stream_id == "contacts":
            yield from self.get_contacts(properties)
        elif self.tap_stream_id == "engagements":
            yield from self.get_engagements()
        elif self.tap_stream_id == "deal_pipelines":
            yield from self.get_deal_pipelines()
        elif self.tap_stream_id == "deals":
            yield from self.get_deals(properties)
        elif self.tap_stream_id == "email_events":
            yield from self.get_email_events()
        elif self.tap_stream_id == "forms":
            yield from self.get_forms()
        elif self.tap_stream_id == "submissions":
            yield from self.get_submissions()
        elif self.tap_stream_id == "contacts_events":
            yield from self.get_contacts_events()
        else:
            raise NotImplementedError(f"unknown stream_id: {self.tap_stream_id}")

    def get_companies(self, properties: List):
        path = "/companies/v2/companies/paged"
        data_field = "companies"
        replication_path = ["properties", "hs_lastmodifieddate", "timestamp"]
        params = {
            "limit": self.limit,
            "properties": properties,
        }
        offset_key = "offset"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_contacts(self, properties: List):
        path = "/crm/v3/objects/contacts"
        data_field = "results"
        offset_key = "after"
        replication_path = ["updatedAt"]
        params = {
            "limit": 100,
            "properties": properties,
        }
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_engagements(self):
        path = "/engagements/v1/engagements/paged"
        data_field = "results"
        replication_path = ["engagement", "lastUpdated"]
        params = {"limit": self.limit}
        offset_key = "offset"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_deal_pipelines(self):
        path = "/crm-pipelines/v1/pipelines/deals"
        data_field = "results"
        replication_path = ["updatedAt"]
        yield from self.get_records(path, replication_path, data_field=data_field)

    def get_deals(self, properties: List):
        path = "/deals/v1/deal/paged"
        data_field = "deals"
        replication_path = ["properties", "hs_lastmodifieddate", "timestamp"]
        params = {
            "count": self.limit,
            "includeAssociations": True,
            "properties": properties,
            "limit": self.limit,
        }
        offset_key = "offset"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_email_events(self):
        start_date: int = self.datetime_to_milliseconds(self.start_date)
        end_date: int = self.datetime_to_milliseconds(self.end_date)
        path = "/email/public/v1/events"
        data_field = "events"
        replication_path = ["created"]
        params = {"startTimestamp": start_date, "endTimestamp": end_date}
        offset_key = "offset"

        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_forms(self):
        path = "/forms/v2/forms"
        replication_path = ["updatedAt"]
        yield from self.get_records(path, replication_path)

    def get_guids_from_contacts(self) -> set:
        forms = set()
        if not hs_calculated_form_submissions:
            return forms
        for submission in hs_calculated_form_submissions:
            forms_times = submission.split(";")

            for form_time in forms_times:
                guid = form_time[: form_time.index(":")]
                forms.add(guid)
        return forms

    def get_guids_from_endpoint(self) -> set:
        forms = set()
        forms_from_endpoint = self.get_forms()
        if not forms_from_endpoint:
            return forms
        for form, _ in forms_from_endpoint:
            guid = form["guid"]
            forms.add(guid)
        return forms

    def get_submissions(self):
        # submission data is retrieved according to guid from forms
        # and hs_calculated_form_submissions field in contacts endpoint
        data_field = "results"
        offset_key = "after"
        params = {"limit": 50}  # maxmimum limit is 50
        guids_from_contacts = self.get_guids_from_contacts()
        guids_from_endpoint = self.get_guids_from_endpoint()
        guids = guids_from_contacts.union(guids_from_endpoint)
        for guid in guids:
            path = f"/form-integrations/v1/submissions/forms/{guid}"
            try:
                # some of the guids don't work
                self.test_form(path)
            except:
                continue
            yield from self.get_records(
                path, params=params, data_field=data_field, offset_key=offset_key,
            )

    def get_contacts_events(self):
        # contacts_events data is retrieved according to contact id
        start_date: str = self.start_date.strftime(DATE_FORMAT)
        end_date: str = self.end_date.strftime(DATE_FORMAT)
        data_field = "results"
        offset_key = "after"
        path = "/events/v3/events"

        for contact_id in event_contact_ids:

            params = {
                "limit": self.limit,
                "objectType": "contact",
                "objectId": contact_id,
                "occurredBefore": end_date,
                "occurredAfter": start_date,
            }
            yield from self.get_records(
                path, params=params, data_field=data_field, offset_key=offset_key,
            )

    def check_id(
        self,
        record: Dict,
        visited_page_date: Optional[str],
        submitted_form_date: Optional[str],
    ):
        contact_id = record["id"]
        if visited_page_date:
            visited_page_date = parser.isoparse(visited_page_date)
            if (
                visited_page_date > self.start_date
                and visited_page_date <= self.end_date
            ):
                return contact_id
        if submitted_form_date:
            submitted_form_date = parser.isoparse(submitted_form_date)
            if (
                submitted_form_date > self.start_date
                and submitted_form_date <= self.end_date
            ):
                return contact_id
        return None

    def store_ids_submissions(self, record: Dict):

        # get form guids from contacts to sync submissions data
        form_summissions = self.get_value(
            record, ["properties", "hs_calculated_form_submissions"]
        )
        if form_summissions:
            hs_calculated_form_submissions.append(form_summissions)

        # get contacts ids to sync events_contacts data
        # check if certain contact_id needs to be synced according to hs_analytics_last_timestamp and recent_conversion_date fields in contact record
        visited_page_date: Optional[str] = self.get_value(
            record, ["properties", "hs_analytics_last_timestamp"]
        )
        submitted_form_date: Optional[str] = self.get_value(
            record, ["properties", "recent_conversion_date"]
        )
        contact_id = self.check_id(
            record=record,
            visited_page_date=visited_page_date,
            submitted_form_date=submitted_form_date,
        )
        if contact_id:
            event_contact_ids.append(contact_id)

    def get_records(
        self, path, replication_path=None, params={}, data_field=None, offset_key=None
    ):
        for record in self.paginate(
            path, params=params, data_field=data_field, offset_key=offset_key,
        ):
            if self.tap_stream_id in ["contacts"]:
                replication_value = parser.isoparse(
                    self.get_value(record, replication_path)
                )
                self.store_ids_submissions(record)

            else:
                replication_value = self.milliseconds_to_datetime(
                    self.get_value(record, replication_path)
                )
            yield record, replication_value

    def get_value(self, obj: dict, path_to_replication_key=None, default=None):
        if not path_to_replication_key:
            return default
        for path_element in path_to_replication_key:
            obj = obj.get(path_element)
            if not obj:
                return default
        return obj

    def milliseconds_to_datetime(self, ms: str):
        return (
            datetime.datetime.fromtimestamp((int(ms) / 1000), datetime.timezone.utc)
            if ms
            else None
        )

    def datetime_to_milliseconds(self, d: datetime):
        return int(d.timestamp() * 1000) if d else None

    def paginate(
        self, path: str, params: Dict = None, data_field: str = None, offset_key=None
    ):
        offset_value = None
        while True:
            if offset_value:
                params[offset_key] = offset_value

            data = self.call_api(path, params=params)
            params[offset_key] = None

            if not data_field:
                # non paginated list
                yield from data
                return
            else:
                d = data.get(data_field, [])
                if not d:
                    return
                yield from d

            if offset_key:
                if "paging" in data:
                    offset_value = self.get_value(data, ["paging", "next", "after"])
                else:
                    offset_value = data.get(offset_key)
            if not offset_value:
                break

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.ReadTimeout,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
        max_tries=10,
    )
    @limits(calls=100, period=10)
    def call_api(self, url, params={}):
        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        try:
            response = self.SESSION.get(url, headers=headers, params=params)
        except requests.exceptions.HTTPError as err:
            if not err.response.status_code == 401:
                raise

            # attempt to refresh access token
            self.refresh_access_token()
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = self.SESSION.get(url, headers=headers, params=params)

        LOGGER.debug(response.url)
        response.raise_for_status()
        return response.json()

    def test_form(self, url, params={}):
        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = self.SESSION.get(url, headers=headers, params=params)
        response.raise_for_status()

    def refresh_access_token(self):
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

        resp = requests.post(self.BASE_URL + "/oauth/v1/token", data=payload)
        resp.raise_for_status()
        if not resp:
            raise Exception(resp.text)
        self.access_token = resp.json()["access_token"]
