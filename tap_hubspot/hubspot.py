import sys
import requests
from ratelimit import limits
import ratelimit
import singer
import backoff
import datetime
from typing import Dict
from tap_hubspot.util import record_nodash
from dateutil import parser

LOGGER = singer.get_logger()
hs_calculated_form_submissions = []


class Hubspot:
    BASE_URL = "https://api.hubapi.com"
    CONTACT_DEFINITION_IDS = {"companyId": 1}

    def __init__(self, config, tap_stream_id, limit=250):
        self.SESSION = requests.Session()
        self.limit = limit
        self.access_token = None
        self.config = config
        self.refresh_access_token()
        self.tap_stream_id = tap_stream_id

    def streams(self, start_date, end_date, properties):
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
            start_date = self.datetime_to_milliseconds(start_date)
            end_date = self.datetime_to_milliseconds(end_date)
            yield from self.get_email_events(start_date, end_date)
        elif self.tap_stream_id == "forms":
            yield from self.get_forms()
        elif self.tap_stream_id == "submissions":
            yield from self.get_submissions()
        else:
            raise NotImplementedError(f"unknown stream_id: {self.tap_stream_id}")

    def get_companies(self, properties):
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

    def get_contacts(self, properties):
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

    def get_association(self, vid, definition_id):
        path = (
            f"/crm-associations/v1/associations/{vid}/HUBSPOT_DEFINED/{definition_id}"
        )
        record = self.call_api(url=path)["results"]
        if record:
            return int(record[0])
        else:
            return None

    def set_associations(self, record):
        for association, definition_id in self.CONTACT_DEFINITION_IDS.items():
            record[association] = self.get_association(record["vid"], definition_id)
        return record

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

    def get_deals(self, properties):
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

    def get_email_events(self, start_date, end_date):
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
        with open("workable_guid.txt", "w") as w:
            for guid in guids:
                path = f"/form-integrations/v1/submissions/forms/{guid}"
                try:
                    # some of the guids don't work
                    self.test_form(path)
                    w.write(str(guid) + "\n")
                except:
                    continue
                yield from self.get_records(
                    path, params=params, data_field=data_field, offset_key=offset_key,
                )

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
                form_summissions = self.get_value(
                    record, ["properties", "hs_calculated_form_submissions"]
                )
                if form_summissions:
                    hs_calculated_form_submissions.append(form_summissions)
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

    def datetime_to_milliseconds(self, d: datetime.datetime):
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

        LOGGER.info(response.url)
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
