import sys
import pendulum
# from copy import deepcopy
from airflow.decorators import dag, task
from tenacity import wait_fixed, stop_after_attempt, Retrying

import dlt
from dlt.common import json

from helpers import AirflowTasks

json_row = """
{"type": "RECORD", "stream": "contacts", "record": {"vid": 103, "canonical-vid": 103, "merged-vids": [], "portal-id": 25962632, "is-contact": true, "properties": {"hs_latest_source_data_2": {"value": "115350574"}, "work_email": {"value": "saree.basnett@.com"}, "hs_latest_source_data_1": {"value": "IMPORT"}, "hs_is_unworked": {"value": true}, "firstname": {"value": "Saree"}, "num_unique_conversion_events": {"value": 0.0}, "hs_latest_source": {"value": "OFFLINE"}, "hs_pipeline": {"value": "contacts-lifecycle-pipeline"}, "hs_analytics_revenue": {"value": 0.0}, "createdate": {"value": "2022-06-15T08:58:17.175000Z"}, "hs_calculated_phone_number_country_code": {"value": "US"}, "hs_analytics_num_visits": {"value": 0.0}, "hs_sequences_actively_enrolled_count": {"value": 0.0}, "hs_analytics_source": {"value": "OFFLINE"}, "hs_created_by_user_id": {"value": 45521752.0}, "mobilephone": {"value": "694-185-1750"}, "hs_searchable_calculated_phone_number": {"value": "8606511814"}, "hs_searchable_calculated_mobile_number": {"value": "6941851750"}, "hs_email_domain": {"value": "gmail.com"}, "hs_analytics_num_page_views": {"value": 0.0}, "hs_calculated_phone_number": {"value": "+18606511814"}, "email": {"value": "saree.basnett@gmail.com"}, "jobtitle": {"value": "Chemical Engineer"}, "lastmodifieddate": {"value": "2022-06-15T08:58:30.131000Z"}, "hs_analytics_first_timestamp": {"value": "2022-06-15T08:58:17.175000Z"}, "hs_analytics_average_page_views": {"value": 0.0}, "lastname": {"value": "Basnett"}, "hs_all_contact_vids": {"value": "103"}, "phone": {"value": "860-651-1814"}, "hs_is_contact": {"value": true}, "num_conversion_events": {"value": 0.0}, "hs_object_id": {"value": 103.0}, "hs_analytics_num_event_completions": {"value": 0.0}, "hs_analytics_source_data_2": {"value": "115350574"}, "hs_analytics_source_data_1": {"value": "IMPORT"}}, "form-submissions": [], "list-memberships": [], "identity-profiles": [{"vid": 103, "saved-at-timestamp": "2022-06-15T08:58:17.563000Z", "deleted-changed-timestamp": "1970-01-01T00:00:00.000000Z", "identities": [{"type": "EMAIL", "value": "saree.basnett@gmail.com", "timestamp": "2022-06-15T08:58:17.554000Z"}, {"type": "LEAD_GUID", "value": "59f700ae-91a2-4a95-bf1e-977eb00a3714", "timestamp": "2022-06-15T08:58:17.560000Z"}]}], "merge-audits": [], "versionTimestamp": "2022-06-15T08:58:30.691000Z", "property_hs_latest_source_data_2": {"value": "115350574"}, "property_work_email": {"value": "saree.basnett@.com"}, "property_hs_latest_source_data_1": {"value": "IMPORT"}, "property_hs_is_unworked": {"value": true}, "property_firstname": {"value": "Saree"}, "property_num_unique_conversion_events": {"value": 0.0}, "property_hs_latest_source": {"value": "OFFLINE"}, "property_hs_pipeline": {"value": "contacts-lifecycle-pipeline"}, "property_hs_analytics_revenue": {"value": 0.0}, "property_createdate": {"value": "2022-06-15T08:58:17.175000Z"}, "property_hs_calculated_phone_number_country_code": {"value": "US"}, "property_hs_analytics_num_visits": {"value": 0.0}, "property_hs_sequences_actively_enrolled_count": {"value": 0.0}, "property_hs_analytics_source": {"value": "OFFLINE"}, "property_hs_created_by_user_id": {"value": 45521752.0}, "property_mobilephone": {"value": "694-185-1750"}, "property_hs_searchable_calculated_phone_number": {"value": "8606511814"}, "property_hs_searchable_calculated_mobile_number": {"value": "6941851750"}, "property_hs_email_domain": {"value": "gmail.com"}, "property_hs_analytics_num_page_views": {"value": 0.0}, "property_hs_calculated_phone_number": {"value": "+18606511814"}, "property_email": {"value": "saree.basnett@gmail.com"}, "property_jobtitle": {"value": "Chemical Engineer"}, "property_lastmodifieddate": {"value": "2022-06-15T08:58:30.131000Z"}, "property_hs_analytics_first_timestamp": {"value": "2022-06-15T08:58:17.175000Z"}, "property_hs_analytics_average_page_views": {"value": 0.0}, "property_lastname": {"value": "Basnett"}, "property_hs_all_contact_vids": {"value": "103"}, "property_phone": {"value": "860-651-1814"}, "property_hs_is_contact": {"value": true}, "property_num_conversion_events": {"value": 0.0}, "property_hs_object_id": {"value": 103.0}, "property_hs_analytics_num_event_completions": {"value": 0.0}, "property_hs_analytics_source_data_2": {"value": "115350574"}, "property_hs_analytics_source_data_1": {"value": "IMPORT"}}, "time_extracted": "2022-06-15T10:03:21.455333Z"}
"""

row = json.loads(json_row)

@dlt.resource
def make_more():
    for _ in range(1, 10000):
        yield json.loads(json_row)


@dlt.resource
def failed_task():
    raise Exception("this task is failed!")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def big_helper():
    # just run it
    # tasks = AirflowTasks("big")

    # with custom retry (3 times, 5 seconds pause)
    # NOTE: remember to create the AirflowTasks before the pipeline is instantiated
    tasks = AirflowTasks(
        "big",
        retry_policy=Retrying(stop=stop_after_attempt(2), wait=wait_fixed(5.0), reraise=True)
    )

    p = dlt.pipeline(pipeline_name='big_helper',
                     dataset_name='big_helper_data',
                     destination='bigquery',
                     full_refresh=True)

    tasks.add_run(p, data=[make_more(), failed_task()])

big_helper()