import sys
import pendulum
# from copy import deepcopy
from airflow.decorators import dag, task
from dlt.common import json

json_row = """
{"type": "RECORD", "stream": "contacts", "record": {"vid": 103, "canonical-vid": 103, "merged-vids": [], "portal-id": 25962632, "is-contact": true, "properties": {"hs_latest_source_data_2": {"value": "115350574"}, "work_email": {"value": "saree.basnett@.com"}, "hs_latest_source_data_1": {"value": "IMPORT"}, "hs_is_unworked": {"value": true}, "firstname": {"value": "Saree"}, "num_unique_conversion_events": {"value": 0.0}, "hs_latest_source": {"value": "OFFLINE"}, "hs_pipeline": {"value": "contacts-lifecycle-pipeline"}, "hs_analytics_revenue": {"value": 0.0}, "createdate": {"value": "2022-06-15T08:58:17.175000Z"}, "hs_calculated_phone_number_country_code": {"value": "US"}, "hs_analytics_num_visits": {"value": 0.0}, "hs_sequences_actively_enrolled_count": {"value": 0.0}, "hs_analytics_source": {"value": "OFFLINE"}, "hs_created_by_user_id": {"value": 45521752.0}, "mobilephone": {"value": "694-185-1750"}, "hs_searchable_calculated_phone_number": {"value": "8606511814"}, "hs_searchable_calculated_mobile_number": {"value": "6941851750"}, "hs_email_domain": {"value": "gmail.com"}, "hs_analytics_num_page_views": {"value": 0.0}, "hs_calculated_phone_number": {"value": "+18606511814"}, "email": {"value": "saree.basnett@gmail.com"}, "jobtitle": {"value": "Chemical Engineer"}, "lastmodifieddate": {"value": "2022-06-15T08:58:30.131000Z"}, "hs_analytics_first_timestamp": {"value": "2022-06-15T08:58:17.175000Z"}, "hs_analytics_average_page_views": {"value": 0.0}, "lastname": {"value": "Basnett"}, "hs_all_contact_vids": {"value": "103"}, "phone": {"value": "860-651-1814"}, "hs_is_contact": {"value": true}, "num_conversion_events": {"value": 0.0}, "hs_object_id": {"value": 103.0}, "hs_analytics_num_event_completions": {"value": 0.0}, "hs_analytics_source_data_2": {"value": "115350574"}, "hs_analytics_source_data_1": {"value": "IMPORT"}}, "form-submissions": [], "list-memberships": [], "identity-profiles": [{"vid": 103, "saved-at-timestamp": "2022-06-15T08:58:17.563000Z", "deleted-changed-timestamp": "1970-01-01T00:00:00.000000Z", "identities": [{"type": "EMAIL", "value": "saree.basnett@gmail.com", "timestamp": "2022-06-15T08:58:17.554000Z"}, {"type": "LEAD_GUID", "value": "59f700ae-91a2-4a95-bf1e-977eb00a3714", "timestamp": "2022-06-15T08:58:17.560000Z"}]}], "merge-audits": [], "versionTimestamp": "2022-06-15T08:58:30.691000Z", "property_hs_latest_source_data_2": {"value": "115350574"}, "property_work_email": {"value": "saree.basnett@.com"}, "property_hs_latest_source_data_1": {"value": "IMPORT"}, "property_hs_is_unworked": {"value": true}, "property_firstname": {"value": "Saree"}, "property_num_unique_conversion_events": {"value": 0.0}, "property_hs_latest_source": {"value": "OFFLINE"}, "property_hs_pipeline": {"value": "contacts-lifecycle-pipeline"}, "property_hs_analytics_revenue": {"value": 0.0}, "property_createdate": {"value": "2022-06-15T08:58:17.175000Z"}, "property_hs_calculated_phone_number_country_code": {"value": "US"}, "property_hs_analytics_num_visits": {"value": 0.0}, "property_hs_sequences_actively_enrolled_count": {"value": 0.0}, "property_hs_analytics_source": {"value": "OFFLINE"}, "property_hs_created_by_user_id": {"value": 45521752.0}, "property_mobilephone": {"value": "694-185-1750"}, "property_hs_searchable_calculated_phone_number": {"value": "8606511814"}, "property_hs_searchable_calculated_mobile_number": {"value": "6941851750"}, "property_hs_email_domain": {"value": "gmail.com"}, "property_hs_analytics_num_page_views": {"value": 0.0}, "property_hs_calculated_phone_number": {"value": "+18606511814"}, "property_email": {"value": "saree.basnett@gmail.com"}, "property_jobtitle": {"value": "Chemical Engineer"}, "property_lastmodifieddate": {"value": "2022-06-15T08:58:30.131000Z"}, "property_hs_analytics_first_timestamp": {"value": "2022-06-15T08:58:17.175000Z"}, "property_hs_analytics_average_page_views": {"value": 0.0}, "property_lastname": {"value": "Basnett"}, "property_hs_all_contact_vids": {"value": "103"}, "property_phone": {"value": "860-651-1814"}, "property_hs_is_contact": {"value": true}, "property_num_conversion_events": {"value": 0.0}, "property_hs_object_id": {"value": 103.0}, "property_hs_analytics_num_event_completions": {"value": 0.0}, "property_hs_analytics_source_data_2": {"value": "115350574"}, "property_hs_analytics_source_data_1": {"value": "IMPORT"}}, "time_extracted": "2022-06-15T10:03:21.455333Z"}
"""

row = json.loads(json_row)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def bigdata_send():
    @task()
    def run_all():
        import os
        
        from airflow.configuration import conf
        plugins_folder = conf.get('core', 'plugins_folder')
        dags_folder = conf.get('core', 'dags_folder')

        from dlt.common.configuration import paths
        paths.DOT_DLT = os.path.join(dags_folder, paths.DOT_DLT)
        print(paths.DOT_DLT)

        from dlt.common.configuration.container import Container
        from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
        if ConfigProvidersContext in Container():
            del Container()[ConfigProvidersContext]

        from dlt.common.configuration.providers.toml import CONFIG_TOML, SECRETS_TOML
        

        # reload config.toml from alternative folder
        print(Container()[ConfigProvidersContext][CONFIG_TOML]._toml_path)
        print(Container()[ConfigProvidersContext][SECRETS_TOML]._toml_path)
        print(f"config.toml:{Container()[ConfigProvidersContext][CONFIG_TOML]._toml.as_string()}")

        import dlt
        from dlt.common import pendulum, logger
        from dlt.common.utils import uniq_id, main_module_file_path
        from dlt.pipeline.progress import log

        print(f"main module: {main_module_file_path()}")

        
        @dlt.resource
        def make_more():
            for _ in range(1, 100000):
                yield json.loads(json_row)

        # check if /data mount is available
        if os.path.exists("/home/airflow/gcs/data"):
            pipelines_dir = os.path.join("/home/airflow/gcs/data", f"pipelines_{uniq_id(8)}")
        else:
            # create random path
            from tempfile import gettempdir
            pipelines_dir = os.path.join(gettempdir(), f"pipelines_{uniq_id(8)}")

        print(f"Will store pipelines in {pipelines_dir}")
        
        p = dlt.pipeline(pipeline_name='hubspot_pipeline',
                     dataset_name='hubspot',
                     destination='duckdb',
                     full_refresh=True,
                     pipelines_dir=pipelines_dir,
                     progress=log(logger=logger.LOGGER))
        
        print(f"Stores pipeline in {p.working_dir}")

        p.extract(make_more())
        p.normalize(workers=1)

    run_all()

bigdata_send()