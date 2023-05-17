import pendulum
from airflow.decorators import dag

import dlt

from stripe_analytics import stripe_source
from helpers import AirflowTasks


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def load_stripe():
    # just run it
    tasks = AirflowTasks("stripe_pipeline")

    p = dlt.pipeline(pipeline_name='stripe_pipeline',
                     dataset_name='stripe_data',
                     destination='bigquery',
                     full_refresh=False  # must be false if we decompose
    )

    # we keep secrets in `dlt_secrets_toml`, same for bigquery credentials
    source = stripe_source()

    # TODO: that should happen in the `add_run` wrapper
    # serial decomposition, it is easy when we have just resources
    # the case when we have transformers, it may result in double load
    pt = None
    for resource_name in source.selected_resources.keys():
        # `with_resources` is not really creating a separate instance so for now source needs
        # to be recreated each time
        # TODO: import source.clone() in dlt core
        source_2 = stripe_source()
        nt = tasks.add_run(p, source_2.with_resources(resource_name))
        if pt is not None:
            pt >> nt
        pt = nt


load_stripe()