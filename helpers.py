import os
from typing import Any, Callable, Sequence
from tenacity import retry_if_exception, wait_exponential, stop_after_attempt, Retrying

from airflow.configuration import conf
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.operators.python import get_current_context

import dlt
from dlt.common import logger
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.utils import uniq_id
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.runtime.collector import NULL_COLLECTOR

from dlt.extract.source import DltSource
from dlt.pipeline.helpers import retry_load
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.progress import log
from dlt.pipeline.typing import TPipelineStep

DEFAULT_RETRY_NO_RETRY = Retrying(stop=stop_after_attempt(1), reraise=True)
DEFAULT_RETRY_BACKOFF = Retrying(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=4, max=10),
                                 reraise=True)


class AirflowTasks(TaskGroup):
    """
    Represents a DLT Airflow pipeline task group.
    """

    def __init__(
            self,
            pipeline_name: str,
            use_data_folder: bool = False,
            use_task_logger: bool = True,
            log_progress_period: float = 30.0,
            buffer_max_items: int = 1000,
            retry_policy: Retrying = DEFAULT_RETRY_NO_RETRY,
            retry_pipeline_steps: Sequence[TPipelineStep] = ("load",),
            fail_task_if_any_job_failed: bool = True,
            abort_task_if_any_job_failed: bool = False,
            wipe_local_data: bool = True,
            save_load_info: bool = False,
            save_trace_info: bool = False,
            **kwargs,
    ) -> None:
        """Creates a task group to which you can add pipeline runs

        The run environment is prepared as follows
        - the .dlt folder (the project folder) is searched under `dags` as configured by Airflow
        - the data folder where pipelines are stored is always unique

        The `data_folder` is available in certain Airflow deployments. In case of Composer, it is a location on the gcs bucket. `use_data_folder` is disabled and should be
        enabled only when needed. The operations on bucket are non-atomic and way slower than on local storage and should be avoided.

        `fail_task_if_any_job_failed` will raise an exception if any of the loading jobs failed permanently and thus fail the current Airflow task.
        This happens **after all dlt loading jobs executed**. See more here: https://dlthub.com/docs/running-in-production/running#failed-jobs

        `abort_task_if_any_job_failed` will abort the other dlt loading jobs and fail the Airflow task in any of the jobs failed. This may put your warehouse in
        inconsistent state so the option is disabled by default.

        The load info and trace info can be optionally saved to the destination. See https://dlthub.com/docs/running-in-production/running#inspect-and-save-the-load-info-and-trace

        Args:
            pipeline_name (str): Name of the task group
            use_data_folder (bool, optional): If well defined 'data' folder is present it will be used. Currently only data folder on Composer is supported. Defaults to False.
            use_task_logger (bool, optional): Will redirect dlt logger into task logger. Defaults to True.
            log_progress_period (float, optional): If progress is not configured for a pipeline, the `log` progress is used with a given period. Set 0 to disable. Defaults to 30.0.
            buffer_max_items (int, optional): Maximum number of buffered items. Use 0 to keep dlt built-in limit. Defaults to 1000.
            retry_policy (_type_, optional): Tenacity retry policy. Defaults to no retry.
            retry_pipeline_steps (Sequence[TPipelineStep], optional): Which pipeline steps are eligible for retry. Defaults to ("load", ).
            fail_task_if_any_job_failed (bool, optional): Will fail a task if any of the dlt load jobs failed. Defaults to True.
            wipe_local_data (bool, optional): Will wipe all the data created by pipeline, also in case of exception. Defaults to False.
            save_load_info (bool, optional): Will save extensive load info to the destination. Defaults to False.
            save_trace_info (bool, optional): Will save trace info to the destination. Defaults to False.
        """

        super().__init__(group_id=pipeline_name, **kwargs)
        self.use_task_logger = use_task_logger
        self.log_progress_period = log_progress_period
        self.buffer_max_items = buffer_max_items
        self.retry_policy = retry_policy
        self.retry_pipeline_steps = retry_pipeline_steps
        self.fail_task_if_any_job_failed = fail_task_if_any_job_failed
        self.abort_task_if_any_job_failed = abort_task_if_any_job_failed
        self.wipe_local_data = wipe_local_data
        self.save_load_info = save_load_info
        self.save_trace_info = save_trace_info

        # reload providers so config.toml in dags folder is included
        dags_folder = conf.get('core', 'dags_folder')

        # set the dlt project folder to dags
        os.environ["DLT_PROJECT_DIR"] = dags_folder

        # check if /data mount is available
        if use_data_folder and os.path.exists("/home/airflow/gcs/data"):
            data_dir = os.path.join("/home/airflow/gcs/data", f"dlt_{uniq_id(8)}")
        else:
            # create random path
            from tempfile import gettempdir
            data_dir = os.path.join(gettempdir(), f"dlt_{uniq_id(8)}")
        os.environ["DLT_DATA_DIR"] = data_dir

        # delete existing config providers in container, they will get reloaded on next use
        if ConfigProvidersContext in Container():
            del Container()[ConfigProvidersContext]

    # TODO: add all run parameters to be passed to pipeline.run()
    def add_run(self, pipeline: Pipeline, data: Any, *, write_disposition: TWriteDisposition = None) -> Any:

        # make sure that pipeline was created after dag was initialized
        if not pipeline.pipelines_dir.startswith(os.environ["DLT_DATA_DIR"]):
            raise ValueError(
                "Please create your Pipeline instance after AirflowTasks are created. The dlt pipelines directory is not set correctly")

        with self:

            # TODO: replace decorator with operator, set name etc.
            @task()
            def _run() -> None:
                # use task logger
                if self.use_task_logger:
                    ti = get_current_context()["ti"]
                    logger.LOGGER = ti.log

                # set global number of buffered items
                if dlt.config.get("data_writer.buffer_max_items") is None and self.buffer_max_items > 0:
                    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = str(self.buffer_max_items)
                    logger.info(f"Set data_writer.buffer_max_items to {self.buffer_max_items}")

                from dlt.common.configuration.providers.toml import CONFIG_TOML, SECRETS_TOML

                print(Container()[ConfigProvidersContext][CONFIG_TOML]._toml_path)
                print(Container()[ConfigProvidersContext][SECRETS_TOML]._toml_path)
                print(f"config.toml:{Container()[ConfigProvidersContext][CONFIG_TOML]._toml.as_string()}")

                # TODO: enable abort package if job failed
                if self.abort_task_if_any_job_failed:
                    os.environ["LOAD__RAISE_ON_FAILED_JOBS"] = "true"
                    logger.info(f"Set load.abort_task_if_any_job_failed to True")

                if self.log_progress_period > 0 and pipeline.collector == NULL_COLLECTOR:
                    pipeline.collector = log(log_period=self.log_progress_period, logger=logger.LOGGER)
                    logger.info(f"Enabled log progress with period {self.log_progress_period}")

                logger.info(f"Pipeline data in {pipeline.working_dir}")

                def log_after_attempt(retry_state):
                    if not retry_state.retry_object.stop(retry_state=retry_state):
                        logger.error("Retrying pipeline run due to exception: %s", retry_state.outcome.exception())

                try:
                    # retry with given policy on selected pipeline steps
                    for attempt in self.retry_policy.copy(
                            retry=retry_if_exception(retry_load(retry_on_pipeline_steps=self.retry_pipeline_steps)),
                            after=log_after_attempt
                    ):
                        with attempt:
                            logger.info("Running the pipeline, attempt=%s" % attempt.retry_state.attempt_number)
                            load_info = pipeline.run(data)
                            logger.info(str(load_info))
                            # save load and trace
                            if self.save_load_info:
                                logger.info("Saving the load info in the destination")
                                pipeline.run([load_info], table_name="_load_info", write_disposition=write_disposition)
                            if self.save_trace_info:
                                logger.info("Saving the trace in the destination")
                                pipeline.run([pipeline.last_trace], table_name="_trace")
                            # raise on failed jobs if requested
                            if self.fail_task_if_any_job_failed:
                                load_info.raise_on_failed_jobs()
                finally:
                    # always completely wipe out pipeline folder, in case of success and failure
                    if self.wipe_local_data:
                        logger.info(f"Removing folder {pipeline.working_dir}")
                        pipeline._wipe_working_folder()

            # TODO: decompose source here
            task_name = pipeline.pipeline_name
            if isinstance(data, DltSource):
                resource_names = list(data.selected_resources.keys())
                task_name = "-".join(resource_names[:4])
                if len(resource_names) > 4:
                    task_name += f"-{len(resource_names) - 4}-more"

            return _run.override(task_id=task_name)()

    def add_fun(self, f: Callable[..., Any]) -> Any:
        """Will execute a function `f` inside an Airflow task. It is up to a function to create pipeline and source"""
        pass