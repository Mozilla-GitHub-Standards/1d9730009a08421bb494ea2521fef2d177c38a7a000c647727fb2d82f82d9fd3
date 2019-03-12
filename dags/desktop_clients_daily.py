# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""clients_daily and derived tables."""

import datetime

from .operators import MozBigQueryOperator

with models.DAG(
    "clients_daily",
    schedule_interval="0 1 * * *",
    default_args={
        "start_date": datetime.datetime(2019, 3, 1),
        "email": ["dthorn@mozilla.com", "dataops+alerts@mozilla.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "depends_on_past": False,
        # If a task fails, retry it once after waiting at least 5 minutes
        "retries": 0,
        "retry_delay": datetime.timedelta(minutes=5),
    },
) as dag:
    clients_daily = BigQueryOperator(
        task_id="clients_daily",
        bql="sql/clients_daily_v7.sql",
        destination_dataset_table="analysis.clients_daily_v7${{ds_nodash}}",
        query_params=[
            {
                "name": "submission_date",
                "parameterType": {"type": "DATE"},
                "parameterValue": {"value": "{{ds}}"},
            }
        ],
    )

    clients_last_seen = BigQueryOperator(
        task_id="clients_last_seen",
        bql="sql/clients_last_seen_v1.sql",
        destination_dataset_table="analysis.clients_last_seen_v1${{ds_nodash}}",
        query_params=[
            {
                "name": "submission_date",
                "parameterType": {"type": "DATE"},
                "parameterValue": {"value": "{{ds}}"},
            }
        ],
        depends_on_past=True,
    )

    clients_daily >> clients_last_seen

    exact_mau_by_dimensions = BigQueryOperator(
        task_id="exact_mau_by_dimensions",
        bql="sql/firefox_desktop_exact_mau28_by_dimensions_v1.sql",
        destination_dataset_table="analysis."
        "firefox_desktop_exact_mau28_by_dimensions_v1${{ds_nodash}}",
        query_params=[
            {
                "name": "submission_date",
                "parameterType": {"type": "DATE"},
                "parameterValue": {"value": "{{ds}}"},
            }
        ],
    )

    clients_last_seen >> exact_mau_by_dimensions

    exact_mau = BigQueryOperator(
        task_id="exact_mau",
        bql="sql/firefox_desktop_exact_mau28_v1.sql",
        destination_dataset_table="analysis."
        "firefox_desktop_exact_mau28_v1${{ds_nodash}}",
        query_params=[
            {
                "name": "submission_date",
                "parameterType": {"type": "DATE"},
                "parameterValue": {"value": "{{ds}}"},
            }
        ],
    )

    exact_mau_by_dimensions >> exact_mau
