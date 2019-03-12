# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.
"""Custom Bigquery Operator."""

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


class MozBigqueryOperator(BigQueryOperator):
    """
    Extends the built-in BigQueryOperator with better defaults and
    templated query params
    """

    # add templating to query_params
    template_fields = ("query_params",)

    @apply_defaults
    def __init__(self, *args, **kwargs):
        # new defaults
        if "project_id" not in kwargs:
            kwargs["project_id"] = models.Variable.get("gcp_project")
        if "use_legacy_sql" not in kwargs:
            kwargs["use_legacy_sql"] = False
        if "write_disposition" not in kwargs:
            kwargs["write_disposition"] = "WRITE_TRUNCATE"
        super(BigQueryOperator, self).__init__(*args, **kwargs)
