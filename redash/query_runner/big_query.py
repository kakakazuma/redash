import datetime
import json
import httplib2
import logging
import sys
import time

from redash.query_runner import *
from redash.utils import JSONEncoder

logger = logging.getLogger(__name__)

types_map = {
    'INTEGER': TYPE_INTEGER,
    'FLOAT': TYPE_FLOAT,
    'BOOLEAN': TYPE_BOOLEAN,
    'STRING': TYPE_STRING,
    'TIMESTAMP': TYPE_DATETIME,
}


def transform_row(row, fields):
    column_index = 0
    row_data = {}

    for cell in row["f"]:
        field = fields[column_index]
        cell_value = cell['v']

        if cell_value is None:
            pass
        # Otherwise just cast the value
        elif field['type'] == 'INTEGER':
            cell_value = int(cell_value)
        elif field['type'] == 'FLOAT':
            cell_value = float(cell_value)
        elif field['type'] == 'BOOLEAN':
            cell_value = cell_value.lower() == "true"
        elif field['type'] == 'TIMESTAMP':
            cell_value = datetime.datetime.fromtimestamp(float(cell_value))

        row_data[field["name"]] = cell_value
        column_index += 1

    return row_data


def _import():
    try:
        import apiclient.errors
        from apiclient.discovery import build
        from apiclient.errors import HttpError
        from oauth2client.client import SignedJwtAssertionCredentials

        return True
    except ImportError:
        logger.warning("Missing dependencies. Please install google-api-python-client and oauth2client.")
        logger.warning("You can use pip:   pip install google-api-python-client oauth2client")

    return False


def _load_key(filename):
    f = file(filename, "rb")
    try:
        return f.read()
    finally:
        f.close()


def _get_bigquery_service(service_account, private_key):
    scope = [
        "https://www.googleapis.com/auth/bigquery",
    ]

    credentials = SignedJwtAssertionCredentials(service_account, private_key, scope=scope)
    http = httplib2.Http()
    http = credentials.authorize(http)

    return build("bigquery", "v2", http=http)


def _get_query_results(jobs, project_id, job_id, start_index):
    query_reply = jobs.getQueryResults(projectId=project_id, jobId=job_id, startIndex=start_index).execute()
    logging.debug('query_reply %s', query_reply)
    if not query_reply['jobComplete']:
        time.sleep(10)
        return _get_query_results(jobs, project_id, job_id, start_index)

    return query_reply


class BigQuery(BaseQueryRunner):
    @classmethod
    def enabled(cls):
        return _import()

    @classmethod
    def configuration_fields(cls):
        return "serviceAccount", "privateKey", "projectId"

    def __init__(self, configuration_json):
        super(BigQuery, self).__init__(configuration_json)
        _import()

        self.private_key = _load_key(self.configuration["privateKey"])

    def run_query(self, query):
        bigquery_service = _get_bigquery_service(self.configuration["serviceAccount"],
                                                 self.private_key)

        jobs = bigquery_service.jobs()
        job_data = {
            "configuration": {
                "query": {
                    "query": query,
                }
            }
        }

        logger.debug("BigQuery got query: %s", query)

        project_id = self.configuration["projectId"]

        try:
            insert_response = jobs.insert(projectId=project_id, body=job_data).execute()
            current_row = 0
            query_reply = _get_query_results(jobs, project_id=project_id,
                                            job_id=insert_response['jobReference']['jobId'], start_index=current_row)

            logger.debug("bigquery replied: %s", query_reply)

            rows = []

            while ("rows" in query_reply) and current_row < query_reply['totalRows']:
                for row in query_reply["rows"]:
                    rows.append(transform_row(row, query_reply["schema"]["fields"]))

                current_row += len(query_reply['rows'])
                query_reply = jobs.getQueryResults(projectId=project_id, jobId=query_reply['jobReference']['jobId'],
                                                   startIndex=current_row).execute()

            columns = [{'name': f["name"],
                        'friendly_name': f["name"],
                        'type': types_map.get(f['type'], "string")} for f in query_reply["schema"]["fields"]]

            data = {
                "columns": columns,
                "rows": rows
            }
            error = None

            json_data = json.dumps(data, cls=JSONEncoder)
        except apiclient.errors.HttpError, e:
            json_data = None
            error = e.content
        except KeyboardInterrupt:
            error = "Query cancelled by user."
            json_data = None
        except Exception:
            raise sys.exc_info()[1], None, sys.exc_info()[2]

        return json_data, error

register("bigquery", BigQuery)