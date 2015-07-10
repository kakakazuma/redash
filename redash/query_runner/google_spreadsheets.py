import json
import logging
import sys
from redash.query_runner import *
from redash.utils import JSONEncoder

logger = logging.getLogger(__name__)

try:
    import gspread
    from oauth2client.client import SignedJwtAssertionCredentials
    from dateutil import parser
    enabled = True
except ImportError:
    logger.warning("Missing dependencies. Please install gspread, dateutil and oauth2client.")
    logger.warning("You can use pip:   pip install gspread dateutil oauth2client")

    enabled = False


def _load_key(filename):
    with open(filename, "rb") as f:
        return json.loads(f.read())


def _guess_type(value):
    try:
        val = int(value)
        return TYPE_INTEGER, val
    except ValueError:
        pass
    try:
        val = float(value)
        return TYPE_FLOAT, val
    except ValueError:
        pass
    if str(value).lower() in ('true', 'false'):
        return TYPE_BOOLEAN, bool(value)
    try:
        val = parser.parse(value)
        return TYPE_DATETIME, val
    except ValueError:
        pass
    return TYPE_STRING, value


class GoogleSpreadsheet(BaseQueryRunner):
    HEADER_INDEX = 0

    @classmethod
    def annotate_query(cls):
        return False

    @classmethod
    def type(cls):
        return "google_spreadsheets"

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'email': {
                    'type': 'string',
                    'title': 'Account email'
                },
                'privateKey': {
                    'type': 'string',
                    'title': 'Private Key Path'
                }
            },
            'required': ['email', 'privateKey']
        }

    def __init__(self, configuration_json):
        super(GoogleSpreadsheet, self).__init__(configuration_json)

    def _get_spreadsheet_service(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
        ]

        private_key = _load_key(self.configuration["privateKey"])
        credentials = SignedJwtAssertionCredentials(self.configuration['email'], private_key["private_key"], scope=scope)
        spreadsheetservice = gspread.authorize(credentials)
        return spreadsheetservice

    def run_query(self, query):
        logger.debug("Spreadsheet is about to execute query: %s", query)
        values = query.split("|")
        key = values[0] #key of the spreadsheet
        worksheet_num = int(values[1]) or 0 # if spreadsheet contains more than one worksheet - this is the number of it
        #logger.debig("%s - worksheet %s" % (key, worksheet_num))
        try:
            spreadsheet_service = self._get_spreadsheet_service()
            spreadsheet = spreadsheet_service.open_by_key(key)
            worksheets = spreadsheet.worksheets()
            allData = worksheets[worksheet_num].get_all_values()
            column_names = []
            columns = []
            for j, column_name in enumerate(allData[self.HEADER_INDEX]):
                column_names.append(column_name)
                columns.append({
                    'name': column_name,
                    'friendly_name': column_name,
                    'type': _guess_type(allData[self.HEADER_INDEX+1][j])
                })
            rows = [dict(zip(column_names, row)) for row in allData[self.HEADER_INDEX+1:]]
            data = {'columns': columns, 'rows': rows}
            json_data = json.dumps(data, cls=JSONEncoder)
            error = None
        except Exception as e:
            raise sys.exc_info()[1], None, sys.exc_info()[2]

        return json_data, error

register(GoogleSpreadsheet)
