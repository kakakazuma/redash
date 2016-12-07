#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from redash.utils import JSONEncoder
from redash.query_runner import *

import logging
logger = logging.getLogger(__name__)

try:
    from py4j.java_gateway import JavaGateway
    gateway = JavaGateway()
    enabled = True

except ImportError:
    enabled = False

class Athena(BaseQueryRunner):
    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'region': {
                    'type': 'string'
                },
                'aws_accesskey': {
                    'type': 'string'
                },
                'aws_secret_accesskey': {
                    'type': 'string'
                },
                's3_staging_dir': {
                    'type': 'string'
                }
            },
            'required': ['region', 'aws_accesskey', 'aws_secret_accesskey', 's3_staging_dir']
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "athena"

    def __init__(self, configuration):
        super(Athena, self).__init__(configuration)

    def get_schema(self, get_stats=False):
        # TODO - same with presto.py
        schema = {}
        query = """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema. error=%s",error)

        results = json.loads(results)

        for row in results['rows']:
            if row['table_schema'] != 'public':
                table_name = '{}.{}'.format(row['table_schema'], row['table_name'])
            else:
                table_name = row['table_name']

            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}

            schema[table_name]['columns'].append(row['column_name'])

        return schema.values()

    def run_query(self, query, user):
        info = gateway.jvm.java.util.Properties()
        info.setProperty("s3_staging_dir", self.configuration.get('s3_staging_dir', ''))
        info.setProperty("user", self.configuration.get('aws_accesskey', ''))
        info.setProperty("password", self.configuration.get('aws_secret_accesskey', ''))
        conn = None

        logger.info("user: %s", self.configuration.get('aws_accesskey', ''))
        logger.info("password: %s", self.configuration.get('aws_secret_accesskey', ''))

        try:
            region = self.configuration.get('region', 'us-east-1')
            host = 'jdbc:awsathena://athena.' + region + '.amazonaws.com:443/'
            logger.info("host: %s", host)

            connector = gateway.jvm.io.redash.queryrunner.athena.AthenaJDBCConnector(info, host)
            conn = connector.getConnection()
            statement = conn.createStatement()
            rs = statement.executeQuery(query)

            rsmd = rs.getMetaData()
            columns = []
            for i in range(rsmd.getColumnCount()):
                column = {}
                column['name'] = rsmd.getColumnName(i + 1)
                column['type'] = rsmd.getColumnType(i + 1)
                columns.append(column)
            print columns
            rows = []
            while rs.next():
                row = {}
                for column in columns:
                    type = column.get('type', 0)
                    name = column.get('name', '')
                    try:
                        if type == gateway.jvm.java.sql.Types.ARRAY:
                            row[name] = rs.getArray(name)
                        elif (type == gateway.jvm.java.sql.Types.BIGINT or
                                      type == gateway.jvm.java.sql.Types.INTEGER or
                                      type == gateway.jvm.java.sql.Types.TINYINT or
                                      type == gateway.jvm.java.sql.Types.SMALLINT):
                            row[name] = rs.getInt(name)
                        elif type == gateway.jvm.java.sql.Types.BOOLEAN:
                            row[name] = rs.getBoolean(name)
                        elif type == gateway.jvm.java.sql.Types.BLOB:
                            row[name] = rs.getBlob(name)
                        elif type == gateway.jvm.java.sql.Types.DOUBLE:
                            row[name] = rs.getDouble(name)
                        elif type == gateway.jvm.java.sql.Types.FLOAT:
                            row[name] = rs.getFloat(name)
                        elif type == gateway.jvm.java.sql.Types.NVARCHAR:
                            row[name] = rs.getNString(name)
                        elif type == gateway.jvm.java.sql.Types.VARCHAR:
                            row[name] = rs.getString(name)
                        elif (type == gateway.jvm.java.sql.Types.DATE or
                                      type == gateway.jvm.java.sql.Types.TIME or
                                      type == gateway.jvm.java.sql.Types.TIMESTAMP):
                            # TODO rs.getTimeStamp cause error in Athena.
                            row[name] = rs.getObject(name).toString()
                        else:
                            row[name] = rs.getObject(name)
                    except Exception:
                        row[name] = None
                rows.append(row)

            data = {'columns': map(lambda column: column.get('name', ''), columns), 'rows': rows}
            json_data = json.dumps(data, cls=JSONEncoder)
            error = None
        except Exception, ex:
            json_data = None
            error = ex
            raise Exception("Failed run query. error=%s", error)

        if conn is not None:
            conn.close()

        return json_data, error

register(Athena)