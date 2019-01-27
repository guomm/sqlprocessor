import unittest

from src.SqlProcessorFactory import SqlProcessorFactory


class TestSql(unittest.TestCase):

    def test_sql(self):

        sqlProcessor = SqlProcessorFactory.getSqlProcessor()

        # insert
        insertJson = {"insertParams":{"project_name":"a", "project_type":"b", "create_user":"guo", "last_modify_user":"d", "last_modify_time":"2019-01-01 10:00:00"}, "tableName":"project_manager", "where":"", "other":""}
        result = sqlProcessor.insert(insertJson)
        print(result)

        # query
        queryJson = {"queryParams":"", "tableName":"project_manager", "where":{"project_name":"a", "project_type":"b"}, "other":" limit 1"}
        result = sqlProcessor.query(queryJson)
        print(result)

        # update
        updateJson = {"updateParams":{"project_type":"test"}, "tableName":"project_manager", "where":{"create_user":"guo"}, "other":" limit 1"}
        result = sqlProcessor.update(updateJson)
        print(result)

        # delete
        deleteJson = {"deleteParams":"", "tableName":"project_manager", "where":{"create_user":"guo"}, "other":" limit 1"}
        result = sqlProcessor.delete(deleteJson)
        print(result)


        # insert test
        insertJson = {
            "insertParams": {"project_name": "a", "project_type": "b", "create_user": "guo", "last_modify_user": "d",
                             "last_modify_time": "2019-01-01 10:00:00"}, "tableName": "project_manager", "where": "",
            "other": ""}
        result = sqlProcessor.insert(insertJson)
        print(result)

        sqlProcessor = SqlProcessorFactory.getSqlProcessor("nowrapper")

        sql = "select * from project_manager"
        result = sqlProcessor.query(sql, mode="dict")
        print(result)

        sqlProcessor.close()