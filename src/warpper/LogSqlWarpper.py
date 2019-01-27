import logging
from src.SqlProcessor import SqlProcessor


class LogSqlWarpper(SqlProcessor):

    def __init__(self, sqlProcessor):
        self.sqlProcessor = sqlProcessor
        self.logger = logging

    def setLogger(self, logger):
        self.logger = logger

    def query(self, sqlStr, mode="dict"):
        self.logger.info(sqlStr)
        result = self.sqlProcessor.query(sqlStr, mode)
        return result

    def update(self, sqlStr, mode="default"):
        self.logger.info(sqlStr)
        result = self.sqlProcessor.update(sqlStr, mode)
        return result

    def delete(self, sqlStr, mode="default"):
        self.logger.info(sqlStr)
        result = self.sqlProcessor.delete(sqlStr, mode)
        return result

    def insert(self, sqlStr, mode="default"):
        self.logger.info(sqlStr)
        result = self.sqlProcessor.insert(sqlStr, mode)
        return result

    def close(self):
        self.sqlProcessor.close()

    def printStr(self):
        return ""

