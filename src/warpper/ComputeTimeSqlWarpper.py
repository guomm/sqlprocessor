import datetime
from src.SqlProcessor import SqlProcessor


class ComputeTimeSqlWarpper(SqlProcessor):

    def __init__(self, sqlProcessor):
        self.sqlProcessor = sqlProcessor
        self.startTime = None
        self.endTime = None

    def query(self, sqlStr, mode="dict"):
        self.startTime = datetime.datetime.now()
        result = self.sqlProcessor.query(sqlStr, mode)
        self.endTime = datetime.datetime.now()
        return result

    def update(self, sqlStr, mode="default"):
        self.startTime = datetime.datetime.now()
        result = self.sqlProcessor.update(sqlStr, mode)
        self.endTime = datetime.datetime.now()
        return result

    def delete(self, sqlStr, mode="default"):
        self.startTime = datetime.datetime.now()
        result = self.sqlProcessor.delete(sqlStr, mode)
        self.endTime = datetime.datetime.now()
        return result

    def insert(self, sqlStr, mode="default"):
        self.startTime = datetime.datetime.now()
        result = self.sqlProcessor.insert(sqlStr, mode)
        self.endTime = datetime.datetime.now()
        return result

    def close(self):
        self.sqlProcessor.close()


    def printStr(self):
        return "[runTime:{}]{}".format((self.endTime - self.startTime).total_seconds(), self.sqlProcessor.printStr())
