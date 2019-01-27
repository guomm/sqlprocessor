
from src.SqlProcessor import SqlProcessor


class ReplaceResultSqlWrapper(SqlProcessor):

    def __init__(self, sqlProcessor):
        self.sqlProcessor = sqlProcessor


    def query(self, sqlStr, mode="dict"):
        queryResult = self.sqlProcessor.query(sqlStr, mode)
        result = []
        for data in queryResult:
            newdata = {}
            for key in data.keys():
                pos = key.find("_")
                if pos > 0:
                    newkey = key.replace("_"+key[pos+1], key[pos+1].upper())
                    #检测第二个下划线，默认情况下最多有两个下划线
                    pos = newkey.find("_")
                    if pos > 0:
                        newkey = newkey.replace("_" + newkey[pos + 1], newkey[pos + 1].upper())
                    newdata[newkey] = data[key]
                else:
                    newdata[key] = data[key]
            result.append(newdata)
        queryResult = None
        return result

    def update(self, sqlStr, mode="default"):
        return self.sqlProcessor.update(sqlStr, mode)

    def delete(self, sqlStr, mode="default"):
        return self.sqlProcessor.delete(sqlStr, mode)

    def insert(self, sqlStr, mode="default"):
        return self.sqlProcessor.insert(sqlStr, mode)

    def close(self):
        self.sqlProcessor.close()

    def printStr(self):
        return ""

