from src.SqlProcessor import SqlProcessor


class SqlAssembleWarpper(SqlProcessor):

    def __init__(self, sqlProcessor):
        self.sqlProcessor = sqlProcessor
        self.sqlStr = None

    def whereAssemble(self, sqlStr, where, other):
        if where:
            condition = []
            for key, val in where.items():
                condition.append("{}='{}'".format(key, val))
            sqlStr = sqlStr + " where " + " and ".join(condition)
        if other:
            sqlStr = sqlStr + other
        self.sqlStr = sqlStr
        return sqlStr

    def getParams(self, queryType, sqlDict):
        val = sqlDict.get("{}Params".format(queryType), None)
        tableName = sqlDict.get("tableName")
        where = sqlDict.get("where", None)
        other = sqlDict.get("other", None)
        return val, tableName, where, other

    def query(self, sqlDict, mode="dict"):
        selectParams, tableName, where, other = self.getParams("query", sqlDict)
        if not selectParams:
            selectParams = ["*"]
        prefix = "select {} from {}".format(",".join(selectParams), tableName)
        sqlStr = self.whereAssemble(prefix, where, other)
        self.sqlStr = sqlStr
        result = self.sqlProcessor.query(sqlStr, mode)
        return result

    def update(self, sqlDict, mode="default"):
        updateParams,tableName, where, other = self.getParams("update", sqlDict)
        prefix = "update {} set ".format(tableName)
        updateVal = []
        for key, val in updateParams.items():
            jsonSeparatorPos = key.find(".")
            if jsonSeparatorPos > 0:
                #json 格式转换，默认两层处理
                jsonFirstKey = key[:jsonSeparatorPos]
                jsonSecondKey = "$" +  key[jsonSeparatorPos:]
                updateVal.append("{}='JSON_SET({},{},{})'".format(key, jsonFirstKey, jsonSecondKey, val))
            else:
                updateVal.append("{}='{}'".format(key, val))
        prefix = prefix + ",".join(updateVal)
        sqlStr = self.whereAssemble(prefix, where, other)
        self.sqlStr = sqlStr
        #print(sqlStr)
        result = self.sqlProcessor.update(sqlStr, mode)
        return result

    def delete(self, sqlDict, mode="default"):
        _, tableName, where, other = self.getParams("delete", sqlDict)
        prefix = "delete from {} ".format(tableName)
        sqlStr = self.whereAssemble(prefix, where, other)
        self.sqlStr = sqlStr
        result = self.sqlProcessor.delete(sqlStr, mode)
        return result

    def insert(self, sqlDict, mode="default"):
        insertParams, tableName, where, other = self.getParams("insert", sqlDict)
        if not insertParams:
            raise NoneInsertParamsException()
        insertVal = []
        for val in insertParams.values():
            insertVal.append("'{}'".format(val))

        sqlStr = "insert into {}({}) values ({}) ".format(tableName, ",".join(insertParams.keys()), ",".join(insertVal))
        self.sqlStr = sqlStr
        result = self.sqlProcessor.insert(sqlStr, mode)
        return result

    def close(self):
        self.sqlProcessor.close()



class NoneInsertParamsException(Exception):
    pass