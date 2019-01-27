
## 背景
对于python编程人员来说，经常会用pymysql操作数据库。利用sql语句操作数据库时经常会有些额外的操作，比如说打印sql语句，记录sql查询时间，统计业务调用次数或者将返回的数据进行格式转换等等，但有些需要记录业务查询次数，有些不用，因此该数据库操作组件应该满足可组装性。该数据库操作组件也需要满足可扩展性，比如说刚开始项目中用mysql存储所有的数据，一段时间后决定将日志存入ES，那么该组件应要很容易扩展。
一般用装饰者模式解决可扩展和组装问题。

## 设计

![](https://upload-images.jianshu.io/upload_images/7762147-714733d0d16e5da7.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
SqlProcessor是一个数据库操作接口，包含增删改查操作。</br>
wrapper是装饰类，装饰真正的数据库操作模块。</br>
SqlProcessorFactory是工厂类，获取包装后的sql处理模块。</br>
## 实现

MySqlProcessor： 具体的mysql操作类。</br>
SqlAssembleWarpper： sql组装类。</br>
LogSqlWarpper： 将sql语句保存到logger。</br>
ReplaceResultSqlWrapper：替换返回的Json结果key中的下划线。举个例子：数据库是user_name，替换为userName。</br>
SqlProcessorFactory：sql处理器创建工厂。</br>

### MySqlProcessor
利用连接池操作mysql操作：
```
import pymysql
from queue import Queue
from conf.conf import *
from src.SqlProcessor import SqlProcessor


class MysqlProcessor(SqlProcessor):
    __v = None

    def __init__(self):

        self.pool = Queue(maxconn)

        # 初始化线程池
        for i in range(maxconn):
            conn = pymysql.connect(host=DB_IP, port=DB_PORT, user=DB_USER, passwd=DB_PWD, db=DB_NAME, charset="utf8")
            conn.autocommit(True)
            self.pool.put(conn)

    @classmethod
    def getInstance(cls, *args, **kwargs):
        if cls.__v:
            return cls.__v
        else:
            cls.__v = MysqlProcessor(*args, **kwargs)
            return cls.__v

    def query(self, sqlStr, mode="dict"):
        conn = None
        cur = None
        try:
            conn = self.pool.get()
            curclass = pymysql.cursors.Cursor
            if mode == "default":
                curclass = pymysql.cursors.Cursor
            elif mode == "dict":
                curclass = pymysql.cursors.DictCursor
            elif mode == "sdefault":
                curclass = pymysql.cursors.SSCursor
            elif mode == "sdict":
                curclass = pymysql.cursors.SSDictCursor
            else:
                pass
            cur = conn.cursor(curclass)
            cur.execute(sqlStr)
            data = cur.fetchall()
        except Exception as e:
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                self.pool.put(conn)
        return data

    def update(self, sqlStr, mode="default"):
        print(sqlStr)
        result = []
        conn = None
        cur = None
        try:
            conn = self.pool.get()
            curclass = pymysql.cursors.Cursor
            if mode == "default":
                curclass = pymysql.cursors.Cursor
            elif mode == "dict":
                curclass = pymysql.cursors.DictCursor
            elif mode == "sdefault":
                curclass = pymysql.cursors.SSCursor
            elif mode == "sdict":
                curclass = pymysql.cursors.SSDictCursor
            else:
                pass
            cur = conn.cursor(curclass)
            data = cur.execute(sqlStr)
        except Exception as e:
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                self.pool.put(conn)
        return data

    def delete(self, sql_str, mode="default"):
        return self.update(sql_str, mode)

    def insert(self, sql_str, mode="default"):
        return self.update(sql_str, mode)

    def close(self):
        for i in range(maxconn):
            self.pool.get().close()
```
### SqlAssembleWarpper
一个简单的sql组装器。每次手写sql语句比较麻烦，特别是当一个表特别大，有20多个字段。这里简单的封装了sql语句组装操作，支持json，不支持子查询。
```
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
```
### SqlAssembleWarpper
```
from src.MysqlProcessor import MysqlProcessor
from src.warpper.SqlAssembleWarpper import SqlAssembleWarpper
from src.warpper.LogSqlWarpper import LogSqlWarpper
from src.warpper.ComputeTimeSqlWarpper import ComputeTimeSqlWarpper
from src.warpper.ReplaceResultSqlWrapper import ReplaceResultSqlWrapper


class SqlProcessorFactory(object):

    @staticmethod
    def getSqlProcessor(sqlProcessorType="default"):
        if sqlProcessorType == "default":
            mysqlProcessor = MysqlProcessor.getInstance() # 最底层的sql处理器
            sqlAssembleWarpper = SqlAssembleWarpper(mysqlProcessor) # sql组装操作
            computeTimeSqlWarpper = ComputeTimeSqlWarpper(sqlAssembleWarpper) # 统计时间操作
            replaceResultSqlWrapper = ReplaceResultSqlWrapper(computeTimeSqlWarpper) # 替换返回结果中的下划线
            logSqlWarpper = LogSqlWarpper(replaceResultSqlWrapper) # 将执行信息记录到日志
            return logSqlWarpper
        elif sqlProcessorType == "nowrapper":
            mysqlProcessor = MysqlProcessor.getInstance()  # 最底层的sql处理器
            computeTimeSqlWarpper = ComputeTimeSqlWarpper(mysqlProcessor)  # 统计时间操作
            replaceResultSqlWrapper = ReplaceResultSqlWrapper(computeTimeSqlWarpper)  # 替换返回结果中的下划线
            logSqlWarpper = LogSqlWarpper(replaceResultSqlWrapper)  # 将执行信息记录到日志
            return logSqlWarpper
        else:
            raise UnknownSqlProcessorType("UnknownSqlProcessorType:{}".format(sqlProcessorType))

class UnknownSqlProcessorType(Exception):
    pass
```

## 测试
测试组件的正确性。
```
import unittest

from src.SqlProcessorFactory import SqlProcessorFactory


class TestSql(unittest.TestCase):

    def test_sql(self):

        sqlProcessor = SqlProcessorFactory.getSqlProcessor()
        #测试组装功能
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
         #测试直接使用sql功能
        sqlProcessor = SqlProcessorFactory.getSqlProcessor("nowrapper")

        sql = "select * from project_manager"
        result = sqlProcessor.query(sql, mode="dict")
        print(result)

        sqlProcessor.close()
```
