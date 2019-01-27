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


