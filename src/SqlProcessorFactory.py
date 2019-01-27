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