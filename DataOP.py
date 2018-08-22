import pymssql 
import pandas as pd
import numpy as np
import cx_Oracle as oracle
import os 
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8' 


#Oracle 数据库读取操作类
class OracleOP:
    def __init__(self,user,password,address):
        self.user=user
        self.password=password
        self.address=address
        self.conn=oracle.connect(self.user,self.password,self.address)
        
    def read_data(self,SSQL):
        cursor = self.conn.cursor()
        if not cursor:
            raise Exception('数据库连接失败！')
        cursor.execute(SSQL)
        data = cursor.fetchall()
        cursor.close()
        return data

    def insert_data(self,ISQL,values):
        cur = self.conn.cursor()
        if not cur:
            raise Exception('数据库连接失败！')
        cur.executemany(ISQL,values)
        self.conn.commit()
        cur.close()

    def delete_data(self,DSQL):
        cur = self.conn.cursor()
        if not cur:
            raise Exception('数据库连接失败！')
        cur.execute(DSQL)
        self.conn.commit()
        cur.close()
    
    def close_conn(self):
        self.conn.close()

#MSSQL 数据库读取操作类
class MSSQLOP:
    def __init__(self,user,password,host,database):
        self.user=user
        self.password=password
        self.host=host
        self.database=database
        self.conn= pymssql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
     
    def delete_data(self,DSQL):
        cur = self.conn.cursor()
        if not cur:
            raise Exception('数据库连接失败！')
        cur.execute(DSQL)
        self.conn.commit()
        cur.close()

    def insert_data(self,ISQL,values):
        cur = self.conn.cursor()
        if not cur:
            raise Exception('数据库连接失败！')
        cur.executemany(ISQL,values)
        self.conn.commit()
        cur.close()

    def read_data(self,SSQL):
        cur = self.conn.cursor()
        if not cur:
            raise Exception('数据库连接失败！')
        cur.execute(SSQL)
        data = cur.fetchall()
        cur.close()
        return data

    def close_conn(self):
        self.conn.close()