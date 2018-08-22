import pymssql 
import pandas as pd
import numpy as np
import datetime
import cx_Oracle as oracle
import os
from sqlalchemy import create_engine
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8' 

import DataOP
from  SqlList import *
#################################
import GlobalSetting
import logging

class HoldingDetail:
    def __init__(self,fundcode,fdate=None):
        logging.info('初始化基金%s信息！'%fundcode)

        self.fundcode=fundcode
        self.fdate=fdate

        #初始化数据库连接类
        logging.info('初始化读取数据库连接类！')
        self.OracleOP_206=DataOP.OracleOP('rm','riskmanager','172.16.100.206:1521/riskcopy')
        self.OracleOP_167=DataOP.OracleOP('tuser','p@ssw0rd','172.16.100.167:1522/fmdbdg')
        self.MSSQLOP_7_NWIND=DataOP.MSSQLOP('riskteam','riskteam','172.16.100.7','NWindDB')

        #初始化存储数据库连接类
        logging.info('初始化读取数据库连接类！')

        self.OracleOP_205 = DataOP.OracleOP('limit', 'limit2018', '172.16.100.205:1521/RISKMANAGER')
        logging.info('初始化数据库连接类完成！')

    #中文字符编码的特殊处理
    def _convert_to_chinese(self,str):
        if str==None:
            return str
        else:
            return str.encode('latin-1').decode('gbk')

    #处理定期存款相关信息，通过名字简称获取
    def _get_detail_from_shortname(self,short_name):
        data=self.MSSQLOP_7_NWIND.read_data(GetDetailFromShortnameSQL%short_name)
        return data

    #获取债券相关信息，通过债券名称获取
    def _get_detail_from_bondbname(self,bond_name):
        data = self.MSSQLOP_7_NWIND.read_data(GetDetailFromBondnameSQL%bond_name)
        return data

    #过去聚源评级信息
    def _get_jy_rank(self,comp):
        data = self.OracleOP_167.read_data(GetJYRankSQL%comp)
        return data

    #获取基金的估值表持仓信息
    def _get_detail_from_fundcode(self):
        data = self.OracleOP_206.read_data(GetFundDetailSQL % (self.fdate,self.fundcode))
        return data

    #获取上一个交易日的时间
    def _get_yesterday(self):
        today = datetime.datetime.now().strftime("%Y%m%d")
        data = self.MSSQLOP_7_NWIND.read_data(GetTradeDaySQL%today)
        fdate=datetime.datetime.strptime(data[0][0], "%Y%m%d").strftime("%Y-%m-%d")
        return fdate



    #获取估值表并解析，关联类别、评级信息
    def GetHoldingDetail(self):
        #处理时间信息
        if self.fdate==None:
            self.fdate=self._get_yesterday()
        logging.info('处理的估值表信息时间为%s'%self.fdate)

        logging.info('获取估值表信息！')
        data=self._get_detail_from_fundcode()
        column=['FUNDCODE','FDATE','TYPE','FKMBM','FKMMC','FZQCB','FCB_JZ_BL']
        df=pd.DataFrame(data,columns=column)
        #字符串转成数字
        df['FCB_JZ_BL']=df['FCB_JZ_BL'].str.strip("%").astype(float)/100
        logging.info('估值表信息获取成功！')

        #分类逐条进行处理
        logging.info('逐条获取类别，持有人，评级等信息！')
        for row_index,row in df.iterrows():
            if row['TYPE']=='银行定期存款':
                comp=row['FKMMC'].split('银行')[0]+'银行'
                data_wind=self._get_detail_from_shortname(comp)
                df.loc[row_index, 'S_INFO_WINDCODE'] = np.nan
                df.loc[row_index,'B_INFO_ISSUER']=self._convert_to_chinese(data_wind[0][0])
                df.loc[row_index,'S_INFO_COMPCODE']=data_wind[0][1]
                df.loc[row_index,'S_INFO_INDUSTRYNAME']=np.nan
                df.loc[row_index,'S_INFO_INDUSTRYNAME2']=np.nan
                df.loc[row_index,'TOT_SHRHLDR_EQY_INCL_MIN_INT']=data_wind[0][2]
                df.loc[row_index,'REPORT_PERIOD']=data_wind[0][3]

                #获取聚源评级
                data_jy=self._get_jy_rank(df.loc[row_index,'B_INFO_ISSUER'])
                if data_jy!=[]:
                    df.loc[row_index,'rank_1year']=data_jy[0][1]
                    df.loc[row_index,'rank_3year']=data_jy[0][2]
                
                
            elif row['TYPE']=='债券':
                bond_name=row['FKMMC'].split('(总价)')[0]
                data=self._get_detail_from_bondbname(bond_name)
                df.loc[row_index,'S_INFO_WINDCODE']=data[0][0]
                df.loc[row_index,'B_INFO_ISSUER']=self._convert_to_chinese(data[0][1])
                df.loc[row_index,'S_INFO_COMPCODE']=data[0][2]
                df.loc[row_index,'S_INFO_INDUSTRYNAME']=self._convert_to_chinese(data[0][3])
                df.loc[row_index,'S_INFO_INDUSTRYNAME2']=self._convert_to_chinese(data[0][4])
                df.loc[row_index,'TOT_SHRHLDR_EQY_INCL_MIN_INT']=data[0][5]
                df.loc[row_index,'REPORT_PERIOD']=data[0][6]

                #获取聚源评级
                data_jy=self._get_jy_rank(df.loc[row_index,'B_INFO_ISSUER'])
                if data_jy!=[]:
                    df.loc[row_index,'rank_1year']=data_jy[0][1]
                    df.loc[row_index,'rank_3year']=data_jy[0][2]
            else:
                df.loc[row_index,'S_INFO_WINDCODE']=np.nan
                df.loc[row_index,'B_INFO_ISSUER']=np.nan
                df.loc[row_index,'S_INFO_COMPCODE']=np.nan
                df.loc[row_index,'S_INFO_INDUSTRYNAME']=np.nan
                df.loc[row_index,'S_INFO_INDUSTRYNAME2']=np.nan
                df.loc[row_index,'TOT_SHRHLDR_EQY_INCL_MIN_INT']=np.nan
                df.loc[row_index,'REPORT_PERIOD']=np.nan
                df.loc[row_index, 'rank_1year'] = np.nan
                df.loc[row_index, 'rank_3year'] = np.nan

        df['TOT_SHRHLDR_EQY_INCL_MIN_INT'] = pd.to_numeric(df['TOT_SHRHLDR_EQY_INCL_MIN_INT'])
        df['FZQCB'] = pd.to_numeric(df['FZQCB'])
        df['FCB_JZ_BL'] = pd.to_numeric(df['FCB_JZ_BL'])
        #是否SCP
        logging.info('判断是否为SCP！')
        df['IS_SCP']=df['FKMMC'].str.contains('SCP', regex=False)

        #处理最终评级
        logging.info('合成最终的评级！')
        df['Rank']=df[['rank_1year','rank_3year','IS_SCP']].apply(lambda x:  x['rank_3year'] if x['IS_SCP']else x['rank_1year'],axis=1)

        #是否利率债
        logging.info('判断是否为利率债！')
        if df[df['TYPE']=='债券'].shape[0]!=0:
            df['IS_Rate_Bond']=np.logical_or(np.logical_or(df['S_INFO_INDUSTRYNAME']=='国债',df['S_INFO_INDUSTRYNAME']=='央行票据'),np.logical_and(df['S_INFO_INDUSTRYNAME']=='金融债',df['S_INFO_INDUSTRYNAME2']=='政策银行债'))
        else:
            df['IS_Rate_Bond']=False

        #错误数据特殊处理
        logging.info('错误数据特殊处理！')

        logging.info('%s 基金% s信息处理完成！'%(self.fundcode,self.fdate))
        logging.info('-'*100)

        #控制特殊处理
        df = df.fillna(np.nan)
        df = df.replace([np.nan],[None])
        #生成的估值明细数据赋给类

        self.holdiingdetail=df

        return df

    def SaveHoindingDetail(self):
        #估值存入数据库逻辑
        logging.info("存入数据库！")
        self.OracleOP_205.delete_data(DeleteHoldingDetailSQL%(self.fdate,self.fundcode))

        self.OracleOP_205.insert_data(InsertHoldingDetailSQL, self.holdiingdetail.values.tolist())

        logging.info("存入数据库完成！")
        logging.info('=' * 100)
    def CloseConnect(self):
        self.OracleOP_206.close_conn()
        self.OracleOP_167.close_conn()
        self.MSSQLOP_7_NWIND.close_conn()
        self.OracleOP_205.close_conn()





#test

#初始化日志设置
GlobalSetting.logging_setting()

#dict={'众赢':'001234','金腾通':'000540','鑫盈':'000439','及第7天':'003002'}

dict={'及第7天':'003002'}
for value in dict.values():
    Ho=HoldingDetail(value,'2018-08-21') #可选参数 fdate 格式 ’2018-07-17‘
    dd=Ho.GetHoldingDetail()
    Ho.SaveHoindingDetail()
    Ho.CloseConnect()

