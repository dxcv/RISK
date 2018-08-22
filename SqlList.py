GetFundDetailSQL="""
SELECT 
FUNDCODE,
FDATE, 
case when SUBSTR(FKMBM,0,6) = '100202' then '银行定期存款'
    when SUBSTR(FKMBM,0,4) = '1103' then '债券'
    when SUBSTR(FKMBM,0,4) = '1202' then '逆回购' end as TYPE,
FKMBM,FKMMC,FZQCB,FCB_JZ_BL
FROM AZ.RISK_GFA_JJHZGZB
where 
((SUBSTR(FKMBM,0,6) = '100202' and length(FKMBM) = 14) or (SUBSTR(FKMBM,0,4) = '1103' and length(FKMBM) = 14 and SUBSTR(FKMBM,7,2) = '01') or (SUBSTR(FKMBM,0,4) = '1202' and length(FKMBM) = 14))
and FDATE =to_date('%s','yyyy-mm-dd hh24:mi:ss')  and   FUNDCODE in ('%s') 
order by type 
"""

GetDetailFromShortnameSQL="""
select 
    S_INFO_COMPNAME,
    S_INFO_COMPCODE,
    TOT_SHRHLDR_EQY_INCL_MIN_INT,
    REPORT_PERIOD
from 
(
    select
    A.*,
    BAL.TOT_SHRHLDR_EQY_INCL_MIN_INT,
    BAL.REPORT_PERIOD,
    ROW_NUMBER() OVER(order by BAL.ANN_DT desc) num
    from
    (
        select top 1 S_INFO_COMPNAME,s_info_compcode from  CBondIssuer where S_INFO_COMPNAME like '%%%s%%'
    )A 
    LEFT JOIN 
    (
        select 
            *
        from CBONDBALANCESHEET  
        where REPORT_PERIOD>='20150331' and STATEMENT_TYPE='408001000' 
    )BAL
    ON BAL.S_INFO_COMPCODE= A.S_INFO_COMPCODE
)B
WHERE B.NUM=1
"""


GetDetailFromBondnameSQL="""
select 
    S_INFO_WINDCODE,
    B_INFO_ISSUER,
    S_INFO_COMPCODE,
    S_INFO_INDUSTRYNAME,
    S_INFO_INDUSTRYNAME2,
    TOT_SHRHLDR_EQY_INCL_MIN_INT,
    REPORT_PERIOD

FROM 
(
    SELECT 
    A.*,
    BAL.TOT_SHRHLDR_EQY_INCL_MIN_INT,
    BAL.REPORT_PERIOD,
    ROW_NUMBER() OVER(order by BAL.ANN_DT desc) num
    FROM
    (
        select  
            DES.S_INFO_WINDCODE,
            DES.B_INFO_ISSUER,
            ISS.S_INFO_COMPCODE,
            IND.S_INFO_INDUSTRYNAME,
            IND.S_INFO_INDUSTRYNAME2
        from 
            CBONDDESCRIPTION DES 
        LEFT JOIN 
            CBondIndustryWind IND
        ON DES.S_INFO_WINDCODE=IND.S_INFO_WINDCODE
        LEFT JOIN 
            CBondIssuer ISS
        ON DES.S_INFO_WINDCODE=ISS.s_info_windcode
        where DES.S_INFO_NAME='%s' 
    )A
    LEFT JOIN 
    (
        select 
            *
        from 
        CBONDBALANCESHEET  where REPORT_PERIOD>='20150331' and STATEMENT_TYPE='408001000' 
    )BAL
    ON BAL.S_INFO_COMPCODE= A.S_INFO_COMPCODE 
)B
WHERE B.NUM=1
"""
GetJYRankSQL="""
SELECT
    TIS.VC_FULL_NAME,
    TDICT1.vc_item_name as rank_1year,
    TDICT2.vc_item_name as rank_3year
FROM
    trade.tissuer TIS
    LEFT JOIN ( SELECT c_lemma_item, vc_item_name FROM trade.tdictionary WHERE l_dictionary_no = '110000' ) TDICT1 ON TDICT1.c_lemma_item = TIS.c_outer_appraise1
    LEFT JOIN ( SELECT c_lemma_item, vc_item_name FROM trade.tdictionary WHERE l_dictionary_no = '110000' ) TDICT2 ON TDICT2.c_lemma_item = TIS.c_appraise 
WHERE
    TIS.VC_FULL_NAME like  '%s' and TIS.c_outer_appraise1 is not null
    """


GetTradeDaySQL="""
select max(TRADE_DAYS) from ASHARECALENDAR where
           S_INFO_EXCHMARKET = 'SZSE' and TRADE_DAYS<'%s'
"""

DeleteHoldingDetailSQL="""
delete from HoldingDetail where to_char(fdate,'yyyy-mm-dd')='%s' and fundcode='%s'
"""

InsertHoldingDetailSQL="""
insert into HoldingDetail values(:1, :2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:6,:17,:18,:19)
"""


InsertHoldingDetailSQL1="""
insert into HoldingDetail1 values(:1, :2, :3)
"""

