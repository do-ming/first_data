##### 패키지 불러오기 ####
# 불러오기
library(lightgbm)
library(DBI)
library(RJDBC)
library(data.table)
library(plyr)
library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)
library(MASS)
library(parallel)
library(doSNOW)
library(e1071)
library(caret)
library(gbm)
library(doParallel)
library(kernlab)
library(useful)
library(magrittr)
library(dygraphs)
library(DiagrammeR)

###### 패키지 #####
print("패키지 호출 완료")

# source(file = "/home/jovyan/DB_con/DB_conn.R")
# source(file = "/home/dio/DB_con/DB_conn.R")
# drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc6.jar", " ")
drv <- JDBC("oracle.jdbc.OracleDriver", classPath="/lib/ojdbc5.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")

print("DB접속 완료")

#----------------------------------------------------------------
# 1. 데이터 호출 
#----------------------------------------------------------------

gc(T);gc(T);

# 에러텀 계산필요시 YES, 미래예측값만 뿌릴시 NO 
yr_df  <- data.frame(YEAR = seq.Date(ymd("2011-01-01"), ymd(paste0(substr(as.character(Sys.time()), 1, 7), "-01")), by = "year"  ))
mon_df <- data.frame(MON = seq.Date(ymd("2011-01-01"), ymd(paste0(substr(as.character(Sys.time()), 1, 7), "-01")), by = "month"  ))
LAST_mon <- last(mon_df$MON) - 1
STRT_mon <- yr_df$YEAR[nrow(yr_df) -  4]

(STRT_DT <- gsub("-", "", substr(as.character(Sys.time()), 1, 10)))
(LAST_DT <- gsub("-", "", ymd(substr(as.character(Sys.time()), 1, 10)) + 10))

# 모델 RUN 날짜
# (SYSDATE <- as.character(Sys.time()) )
(SYSDATE <- as.character(paste0(ymd(STRT_DT), " 03:00:00") ))

print(STRT_DT); print(LAST_DT); print(SYSDATE)

#
print("data1")
SCM_BRD_df_bs <- dbGetQuery(conn, paste0(" SELECT
                                         PGM_ID 
                                         ,ITEM_CD
                                         ,BROAD_DT
                                         ,DLVS_CO_BRN_NM
                                         ,SUM(TOT_ORD_QTY) TOT_ORD_QTY
                                         
                                         FROM(
                                         SELECT /*+ FULL(A) FULL(B) FULL(C) PARALLEL (A 4) */
                                         A.PGM_ID 
                                         -- A.PRD_CD, 
                                         ,B.ITEM_CD
                                         ,A.BROAD_DT
                                         ,DLVS_CO_BRN_NM
                                         ,A.TOT_ORD_QTY
                                         , CASE WHEN A.REP_PAY_MEAN_CD='01' AND A.DPO_CNF_DTM BETWEEN C.MIN_Q_START_DATE AND C.MAX_Q_END_DATE THEN '1'
                                         WHEN A.REP_PAY_MEAN_CD='01' THEN '2' ELSE '3' END AS CUST_DV
                                         
                                         FROM GSBI_OWN.F_ORD_ORD_D A
                                         INNER JOIN D_PRD_PRD_M B ON A.PRD_CD = B.PRD_CD
                                         INNER JOIN (SELECT /*+ FULL(A) */ BROAD_DT,PGM_ID,ITEM_CD,MIN_Q_START_DATE,MAX_Q_END_DATE
                                         FROM DTA_OWN.GSTS_PGM_ITEM_ORD A) C
                                         ON A.BROAD_DT = C.BROAD_DT AND A.PGM_ID = C.PGM_ID AND B.ITEM_CD = C.ITEM_CD
                                         
                                         WHERE BROAD_ORD_TIME_DTL_CD = 'O1' 
                                         AND EXCHRG_DLVS ='전담배송'
                                         AND DLVS_CO_BRN_NM IS NOT NULL AND (DLVS_CO_BRN_NM LIKE '%구로%' OR 
                                         DLVS_CO_BRN_NM LIKE '%인천%' OR
                                         DLVS_CO_BRN_NM LIKE '%강북%' OR
                                         DLVS_CO_BRN_NM LIKE '%강동%' OR
                                         DLVS_CO_BRN_NM LIKE '%김포%' OR
                                         DLVS_CO_BRN_NM LIKE '%분당%' OR
                                         DLVS_CO_BRN_NM LIKE '%군포%' OR
                                         DLVS_CO_BRN_NM LIKE '%고양%' OR
                                         DLVS_CO_BRN_NM LIKE '%강남%' OR
                                         DLVS_CO_BRN_NM LIKE '%안산%' OR
                                         DLVS_CO_BRN_NM LIKE '%중앙%')
                                         AND TOT_ORD_QTY > 0 
                                         AND A.BROAD_DT BETWEEN '20160101' AND '" , STRT_DT ,"') K
                                         
                                         WHERE K.CUST_DV IN ('1','3')
                                         GROUP BY 
                                         PGM_ID 
                                         ,ITEM_CD
                                         ,BROAD_DT
                                         ,DLVS_CO_BRN_NM "))

########################

print("data2")
CALL_BRD_df_bs <- dbGetQuery(conn, paste0("SELECT  ITEM.ITEM_NM
                                          , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1) AS BRAND
                                          , PRD_GRP.PRD_GRP_NM
                                          , A.*
                                          , DT.WEKDY_NM
                                          , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일
                                          , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)
                                          , B.WEIHT_MI
                                          
                                          FROM DTA_OWN.GSTS_PGM_ITEM_ORD A   /* PGM,ITEM별 주문*/
                                          inner join GSBI_OWN.D_PRD_ITEM_M ITEM on A.ITEM_CD = ITEM.ITEM_CD
                                          inner join DHUB_OWN.DH_DT DT on A.BROAD_DT = DT.DT_CD  
                                          inner join GSBI_OWN.V_CMM_PRD_GRP_C PRD_GRP on A.PRD_GRP_CD = PRD_GRP.PRD_GRP_CD
                                          left outer join (
                                          select /*+ full(a) full(b) parallel(4) */
                                          a.pgm_id
                                          ,b.item_cd
                                          ,sum(a.PLAN_WEIHT_SE) as WEIHT_MI
                                          from d_brd_broad_form_prd_m a
                                          inner join d_prd_prd_m b
                                          on a.prd_cd = b.prd_cd
                                          group by a.pgm_id, b.item_cd
                                          ) b on a.pgm_id = b.pgm_id and a.item_cd = b.item_cd 
                                          
                                          WHERE A.BROAD_DT < '" , STRT_DT,"' AND b.pgm_id IS NOT NULL"))

CALL_BRD_df_bs <- merge(SCM_BRD_df_bs,CALL_BRD_df_bs,by = c("PGM_ID","ITEM_CD","BROAD_DT"))

# ###### 편성 데이터 예측에 활용 #####
# CALL_SCH_df <- dbGetQuery(conn, paste0("SELECT * FROM DTA_OWN.PRE_SCHDL_TB WHERE ETL_DATE = '",STRT_DT,"'  "  ))

print("data3")
# 가중분
CALL_SCH_df_w <- dbGetQuery(conn, paste0("SELECT BB.PGM_ID, ITEM_CD, ROUND(SUM(PLAN_WEIHT_SE/60),0) AS WEIHT_MI
                                         FROM d_brd_broad_form_prd_m AA
                                         INNER JOIN d_brd_broad_form_M BB ON AA.PGM_ID = BB.PGM_ID 
                                         WHERE ITEM_CD >0 AND BB.BROAD_DT BETWEEN '",STRT_DT,"' AND '",LAST_DT,"'
                                         GROUP BY BB.PGM_ID,ITEM_CD"))

print("data4")
#방송 편성
CALL_SCH_df <- dbGetQuery(conn, paste0("
                                       SELECT A.PGM_ID
                                       , A.PGM_NM, A.TITLE_ID
                                       , A.TITLE_NM
                                       , (CASE WHEN A.TITLE_NM IN ('B special', '최은경의 W', '테이스티 샵') THEN '브랜드PGM' ELSE A.PGM_GBN END) AS PGM_GBN
                                       , (CASE WHEN A.TITLE_NM IN ('B special', '최은경의 W', '테이스티 샵') THEN A.TITLE_NM ELSE A.BRAND_PGM_NM END) AS BRAND_PGM_NM
                                       , A.BROAD_DT 
                                       , A.BROAD_STR_DTM
                                       , A.BROAD_END_DTM
                                       , (CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_STR_DTM ELSE LAG(A.BROAD_IT_STR_DTM, 1, A.BROAD_STR_DTM) OVER (
                                       PARTITION BY A.PGM_ID, A.BROAD_STR_DTM 
                                       ORDER BY A.BROAD_STR_DTM, A.PRD_FORM_SEQ) END + 1/60/24/60 )                          AS MIN_Q_START_DATE
                                       , CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_END_DTM 
                                       WHEN A.BROAD_END_DTM < A.BROAD_IT_STR_DTM THEN A.BROAD_END_DTM ELSE A.BROAD_IT_STR_DTM END AS MAX_Q_END_DATE
                                       --, A.BROAD_IT_STR_DTM
                                       , A.PRD_GRP_NM
                                       , A.ITEM_CD
                                       , A.BRAND
                                       , A.ITEM_NM
                                       , A.WEKDY_NM
                                       , A.WEKDY_YN --1:주말 /  0:평일
                                       , A.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)  
                                       , A.CN_RS_YN
                                       , A.CNT_PRDCD
                                       , A.PRD_FORM_SEQ
                                       , A.SORT_SEQ
                                       , ROUND(CASE WHEN A.RUN_TIME <> 0 THEN A.RUN_TIME ELSE ( (CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_END_DTM 
                                       WHEN A.BROAD_END_DTM < A.BROAD_IT_STR_DTM THEN A.BROAD_END_DTM ELSE A.BROAD_IT_STR_DTM END) - 
                                       (CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_STR_DTM ELSE LAG(A.BROAD_IT_STR_DTM, 1, A.BROAD_STR_DTM) OVER (
                                       PARTITION BY A.PGM_ID, A.BROAD_STR_DTM 
                                       ORDER BY A.BROAD_STR_DTM, A.PRD_FORM_SEQ) END + 1/60/24/60 )
                                       )*60*24 END) AS SUM_RUNTIME
                                       , A.FST_BROAD_DTM  --메인상품코드 기준의 최초방송일자
                                       , A.AVG_PRICE
                                       , A.MIN_PRICE
                                       , A.MAX_PRICE
                                       , A.ITEM_CNT
                                       
                                       FROM 
                                       (
                                       SELECT A.* 
                                       , (CASE WHEN (COUNT(A.ITEM_CD) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM)) <> 1 THEN 
                                       A.BROAD_STR_DTM + ( SUM(A.RUN_TIME) OVER(
                                       PARTITION BY A.PGM_ID,A.BROAD_STR_DTM 
                                       ORDER BY A.PGM_ID,A.BROAD_STR_DTM,A.PRD_FORM_SEQ 
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))/60/24 ELSE A.BROAD_STR_DTM END) AS BROAD_IT_STR_DTM
                                       , COUNT(A.ITEM_CD) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS ITEM_CNT
                                       FROM (
                                       SELECT A.PGM_ID
                                       , A.PGM_NM, PGM.TITLE_ID, PGM.TITLE_NM
                                       , PGM.PGM_GBN 
                                       , TO_CHAR(NVL((CASE WHEN PGM.TITLE_NM LIKE '%B special%'   THEN 'B special'
                                       WHEN PGM.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
                                       WHEN PGM.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
                                       WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
                                       ELSE '일반'
                                       END), NULL)) AS BRAND_PGM_NM
                                       , A.BROAD_DT 
                                       , A.BROAD_STR_DTM
                                       , A.BROAD_END_DTM
                                       , B.PRD_GRP_NM
                                       , ITEM.ITEM_CD
                                       , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1) AS BRAND
                                       , ITEM.ITEM_NM
                                       
                                       , DT.WEKDY_NM
                                       , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일
                                       , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)  
                                       , CASE WHEN RSV.PRD_CD IS NOT NULL THEN 'Y' ELSE 'N' END AS CN_RS_YN
                                       
                                       
                                       , MIN(A.PRD_FORM_SEQ) AS PRD_FORM_SEQ
                                       , MIN(A.SORT_SEQ)     AS SORT_SEQ
                                       , SUM(A.EXPCT_REQUIR_TIME_SE) AS RUN_TIME
                                       , COUNT(DISTINCT A.PRD_CD)    AS CNT_PRDCD
                                       
                                       , MIN(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(FST2.FST_BROAD_DT, FST1.FST_BROAD_DT) END) AS FST_BROAD_DTM  --메인상품코드 기준의 최초방송일자
                                       , AVG(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(A.BROAD_SALE_PRC,PRD.PRD_SALE_PRC) END) AS AVG_PRICE
                                       , MIN(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(A.BROAD_SALE_PRC,PRD.PRD_SALE_PRC) END) AS MIN_PRICE
                                       , MAX(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(A.BROAD_SALE_PRC,PRD.PRD_SALE_PRC) END) AS MAX_PRICE
                                       
                                       
                                       FROM (
                                       SELECT A.PGM_ID
                                       , A.PGM_NM 
                                       , A.CHANL_CD
                                       , A.MNFC_GBN_CD
                                       , A.BROAD_DT
                                       , A.BROAD_STR_TIME
                                       , A.BROAD_TIMERNG_CD
                                       , B.EXPCT_REQUIR_TIME_SE 
                                       --, C.RL_EXPOS_MI/60 AS EXPCT_REQUIR_TIME_SE  -- ROUND(SUM(RL_EXPOS_MI)/60)
                                       , A.WEKDY_CD
                                       , A.BROAD_STR_DTM
                                       , A.BROAD_END_DTM
                                       , A.PGM_GBN_CD
                                       , B.BROAD_SALE_PRC 
                                       , B.BROAD_YN
                                       , B.ITEM_SEQ  
                                       
                                       , B.PRD_FORM_SEQ
                                       , B.SORT_SEQ
                                       
                                       , B.PRD_CD
                                       , B.MAIN_PRD_YN
                                       
                                       --, A.REG_DTM
                                       --, A.MOD_DTM
                                       FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
                                       , GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B
                                       --, GSBI_OWN.F_ORD_BROAD_TGT_D C
                                       WHERE A.BROAD_DT BETWEEN '",STRT_DT,"' AND '",LAST_DT,"'
                                       AND A.PGM_ID = B.PGM_ID
                                       AND A.USE_YN = 'Y' 
                                       AND A.CHANL_CD = 'C'
                                       AND B.USE_YN = 'Y'
                                       AND A.MNFC_GBN_CD = '1'
                                       --AND B.BROAD_YN = 'Y' 
                                       --AND B.PRD_CD = C.PRD_CD AND A.PGM_ID = C.PGM_ID
                                       --ORDER BY A.BROAD_STR_DTM, ITEM.ITEM_CD, B.SORT_SEQ
                                       ) A
                                       LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON A.PRD_CD = PRD.PRD_CD  
                                       LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C B ON PRD.ENTPR_PRD_GRP_CD = B.PRD_GRP_CD 
                                       LEFT JOIN DHUB_OWN.CMM_PGM_M PGM ON A.PGM_ID = PGM.PGM_ID
                                       LEFT JOIN GSBI_OWN.D_PRD_ITEM_M ITEM ON PRD.ITEM_CD = ITEM.ITEM_CD 
                                       LEFT JOIN DHUB_OWN.DH_DT DT ON  TO_CHAR(A.BROAD_STR_DTM,'YYYYMMDD') = DT.DT_CD  
                                       LEFT JOIN 
                                       ( /*최초방송일자*/
                                       SELECT  B.PRD_CD, MIN(BROAD_DT) FST_BROAD_DT
                                       FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
                                       ,GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B             
                                       WHERE A.BROAD_DT >= '201601'  AND A.PGM_ID = B.PGM_ID -- 2016년1월~ 현재까지 (BROAD_DT 대신, 실제방송일자를 넣어서 방송한 것까지만 최초방송일자 가져오기)
                                       AND A.USE_YN = 'Y'
                                       AND A.CHANL_CD = 'C'
                                       AND A.MNFC_GBN_CD = '1'
                                       AND B.USE_YN = 'Y'
                                       GROUP BY B.PRD_CD
                                       ) FST1 ON A.PRD_CD = FST1.PRD_CD
                                       LEFT JOIN 
                                       (
                                       SELECT PRD_CD, FST_BROAD_DT FROM GSBI_OWN.D_PRD_PRD_M WHERE FST_BROAD_DT IS NOT NULL
                                       ) FST2 ON A.PRD_CD = FST2.PRD_CD 
                                       LEFT JOIN
                                       (/*상담예약 상품*/
                                       SELECT PRD_CD
                                       FROM
                                       (
                                       SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_ADDPRC_D GROUP BY PRD_CD--핸드폰
                                       UNION 
                                       SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_RSRV_D GROUP BY PRD_CD-- 렌탈, 여행, 가구(시공), 교육문화(일부) 
                                       UNION
                                       SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_INSU_D GROUP BY PRD_CD-- 보험
                                       )
                                       ) RSV ON A.PRD_CD = RSV.PRD_CD
                                       
                                       GROUP BY A.PGM_ID
                                       , A.PGM_NM, PGM.TITLE_ID, PGM.TITLE_NM
                                       , PGM.PGM_GBN 
                                       , TO_CHAR(NVL((CASE WHEN PGM.TITLE_NM LIKE '%B special%'   THEN 'B special'
                                       WHEN PGM.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
                                       WHEN PGM.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
                                       WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
                                       ELSE '일반'
                                       END), NULL))
                                       , A.BROAD_DT 
                                       , A.BROAD_STR_DTM
                                       , A.BROAD_END_DTM
                                       , B.PRD_GRP_NM
                                       , ITEM.ITEM_CD
                                       , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1)
                                       , ITEM.ITEM_NM
                                       , DT.WEKDY_NM
                                       , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END)  --1:주말 /  0:평일
                                       , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)
                                       , CASE WHEN RSV.PRD_CD IS NOT NULL THEN 'Y' ELSE 'N' END
                                       
                                       
                                       UNION ALL  
                                       
                                       
                                       --PGM_ID	PGM_NM	TITLE_ID	TITLE_NM	PGM_GBN	BRAND_PGM_NM	BROAD_DT	BROAD_STR_DTM	BROAD_END_DTM
                                       --PRD_GRP_NM	ITEM_CD	BRAND	ITEM_NM	WEKDY_NM	WEKDY_YN	HOLDY_YN	CN_RS_YN	PRD_FORM_SEQ	SORT_SEQ	RUN_TIME
                                       --CNT_PRDCD	FST_BROAD_DTM	AVG_PRICE	MIN_PRICE	MAX_PRICE
                                       
                                       
                                       
                                       
                                       SELECT A.PGM_ID
                                       , 'NOT' AS PGM_NM 
                                       , A.TITLE_ID, A.TITLE_NM
                                       , (CASE WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') 
                                       AND A.TITLE_NM IN ('SHOW me the Trend', 'The Collection',
                                       '똑소리살림법', '리얼뷰티쇼',
                                       '왕영은의 톡톡톡', 'B special', '최은경의 W', '테이스티 샵')                   THEN '브랜드PGM' 
                                       WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') AND A.TITLE_NM = '순환방송(재방송)' THEN '순환방송' 
                                       ELSE '일반PGM' END)  AS PGM_GBN 
                                       
                                       , TO_CHAR(NVL((CASE WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%B special%'   THEN 'B special'
                                       WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
                                       WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
                                       WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
                                       ELSE '일반'
                                       END), NULL)) AS BRAND_PGM_NM
                                       , A.BROAD_DT 
                                       , A.BROAD_STR_DTM
                                       , A.BROAD_END_DTM
                                       , A.PRD_GRP_NM AS PRD_GRP_NM
                                       , A.ITEM_CD
                                       , SUBSTR(REGEXP_SUBSTR(A.ITEM_NM,'[^:]+',1,1),1) AS BRAND
                                       , A.ITEM_NM
                                       , DT.WEKDY_NM
                                       , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일 
                                       , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)
                                       , CASE WHEN RSV_ITM.ITEM_CD IS NOT NULL THEN 'Y' ELSE 'N' END AS CN_RS_YN
                                       , A.ITEM_SEQ  AS PRD_FORM_SEQ
                                       , A.ITEM_SEQ  AS SORT_SEQ
                                       , A.EXPCT_REQUIR_TIME_SE AS RUN_TIME
                                       , 1 AS CNT_PRDCD
                                       , NULL AS FST_BROAD_DTM
                                       , NULL AS AVG_PRICE
                                       , NULL AS MIN_PRICE
                                       , NULL AS MAX_PRICE
                                       
                                       FROM (
                                       
                                       -- 선편성 아이템 
                                       SELECT A.PGM_ID 
                                       , A.TITLE_ID
                                       , NVL((CASE WHEN A.TITLE_NAME LIKE '%B special%'   THEN 'B special'
                                       WHEN A.TITLE_NAME LIKE '%최은경의 W%'  THEN '최은경의 W'
                                       WHEN A.TITLE_NAME LIKE '%테이스티 샵%' THEN '테이스티 샵'
                                       ELSE A.TITLE_NAME
                                       END), NULL) AS TITLE_NM
                                       , A.BROAD_DT
                                       , A.BROAD_STR_DTM
                                       , A.BROAD_END_DTM
                                       , A.PRD_GRP_NM
                                       , A.ITEM_CD
                                       , A.ITEM_NM
                                       , A.ITEM_SEQ 
                                       , A.EXPCT_REQUIR_TIME_SE
                                       FROM
                                       (
                                       SELECT A.* 
                                       , COUNT(A.ITEM_CD) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS ITEM_CNT
                                       , SUM(A.EXPCT_REQUIR_TIME_SE) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS TOT_RUN_TIME
                                       , MAX(A.MODIFY_DATE) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS MAX_MODIFY_DATE
                                       , ROUND(((BROAD_END_DTM) - (BROAD_STR_DTM))*60*24)  AS TIME_GAP
                                       FROM (
                                       SELECT A.PGM_ID 
                                       , A.TITLE_ID
                                       , A.TITLE_NAME
                                       , TO_CHAR(A.BROAD_DATE, 'YYYYMMDD') AS BROAD_DT
                                       , A.BROAD_DATE AS BROAD_STR_DTM
                                       , A.CLOSING_DATE AS BROAD_END_DTM
                                       , PRDGRP.PRD_GRP_NM
                                       , B.ITEM_CODE AS ITEM_CD
                                       , B.ITEM_NAME AS ITEM_NM
                                       , B.ITEM_SEQ 
                                       , B.HOPE_RUN_TIME AS EXPCT_REQUIR_TIME_SE
                                       
                                       , B.MODIFY_DATE -- 수정일 
                                       FROM  SDHUB_OWN.STG_TB_BY100 A
                                       , SDHUB_OWN.STG_TB_BY212 B
                                       , SDHUB_OWN.STG_PRD_ITEM_M C
                                       , GSBI_OWN.V_CMM_PRD_GRP_C PRDGRP
                                       WHERE A.PGM_ID = B.PGM_ID
                                       AND A.BROAD_DATE NOT IN (
                                       SELECT DISTINCT A.BROAD_STR_DTM
                                       FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
                                       , GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B
                                       WHERE A.BROAD_DT BETWEEN '",STRT_DT,"' AND '",LAST_DT,"'
                                       AND A.PGM_ID = B.PGM_ID
                                       AND A.USE_YN = 'Y' 
                                       AND A.CHANL_CD = 'C'
                                       AND B.USE_YN = 'Y'
                                       AND A.MNFC_GBN_CD = '1')
                                       AND B.ITEM_CODE = C.ITEM_CD
                                       AND C.PRD_GRP_CD = PRDGRP.PRD_GRP_CD
                                       AND A.BROAD_DATE BETWEEN TO_DATE('",STRT_DT,"', 'YYYYMMDD') AND TO_DATE('",LAST_DT,"', 'YYYYMMDD') + 0.99999
                                       AND A.MEDIA = 'C'
                                       AND A.PROD_TYPE = '1'
                                       AND A.DELETE_YN = 'N'
                                       ORDER BY BROAD_STR_DTM, ITEM_SEQ
                                       ) A
                                       ) A
                                       WHERE A.MODIFY_DATE = A.MAX_MODIFY_DATE
                                       ) A
                                       --ORDER BY A.BROAD_DATE 
                                       
                                       -- LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C B ON PRD.ENTPR_PRD_GRP_CD = B.PRD_GRP_CD 
                                       LEFT JOIN DHUB_OWN.CMM_PGM_M PGM ON A.PGM_ID = PGM.PGM_ID
                                       LEFT JOIN DHUB_OWN.DH_DT DT ON  TO_CHAR(A.BROAD_STR_DTM,'YYYYMMDD') = DT.DT_CD  
                                       LEFT JOIN (SELECT DISTINCT PRD.ITEM_CD
                                       FROM
                                       (
                                       SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_ADDPRC_D GROUP BY PRD_CD--핸드폰
                                       UNION 
                                       SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_RSRV_D GROUP BY PRD_CD-- 렌탈, 여행, 가구(시공), 교육문화(일부) 
                                       UNION
                                       SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_INSU_D GROUP BY PRD_CD-- 보험
                                       ) A
                                       LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON A.PRD_CD = PRD.PRD_CD
                                       ) RSV_ITM ON A.ITEM_CD = RSV_ITM.ITEM_CD
                                       GROUP BY A.PGM_ID
                                       , 'NOT'
                                       , A.TITLE_ID, A.TITLE_NM
                                       , (CASE WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') 
                                       AND A.TITLE_NM IN ('SHOW me the Trend', 'The Collection',
                                       '똑소리살림법', '리얼뷰티쇼',
                                       '왕영은의 톡톡톡', 'B special', '최은경의 W', '테이스티 샵')                   THEN '브랜드PGM' 
                                       WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') AND A.TITLE_NM = '순환방송(재방송)' THEN '순환방송' 
                                       ELSE '일반PGM' END) 
                                       , TO_CHAR(NVL((CASE WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%B special%'   THEN 'B special'
                                       WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
                                       WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
                                       WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
                                       ELSE '일반'
                                       END), NULL))
                                       , A.BROAD_DT 
                                       , A.BROAD_STR_DTM
                                       , A.BROAD_END_DTM
                                       , A.PRD_GRP_NM
                                       , A.ITEM_CD
                                       , SUBSTR(REGEXP_SUBSTR(A.ITEM_NM,'[^:]+',1,1),1)
                                       , A.ITEM_NM
                                       , DT.WEKDY_NM
                                       , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END)  --1:주말 /  0:평일
                                       , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)
                                       , CASE WHEN RSV_ITM.ITEM_CD IS NOT NULL THEN 'Y' ELSE 'N' END 
                                       , A.ITEM_SEQ  
                                       , A.ITEM_SEQ  
                                       , A.EXPCT_REQUIR_TIME_SE 
                                       , 1 
                                       
                                       ) A
                                       ) A
                                       WHERE (ITEM_CNT = 1 OR RUN_TIME > 0 ) 
                                       ORDER BY BROAD_STR_DTM, MIN_Q_START_DATE, PRD_FORM_SEQ
                                       "))


CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$PRD_GRP_NM), ]
CALL_SCH_df$TITLE_ID <- ifelse(is.na(CALL_SCH_df$TITLE_ID), 0, CALL_SCH_df$TITLE_ID)
CALL_SCH_df$FST_BROAD_DTM <- ifelse(is.na(CALL_SCH_df$FST_BROAD_DTM), 99999999, CALL_SCH_df$FST_BROAD_DTM)
CALL_SCH_df$AVG_PRICE <- ifelse(is.na(CALL_SCH_df$AVG_PRICE), 0, CALL_SCH_df$AVG_PRICE)
CALL_SCH_df$MIN_PRICE <- ifelse(is.na(CALL_SCH_df$MIN_PRICE), 0, CALL_SCH_df$MIN_PRICE)
CALL_SCH_df$MAX_PRICE <- ifelse(is.na(CALL_SCH_df$MAX_PRICE), 0, CALL_SCH_df$MAX_PRICE)

CALL_SCH_df <- merge(CALL_SCH_df,CALL_SCH_df_w, by=c("PGM_ID","ITEM_CD"),all.x=T)
rm(CALL_SCH_df_w)

print("data그외")
###### 휴일데이터 활용 HOLDY_DT ###### 
HOLDY_DT <- dbGetQuery(conn, paste0("SELECT DISTINCT DT_CD 
                                    FROM DTA_OWN.CMM_STD_DT
                                    WHERE (HOLDY_NM LIKE '구정%' OR HOLDY_NM LIKE '추석%' OR HOLDY_NM LIKE '설날%')
                                    AND DT_CD >= '" , gsub("-", "", STRT_mon), "'
                                    ORDER BY DT_CD"))

FESTA_YN <- dbGetQuery(conn, paste0("SELECT DT_CD, FESTA_YN 
                                    FROM DTA_OWN.CMM_STD_DT
                                    WHERE DT_CD >= '" , gsub("-", "", STRT_mon), "'
                                    ORDER BY DT_CD"))


###### 휴일데이터 활용 #####

print("data_call_cmt_df")
###### 콜 상품평 CMT_df ######
CMT_df <- dbGetQuery(conn, paste0(" SELECT /*+ FULL(PRD) FULL(B) FULL(ITEM) */
                                  --A.PMO_NM,
                                  A.PMO_NO, A.PMO_SEQ, A.PMO_STR_DTM, A.PMO_END_DTM
                                  , A.PRD_CD, PRD.PRD_NM, PRD.ITEM_CD, ITEM_NM
                                  , PRD.SRCNG_GBN_CD AS SRCH
                                  , B.PRD_GRP_NM
                                  FROM (
                                  SELECT /*+ FULL(A) FULL(B) FULL(C) */
                                  A.PMO_SEQ, A.MOD_DTM, A.PMO_NO, A.CHANL_CD, A.CATV_YN, A.TC_YN, A.EC_YN, A.DM_YN, A.MC_YN, A.SNRM_YN, A.PMO_NM, A.PMO_STR_DTM,
                                  A.PMO_END_DTM, A.ST_CD, C.PRD_CD, B.DM_MULTI_CD
                                  FROM SDHUB_OWN.STG_MKT_PMO_M           A
                                  , SDHUB_OWN.STG_MKT_PMO_BNFT_D      B
                                  , SDHUB_OWN.STG_MKT_PMO_TGT_PRD_D   C 
                                  WHERE A.PMO_SEQ = B.PMO_SEQ AND A.PMO_SEQ = C.PMO_SEQ
                                  AND B.BNFT_GBN_CD IN ('10', '60')
                                  AND A.ST_CD >= '30'
                                  AND TO_CHAR(PMO_STR_DTM, 'YYYYMMDD') >= '20160701'
                                  AND PMO_NM LIKE '%상품평%'
                                  --AND A.PMO_SEQ = '121842'
                                  ) A 
                                  LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON A.PRD_CD = PRD.PRD_CD  
                                  LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C B ON PRD.ENTPR_PRD_GRP_CD = B.PRD_GRP_CD
                                  LEFT JOIN GSBI_OWN.D_PRD_ITEM_M ITEM ON PRD.ITEM_CD = ITEM.ITEM_CD 
                                  
                                  WHERE PRD.SRCNG_GBN_CD = 'CA' "))


RMSE <- function(m, o){ sqrt(mean((m - o)^2))}
MAPE <- function(m, o){ n <- length(m)
res <- (100 / n) * sum(abs(m-o)/m)
res  }


dbDisconnect(conn)
rm(conn,drv)

print("데이터 호출 완료")

range(CALL_BRD_df_bs$MIN_Q_START_DATE)
range(CALL_SCH_df$MIN_Q_START_DATE)


# 
A <- names(table(CALL_SCH_df$MIN_Q_START_DATE)[table(CALL_SCH_df$MIN_Q_START_DATE)>1])
if(length(A) > 0) {
  # tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
  which_ptb <- which(CALL_SCH_df$MIN_Q_START_DATE %in% A)
  diff_v <- diff(which_ptb)
  check_df <- c(which(diff_v != 1) - 1, length(diff_v)) + 1
  which_out <- which_ptb[-check_df]   
  
  if(length(which_out) == 0) {
    CALL_SCH_df <- CALL_SCH_df[-which_ptb[2], ]
  } else {
    CALL_SCH_df <- CALL_SCH_df[-which_out, ]   
  }
  
}

B <- names(table(CALL_SCH_df$MAX_Q_END_DATE)[table(CALL_SCH_df$MAX_Q_END_DATE)>1])
A ; B
if(length(B) > 0) {
  # tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
  which_ptb <- which(CALL_SCH_df$MAX_Q_END_DATE %in% B)
  diff_v <- diff(which_ptb)
  check_df <- c(which(diff_v != 1) - 1, length(diff_v)) + 1
  which_out <- which_ptb[-check_df]
  
  if(length(which_out) == 0) {
    CALL_SCH_df <- CALL_SCH_df[-which_ptb[2], ]
  } else {
    CALL_SCH_df <- CALL_SCH_df[-which_out, ]   
  }
}

CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$PRD_GRP_NM), ]
# 가중분 결측값 처리 어떻게 할까....
CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$WEIHT_MI), ] #40개 삭제
head(CALL_SCH_df)

#----------------------------------------------------------------
# 주 편성표 결과 저장 2~5 시 돌리는 경우만 
#---------------------------------------------

# end <- 1
# if (end == 1) { 


print(paste(" EDA 마트생성시작 ", as.character(Sys.time())))
#----------------------------------------------------------------
# 2. EDA 수행
#----------------------------------------------------------------
CALL_BRD_df_bs <- data.table(CALL_BRD_df_bs)

# 시간 생성
CALL_BRD_df_bs <- CALL_BRD_df_bs[, HH := as.numeric(BROAD_HH) ]
CALL_BRD_df_bs$HH <- ifelse(CALL_BRD_df_bs$HH == 0, 24, CALL_BRD_df_bs$HH)
CALL_BRD_df_bs <- na.omit(CALL_BRD_df_bs)

# 요일 생성
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , WEK := wday(ymd(BROAD_DT))]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , WEK := WEK - 1 ]
CALL_BRD_df_bs$WEK <- ifelse(CALL_BRD_df_bs$WEK == 0, 7, CALL_BRD_df_bs$WEK)
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , WEK_G := ifelse(WEK == 1, "1.월",
                                                    ifelse(WEK == 2, "2.화",
                                                           ifelse(WEK == 3, "3.수",
                                                                  ifelse(WEK == 4, "4.목",
                                                                         ifelse(WEK == 5, "5.금",
                                                                                ifelse(WEK == 6, "6.토","7.일")))))) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , WEK := ifelse(WEK %in% c(1:5), "1.주중",
                                                  ifelse(WEK == 6, "2.토","3.일")) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , mon := ymd(paste0(substr(BROAD_DT, 1, 6), "-01"))]


CALL_BRD_df_bs <- CALL_BRD_df_bs[!PRD_GRP_NM %in% c("렌탈","여행","핸드폰","교육문화","보험")]


#WEK = 1주중 , 2토, 3일
#WEK_G = 1~7 : 월~일



# 개월별 시간대별 
CALL_df <- CALL_BRD_df_bs[ , .(
  전달주문수량 = round(mean(TOT_ORD_QTY))),  #그 달에 주문 평균 수량
  by = c("DLVS_CO_BRN_NM","mon", "WEK", "HH")]

CALL_df <- CALL_df[order(mon, WEK, HH)]


# 개월별 시간대별 + 요일별
CALL_df2 <- CALL_BRD_df_bs[ , .(
  전달주문수량 = round(mean(TOT_ORD_QTY))),
  by = c("DLVS_CO_BRN_NM","mon", "WEK_G", "HH")]
CALL_df2 <- CALL_df2[order(mon, WEK_G, HH)]


# 수량 인입 예측
CALL_BRD_df_bs <- data.table(CALL_BRD_df_bs)
CALL_SCH_df <- data.table(CALL_SCH_df)

## test data 추가
# 전담사로 늘리기
CALL_SCH_df <- do.call(rbind,lapply(1:nrow(CALL_SCH_df),function(i){
  
  # print(i)
  
  tmp <- CALL_SCH_df[i,]
  tmp2 <- do.call(rbind,lapply(1:length(unique(CALL_BRD_df_bs$DLVS_CO_BRN_NM)),function(j){
    tmp
  }))
  tmp2$DLVS_CO_BRN_NM <-unique(CALL_BRD_df_bs$DLVS_CO_BRN_NM)
  tmp2
}))

# 순서
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_SCH_df <- CALL_SCH_df[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_BRD_df_bs$no <- 1:nrow(CALL_BRD_df_bs)
CALL_SCH_df$no <- (nrow(CALL_BRD_df_bs) + 1 ):(nrow(CALL_BRD_df_bs) + nrow(CALL_SCH_df) )
CALL_SCH_df$BROAD_HH <- substr(CALL_SCH_df$BROAD_STR_DTM, 12,13)

# CALL_SCH_df <- CALL_SCH_df[!PRD_GRP_NM %in% c("렌탈","여행","핸드폰","교육문화","보험")]


#
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("BROAD_PRD_CNT"))] <- "CNT_PRDCD"
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("AVG_BROAD_SALE_PRC"))] <- "AVG_PRICE"
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("FST_BROAD_DT"))] <- "FST_BROAD_DTM"
CALL_BRD_df_bs$SUM_RUNTIME <- round(CALL_BRD_df_bs$SUM_RUNTIME/60)
CALL_BRD_df_bs$WEIHT_MI <- round(CALL_BRD_df_bs$WEIHT_MI/60)
CALL_SCH_df <- CALL_SCH_df[,BROAD_TM :=gsub(":","",substr(BROAD_STR_DTM,12,16)) ]

# 백업용
CALL_BRD_BACK_df <- CALL_BRD_df_bs

# 편성예측때 SALE_PERCENT_ONAIR로 활용
CALL_BRD_df_bs    <- CALL_BRD_df_bs[, .(no,DLVS_CO_BRN_NM, BROAD_DT, BROAD_HH, BROAD_TM, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME,
                                        PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM,
                                        ITEM_CD, ITEM_NM, BRAND, WEKDY_NM, WEKDY_YN, HOLDY_YN, CN_RS_YN, CNT_PRDCD, FST_BROAD_DTM,
                                        AVG_PRICE,
                                        PRD_GRP_NM, WEIHT_MI,
                                        T_QTY_ONAIR, T_QTY_ONAIR_MCPC, T_CNT_ONAIR, T_CNT_PRE, T_CNT_ONAIR_MCPC, TOT_ORD_QTY)]


CALL_BRDSCH_df <- CALL_SCH_df[, .(no, DLVS_CO_BRN_NM,BROAD_DT, BROAD_HH, BROAD_TM, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME,
                                  PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM,
                                  ITEM_CD, ITEM_NM, BRAND, WEKDY_NM, WEKDY_YN, HOLDY_YN, CN_RS_YN, CNT_PRDCD, FST_BROAD_DTM,
                                  AVG_PRICE,
                                  PRD_GRP_NM,WEIHT_MI)]


#
CALL_BRD_df_bs <- rbind.fill(CALL_BRD_df_bs, CALL_BRDSCH_df)
CALL_BRD_df_bs <- data.table(CALL_BRD_df_bs)

CALL_BRD_df_bs <- CALL_BRD_df_bs[!PRD_GRP_NM %in% c("렌탈","보험","여행","핸드폰")] #특정 카테고리 제외
CALL_BRD_df_bs <- CALL_BRD_df_bs[PGM_GBN != "순환방송"]
CALL_BRD_df_bs <- CALL_BRD_df_bs[WEIHT_MI > 5]
CALL_BRD_df_bs <- CALL_BRD_df_bs[SUM_RUNTIME > 5]


#상품평
CMT_df <- data.table(CMT_df)


# 표준화 총콜
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분수량 := round(T_QTY_ONAIR / SUM_RUNTIME) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분모수량 := round(T_QTY_ONAIR_MCPC / SUM_RUNTIME) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 전담_분수량 := TOT_ORD_QTY / SUM_RUNTIME ]



CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분수량 := round(T_QTY_ONAIR / WEIHT_MI) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분모수량 := round(T_QTY_ONAIR_MCPC / WEIHT_MI) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 전담_가중분수량 := TOT_ORD_QTY / WEIHT_MI ]



# 상품 시작시간
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 노출HH := BROAD_HH ]
CALL_BRD_df_bs$노출HH <- as.numeric(CALL_BRD_df_bs$노출HH)
CALL_BRD_df_bs$노출HH <- ifelse(CALL_BRD_df_bs$노출HH == 0, 24, CALL_BRD_df_bs$노출HH)


# 날짜
CALL_BRD_df_bs$BROAD_DT  <- ymd(CALL_BRD_df_bs$BROAD_DT)
CALL_BRD_df_bs$BRD_DAYT  <- ymd_h(substr(paste(CALL_BRD_df_bs$BROAD_DT, CALL_BRD_df_bs$BROAD_HH) , 1, 13))
CALL_BRD_df_bs$BRD_DAYTM  <- ymd_hm(paste(CALL_BRD_df_bs$BROAD_DT, CALL_BRD_df_bs$BROAD_TM))
head(CALL_BRD_df_bs)

CALL_BRD_df_bs$MIN_Q_START_DATE  <- ymd_hms(CALL_BRD_df_bs$MIN_Q_START_DATE)
CALL_BRD_df_bs$MAX_Q_END_DATE  <- ymd_hms(CALL_BRD_df_bs$MAX_Q_END_DATE)

### 회차 먹이기
CALL_BRD_df_bs <- CALL_BRD_df_bs[SUM_RUNTIME != 0 ,]

# 순서
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]

# 아이템, 브랜드, 상품분류
CALL_BRD_df_bs <- CALL_BRD_df_bs[, IT_SF := seq(.N), by = c("DLVS_CO_BRN_NM","ITEM_NM")]

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 모_수량 := round(T_QTY_ONAIR_MCPC/T_QTY_ONAIR, 3) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 모_고객수 := round(T_CNT_ONAIR_MCPC/T_CNT_ONAIR, 3) ]

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , WEK := ifelse(WEKDY_NM == "Sun", "3.일",
                                                  ifelse(WEKDY_NM == "Sat", "2.토", "1.주중")) ]


# 상담여부
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) == "CN_RS_YN")] <- "상담"
# 신상여부
CALL_BRD_df_bs[is.na(FST_BROAD_DTM) | FST_BROAD_DTM == "99999999", "FST_BROAD_DTM"] <- "99991231"
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 신상 := ifelse(BROAD_DT == ymd(substr(FST_BROAD_DTM,1,8)), "YES", "NO" )]


CALL_BRD_df_bs[ is.na(모_수량) , "모_수량"] <- 0
CALL_BRD_df_bs[ is.na(모_고객수) , "모_고객수"] <- 0


### 추가변수
# 아이템명 우측 생성 ITEM_NM_SEP, 좌측 BRAND
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , ITEM_NM_SEP := unlist(strsplit(ITEM_NM, "[:]"))[2], by = "no" ]

# 순서
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]

# 아이템, 브랜드, 상품분류
CALL_BRD_df_bs <- CALL_BRD_df_bs[, BR_SF := seq(.N), by = c("DLVS_CO_BRN_NM","BRAND")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[, IT2_SF := seq(.N), by = c("DLVS_CO_BRN_NM","ITEM_NM_SEP")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[, PG_SF := seq(.N), by = c("DLVS_CO_BRN_NM","PRD_GRP_NM")]


PUMP_CMT_df <- CMT_df[PMO_STR_DTM >= ymd("2017-01-01") & SRCH == "CA" ,]
PUMP_CMT_df <- unique(PUMP_CMT_df[, .(PMO_NO, PMO_STR_DTM, PMO_END_DTM, ITEM_CD , ITEM_NM )])
PUMP_CMT_df <- PUMP_CMT_df[order(PMO_STR_DTM)]
PUMP_CMT_df <- PUMP_CMT_df[, SEQ := 1:nrow(PUMP_CMT_df)]

# 프로모션 상품평 테이블
pump_cmt_all_df <- do.call(rbind, lapply(1:nrow(PUMP_CMT_df) , function(i) {
  # i <- 1
  # print(i)
  TMP <- PUMP_CMT_df[i,]
  
  st <- ymd(substr(TMP$PMO_STR_DTM,1,10))
  lt <- ymd(substr(TMP$PMO_END_DTM,1,10))
  
  day_df <- data.frame(DAY = seq.Date(st, lt, by = "days"  ))
  result_df <- data.frame(SEQ = TMP$SEQ, day_df,
                          SEQ_EVT = 1:nrow(day_df),
                          # PRD_CD = TMP$PRD_CD,    PRD_NM = TMP$PRD_NM,
                          ITEM_CD = TMP$ITEM_CD,  ITEM_NM = TMP$ITEM_NM,
                          PMO_STR_DTM = TMP$PMO_STR_DTM,
                          PMO_END_DTM = TMP$PMO_END_DTM)
  result_df
  
}))

#
pump_cmt_all_df <- data.table(pump_cmt_all_df)

#
names(pump_cmt_all_df)[2] <- "BROAD_DT"
pump_cmt_all_df$BROAD_DT <- ymd(pump_cmt_all_df$BROAD_DT)
pump_df <- pump_cmt_all_df[ , .(
  PROMO_CNT = .N
),  by = c("BROAD_DT", "ITEM_CD", "ITEM_NM")]


# 일별 아이템별 상품평 카운트 결합
CALL_BRD_df_bs <- merge(CALL_BRD_df_bs, pump_df, by = c("BROAD_DT", "ITEM_CD", "ITEM_NM"), all.x = T)
CALL_BRD_df_bs[is.na(CALL_BRD_df_bs$PROMO_CNT), "PROMO_CNT"] <- 0
CALL_BRD_df_bs$PROMO_CNT_G <- ifelse(CALL_BRD_df_bs$PROMO_CNT > 0 , 1, 0 )


# 모델 생성 데이터
MDL_DT <- "2017-01-01"
FIND_PAST_df <- CALL_BRD_df_bs[BROAD_DT >= ymd(MDL_DT), ]

# 일자 갭 계산
GRP <- data.table(data.frame(DG = 1:(360*10), DGG = rep(seq(30, (360*10), by = 30), each = 30)))
#
DT_df <- data.frame(DT = seq(ymd("2016-01-01"), ymd(substr(SYSDATE,1,10)) + 365, by =  "1 month") )
DT_df$DT_VF <- c(ymd("2015-12-01"), lag(DT_df$DT)[-1])

# 직전, 이전 데이터 결합  nrow(FIND_PAST_df)
# i<- 300000

print("체인룰 시작")

FIND_PAST_df <- FIND_PAST_df %>% filter(WEIHT_MI!=0)


system.time(
  result_df <- do.call(rbind.fill, mclapply(1 : nrow(FIND_PAST_df)  , function(i) {
    # i <- 323601
    # i <- 223600
    
    # print(i)
    
    (tmp1 <- FIND_PAST_df[i, ] )
    
    if(is.na(tmp1$PGM_GBN)) {
      tmp1$PGM_GBN <- "일반PGM"
    }
    
    # 방송정보
    day    <- tmp1$BROAD_DT # 날짜
    mnt    <- ymd(paste0(substr(tmp1$BROAD_DT, 1, 7), "-01")) # 방송월
    mnt_bf <- DT_df[DT_df$DT == mnt, ]$DT_VF # 방송전월
    PGM    <- tmp1$PGM_GBN # pgm
    hh     <- tmp1$노출HH  # hh
    HDY_YN <- tmp1$HOLDY_YN
    DLVS <- tmp1$DLVS_CO_BRN_NM
    
    # 편성표
    day_seq <- data.frame(day = tmp1$BROAD_DT - seq(1,30, by = 1))
    day_seq$WEKPT <- wday(day_seq$day)
    (day_chk <- day_seq$day[3])
    
    if(tmp1$no %in% CALL_SCH_df$no){
      (day_chk <- Sys.Date()-1)
    } else {
      day_seq <- data.frame(day = tmp1$BROAD_DT - seq(1,30, by = 1))
      day_seq$WEKPT <- wday(day_seq$day)
      (day_chk <- day_seq$day[1])
    }
    
    
    if(PGM == "브랜드PGM") {
      # item
      sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM & DLVS_CO_BRN_NM == DLVS,]
      if (nrow(sub_df) == 0) {
        sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & DLVS_CO_BRN_NM == DLVS & PGM_GBN == "일반PGM", ]
      }
      sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
      sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
      # 과거말일 이전 데이터만 추출
      sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    } else {
      # item
      sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM & DLVS_CO_BRN_NM == DLVS,]
      sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
      sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
      # 과거말일 이전 데이터만 추출
      sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    }
    
    # 시간  일치
    sub_df2 <- sub_df[노출HH == hh & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    sub_df2$HHG <- 0
    if (nrow(sub_df2) == 0) {
      # +- 1시간
      sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
      sub_df2$HHG <- 1
      if (nrow(sub_df2) == 0) {
        # +- 2시간
        sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
        sub_df2$HHG <- 2
        if (nrow(sub_df2) == 0) {
          # +- 4시간
          sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
          sub_df2$HHG <- 3
        }
      }
    }
    
    # 유사시간대 주중/주말 마저 못찾았다면! 주중/주말은  포기 !
    if (nrow(sub_df2) == 0) {
      
      # 시간  일치
      sub_df2 <- sub_df[노출HH == hh , ]
      sub_df2$HHG <- 10
      if (nrow(sub_df2) == 0) {
        # +- 1시간
        sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
        sub_df2$HHG <- 11
        if (nrow(sub_df2) == 0) {
          # +- 2시간
          sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
          sub_df2$HHG <- 12
          if (nrow(sub_df2) == 0) {
            # +- 4시간
            sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
            sub_df2$HHG <- 13
          }
        }
      }
      # CALL_BRD_df[IT_SF < tmp1$IT_SF & PRD_GRP_NM == tmp1$PRD_GRP_NM & PGM_GBN == PGM & 노출HH %in% c(22,23, 0 ),]
    }
    
    # new_yo <- 0
    # if (new_yo == 1) {
    #   # 이렇게 해도 못찾았다면! 아이템명(브랜드명제외) 시간대 매칭!
    #   if (nrow(sub_df2) == 0) {
    #
    #     if(PGM == "브랜드PGM") {
    #       # item
    #       sub_df <- CALL_BRD_df[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
    #       if (nrow(sub_df) == 0) {
    #         sub_df <- CALL_BRD_df[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == "일반PGM", ]
    #       }
    #       sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #       sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #       # 과거말일 이전 데이터만 추출
    #       sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #     } else {
    #       # item
    #       sub_df <- CALL_BRD_df[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
    #       sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #       sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #       # 과거말일 이전 데이터만 추출
    #       sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #     }
    #
    #
    #     # 상품군 & 주중/주말 및 시간  일치
    #     sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #     sub_df2$HHG <- 200
    #     if (nrow(sub_df2) == 0) {
    #       # +- 1시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #       sub_df2$HHG <- 210
    #       if (nrow(sub_df2) == 0) {
    #         # +- 2시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #         sub_df2$HHG <- 220
    #         if (nrow(sub_df2) == 0) {
    #           # +- 4시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #           sub_df2$HHG <- 230
    #         }
    #       }
    #     }
    #
    #
    #     # 주중/주말포기! 상품군 & 시간  일치
    #     if (nrow(sub_df2) == 0) {
    #
    #       # 시간  일치
    #       sub_df2 <- sub_df[노출HH == hh  , ]
    #       sub_df2$HHG <- 2000
    #       if (nrow(sub_df2) == 0) {
    #         # +- 1시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
    #         sub_df2$HHG <- 2100
    #         if (nrow(sub_df2) == 0) {
    #           # +- 2시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
    #           sub_df2$HHG <- 2200
    #           if (nrow(sub_df2) == 0) {
    #             # +- 4시간
    #             sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
    #             sub_df2$HHG <- 2300
    #           }
    #         }
    #       }
    #
    #     }
    #
    #   }
    #
    #   # 이렇게 해도 못찾았다면! 브랜드명 시간대 매칭!
    #   if (nrow(sub_df2) == 0) {
    #
    #     if(PGM == "브랜드PGM") {
    #       # item
    #       sub_df <- CALL_BRD_df[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
    #       if (nrow(sub_df) == 0) {
    #         sub_df <- CALL_BRD_df[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == "일반PGM", ]
    #       }
    #       sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #       sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #       # 과거말일 이전 데이터만 추출
    #       sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #     } else {
    #       # item
    #       sub_df <- CALL_BRD_df[BR_SF < tmp1$BR_SF & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
    #       sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #       sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #       # 과거말일 이전 데이터만 추출
    #       sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #     }
    #
    #
    #     # 상품군 & 주중/주말 및 시간  일치
    #     sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #     sub_df2$HHG <- 300
    #     if (nrow(sub_df2) == 0) {
    #       # +- 1시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #       sub_df2$HHG <- 310
    #       if (nrow(sub_df2) == 0) {
    #         # +- 2시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #         sub_df2$HHG <- 320
    #         if (nrow(sub_df2) == 0) {
    #           # +- 4시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #           sub_df2$HHG <- 330
    #         }
    #       }
    #     }
    #
    #
    #     # 주중/주말포기! 상품군 & 시간  일치
    #     if (nrow(sub_df2) == 0) {
    #
    #       # 시간  일치
    #       sub_df2 <- sub_df[노출HH == hh  , ]
    #       sub_df2$HHG <- 3000
    #       if (nrow(sub_df2) == 0) {
    #         # +- 1시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
    #         sub_df2$HHG <- 3100
    #         if (nrow(sub_df2) == 0) {
    #           # +- 2시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
    #           sub_df2$HHG <- 3200
    #           if (nrow(sub_df2) == 0) {
    #             # +- 4시간
    #             sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
    #             sub_df2$HHG <- 3300
    #           }
    #         }
    #       }
    #
    #     }
    #
    #   }
    # }
    
    
    # 이렇게 해도 못찾았다면! 상품군 시간대 매칭!
    if (nrow(sub_df2) == 0) {
      
      if(PGM == "브랜드PGM") {
        # item
        sub_df <- CALL_BRD_df_bs[PG_SF < tmp1$PG_SF & PRD_GRP_NM == tmp1$PRD_GRP_NM & PGM_GBN == PGM,]
        if (nrow(sub_df) == 0) {
          sub_df <- CALL_BRD_df_bs[PG_SF < tmp1$PG_SF & PRD_GRP_NM == tmp1$PRD_GRP_NM & PGM_GBN == "일반PGM", ]
        }
        sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
        sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
        # 과거말일 이전 데이터만 추출
        sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
      } else {
        # item
        sub_df <- CALL_BRD_df_bs[PG_SF < tmp1$PG_SF & PRD_GRP_NM == tmp1$PRD_GRP_NM & PGM_GBN == PGM,]
        sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
        sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
        # 과거말일 이전 데이터만 추출
        sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
      }
      
      
      # 상품군 & 주중/주말 및 시간  일치
      sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
      sub_df2$HHG <- 100
      if (nrow(sub_df2) == 0) {
        # +- 1시간
        sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
        sub_df2$HHG <- 110
        if (nrow(sub_df2) == 0) {
          # +- 2시간
          sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
          sub_df2$HHG <- 120
          if (nrow(sub_df2) == 0) {
            # +- 4시간
            sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
            sub_df2$HHG <- 130
          }
        }
      }
      
      
      # 주중/주말포기! 상품군 & 시간  일치
      if (nrow(sub_df2) == 0) {
        
        # 시간  일치
        sub_df2 <- sub_df[노출HH == hh  , ]
        sub_df2$HHG <- 1000
        if (nrow(sub_df2) == 0) {
          # +- 1시간
          sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
          sub_df2$HHG <- 1100
          if (nrow(sub_df2) == 0) {
            # +- 2시간
            sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
            sub_df2$HHG <- 1200
            if (nrow(sub_df2) == 0) {
              # +- 4시간
              sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
              sub_df2$HHG <- 1300
            }
          }
        }
        
      }
      
    }
    
    
    ## 요약 데이터 생성
    if (nrow(sub_df2) == 0) {
      
      # 전달
      if ( substr(mnt,1,7) != substr(SYSDATE,1,7) ) {
        mnt_bf <- DT_df[which(DT_df$DT == mnt) - 1, ]$DT_VF # 방송전월
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2)  & DLVS_CO_BRN_NM == DLVS , ][, .(전달주문수량)]
          tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1)  & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            }
      } else {
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2) & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
          tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1) & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            }
        
      }
      
      tmp <- cbind.data.frame(tmp1,  tmp2)
      tmp$FIND_YN <- "NO"
      
    } else {
      
      # 기간
      subset_df <- head(sub_df2[order(IT_SF, decreasing = T)],6)
      subset_df <- subset_df[,   .(
        평_노     = mean(SUM_RUNTIME, na.rm = T),
        평_가     = mean(WEIHT_MI, na.rm = T),
        
        평_총수   = mean(T_QTY_ONAIR, na.rm = T),
        평_총고수   = mean(T_CNT_ONAIR, na.rm = T),
        전담_평_총수   = mean(TOT_ORD_QTY, na.rm = T), #배송
        
        평_분수   = mean(분수량, na.rm = T),
        평_가분수   = mean(가중분수량, na.rm = T),
        전담_평_분수   = mean(전담_분수량, na.rm = T), #배송
        전담_평_가분수   = mean(전담_가중분수량, na.rm = T), #배송
        
        
        평_총모수 = mean(T_QTY_ONAIR_MCPC, na.rm = T),
        평_총모고수 = mean(T_CNT_ONAIR_MCPC, na.rm = T),
        
        평_모비 = mean(모_수량, na.rm = T) * 100,
        평_모고비 = mean(모_고객수, na.rm = T) * 100,
        
        평_가격   = mean(AVG_PRICE, na.rm = T),
        평_상품수 = mean(CNT_PRDCD, na.rm = T),
        
        
        ##
        
        직_노     = (SUM_RUNTIME[1]),
        직_총수   = (T_QTY_ONAIR[1]),
        직_분수   = (분수량[1]),
        직_총모수 = (T_QTY_ONAIR_MCPC[1]),
        직_모비 = (모_수량[1]) * 100,
        직_가격   = (AVG_PRICE[1]),
        직_상품수 = (CNT_PRDCD[1]),
        
        전담_직_총수   = (TOT_ORD_QTY[1]), #배송
        전담_직_분수   = (전담_분수량[1]),  #배송
        
        
        
        직_가     = (WEIHT_MI[1]),
        직_총고수   = (T_CNT_ONAIR[1]),
        직_가분수   = (가중분수량[1]),
        직_총모고수 = (T_CNT_ONAIR_MCPC[1]),
        직_모고비 = (모_고객수[1]) * 100,
        
        전담_직_가분수   = (전담_가중분수량[1]),  #배송
        
        
        HHG       = unique(HHG),
        최근방송일= as.numeric(day - BROAD_DT[1]) )]
      
      # 전달
      
      if ( substr(mnt,1,7) != substr(SYSDATE,1,7) ) {
        mnt_bf <- DT_df[which(DT_df$DT == mnt) - 1, ]$DT_VF # 방송전월
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2) & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
          tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1) & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            }
      } else {
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2) & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
          tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1) & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            tmp2 <- data.table(전달주문수량 = mean(tmp2_1$전달주문수량))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh  & DLVS_CO_BRN_NM == DLVS, ][, .(전달주문수량)]
            }
        
      }
      
      tmp <- cbind.data.frame(tmp1, subset_df, tmp2)
      tmp$FIND_YN <- "YES"
      
    }
    
    
    tmp
    
  }, mc.cores = 10))
)


# user    system   elapsed
# 16905.260   351.627  2221.591

rm(GRP, DT_df, FIND_PAST_df)

print("체인룰 종료")


# 데이터 정리
CALL_VAR_df <- result_df %>% dplyr::select(no, DLVS_CO_BRN_NM,BROAD_DT, WEKDY_NM, WEK, BRD_DAYT,BRD_DAYTM , MIN_Q_START_DATE, MAX_Q_END_DATE,노출HH, HOLDY_YN,
                                           #
                                           IT_SF, BR_SF, IT2_SF, PG_SF, ITEM_CD, ITEM_NM, ITEM_NM_SEP, BRAND, PRD_GRP_NM,
                                           #
                                           PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM,
                                           #
                                           CNT_PRDCD, AVG_PRICE, SUM_RUNTIME, WEIHT_MI,
                                           #
                                           TOT_ORD_QTY, T_QTY_ONAIR, T_QTY_ONAIR_MCPC, T_CNT_ONAIR, 분수량, 분모수량, 모_수량, 가중분수량, 가중분모수량, 모_고객수,
                                           #
                                           평_노, 평_총수, 평_분수, 평_총모수, 평_모비, 평_가격, 평_상품수,
                                           #
                                           직_노, 직_총수, 직_분수, 직_총모수, 직_모비, 직_가격, 직_상품수,
                                           #
                                           평_가, 평_총고수, 평_가분수, 평_총모고수, 평_모고비,
                                           #
                                           직_가, 직_총고수, 직_가분수, 직_총모고수, 직_모고비,
                                           #
                                           HHG, 최근방송일, 전달주문수량, FIND_YN, 상담, 신상,
                                           #
                                           ## 추가변수
                                           PROMO_CNT,
                                           PROMO_CNT_G,
                                           
                                           #배송변수
                                           전담_분수량, 전담_가중분수량, 전담_평_총수, 전담_평_분수,
                                           전담_직_총수, 전담_평_가분수, 전담_직_분수, 전담_직_가분수
                                           
                                           
)

CALL_VAR_df <- data.table(CALL_VAR_df)

### 파생 변수 생성
CALL_VAR_df <- CALL_VAR_df[, 평_가격갭 := round( (AVG_PRICE - 평_가격)/AVG_PRICE , 3 ) * 100]
CALL_VAR_df <- CALL_VAR_df[, 직_가격갭 := round( (AVG_PRICE - 직_가격)/AVG_PRICE , 3 ) * 100]
CALL_VAR_df$평_가격갭 <- ifelse(CALL_VAR_df$평_가격갭 == "-Inf", 0, CALL_VAR_df$평_가격갭)
CALL_VAR_df$직_가격갭 <- ifelse(CALL_VAR_df$직_가격갭 == "-Inf", 0, CALL_VAR_df$직_가격갭)

CALL_VAR_df <- CALL_VAR_df[, 평_상품갭 := CNT_PRDCD - 평_상품수]
CALL_VAR_df <- CALL_VAR_df[, 직_상품갭 := CNT_PRDCD - 직_상품수]

CALL_VAR_df <- CALL_VAR_df[, 평_노출갭 := SUM_RUNTIME - 평_노]
CALL_VAR_df <- CALL_VAR_df[, 직_노출갭 := SUM_RUNTIME - 직_노]

CALL_VAR_df <- CALL_VAR_df[, 평_가중노출갭 := WEIHT_MI - 평_가]
CALL_VAR_df <- CALL_VAR_df[, 직_가중노출갭 := WEIHT_MI - 직_가]

CALL_VAR_df <- CALL_VAR_df[, 노_전달시수량 := (전달주문수량/60) * SUM_RUNTIME ]
CALL_VAR_df <- CALL_VAR_df[, 노_전달가중시수량 := (전달주문수량/60) * WEIHT_MI ]


CALL_VAR_df <- CALL_VAR_df[, 노_평_분수량 := (평_분수) * SUM_RUNTIME ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분수량 := (직_분수) * SUM_RUNTIME ]

CALL_VAR_df <- CALL_VAR_df[, 노_평_가중분수량 := (평_가분수) * WEIHT_MI ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_가중분수량 := (직_가분수) * WEIHT_MI ]

##배송
CALL_VAR_df <- CALL_VAR_df[, 전담_노_평_분수량 := (전담_평_분수) * SUM_RUNTIME ]
CALL_VAR_df <- CALL_VAR_df[, 전담_노_직_분수량 := (전담_직_분수) * SUM_RUNTIME ]

CALL_VAR_df <- CALL_VAR_df[, 전담_노_평_가중분수량 := (전담_평_가분수) * WEIHT_MI ]
CALL_VAR_df <- CALL_VAR_df[, 전담_노_직_가중분수량 := (전담_직_가분수) * WEIHT_MI ]
####


# 브랜드 PGM
CALL_VAR_df$BRAND_PGM_GRP <- ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("B special", "The Collection"   ), "1.B스페샬_더컬",
                                    ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("최은경의 W", "왕영은의 톡톡톡", "SHOW me the Trend"),
                                           "2.왕톡_쇼미_최W",
                                           "3.똑소리_리뷰_일반"))

#
CALL_VAR_df <- CALL_VAR_df[ , 신상_IT := ifelse(신상 == "YES" & IT_SF == min(IT_SF), "YES", "NO") , by = "ITEM_NM"]

#
CALL_VAR_df$상담 <- as.factor(CALL_VAR_df$상담)
CALL_VAR_df$신상 <- as.factor(CALL_VAR_df$신상)
CALL_VAR_df$신상_IT <- as.factor(CALL_VAR_df$신상_IT)
CALL_VAR_df$PGM_GBN <- as.factor(CALL_VAR_df$PGM_GBN)
CALL_VAR_df$BRAND_PGM_GRP <- as.factor(CALL_VAR_df$BRAND_PGM_GRP)

#
CALL_VAR_df$PROMO_CNT_G <- as.factor(CALL_VAR_df$PROMO_CNT_G)
CALL_VAR_df$BRAND_PGM_NM <- as.factor(CALL_VAR_df$BRAND_PGM_NM)

#
CALL_VAR_df <- CALL_VAR_df[order(no)]

# 휴일데이터 결합
HOLDY_DT <- ymd(HOLDY_DT$DT_CD)

HOLDY_DT <- HOLDY_DT[c(2, which(as.numeric(HOLDY_DT[-1] - HOLDY_DT[-length(HOLDY_DT)]) != 1) + 2)]

CALL_VAR_df <- CALL_VAR_df[ , HOLDY_SEQ := "A.연휴아님" ]

for( i in 1:length(HOLDY_DT)) {
  # i <- 1
  CALL_VAR_df <- CALL_VAR_df[ , HOLDY_SEQ := ifelse(BROAD_DT == HOLDY_DT[i] - 2, "B.연휴2일전",
                                                    ifelse(BROAD_DT == HOLDY_DT[i] - 1, "C.연휴1일전",
                                                           ifelse(BROAD_DT == HOLDY_DT[i] , "D.연휴당일",
                                                                  ifelse(BROAD_DT == HOLDY_DT[i] + 1, "E.연휴1일후",
                                                                         ifelse(BROAD_DT == HOLDY_DT[i] + 2, "F.연휴2일후", HOLDY_SEQ))))) ]
  
}

CALL_VAR_df <- CALL_VAR_df[ , HOLDY_YN2 := HOLDY_YN ]
CALL_VAR_df <- CALL_VAR_df[ , HOLDY_YN2 := ifelse(HOLDY_YN == "Y" & WEKDY_NM %in% c("Sun", "Sat") & HOLDY_SEQ == "A.연휴아님", "N", HOLDY_YN)]

# 순서정렬
CALL_VAR_df <- CALL_VAR_df[order(no)]
### 추가고려변수
CALL_VAR_df <- CALL_VAR_df[ , 직템__평_총수 := c(평_총수[1], lag(평_총수)[-1] ) ]
setDT(FESTA_YN)
FESTA_YN$BROAD_DT <- ymd(FESTA_YN$DT_CD)
CALL_VAR_df <- merge(CALL_VAR_df, FESTA_YN[,.(BROAD_DT, FESTA_YN)], by = c("BROAD_DT"), all.x = T)
#
CALL_VAR_df <- CALL_VAR_df[ , 날짜순서 := substr(BROAD_DT, 9, 10)]
CALL_VAR_df <- CALL_VAR_df[ , 날짜순서 := (ifelse(FESTA_YN != "Y" , "00", 날짜순서))]
CALL_VAR_df <- CALL_VAR_df[ , 날짜순서 := as.factor(ifelse(날짜순서 >= "11" , "11", 날짜순서))]

#----------------------------------------------------------------
print(paste(" EDA 마트생성종료 ", as.character(Sys.time())))

# ##### 데이터 파악하기
# library(tidymodels)
# library(embed)
# library(tidytext)
#
# var_nm2 <- c(
#   "PRD_GRP_NM",
#   "CNT_PRDCD", "AVG_PRICE", "SUM_RUNTIME", "WEIHT_MI" ,
#   "평_노", "평_총수", "평_분수", "평_총모수", "평_모비", "평_가격", "평_상품수",
#   #
#   "평_가", "평_총고수", "평_가분수", "평_총모고수", "평_모고비",
#   #
#   "직_노", "직_총수", "직_분수", "직_총모수", "직_모비", "직_가격", "직_상품수",
#   #
#   "직_가", "직_총고수", "직_가분수", "직_총모고수", "직_모고비",
#   #
#   "최근방송일", "전달주문수량",
#   #
#   "평_가격갭", "직_가격갭", "평_상품갭", "직_상품갭", "평_노출갭", "직_노출갭", "평_가중노출갭", "직_가중노출갭",
#   #
#   "노_전달시수량", "노_평_분수량", "노_직_분수량",
#   #
#   "노_전달가중시수량", "노_평_가중분수량", "노_직_가중분수량",
#
#   "직템__평_총수") # , "PROMO_CNT" "FESTA_YN", , "날짜순서"
# #
#
# CALL_VAR_df_umpa <- data.frame(CALL_VAR_df)
# CALL_VAR_df_umpa <- CALL_VAR_df_umpa[,var_nm2]
# str(CALL_VAR_df_umpa)
#
#
# pca_rec <- recipe(~., data = CALL_VAR_df_umpa) %>%
#   update_role(PRD_GRP_NM, new_role = "id") %>%
#   step_normalize(all_predictors()) %>%
#   step_pca(all_predictors())
#
# pca_prep <- prep(pca_rec)
# pca_prep
#

#----------------------------------------------------------------
# 3. 모델링 수행
#----------------------------------------------------------------

# wek <- ymd(substr(SYSDATE, 1, 10))-2
# wek2 <- wek + 5

#----------------------------------------------------------------
# 모델 공식
#----------------------------------------------------------------

# 2.
var_nm <- c(
  "CNT_PRDCD", "AVG_PRICE", "SUM_RUNTIME", "WEIHT_MI" , "DLVS_CO_BRN_NM",
  "평_노", "평_총수", "평_분수", "평_총모수", "평_모비", "평_가격", "평_상품수",
  #
  "평_가", "평_총고수", "평_가분수", "평_총모고수", "평_모고비",
  #
  "직_노", "직_총수", "직_분수", "직_총모수", "직_모비", "직_가격", "직_상품수",
  #
  "직_가", "직_총고수", "직_가분수", "직_총모고수", "직_모고비",
  #
  "최근방송일", "전달주문수량",
  #
  "평_가격갭", "직_가격갭", "평_상품갭", "직_상품갭", "평_노출갭", "직_노출갭", "평_가중노출갭", "직_가중노출갭",
  #
  "노_전달시수량", "노_평_분수량", "노_직_분수량",
  #
  "노_전달가중시수량", "노_평_가중분수량", "노_직_가중분수량",
  
  # 배송
  "전담_평_총수", "전담_평_분수", "전담_직_총수",
  "전담_평_가분수", "전담_직_분수", "전담_직_가분수", "전담_노_평_분수량","전담_노_직_분수량","전담_노_평_가중분수량","전담_노_직_가중분수량",
  
  #
  "FIND_YN", "신상", "WEK",  "BRAND_PGM_NM", "신상_IT", "노출HH",
  #
  "PRD_GRP_NM", "HOLDY_YN2", "HOLDY_SEQ",
  "직템__평_총수",
  "PROMO_CNT_G") # , "PROMO_CNT" "FESTA_YN", , "날짜순서"
#


if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {
  var_nm <- c(var_nm, "FESTA_YN", "날짜순서")
}

# formula1 <- formula(paste("총인콜수_M ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)" ), collapse = "+")))
# formula2 <- formula(paste("log10(총인콜수_M) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)"), collapse = "+")))

formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)", "I(AVG_PRICE^3)","I(SUM_RUNTIME^2)", "I(노_전달시수량^2)",
                                                       "I(전담_평_분수^2)", "I(전담_직_분수^2)"), collapse = "+")))

#
CALL_VAR_df$FIND_YN <- as.factor(CALL_VAR_df$FIND_YN)
CALL_VAR_df$WEK <- as.factor(CALL_VAR_df$WEK)
CALL_VAR_df$PRD_GRP_NM <- as.factor(CALL_VAR_df$PRD_GRP_NM)
CALL_VAR_df$노출HH <- as.factor(CALL_VAR_df$노출HH)
CALL_VAR_df$HOLDY_YN2 <- as.factor(CALL_VAR_df$HOLDY_YN2)
CALL_VAR_df$HOLDY_SEQ <- as.factor(CALL_VAR_df$HOLDY_SEQ)
CALL_VAR_df$BRAND_PGM_GRP <- as.factor(CALL_VAR_df$BRAND_PGM_GRP)
CALL_VAR_df$BRAND_PGM_NM <- as.factor(CALL_VAR_df$BRAND_PGM_NM)
CALL_VAR_df$FESTA_YN <- as.factor(CALL_VAR_df$FESTA_YN)
CALL_VAR_df$PROMO_CNT_G <- as.factor(CALL_VAR_df$PROMO_CNT_G)
CALL_VAR_df$DLVS_CO_BRN_NM <- as.factor(CALL_VAR_df$DLVS_CO_BRN_NM)


#
mdl_df <- data.frame(CALL_VAR_df)
mdl_df[is.na(mdl_df)] <- 0
#

mdl_df <- mdl_df[mdl_df$상담=="N",]
mdl_df$상담 <- NULL

rm(HOLDY_DT, i)

#----------------------------------------------------------------
print("모델링 시작")

names(mdl_df)[which(names(mdl_df) == "TOT_ORD_QTY")] <- "Y_REAL"

#### test 하기
# wek <- ymd("20200423")
# wek2 <- wek+3

# wek <- ymd("20200423")
# wek2 <- wek+10


##############
# train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < wek,] %>% filter(Y_REAL > 0))
# train_df <- train_df %>% filter(직_가격갭 > -200 & 직_가격갭 < 200 & 직_분수 < 1000 )
#
# test_df <- mdl_df[mdl_df$BROAD_DT >= wek & mdl_df$BROAD_DT < wek2,]
#
# Y_tr <- train_df$Y_REAL
# Y_tt <- test_df$Y_REAL

# par(mfrow = c(1,2)) ; hist(Y_tr, xlim = c(0,10000), main = "ORD_CALL") ; hist(log10(Y_tr), main = "log(ORD_CALL)"); par(mfrow = c(1,1))

# Y_trl <- log10(train_df$Y_REAL )
#
# set.seed(123)
# mdl_mt   <- model.matrix(formula2, data= rbind.data.frame(train_df, test_df))[,-1]
#
# train_mt <- mdl_mt[1:nrow(train_df) , ]
# test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]
#
# set.seed(123)
# smp <- sample(dim(train_mt)[1], 400)

# xgTrain <- xgb.DMatrix(data=train_mt[-smp,], label=Y_trl[-smp])
# xgVal <- xgb.DMatrix(data=train_mt[smp,], label=Y_trl[smp])
# xgTest <- xgb.DMatrix(data=test_mt)

# remodel_yn <- "Q"

# 목요일이면 모델생성
# if ( wday(wek) != make_mdl ) {
#
#   print(paste( wek , wek2, " 목요일X 예측테스트 " ))
#
#   # 목요일이 아니면
#   load(file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu] ] ) ,"__", GBN_NM, ".RData") )
#
#   # 예측 테스트 실패시 NOT출현 // 재모델링
#   remodel_yn   <- tryCatch(predict(LS_MDL, newx = test_mt),
#                            error = function(e) print("NOT"),
#                            warning = function(w) print("NOT"))
# }
#
# # 목요일이거나 예측실패한경우 모델 재생성
# if ( wday(wek[clu]) == make_mdl | remodel_yn == "NOT" ) {
#   print(paste( wek , wek2, " 목요일O 모델링 및 예측수행" ))
#
#   print(paste(" XGB ", as.character(Sys.time())))
#
#   # ###### 5.XGB 시작 ######
#
#   if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {


# set.seed(123)
# xgbFitl <- xgb.train(
#   data = xgTrain, nfold = 5, label = as.matrix(Y_trl),
#   objective='reg:linear',
#   nrounds=1200,
#   eval_metric='rmse',
#   watchlist=list(train=xgTrain, validate=xgVal),
#   print_every_n=10,
#   nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
#   early_stopping_rounds=100
# )

# png(file = paste0( "/home/DTA_CALL/VARPLOT/", fld_nm, "/",STRT_DT, GBN_NM,".png"), height = 1100, width = 700)

# xgbFitl %>%
#   xgb.importance(feature_names=colnames(xgTrain)) %>%
#   dplyr::slice(1:30) %>%
#   data.table() %>%
#   xgb.plot.importance()
# dev.off()

# dim(train_mt)
# } else {

# print(xgbFit)
# set.seed(123)
# xgbFitl <- xgboost(data = train_mt, nfold = 5, label = as.matrix(Y_trl),
#                    nrounds = 2300, verbose = FALSE, eval_metric = "rmse",
#                    nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
#                    subsample = 0.5213, colsample_bytree = 0.4603)
#
# preds2l <- predict(xgbFitl,test_mt)
#
# er_df3 <- data.frame(no_m = 1:nrow(test_df),
#                     PGM_ID = test_df$PGM_ID,
#                     DT = test_df$BROAD_DT,
#                     MIN_Q_START_DATE = test_df$MIN_Q_START_DATE,
#                     MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
#                     ITEM_CD = test_df$ITEM_CD,
#                     ITEM_NM = test_df$ITEM_NM,
#                     PRD_GRP_NM = test_df$PRD_GRP_NM,
#                     DLVS_CO_BRN_NM = test_df$DLVS_CO_BRN_NM,
#                     SUM_RUNTIME = test_df$SUM_RUNTIME,
#                     Y = Y_tt,
#                     LGBL = round(10^(as.numeric(preds2l))),
#                     # LGBL_U = round(10^(as.numeric(preds3l_up))),
#                     # LGBL_L = round(10^(as.numeric(preds3l_down))),
#                     INSERT_D = ymd(substr(SYSDATE, 1, 10))
# )
#
# er_df3 %>% group_by(DT,PGM_ID,ITEM_CD) %>% dplyr::summarise(sum = sum(Y), sum2 = sum(LGBL)) %>% mutate(rate = 1-abs(sum-sum2)/sum)

# booster = "dart")
# }



## print(xgbFitl)
# ###### 5.XGB 시작 ######

#   save(xgbFitl,
#        file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu]] ), "__", GBN_NM, ".RData") )
#
# }

#####---------------------------------- 예측 수행 ---------------------------------#####

# clu <- 109
# if ( wday(wek[clu]) != make_mdl ) {
#   print(paste( clu, wek[clu] , wek2[clu ], " 목요일X 예측수행" ))
# } else {
#   print(paste( clu, wek[clu] , wek2[clu ], " 목요일O 예측수행" ))
# }


# set.seed(123)
# mdl_mt   <- model.matrix(formula1, data=rbind.data.frame(train_df, test_df))[,-1]
#
# train_mt <- mdl_mt[1:nrow(train_df) , ]
# test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]


# ###### 5.XGB  ######

# er_df <- data.frame(no_m = 1:nrow(test_df),
#                     PGM_ID = test_df$PGM_ID,
#                     DT = test_df$BROAD_DT,
#                     MIN_Q_START_DATE = test_df$MIN_Q_START_DATE,
#                     MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
#                     ITEM_CD = test_df$ITEM_CD,
#                     ITEM_NM = test_df$ITEM_NM,
#                     PRD_GRP_NM = test_df$PRD_GRP_NM,
#                     SUM_RUNTIME = test_df$SUM_RUNTIME,
#                     Y = Y_tt, XGBL = round(10^(as.numeric(preds2l))))

#
#
# ###### XGB 끝 ######


# ###### lightgbm 시작 ######

# wek <- ymd("20200425")
# wek2 <- wek+3

wek <- ymd(substr(SYSDATE, 1, 10)) -3
wek2 <- wek + 6 + 3


train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < wek,] %>% filter(Y_REAL > 0))
train_df <- train_df %>% filter(직_가격갭 > -200 & 직_가격갭 < 200 & 직_분수 < 1000 )
train_df <- train_df %>% filter(노출HH %in% c(1,6,23,24) | (노출HH %in% c(7,8) & gsub(":","",substr(MAX_Q_END_DATE,12,16))<='0820'))


test_df <- mdl_df[mdl_df$BROAD_DT >= wek & mdl_df$BROAD_DT < wek2,]
test_df <- test_df %>% filter(노출HH %in% c(1,6,23,24) | (노출HH %in% c(7,8) & gsub(":","",substr(MAX_Q_END_DATE,12,16))<='0820'))

Y_tt <- test_df$Y_REAL
Y_trl <- log10(train_df$Y_REAL )


set.seed(123)
mdl_mt   <- model.matrix(formula2, data= rbind.data.frame(train_df, test_df))[,-1]

train_mt <- mdl_mt[1:nrow(train_df) , ]
test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]


# lgb.train <- lgb.Dataset(data = train_mt, label = Y_tr)
lgb.train <- lgb.Dataset(data = train_mt, label = Y_trl)
# lgb.test <- lgb.Dataset(data = test_mt, label = Y_tt)

set.seed(123)
smp <- sample(dim(train_mt)[1], round(dim(train_mt)[1]*0.2))

lgTrain <- lgb.Dataset(data=train_mt[-smp,], label=Y_trl[-smp])
lgVal <- lgb.Dataset(data=train_mt[smp,], label=Y_trl[smp])
# lgTest <- lgb.Dataset(data=test_mt)

## 신뢰구간 확보
# lgb.model.cv = lgb.cv(data = lgb.train,
#                       objective = "regression",
#                       nrounds = 1200,
#                       metric = "rmse",
#                       watchlist=list(train=lgTrain, validate=lgVal),
#                       feature_fraction = 0.8,
#                       booster = "dart",
#                       nfold = 5)
#
# best.iter = lgb.model.cv$best_iter
# best.iter = 525

set.seed(1234)
lightFitl <- lightgbm(data = lgTrain,
                      objective = "regression",
                      nrounds = 4000,
                      metric = "rmse",
                      watchlist=list(train=lgTrain, validate=lgVal),
                      feature_fraction = 0.8,
                      booster = "dart")

set.seed(1234)
lightFitl_up <- lightgbm(data = lgTrain,
                         # nfold = 5,
                         nrounds = 4000,
                         eval_metric = "rmse",
                         watchlist=list(train=lgTrain, validate=lgVal),
                         feature_fraction = 0.8,
                         objective = 'quantile',
                         booster = "dart",
                         alpha = 0.8)


# set.seed(1234)
# lightFitl_down <- lightgbm(data = lgb.train,
#                            nrounds = best.iter,
#                            eval_metric = "rmse",
#                            watchlist=list(train=lgTrain, validate=lgVal),
#                            objective = 'quantile',
#                            feature_fraction = 0.8,
#                            booster = "dart",
#                            alpha = 0.2)

preds3l <- predict(lightFitl,test_mt)

## 신뢰구간
preds3l_up <- predict(lightFitl_up,test_mt)
# preds3l_down <- predict(lightFitl_down,test_mt)

# RMSE(Y_tt,10^preds3l)
# MAPE(Y_tt,10^preds3l)

# RMSE(Y_tt,10^preds3l_up)
# RMSE(Y_tt,10^preds3l_down)

er_df <- data.frame(no_m = 1:nrow(test_df),
                    PGM_ID = test_df$PGM_ID,
                    DT = test_df$BROAD_DT,
                    MIN_Q_START_DATE = test_df$MIN_Q_START_DATE,
                    MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
                    ITEM_CD = test_df$ITEM_CD,
                    ITEM_NM = test_df$ITEM_NM,
                    PRD_GRP_NM = test_df$PRD_GRP_NM,
                    DLVS_CO_BRN_NM = test_df$DLVS_CO_BRN_NM,
                    SUM_RUNTIME = test_df$SUM_RUNTIME,
                    Y = Y_tt,
                    LGBL = round(10^(as.numeric(preds3l))),
                    LGBL_U = round(10^(as.numeric(preds3l_up))),
                    # LGBL_L = round(10^(as.numeric(preds3l_down))),
                    INSERT_D = ymd(substr(SYSDATE, 1, 10))
)

# er_df %>% group_by(DT) %>% dplyr::summarise(sum = sum(Y), sum2 = sum(LGBL)) %>% mutate(rate = 1-abs(sum-sum2)/sum)

# source(file = "/home/dio/DB_con/DB_conn.R")
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc6.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")


PRD_PGM <- paste(unique(er_df$PGM_ID), collapse=",")

# prd_match <- dbGetQuery(conn, paste0("
# SELECT K.*,
#                                 CASE WHEN K.상품개수 = 1 THEN 1
#                                      ELSE ( CASE WHEN K.아이템PGM목표수량 = 0 THEN 0 ELSE ROUND(K.상품PGM목표수량/K.아이템PGM목표수량, 4) END ) END AS 상품별PGM목표비율
#                                      FROM
#                                      (
#                                      SELECT
#                                      a.PGM_ID AS PGM_ID,
#                                      a.ITEM_CD AS ITEM_CD,
#                                      a.PRD_CD AS PRD_CD,
#                                      COUNT(a.PRD_CD)OVER(PARTITION BY a.PGM_ID, a.ITEM_CD) AS 상품개수,
#                                      SUM(a.SUPLY_PLAN_QTY)OVER(PARTITION BY a.PGM_ID, a.ITEM_CD) AS 아이템PGM목표수량,
#                                      a.SUPLY_PLAN_QTY AS 상품PGM목표수량,
#                                      a12.SUP_CD AS 협력사코드,
#                                      a17.SUP_NM AS 협력사명,
#                                      a16.TXN_TYP_NM AS 매입형태,
#                                      a18.CD_VAL AS 배송형태
#                                      FROM    D_BRD_BROAD_FORM_PRD_M a
#                                      join    D_PRD_PRD_M a12
#                                      on     (a.PRD_CD = a12.PRD_CD)
#                                      join V_ORD_TXN_TYP_C a16
#                                      on  (a12.PRCH_TYP_CD = a16.TXN_TYP_CD)
#                                      join D_SUP_SUP_M a17
#                                      on  (a12.SUP_CD = a17.SUP_CD)
#                                      join    ( SELECT CMM_CD, CD_VAL
#                                      FROM SDHUB_OWN.STG_CMM_CMM_C
#                                      WHERE 1=1
#                                      AND CMM_GRP_CD = 'PRD081' ) a18
#                                      on     (a12.DLV_PICK_MTHOD_CD = a18.CMM_CD)
#                                      WHERE 1=1
#                                      --AND a.ITEM_BROAD_STR_DTM BETWEEN TO_DATE('20200601','YYYYMMDD')  --방송일자
#                                      --AND TO_DATE('20200716','YYYYMMDD') + 0.99999
#                                      AND a.USE_YN = 'Y'
#                                      AND TO_NUMBER(a.PGM_ID) IN (", PRD_PGM , ")
#                                      ) K"))

prd_match <- dbGetQuery(conn, paste0("SELECT K.*,
                                     CASE WHEN K.상품개수 = 1 THEN 1
                                     ELSE ( CASE WHEN K.아이템PGM목표수량 = 0 AND K.상품PGM목표수량 = 0 THEN 0 ELSE ROUND(K.상품PGM목표수량/K.아이템PGM목표수량, 4) END ) END AS 상품별PGM목표비율
                                     FROM
                                     (
                                     SELECT
                                     a.PGM_ID AS PGM_ID,
                                     a.ITEM_BROAD_STR_DTM AS 방송시작일시,
                                     a.ITEM_BROAD_END_DTM AS 방송종료일시,
                                     a.ITEM_CD AS ITEM_CD,
                                     a.PRD_CD AS PRD_CD,
                                     a12.PRD_NM AS 상품명,
                                     a12.SUP_CD AS 협력사코드,
                                     a17.SUP_NM AS 협력사명,
                                     a16.TXN_TYP_NM AS 매입형태,
                                     a18.CD_VAL AS 배송형태,
                                     COUNT(a.PRD_CD)OVER(PARTITION BY a.PGM_ID, a.ITEM_CD) AS 상품개수,
                                     a.BROAD_SALE_PRC AS 방송판매가,
                                     a.OVRALL_TGT_EXPCT_SAL_AMT AS 종합목표예상취급액,
                                     SUM(ROUND(a.OVRALL_TGT_EXPCT_SAL_AMT/a.BROAD_SALE_PRC,0))OVER(PARTITION BY a.PGM_ID, a.ITEM_CD) AS 아이템PGM목표수량,
                                     ROUND(a.OVRALL_TGT_EXPCT_SAL_AMT/a.BROAD_SALE_PRC,0) AS 상품PGM목표수량
                                     FROM    D_BRD_BROAD_FORM_PRD_M a
                                     join    D_PRD_PRD_M a12
                                     on     (a.PRD_CD = a12.PRD_CD)
                                     join V_ORD_TXN_TYP_C a16
                                     on  (a12.PRCH_TYP_CD = a16.TXN_TYP_CD)
                                     join D_SUP_SUP_M a17
                                     on  (a12.SUP_CD = a17.SUP_CD)
                                     join    ( SELECT CMM_CD, CD_VAL
                                     FROM SDHUB_OWN.STG_CMM_CMM_C
                                     WHERE 1=1
                                     AND CMM_GRP_CD = 'PRD081' ) a18
                                     on     (a12.DLV_PICK_MTHOD_CD = a18.CMM_CD)
                                     WHERE 1=1
                                     --AND a.ITEM_BROAD_STR_DTM BETWEEN TO_DATE('20200601','YYYYMMDD')  --방송일자
                                     --AND TO_DATE('20200716','YYYYMMDD') + 0.99999
                                     AND a.OVRALL_TGT_EXPCT_SAL_AMT > 0
                                     AND a.BROAD_SALE_PRC > 0
                                     AND a.USE_YN = 'Y'
                                     AND a.BROAD_YN = 'Y'
                                     ) K
                                     WHERE 1=1
                                     AND K.아이템PGM목표수량 > 0
                                     AND TO_NUMBER(PGM_ID) IN (", PRD_PGM , ")"))





TT <- merge(er_df,prd_match,by=c("PGM_ID","ITEM_CD"),all.x=T)
TT <- TT %>% dplyr::mutate(PRD_PRED = round(LGBL*상품별PGM목표비율,0))
TT <- TT %>% dplyr::mutate(PRD_PRED_U = round(LGBL_U*상품별PGM목표비율,0))
TT <- TT[TT$LGBL!=0,]

TT$attr_prd_cd <- paste0(TT$PRD_CD,"001")
TT$INSERT_DT <-STRT_DT


######################################################
######################################################

# 세일즈원 적재용
# print("saleone 적재 시작")
#
# TIME <- dbGetQuery(conn, paste0("SELECT A.PGM_ID, A.BROAD_STR_DTM, A.BROAD_END_DTM
# FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
# --LEFT OUTER JOIN D_BRD_BROAD_FORM_ITEM_M B ON A.PGM_ID = B.PGM_ID
# WHERE A.PGM_ID IN (",PRD_PGM,")"))
#
# head(TT)
# TT <- merge(TT,TIME,by=c("PGM_ID"),all.x=T)


salesone <- TT %>% dplyr::select(PGM_ID, 방송시작일시, 방송종료일시, DLVS_CO_BRN_NM, ITEM_CD, ITEM_NM, PRD_CD, 상품명,
                                 attr_prd_cd , 협력사코드, 협력사명, 매입형태, PRD_PRED_U, INSERT_DT) %>%
  dplyr::rename(PRD_NM = 상품명,
                sup_cd = 협력사코드,
                sup_nm = 협력사명,
                prch_typ_nm = 매입형태,
                frc_qty = PRD_PRED_U,
                BROAD_STR_DTM = 방송시작일시,
                BROAD_END_DTM = 방송종료일시) %>%
  dplyr::arrange(INSERT_DT,BROAD_STR_DTM)

dbDisconnect(conn)
rm(conn,drv)


# # DB접근
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc6.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")

# dbSendUpdate(conn, paste0("DELETE FROM DTA_OWN.SCM_PRED_SALESONE"))
# dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.SCM_PRED_SALESONE"))
# dbWriteTable(conn, "DTA_OWN.SCM_PRED_SALESONE", salesone , row.names = F)
# dbSendUpdate(conn, paste0("DELETE FROM DTA_OWN.SCM_PRED_SALESONE WHERE INSERT_DT = '20201021'"))

for (i in 1:nrow(salesone)) {
  print(paste(i, "//", nrow(salesone)))
  query.x <- paste0( "INSERT INTO DTA_OWN.SCM_PRED_SALESONE
                     (PGM_ID, BROAD_STR_DTM, BROAD_END_DTM, DLVS_CO_BRN_NM, ITEM_CD, ITEM_NM, PRD_CD, PRD_NM,
                     attr_prd_cd , sup_cd, sup_nm, prch_typ_nm, frc_qty, INSERT_DT)
                     VALUES (",
                     salesone$PGM_ID[i], ",'",
                     salesone$BROAD_STR_DTM[i], "','",
                     salesone$BROAD_END_DTM[i], "','",
                     salesone$DLVS_CO_BRN_NM[i], "',",
                     salesone$ITEM_CD[i], ",'",
                     salesone$ITEM_NM[i], "',",
                     salesone$PRD_CD[i], ",'",
                     salesone$PRD_NM[i], "','",
                     salesone$attr_prd_cd[i], "',",
                     salesone$sup_cd[i], ",'",
                     salesone$sup_nm[i], "','",
                     salesone$prch_typ_nm[i], "',",
                     salesone$frc_qty[i], ",'",
                     salesone$INSERT_DT[i], "')")
  tryCatch(dbSendUpdate(conn, query.x), error = function(e) print("NOT"), warning = function(w) print("NOT"))
  
}

dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.SCM_PRED_SALESONE TO MSTR_APP"))
dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.SCM_PRED_SALESONE TO DTA_APP"))
dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.SCM_PRED_SALESONE TO I_KIMDY9"))
dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.SCM_PRED_SALESONE TO I_PARKGE"))
dbSendUpdate(conn, paste0("COMMIT"))
dbDisconnect(conn)
rm(conn,drv)

print("saleone 적재 완료")
########################################################################
########################################################################
#
#

# source(file = "/home/dio/DB_con/DB_conn.R")
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc6.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")

y_prd_match <- dbGetQuery(conn, paste0("
                                       SELECT /*+ FULL(K) PARALLEL (K 4) */
                                       PGM_ID
                                       ,PRD_CD
                                       --,ATTR_PRD_CD
                                       ,ITEM_CD
                                       ,DLVS_CO_BRN_NM
                                       ,SUM(TOT_ORD_QTY) TOT_ORD_QTY
                                       FROM(
                                       SELECT /*+ FULL(A) FULL(B) FULL(C) PARALLEL (A 6) */
                                       A.PGM_ID
                                       ,A.PRD_CD
                                       --,A.ATTR_PRD_CD
                                       ,B.ITEM_CD
                                       ,DLVS_CO_BRN_NM
                                       ,A.TOT_ORD_QTY
                                       , CASE WHEN A.REP_PAY_MEAN_CD='01' AND A.DPO_CNF_DTM BETWEEN C.MIN_Q_START_DATE AND C.MAX_Q_END_DATE THEN '1'
                                       WHEN A.REP_PAY_MEAN_CD='01' THEN '2' ELSE '3' END AS CUST_DV
                                       FROM GSBI_OWN.F_ORD_ORD_D A
                                       INNER JOIN D_PRD_PRD_M B ON A.PRD_CD = B.PRD_CD
                                       INNER JOIN (SELECT BROAD_DT,PGM_ID,ITEM_CD,MIN_Q_START_DATE,MAX_Q_END_DATE
                                       FROM DTA_OWN.GSTS_PGM_ITEM_ORD) C
                                       ON A.BROAD_DT = C.BROAD_DT AND A.PGM_ID = C.PGM_ID AND B.ITEM_CD = C.ITEM_CD
                                       WHERE 1=1
                                       AND BROAD_ORD_TIME_DTL_CD = 'O1' --ONTIME
                                       AND EXCHRG_DLVS ='전담배송'
                                       AND A.BROAD_DT BETWEEN '",gsub("-","",wek),"' AND '",gsub("-","",wek2-1),"'
                                       ) K
                                       WHERE 1=1
                                       AND K.CUST_DV IN ('1','3')
                                       GROUP BY
                                       PGM_ID
                                       ,PRD_CD
                                       --,ATTR_PRD_CD
                                       ,ITEM_CD
                                       ,DLVS_CO_BRN_NM"))


TT2 <- merge(TT,y_prd_match,by=c("PGM_ID","ITEM_CD","PRD_CD","DLVS_CO_BRN_NM"),all.x=T)
TT2[is.na(TT2)] <- 0
TT2 <- TT2 %>% dplyr::filter(상품별PGM목표비율!=0)


er_df2 <- TT2 %>% dplyr::select(PGM_ID, DT,  방송시작일시, 방송종료일시, ITEM_CD,ITEM_NM,PRD_GRP_NM,DLVS_CO_BRN_NM, SUM_RUNTIME, Y,  LGBL, LGBL_U, INSERT_D,
                                PRD_CD,상품명,상품개수,아이템PGM목표수량,상품PGM목표수량,협력사코드,협력사명,매입형태,배송형태,상품별PGM목표비율,PRD_PRED,PRD_PRED_U,TOT_ORD_QTY) %>%
  dplyr::rename(PRD_NM = 상품명,
                MIN_Q_START_DATE = 방송시작일시,
                MAX_Q_END_DATE = 방송종료일시,
                PRD_NUM = 상품개수,
                ITEM_PGM_TARGET = 아이템PGM목표수량,
                PRD_PGM_TARGET = 상품PGM목표수량,
                PARTNER_CD = 협력사코드,
                PARTNER_NM = 협력사명,
                PURCHASE_TYPE = 매입형태,
                SCM_TYPE = 배송형태,
                PRD_TARGET_RATE = 상품별PGM목표비율)

dbDisconnect(conn)
rm(conn,drv)

drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc6.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")


# er_df2 %>% dplyr::select(PRD_GRP_NM,DT,PRD_PRED,TOT_ORD_QTY) %>% mutate(total = ifelse(PRD_PRED-TOT_ORD_QTY<0,PRD_PRED/TOT_ORD_QTY,TOT_ORD_QTY/PRD_PRED)) %>%
# filter(total>0) %>%
# filter(TOT_ORD_QTY >= 30) %>%
# group_by(PRD_GRP_NM) %>%
# dplyr::summarise(mean = mean(total))

# er_df2 <- er_df2 %>% filter(DT == c("2020-07-17") | DT == c("2020-07-21"))

# er_df2 %>% group_by(PGM_ID,ITEM_CD,PRD_CD) %>% dplyr::summarise(sum = sum(TOT_ORD_QTY), sum2 = sum( PRD_PRED)) %>% mutate(rate = 1-abs(sum-sum2)/sum) %>% ungroup() %>% dplyr::summarise(mean=mean(rate))
# er_df %>% group_by(DT,PGM_ID,ITEM_CD) %>% dplyr::summarise(sum = sum(Y), sum2 = sum(LGBL)) %>% mutate(rate = 1-abs(sum-sum2)/sum)

### 시각화 추가
# data.frame(Y = Y_tt,
#            LGBL = round(10^(as.numeric(preds3l))),
#            # LGBL_U = round(10^(as.numeric(preds3l_up))),
#            DLVS_CO_BRN_NM = test_df$DLVS_CO_BRN_NM
# ) %>%
#
#   ggplot(aes(x=1:nrow(test_df),y=Y,color=DLVS_CO_BRN_NM,group = DLVS_CO_BRN_NM)) + geom_line() +
#   # geom_line(color="blue") +
#   geom_line(aes(x=1:nrow(test_df),y=LGBL,color="PRED",group = DLVS_CO_BRN_NM)) +
#   facet_wrap(~DLVS_CO_BRN_NM)
#
#
# tree_imp <- lgb.importance(lightFitl, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 30, measure = "Gain")


# dada <- dbGetQuery(conn, paste0("SELECT * FROM I_KIMDY9.SCM_PRED_DLVS2 ORDER BY DT" ))
# dbWriteTable(conn, "DTA_OWN.SCM_PRED_DLVS2", dada , row.names = F)
# dbSendUpdate(conn, paste0("DROP TABLE I_KIMDY9.SCM_PRED_DLVS2"))


# 기존 정답이 없는 데이터 적재

al_table <- dbGetQuery(conn, paste0("SELECT * FROM DTA_OWN.SCM_PRED_DLVS2 WHERE DT >= '",wek,"' ORDER BY DT" ))
al_table1 <- al_table %>% filter(DT < wek+4)
al_table1 <- merge(al_table1[,-which(names(al_table1)=="TOT_ORD_QTY")],y_prd_match,by=c("PGM_ID","ITEM_CD","PRD_CD","DLVS_CO_BRN_NM"),all.x=T)
al_table1[is.na(al_table1)] <- 0

# y_insert <- mdl_df %>% filter(BROAD_DT < wek+4, BROAD_DT >= wek, 노출HH %in% c(6,7,8,9)) %>% dplyr::select(PGM_ID,ITEM_CD,DLVS_CO_BRN_NM,Y_REAL)
# y_insert$MIN_Q_START_DATE <- as.character(y_insert$MIN_Q_START_DATE)
# al_table2_2 <- merge(al_table1,y_insert,by=c("PGM_ID","ITEM_CD","DLVS_CO_BRN_NM"),all.x=T)
# al_table2_2[is.na(al_table2_2$Y_REAL),"Y_REAL"] <- 0

# al_table2_2$Y <- al_table2_2$Y_REAL
# al_table2_2 <- al_table2_2 %>% dplyr::select(-"Y_REAL")
# names(al_table2_2)[which(names(al_table2_2)=="NO_M")] <- "no_m"
er_df2 <- er_df2 %>% dplyr::filter(DT >= wek+4)
# er_df1 <- er_df %>% dplyr::filter(DT >= wek+4)
er_df2 <- rbind.fill(al_table1,er_df2)



# 새롭게 쌓는 테입블
# al_table2 <- al_table %>% filter(DT == wek-1)
# al_table3 <- al_table %>% filter(DT > wek-1)
#
# y_insert <- train_df %>% filter(BROAD_DT == wek-1) %>% dplyr::select(MIN_Q_START_DATE,DLVS_CO_BRN_NM,Y_REAL)
# y_insert$MIN_Q_START_DATE <- as.character(y_insert$MIN_Q_START_DATE)
#
# al_table2_2 <- merge(al_table2,y_insert,by=c("MIN_Q_START_DATE","DLVS_CO_BRN_NM"))
# al_table2_2$Y <- al_table2_2$Y_REAL
# al_table2_2 <- al_table2_2 %>% dplyr::select(-"Y_REAL")
# names(al_table2_2)[which(names(al_table2_2)=="NO_M")] <- "no_m"
#
# er_df2 <-rbind.fill(al_table2_2,er_df)
# er_df2 <- er_df2 %>% dplyr::select(no_m, PGM_ID, DT,  MIN_Q_START_DATE, MAX_Q_END_DATE, ITEM_CD,ITEM_NM,PRD_GRP_NM,DLVS_CO_BRN_NM, SUM_RUNTIME, Y,  LGBL, LGBL_U, INSERT_D)
#
# er_df2 <- er_df %>% filter(DT == "2020-07-11")
dbSendUpdate(conn, paste0("DELETE FROM DTA_OWN.SCM_PRED_DLVS2 WHERE DT >= '",wek,"'"))
# er_df2 <- er_df %>% filter(DT %in% c(wek,wek2-1))

for (i in 1:nrow(er_df2)) {
  print(paste(i, "//", nrow(er_df2)))
  query.x <- paste0( "INSERT INTO DTA_OWN.SCM_PRED_DLVS2
                     (PGM_ID, DT,  MIN_Q_START_DATE, MAX_Q_END_DATE, ITEM_CD,ITEM_NM,PRD_GRP_NM,DLVS_CO_BRN_NM, SUM_RUNTIME, Y,  LGBL, LGBL_U, INSERT_D,
                     PRD_CD, PRD_NM, PRD_NUM, ITEM_PGM_TARGET, PRD_PGM_TARGET, PARTNER_CD, PARTNER_NM, PURCHASE_TYPE, SCM_TYPE, PRD_TARGET_RATE, PRD_PRED, PRD_PRED_U, TOT_ORD_QTY)
                     VALUES (",
                     er_df2$PGM_ID[i], ",'",
                     er_df2$DT[i], "','",
                     er_df2$MIN_Q_START_DATE[i], "','",
                     er_df2$MAX_Q_END_DATE[i], "',",
                     er_df2$ITEM_CD[i], ",'",
                     er_df2$ITEM_NM[i], "','",
                     er_df2$PRD_GRP_NM[i], "','",
                     er_df2$DLVS_CO_BRN_NM[i], "',",
                     er_df2$SUM_RUNTIME[i], ",",
                     er_df2$Y[i], ",",
                     er_df2$LGBL[i], ",",
                     er_df2$LGBL_U[i], ",'",
                     er_df2$INSERT_D[i], "',",
                     er_df2$PRD_CD[i], ",'",
                     er_df2$PRD_NM[i], "',",
                     er_df2$PRD_NUM[i], ",",
                     er_df2$ITEM_PGM_TARGET[i], ",",
                     er_df2$PRD_PGM_TARGET[i], ",",
                     er_df2$PARTNER_CD[i], ",'",
                     er_df2$PARTNER_NM[i], "','",
                     er_df2$PURCHASE_TYPE[i], "','",
                     er_df2$SCM_TYPE[i], "',",
                     er_df2$PRD_TARGET_RATE[i], ",",
                     er_df2$PRD_PRED[i], ",",
                     er_df2$PRD_PRED_U[i], ",",
                     er_df2$TOT_ORD_QTY[i], ")")
  tryCatch(dbSendUpdate(conn, query.x), error = function(e) print("NOT"), warning = function(w) print("NOT"))
  
}

dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.SCM_PRED_DLVS2 TO MSTR_APP"))
dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.SCM_PRED_DLVS2 TO I_PARKGE"))
dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.SCM_PRED_DLVS2 TO DTA_APP"))
dbSendUpdate(conn, paste0("COMMIT"))

dbDisconnect(conn)
rm(conn,drv)

