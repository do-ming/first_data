주예측

# .rs.restartR()

##### 패키지 불러오기 ####
library(data.table)
library(plyr)
library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)
library(MASS)

library(parallel)
library(doSNOW)
library(randomForest)
library(party)
library(glmnet)

library(quantreg)
library(gclus)
library(e1071)
library(caret)

library(gbm)
library(doParallel)
library(kernlab)
library(xgboost)

library(useful)
library(magrittr)
library(dygraphs)
library(readr)
library(DiagrammeR)

###### 패키지 #####

source(file = "/home/DTA_CALL/DB_CON.R")

#----------------------------------------------------------------
# 1. 데이터 호출 
#----------------------------------------------------------------
gc(T);gc(T);

# 에러텀 계산필요시 YES, 미래예측값만 뿌릴시 NO 
TEST_YN <- "NO" 

yr_df  <- data.frame(YEAR = seq.Date(ymd("2011-01-01"), ymd(paste0(substr(as.character(Sys.time()), 1, 7), "-01")), by = "year"  ))
mon_df <- data.frame(MON = seq.Date(ymd("2011-01-01"), ymd(paste0(substr(as.character(Sys.time()), 1, 7), "-01")), by = "month"  ))
LAST_mon <- last(mon_df$MON) - 1
STRT_mon <- yr_df$YEAR[nrow(yr_df) -  4]

(STRT_DT <- gsub("-", "", substr(as.character(Sys.time()), 1, 10)))
(LAST_DT <- gsub("-", "", ymd(substr(as.character(Sys.time()), 1, 10)) + 10))
# 모델 RUN 날짜
(SYSDATE <- as.character(Sys.time()) )


# ##
# STRT_DT <- "20180829" ; LAST_DT <- gsub("-" , "",ymd(STRT_DT ) + 10) ;
# STRT_DT <- gsub( "-", "",dayday[dayy,]) ; LAST_DT <- gsub("-" , "",ymd(STRT_DT ) + 10) ;
(SYSDATE <- as.character(paste0(ymd(STRT_DT), " 03:00:00") ))
print(STRT_DT); print(LAST_DT); print(SYSDATE)
# 
###### 방송, 콜 데이터 실적 주문 CALL_BRD_df ######
CALL_BRD_df <- dbGetQuery(conn, paste0(" SELECT ITEM.ITEM_NM
                                            , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1) AS BRAND
                                            , PRD_GRP.PRD_GRP_NM
                                            , A.*
                                            , DT.WEKDY_NM
                                            , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일
                                           , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)  
                                            , B.ANI_CALL, B.ANI_CALL_ARS, B.ANI_CALL_CNSL, B.ANI_CALL_ORD, B.ANI_CALL_SR, B.ADN_CALL, B.ADN_CALL_CNSL
                                            FROM DTA_OWN.GSTS_PGM_ITEM_ORD A   /* PGM,ITEM별 주문*/
                                            ,DTA_OWN.GSTS_PGM_ITEM_CALL B       /*PGM,ITEM별 콜*/
                                            ,GSBI_OWN.D_PRD_ITEM_M ITEM
                                            ,DHUB_OWN.DH_DT DT
                                            ,GSBI_OWN.V_CMM_PRD_GRP_C PRD_GRP
                                            WHERE A.PGM_ID = B.PGM_ID
                                            AND A.ITEM_CD = B.ITEM_CD
                                            AND A.ITEM_CD = ITEM.ITEM_CD
                                            AND A.BROAD_DT = DT.DT_CD  
                                            AND A.PRD_GRP_CD = PRD_GRP.PRD_GRP_CD
                                            AND A.BROAD_DT < '", STRT_DT,"' "))

###### 방송, 콜 데이터 실적 ######

###### 콜데이터 시간단위 CALL_HH_df ###### 
CALL_HH_df <- dbGetQuery(conn, paste0("WITH VW_OZ_CALLS_HOURLY AS
                                           (
                                           SELECT  WORK_DATE              AS STD_DT
                                           ,SUBSTR(CALL_TIME, 1,2) AS STD_HOUR
                                           ,SUBSTR(WORK_DATE, 1,6) AS STD_YM
                                           ,SUM(ARSINCALL)         AS ARS_INCALLS
                                           ,SUM(JUMUN_IN)          AS ORD_INCALLS
                                           ,SUM(JUMUN_SR)          AS SR_INCALLS
                                           ,SUM(ARSACDCALL)        AS ARS_ACDCALLS
                                           ,SUM(JUMUN_ACD)         AS ORD_ACDCALLS
                                           ,SUM(SRACD)             AS SR_ACDCALLS
                                           FROM SDHUB_OWN.STG_WF_GUIDE_SKLITM_MG_DAILY     
                                           GROUP BY WORK_DATE
                                           , SUBSTR(CALL_TIME, 1,2)
                                           , SUBSTR(WORK_DATE, 1,6)
                                           )
                                           SELECT  STD_YM        /*기준월*/ AS 기준월
                                          ,STD_DT        /*기준일자*/ AS 기준일자
                                          ,STD_HOUR      /*기준시간대*/ AS 기준시간
                                          , (ARS_INCALLS + ORD_INCALLS + SR_INCALLS) AS 총_인입콜수
                                          ,ARS_INCALLS   /*ARS 인입콜*/ AS ARS_인입콜수
                                          , (ORD_INCALLS + SR_INCALLS) AS 상담원_인입콜수
                                          ,ORD_INCALLS   /*주문 인입콜*/ AS 상담원_주문_인입콜수
                                          ,SR_INCALLS    /*SR 인입콜*/ AS 상담원_SR_인입콜수
                                          
                                           FROM VW_OZ_CALLS_HOURLY  /*SDHUB_OWN.VW_OZ_CALLS_HOURLY로 대체 가능*/
                                           WHERE STD_YM >= '", substr(gsub("-", "", STRT_mon),1,6),"' 
                                           ORDER BY 1,2,3"))
###### 콜데이터 시간단위 ######

# ###### 편성 데이터 예측에 활용 CALL_SCH_df #####
# CALL_SCH_df <- dbGetQuery(conn, paste0("
#                                        SELECT A.PGM_ID
#                                        , A.PGM_NM, A.TITLE_ID
#                                        , A.TITLE_NM
#                                        , (CASE WHEN A.TITLE_NM IN ('B special', '최은경의 W', '테이스티 샵') THEN '브랜드PGM' ELSE A.PGM_GBN END) AS PGM_GBN
#                                        , (CASE WHEN A.TITLE_NM IN ('B special', '최은경의 W', '테이스티 샵') THEN A.TITLE_NM ELSE A.BRAND_PGM_NM END) AS BRAND_PGM_NM
#                                        , A.BROAD_DT 
#                                        , A.BROAD_STR_DTM
#                                        , A.BROAD_END_DTM
#                                        , (CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_STR_DTM ELSE LAG(A.BROAD_IT_STR_DTM, 1, A.BROAD_STR_DTM) OVER (
#                                        PARTITION BY A.PGM_ID, A.BROAD_STR_DTM 
#                                        ORDER BY A.BROAD_STR_DTM, A.PRD_FORM_SEQ) END + 1/60/24/60 )                          AS MIN_Q_START_DATE
#                                        , CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_END_DTM 
#                                        WHEN A.BROAD_END_DTM < A.BROAD_IT_STR_DTM THEN A.BROAD_END_DTM ELSE A.BROAD_IT_STR_DTM END AS MAX_Q_END_DATE
#                                        --, A.BROAD_IT_STR_DTM
#                                        , A.PRD_GRP_NM
#                                        , A.ITEM_CD
#                                        , A.BRAND
#                                        , A.ITEM_NM
#                                        , A.WEKDY_NM
#                                        , A.WEKDY_YN --1:주말 /  0:평일
#                                        , A.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)  
#                                        , A.CN_RS_YN
#                                        , A.CNT_PRDCD
#                                        , A.PRD_FORM_SEQ
#                                        , A.SORT_SEQ
#                                        , ROUND(CASE WHEN A.RUN_TIME <> 0 THEN A.RUN_TIME ELSE ( (CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_END_DTM 
#                                        WHEN A.BROAD_END_DTM < A.BROAD_IT_STR_DTM THEN A.BROAD_END_DTM ELSE A.BROAD_IT_STR_DTM END) - 
#                                        (CASE WHEN A.ITEM_CNT = 1 THEN A.BROAD_STR_DTM ELSE LAG(A.BROAD_IT_STR_DTM, 1, A.BROAD_STR_DTM) OVER (
#                                        PARTITION BY A.PGM_ID, A.BROAD_STR_DTM 
#                                        ORDER BY A.BROAD_STR_DTM, A.PRD_FORM_SEQ) END + 1/60/24/60 )
#                                        )*60*24 END) AS SUM_RUNTIME
#                                        , A.FST_BROAD_DTM  --메인상품코드 기준의 최초방송일자
#                                        , A.AVG_PRICE
#                                        , A.MIN_PRICE
#                                        , A.MAX_PRICE
#                                        , A.ITEM_CNT
#                                        
#                                        FROM 
#                                        (
#                                        SELECT A.* 
#                                        , (CASE WHEN (COUNT(A.ITEM_CD) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM)) <> 1 THEN 
#                                        A.BROAD_STR_DTM + ( SUM(A.RUN_TIME) OVER(
#                                        PARTITION BY A.PGM_ID,A.BROAD_STR_DTM 
#                                        ORDER BY A.PGM_ID,A.BROAD_STR_DTM,A.PRD_FORM_SEQ 
#                                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))/60/24 ELSE A.BROAD_STR_DTM END) AS BROAD_IT_STR_DTM
#                                        , COUNT(A.ITEM_CD) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS ITEM_CNT
#                                        FROM (
#                                        SELECT A.PGM_ID
#                                        , A.PGM_NM, PGM.TITLE_ID, PGM.TITLE_NM
#                                        , PGM.PGM_GBN 
#                                        , TO_CHAR(NVL((CASE WHEN PGM.TITLE_NM LIKE '%B special%'   THEN 'B special'
#                                        WHEN PGM.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
#                                        WHEN PGM.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
#                                        WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
#                                        ELSE '일반'
#                                        END), NULL)) AS BRAND_PGM_NM
#                                        , A.BROAD_DT 
#                                        , A.BROAD_STR_DTM
#                                        , A.BROAD_END_DTM
#                                        , B.PRD_GRP_NM
#                                        , ITEM.ITEM_CD
#                                        , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1) AS BRAND
#                                        , ITEM.ITEM_NM
#                                        
#                                        , DT.WEKDY_NM
#                                        , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일
#                                        , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)  
#                                        , CASE WHEN RSV.PRD_CD IS NOT NULL THEN 'Y' ELSE 'N' END AS CN_RS_YN
#                                        
#                                        
#                                        , MIN(A.PRD_FORM_SEQ) AS PRD_FORM_SEQ
#                                        , MIN(A.SORT_SEQ)     AS SORT_SEQ
#                                        , SUM(A.EXPCT_REQUIR_TIME_SE) AS RUN_TIME
#                                        , COUNT(DISTINCT A.PRD_CD)    AS CNT_PRDCD
#                                        
#                                        , MIN(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(FST2.FST_BROAD_DT, FST1.FST_BROAD_DT) END) AS FST_BROAD_DTM  --메인상품코드 기준의 최초방송일자
#                                        , AVG(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(A.BROAD_SALE_PRC,PRD.PRD_SALE_PRC) END) AS AVG_PRICE
#                                        , MIN(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(A.BROAD_SALE_PRC,PRD.PRD_SALE_PRC) END) AS MIN_PRICE
#                                        , MAX(CASE WHEN A.MAIN_PRD_YN='Y' THEN NVL(A.BROAD_SALE_PRC,PRD.PRD_SALE_PRC) END) AS MAX_PRICE
#                                        
#                                        
#                                        FROM (
#                                        SELECT A.PGM_ID
#                                        , A.PGM_NM 
#                                        , A.CHANL_CD
#                                        , A.MNFC_GBN_CD
#                                        , A.BROAD_DT
#                                        , A.BROAD_STR_TIME
#                                        , A.BROAD_TIMERNG_CD
#                                        , B.EXPCT_REQUIR_TIME_SE 
#                                        --, C.RL_EXPOS_MI/60 AS EXPCT_REQUIR_TIME_SE  -- ROUND(SUM(RL_EXPOS_MI)/60)
#                                        , A.WEKDY_CD
#                                        , A.BROAD_STR_DTM
#                                        , A.BROAD_END_DTM
#                                        , A.PGM_GBN_CD
#                                        , B.BROAD_SALE_PRC 
#                                        , B.BROAD_YN
#                                        , B.ITEM_SEQ  
#                                        
#                                        , B.PRD_FORM_SEQ
#                                        , B.SORT_SEQ
#                                        
#                                        , B.PRD_CD
#                                        , B.MAIN_PRD_YN
#                                        
#                                        --, A.REG_DTM
#                                        --, A.MOD_DTM
#                                        FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
#                                        , GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B
#                                        --, GSBI_OWN.F_ORD_BROAD_TGT_D C
#                                        WHERE A.BROAD_DT BETWEEN '",STRT_DT,"' AND '",LAST_DT,"'
#                                        AND A.PGM_ID = B.PGM_ID
#                                        AND A.USE_YN = 'Y' 
#                                        AND A.CHANL_CD = 'C'
#                                        AND B.USE_YN = 'Y'
#                                        AND A.MNFC_GBN_CD = '1'
#                                        --AND B.BROAD_YN = 'Y' 
#                                        --AND B.PRD_CD = C.PRD_CD AND A.PGM_ID = C.PGM_ID
#                                        --ORDER BY A.BROAD_STR_DTM, ITEM.ITEM_CD, B.SORT_SEQ
#                                        ) A
#                                        LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON A.PRD_CD = PRD.PRD_CD  
#                                        LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C B ON PRD.ENTPR_PRD_GRP_CD = B.PRD_GRP_CD 
#                                        LEFT JOIN DHUB_OWN.CMM_PGM_M PGM ON A.PGM_ID = PGM.PGM_ID
#                                        LEFT JOIN GSBI_OWN.D_PRD_ITEM_M ITEM ON PRD.ITEM_CD = ITEM.ITEM_CD 
#                                        LEFT JOIN DHUB_OWN.DH_DT DT ON  TO_CHAR(A.BROAD_STR_DTM,'YYYYMMDD') = DT.DT_CD  
#                                        LEFT JOIN 
#                                        ( /*최초방송일자*/
#                                        SELECT  B.PRD_CD, MIN(BROAD_DT) FST_BROAD_DT
#                                        FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
#                                        ,GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B             
#                                        WHERE A.BROAD_DT >= '201601'  AND A.PGM_ID = B.PGM_ID -- 2016년1월~ 현재까지 (BROAD_DT 대신, 실제방송일자를 넣어서 방송한 것까지만 최초방송일자 가져오기)
#                                        AND A.USE_YN = 'Y'
#                                        AND A.CHANL_CD = 'C'
#                                        AND A.MNFC_GBN_CD = '1'
#                                        AND B.USE_YN = 'Y'
#                                        GROUP BY B.PRD_CD
#                                        ) FST1 ON A.PRD_CD = FST1.PRD_CD
#                                        LEFT JOIN 
#                                        (
#                                        SELECT PRD_CD, FST_BROAD_DT FROM GSBI_OWN.D_PRD_PRD_M WHERE FST_BROAD_DT IS NOT NULL
#                                        ) FST2 ON A.PRD_CD = FST2.PRD_CD 
#                                        LEFT JOIN
#                                        (/*상담예약 상품*/
#                                        SELECT PRD_CD
#                                        FROM
#                                        (
#                                        SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_ADDPRC_D GROUP BY PRD_CD--핸드폰
#                                        UNION 
#                                        SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_RSRV_D GROUP BY PRD_CD-- 렌탈, 여행, 가구(시공), 교육문화(일부) 
#                                        UNION
#                                        SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_INSU_D GROUP BY PRD_CD-- 보험
#                                        )
#                                        ) RSV ON A.PRD_CD = RSV.PRD_CD
#                                        
#                                        GROUP BY A.PGM_ID
#                                        , A.PGM_NM, PGM.TITLE_ID, PGM.TITLE_NM
#                                        , PGM.PGM_GBN 
#                                        , TO_CHAR(NVL((CASE WHEN PGM.TITLE_NM LIKE '%B special%'   THEN 'B special'
#                                        WHEN PGM.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
#                                        WHEN PGM.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
#                                        WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
#                                        ELSE '일반'
#                                        END), NULL))
#                                        , A.BROAD_DT 
#                                        , A.BROAD_STR_DTM
#                                        , A.BROAD_END_DTM
#                                        , B.PRD_GRP_NM
#                                        , ITEM.ITEM_CD
#                                        , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1)
#                                        , ITEM.ITEM_NM
#                                        , DT.WEKDY_NM
#                                        , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END)  --1:주말 /  0:평일
#                                        , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)
#                                        , CASE WHEN RSV.PRD_CD IS NOT NULL THEN 'Y' ELSE 'N' END
#                                        
#                                        
#                                        UNION ALL  
#                                        
#                                        
#                                        --PGM_ID PGM_NM TITLE_ID TITLE_NM PGM_GBN BRAND_PGM_NM BROAD_DT BROAD_STR_DTM BROAD_END_DTM
#                                        --PRD_GRP_NM ITEM_CD BRAND ITEM_NM WEKDY_NM WEKDY_YN HOLDY_YN CN_RS_YN PRD_FORM_SEQ SORT_SEQ RUN_TIME
#                                        --CNT_PRDCD FST_BROAD_DTM AVG_PRICE MIN_PRICE MAX_PRICE
#                                        
#                                        
#                                        
#                                        
#                                        SELECT A.PGM_ID
#                                        , 'NOT' AS PGM_NM 
#                                        , A.TITLE_ID, A.TITLE_NM
#                                        , (CASE WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') 
#                                        AND A.TITLE_NM IN ('SHOW me the Trend', 'The Collection',
#                                        '똑소리살림법', '리얼뷰티쇼',
#                                        '왕영은의 톡톡톡', 'B special', '최은경의 W', '테이스티 샵')                   THEN '브랜드PGM' 
#                                        WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') AND A.TITLE_NM = '순환방송(재방송)' THEN '순환방송' 
#                                        ELSE '일반PGM' END)  AS PGM_GBN 
#                                        
#                                        , TO_CHAR(NVL((CASE WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%B special%'   THEN 'B special'
#                                        WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
#                                        WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
#                                        WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
#                                        ELSE '일반'
#                                        END), NULL)) AS BRAND_PGM_NM
#                                        , A.BROAD_DT 
#                                        , A.BROAD_STR_DTM
#                                        , A.BROAD_END_DTM
#                                        , A.PRD_GRP_NM AS PRD_GRP_NM
#                                        , A.ITEM_CD
#                                        , SUBSTR(REGEXP_SUBSTR(A.ITEM_NM,'[^:]+',1,1),1) AS BRAND
#                                        , A.ITEM_NM
#                                        , DT.WEKDY_NM
#                                        , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일 
#                                        , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)
#                                        , CASE WHEN RSV_ITM.ITEM_CD IS NOT NULL THEN 'Y' ELSE 'N' END AS CN_RS_YN
#                                        , A.ITEM_SEQ  AS PRD_FORM_SEQ
#                                        , A.ITEM_SEQ  AS SORT_SEQ
#                                        , A.EXPCT_REQUIR_TIME_SE AS RUN_TIME
#                                        , 1 AS CNT_PRDCD
#                                        , NULL AS FST_BROAD_DTM
#                                        , NULL AS AVG_PRICE
#                                        , NULL AS MIN_PRICE
#                                        , NULL AS MAX_PRICE
#                                        
#                                        FROM (
#                                        
#                                        -- 선편성 아이템 
#                                        SELECT A.PGM_ID 
#                                        , A.TITLE_ID
#                                        , NVL((CASE WHEN A.TITLE_NAME LIKE '%B special%'   THEN 'B special'
#                                        WHEN A.TITLE_NAME LIKE '%최은경의 W%'  THEN '최은경의 W'
#                                        WHEN A.TITLE_NAME LIKE '%테이스티 샵%' THEN '테이스티 샵'
#                                        ELSE A.TITLE_NAME
#                                        END), NULL) AS TITLE_NM
#                                        , A.BROAD_DT
#                                        , A.BROAD_STR_DTM
#                                        , A.BROAD_END_DTM
#                                        , A.PRD_GRP_NM
#                                        , A.ITEM_CD
#                                        , A.ITEM_NM
#                                        , A.ITEM_SEQ 
#                                        , A.EXPCT_REQUIR_TIME_SE
#                                        FROM
#                                        (
#                                        SELECT A.* 
#                                        , COUNT(A.ITEM_CD) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS ITEM_CNT
#                                        , SUM(A.EXPCT_REQUIR_TIME_SE) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS TOT_RUN_TIME
#                                        , MAX(A.MODIFY_DATE) OVER(PARTITION BY A.PGM_ID,A.BROAD_STR_DTM) AS MAX_MODIFY_DATE
#                                        , ROUND(((BROAD_END_DTM) - (BROAD_STR_DTM))*60*24)  AS TIME_GAP
#                                        FROM (
#                                        SELECT A.PGM_ID 
#                                        , A.TITLE_ID
#                                        , A.TITLE_NAME
#                                        , TO_CHAR(A.BROAD_DATE, 'YYYYMMDD') AS BROAD_DT
#                                        , A.BROAD_DATE AS BROAD_STR_DTM
#                                        , A.CLOSING_DATE AS BROAD_END_DTM
#                                        , PRDGRP.PRD_GRP_NM
#                                        , B.ITEM_CODE AS ITEM_CD
#                                        , B.ITEM_NAME AS ITEM_NM
#                                        , B.ITEM_SEQ 
#                                        , B.HOPE_RUN_TIME AS EXPCT_REQUIR_TIME_SE
#                                        
#                                        , B.MODIFY_DATE -- 수정일 
#                                        FROM  SDHUB_OWN.STG_TB_BY100 A
#                                        , SDHUB_OWN.STG_TB_BY212 B
#                                        , SDHUB_OWN.STG_PRD_ITEM_M C
#                                        , GSBI_OWN.V_CMM_PRD_GRP_C PRDGRP
#                                        WHERE A.PGM_ID = B.PGM_ID
#                                        AND A.BROAD_DATE NOT IN (
#                                        SELECT DISTINCT A.BROAD_STR_DTM
#                                        FROM GSBI_OWN.D_BRD_BROAD_FORM_M A
#                                        , GSBI_OWN.D_BRD_BROAD_FORM_PRD_M B
#                                        WHERE A.BROAD_DT BETWEEN '",STRT_DT,"' AND '",LAST_DT,"'
#                                        AND A.PGM_ID = B.PGM_ID
#                                        AND A.USE_YN = 'Y' 
#                                        AND A.CHANL_CD = 'C'
#                                        AND B.USE_YN = 'Y'
#                                        AND A.MNFC_GBN_CD = '1')
#                                        AND B.ITEM_CODE = C.ITEM_CD
#                                        AND C.PRD_GRP_CD = PRDGRP.PRD_GRP_CD
#                                        AND A.BROAD_DATE BETWEEN TO_DATE('",STRT_DT,"', 'YYYYMMDD') AND TO_DATE('",LAST_DT,"', 'YYYYMMDD') + 0.99999
#                                        AND A.MEDIA = 'C'
#                                        AND A.PROD_TYPE = '1'
#                                        AND A.DELETE_YN = 'N'
#                                        ORDER BY BROAD_STR_DTM, ITEM_SEQ
#                                        ) A
#                                        ) A
#                                        WHERE A.MODIFY_DATE = A.MAX_MODIFY_DATE
#                                        ) A
#                                        --ORDER BY A.BROAD_DATE 
#                                        
#                                        -- LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C B ON PRD.ENTPR_PRD_GRP_CD = B.PRD_GRP_CD 
#                                        LEFT JOIN DHUB_OWN.CMM_PGM_M PGM ON A.PGM_ID = PGM.PGM_ID
#                                        LEFT JOIN DHUB_OWN.DH_DT DT ON  TO_CHAR(A.BROAD_STR_DTM,'YYYYMMDD') = DT.DT_CD  
#                                        LEFT JOIN (SELECT DISTINCT PRD.ITEM_CD
#                                        FROM
#                                        (
#                                        SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_ADDPRC_D GROUP BY PRD_CD--핸드폰
#                                        UNION 
#                                        SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_RSRV_D GROUP BY PRD_CD-- 렌탈, 여행, 가구(시공), 교육문화(일부) 
#                                        UNION
#                                        SELECT PRD_CD, COUNT(*) AS CNT FROM GSBI_OWN.F_ORD_INSU_D GROUP BY PRD_CD-- 보험
#                                        ) A
#                                        LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON A.PRD_CD = PRD.PRD_CD
#                                        ) RSV_ITM ON A.ITEM_CD = RSV_ITM.ITEM_CD
#                                        GROUP BY A.PGM_ID
#                                        , 'NOT'
#                                        , A.TITLE_ID, A.TITLE_NM
#                                        , (CASE WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') 
#                                        AND A.TITLE_NM IN ('SHOW me the Trend', 'The Collection',
#                                        '똑소리살림법', '리얼뷰티쇼',
#                                        '왕영은의 톡톡톡', 'B special', '최은경의 W', '테이스티 샵')                   THEN '브랜드PGM' 
#                                        WHEN (PGM.PGM_GBN IS NULL OR PGM.PGM_GBN = '일반PGM') AND A.TITLE_NM = '순환방송(재방송)' THEN '순환방송' 
#                                        ELSE '일반PGM' END) 
#                                        , TO_CHAR(NVL((CASE WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%B special%'   THEN 'B special'
#                                        WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%최은경의 W%'  THEN '최은경의 W'
#                                        WHEN PGM.PGM_GBN IS NULL AND A.TITLE_NM LIKE '%테이스티 샵%' THEN '테이스티 샵'
#                                        WHEN PGM.PGM_GBN ='브랜드PGM' THEN PGM.BRAND_PGM_NM
#                                        ELSE '일반'
#                                        END), NULL))
#                                        , A.BROAD_DT 
#                                        , A.BROAD_STR_DTM
#                                        , A.BROAD_END_DTM
#                                        , A.PRD_GRP_NM
#                                        , A.ITEM_CD
#                                        , SUBSTR(REGEXP_SUBSTR(A.ITEM_NM,'[^:]+',1,1),1)
#                                        , A.ITEM_NM
#                                        , DT.WEKDY_NM
#                                        , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END)  --1:주말 /  0:평일
#                                        , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)
#                                        , CASE WHEN RSV_ITM.ITEM_CD IS NOT NULL THEN 'Y' ELSE 'N' END 
#                                        , A.ITEM_SEQ  
#                                        , A.ITEM_SEQ  
#                                        , A.EXPCT_REQUIR_TIME_SE 
#                                        , 1 
#                                        
#                                        ) A
#                                        ) A
#                                        WHERE (ITEM_CNT = 1 OR RUN_TIME > 0 ) 
#                                        ORDER BY BROAD_STR_DTM, MIN_Q_START_DATE, PRD_FORM_SEQ
#                                        "))
# 
# ###### 편성 데이터 예측에 활용 #####

CALL_SCH_df <- dbGetQuery(conn, paste0("SELECT * FROM DTA_OWN.PRE_SCHDL_TB WHERE ETL_DATE = '",STRT_DT,"'  "  ))

###### 휴일데이터 활용 HOLDY_DT ###### 
HOLDY_DT <- dbGetQuery(conn, paste0("SELECT DISTINCT DT_CD 
                                         FROM DTA_OWN.CMM_STD_DT
                                         WHERE (HOLDY_NM LIKE '구정%' OR HOLDY_NM LIKE '추석%' OR HOLDY_NM LIKE '설날%')
                                         AND DT_CD >= '" , gsub("-", "", STRT_mon), "'
                                         ORDER BY DT_CD")
)
FESTA_YN <- dbGetQuery(conn, paste0("SELECT DT_CD, FESTA_YN 
                                         FROM DTA_OWN.CMM_STD_DT
                                         WHERE DT_CD >= '" , gsub("-", "", STRT_mon), "'
                                         ORDER BY DT_CD")
)


###### 휴일데이터 활용 #####

###### 콜 상품평 CMT_df ######
CMT_df <- dbGetQuery(conn, paste0(" SELECT 
                                       A.PMO_NM, A.PMO_NO, A.PMO_SEQ, A.PMO_STR_DTM, A.PMO_END_DTM
                                       , A.PRD_CD, PRD.PRD_NM, PRD.ITEM_CD, ITEM_NM
                                       , PRD.SRCNG_GBN_CD AS SRCH
                                       , B.PRD_GRP_NM
                                       FROM (
                                       SELECT A.PMO_SEQ, A.MOD_DTM, A.PMO_NO, A.CHANL_CD, A.CATV_YN, A.TC_YN, A.EC_YN, A.DM_YN, A.MC_YN, A.SNRM_YN, A.PMO_NM, A.PMO_STR_DTM,
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
                                       
                                       WHERE PRD.SRCNG_GBN_CD = 'CA'  
                                       "))
###### 콜 상품평 ######

RMSE <- function(m, o){ sqrt(mean((m - o)^2))}  
MAPE <- function(m, o){ n <- length(m)
res <- (100 / n) * sum(abs(m-o)/m)
res  }


dbDisconnect(conn)
rm(conn,drv)

range(CALL_BRD_df$MIN_Q_START_DATE)
range(CALL_HH_df$기준일자)
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
# rm(which_ptb, diff_v, check_df, which_out)
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
# rm(A,B,which_ptb, diff_v, check_df, which_out)
CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$PRD_GRP_NM), ]
#----------------------------------------------------------------
# 주 편성표 결과 저장 2~5 시 돌리는 경우만 
#----------------------------------------------------------------

# end <- 1
# if (end == 1) { 


print(paste(" EDA 마트생성시작 ", as.character(Sys.time())))
#----------------------------------------------------------------
# 2. EDA 수행  
#----------------------------------------------------------------




# oz 테이블 데이터 변수 생성 
CALL_HH_df <- data.table(CALL_HH_df)[,.(기준일자 , 기준시간, 총_인입콜수, ARS_인입콜수, 상담원_인입콜수, 상담원_주문_인입콜수, 상담원_SR_인입콜수)]
CALL_HH_df <- CALL_HH_df[order(기준일자 , 기준시간)]
CALL_HH_df$기준시간 <- gsub(" ", "", CALL_HH_df$기준시간)
CALL_HH_df$기준시간 <- ifelse(nchar(CALL_HH_df$기준시간) == 4, paste0("0", CALL_HH_df$기준시간), CALL_HH_df$기준시간)

CALL_HH_df <- CALL_HH_df[, HH := as.numeric(substr(기준시간, 1, 2)) ]
CALL_HH_df$HH <- ifelse(CALL_HH_df$HH == 0, 24, CALL_HH_df$HH)
CALL_HH_df <- na.omit(CALL_HH_df)
CALL_HH_df <- CALL_HH_df[ , WEK := wday(ymd(기준일자))]
CALL_HH_df <- CALL_HH_df[ , WEK := WEK - 1 ]
CALL_HH_df$WEK <- ifelse(CALL_HH_df$WEK == 0, 7, CALL_HH_df$WEK)
CALL_HH_df <- CALL_HH_df[ , WEK_G := ifelse(WEK == 1, "1.월", 
                                            ifelse(WEK == 2, "2.화",
                                                   ifelse(WEK == 3, "3.수",
                                                          ifelse(WEK == 4, "4.목",
                                                                 ifelse(WEK == 5, "5.금",
                                                                        ifelse(WEK == 6, "6.토","7.일")))))) ] 
CALL_HH_df <- CALL_HH_df[ , WEK := ifelse(WEK %in% c(1:5), "1.주중", 
                                          ifelse(WEK == 6, "2.토","3.일")) ]
CALL_HH_df <- CALL_HH_df[ , mon := ymd(paste0(substr(기준일자, 1, 6), "-01"))]
# 개월별 시간대별 
CALL_df <- CALL_HH_df[ , .(
  전달시콜  =  round(mean(총_인입콜수)),
  전달시ARS =  round(mean(ARS_인입콜수)),
  전달상담  =  round(mean(상담원_인입콜수)),
  전달상담비  =  round( sum(상담원_인입콜수)/sum(총_인입콜수) , 2),
  
  전달상담주문 = round(mean(상담원_주문_인입콜수)),
  전달상담SR = round(mean(상담원_SR_인입콜수))), 
  
  by = c("mon", "WEK", "HH")]
CALL_df <- CALL_df[order(mon, WEK, HH)]
# 개월별 시간대별 + 요일별 
CALL_df2 <- CALL_HH_df[ , .(
  전달시콜  =  round(mean(총_인입콜수)),
  전달시ARS =  round(mean(ARS_인입콜수)),
  전달상담  =  round(mean(상담원_인입콜수)),
  전달상담비  =  round( sum(상담원_인입콜수)/sum(총_인입콜수) , 2),
  
  전달상담주문 = round(mean(상담원_주문_인입콜수)),
  전달상담SR = round(mean(상담원_SR_인입콜수))), 
  
  by = c("mon", "WEK_G", "HH")]
CALL_df2 <- CALL_df2[order(mon, WEK_G, HH)]

# CALL 인입 예측 
CALL_BRD_df <- data.table(CALL_BRD_df)
CALL_SCH_df <- data.table(CALL_SCH_df)
# 순서 
CALL_BRD_df <- CALL_BRD_df[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_SCH_df <- CALL_SCH_df[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_BRD_df$no <- 1:nrow(CALL_BRD_df)
CALL_SCH_df$no <- (nrow(CALL_BRD_df) + 1 ):(nrow(CALL_BRD_df) + nrow(CALL_SCH_df) )
CALL_SCH_df$BROAD_HH <- substr(CALL_SCH_df$BROAD_STR_DTM, 12,13)

#
names(CALL_BRD_df)[which(names(CALL_BRD_df) %in% c("ANI_CALL", "ANI_CALL_ARS", "ANI_CALL_CNSL", "ANI_CALL_ORD", "ANI_CALL_SR") )] <- 
  c("총인콜수_M", "인입콜_ARS_M", "인입콜_상담_M", "인입콜_주문_M", "인입콜_SR_M")
names(CALL_BRD_df)[which(names(CALL_BRD_df) %in% c("BROAD_PRD_CNT"))] <- "CNT_PRDCD"
names(CALL_BRD_df)[which(names(CALL_BRD_df) %in% c("AVG_BROAD_SALE_PRC"))] <- "AVG_PRICE"
names(CALL_BRD_df)[which(names(CALL_BRD_df) %in% c("FST_BROAD_DT"))] <- "FST_BROAD_DTM"
CALL_BRD_df$SUM_RUNTIME <- round(CALL_BRD_df$SUM_RUNTIME/60)



# 백업용 
CALL_BRD_BACK_df <- CALL_BRD_df
CALL_BRD_df    <- CALL_BRD_BACK_df[, .(no, BROAD_DT, BROAD_HH, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME, 
                                       PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                       ITEM_CD, ITEM_NM, BRAND, WEKDY_NM, WEKDY_YN, HOLDY_YN, CN_RS_YN, CNT_PRDCD, FST_BROAD_DTM,
                                       AVG_PRICE, 
                                       PRD_GRP_NM, 
                                       T_CNT_ONAIR, T_CNT_ONAIR_MCPC, T_CNT_PRE, 
                                       총인콜수_M, 인입콜_ARS_M, 인입콜_상담_M, 인입콜_주문_M, 인입콜_SR_M
)] 

CALL_BRDSCH_df <- CALL_SCH_df[, .(no, BROAD_DT, BROAD_HH, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME, 
                                  PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                  ITEM_CD, ITEM_NM, BRAND, WEKDY_NM, WEKDY_YN, HOLDY_YN, CN_RS_YN, CNT_PRDCD, FST_BROAD_DTM,
                                  AVG_PRICE, 
                                  PRD_GRP_NM)] 
# 
CALL_BRD_df <- rbind.fill(CALL_BRD_df, CALL_BRDSCH_df)
CALL_BRD_df <- data.table(CALL_BRD_df)
CMT_df <- data.table(CMT_df)


# 표준화 총콜
CALL_BRD_df <- CALL_BRD_df[ , 분콜 := round(총인콜수_M / SUM_RUNTIME) ]
CALL_BRD_df <- CALL_BRD_df[ , 분상주콜 := round(인입콜_주문_M / SUM_RUNTIME) ]
CALL_BRD_df <- CALL_BRD_df[ , 분상SR콜 := round(인입콜_SR_M / SUM_RUNTIME) ]
# 상품 시작시간
CALL_BRD_df <- CALL_BRD_df[ , 노출HH := substr(MIN_Q_START_DATE, 12, 13) ]
CALL_BRD_df$노출HH <- as.numeric(CALL_BRD_df$노출HH)
CALL_BRD_df$노출HH <- ifelse(CALL_BRD_df$노출HH == 0, 24, CALL_BRD_df$노출HH)


# 날짜 
# CALL_BRD_df <- CALL_BRD_df[!is.na(MIN_Q_START_DATE) , ]
CALL_BRD_df$BROAD_DT  <- ymd(CALL_BRD_df$BROAD_DT)
CALL_BRD_df$BRD_DAYT  <- ymd_h(substr(paste(CALL_BRD_df$BROAD_DT, CALL_BRD_df$BROAD_HH) , 1, 13))

CALL_BRD_df$MIN_Q_START_DATE  <- ymd_hms(CALL_BRD_df$MIN_Q_START_DATE)
CALL_BRD_df$MAX_Q_END_DATE  <- ymd_hms(CALL_BRD_df$MAX_Q_END_DATE)

### 회차 먹이기 
CALL_BRD_df <- CALL_BRD_df[SUM_RUNTIME != 0 ,]
# 순서 
CALL_BRD_df <- CALL_BRD_df[order(BROAD_DT, MIN_Q_START_DATE)]
# 아이템, 브랜드, 상품분류
CALL_BRD_df <- CALL_BRD_df[, IT_SF := seq(.N), by = c("ITEM_NM")]

CALL_BRD_df <- CALL_BRD_df[ , 모건비 := round(T_CNT_ONAIR_MCPC/T_CNT_ONAIR, 3) ]
CALL_BRD_df <- CALL_BRD_df[ , WEK := ifelse(WEKDY_NM == "Sun", "3.일",
                                            ifelse(WEKDY_NM == "Sat", "2.토", "1.주중")) ]

CALL_BRD_df <- CALL_BRD_df[ , 미리비 := round(T_CNT_PRE/(T_CNT_PRE + T_CNT_ONAIR), 3) ]
CALL_BRD_df <- CALL_BRD_df[ , 상담비 := round(인입콜_상담_M/총인콜수_M, 3) ]
CALL_BRD_df <- CALL_BRD_df[ , 상담SR비 := round(인입콜_SR_M/총인콜수_M, 3) ]
CALL_BRD_df <- CALL_BRD_df[ , 상상담SR비 := round(인입콜_SR_M/인입콜_상담_M, 3) ]

# 상담여부 
names(CALL_BRD_df)[which(names(CALL_BRD_df) == "CN_RS_YN")] <- "상담"
# 신상여부
CALL_BRD_df[is.na(FST_BROAD_DTM) | FST_BROAD_DTM == "99999999", "FST_BROAD_DTM"] <- "99991231"   
CALL_BRD_df <- CALL_BRD_df[ , 신상 := ifelse(BROAD_DT == ymd(substr(FST_BROAD_DTM,1,8)), "YES", "NO" )]
CALL_BRD_df[ is.na(상상담SR비) , "상상담SR비"] <- 0
CALL_BRD_df[ is.na(모건비) , "모건비"] <- 0

### 추가변수
# 아이템명 우측 생성 ITEM_NM_SEP, 좌측 BRAND 
CALL_BRD_df <- CALL_BRD_df[ , ITEM_NM_SEP := unlist(strsplit(ITEM_NM, "[:]"))[2], by = "no" ]
# 순서 
CALL_BRD_df <- CALL_BRD_df[order(BROAD_DT, MIN_Q_START_DATE)]
# 아이템, 브랜드, 상품분류
CALL_BRD_df <- CALL_BRD_df[, BR_SF := seq(.N), by = c("BRAND")]
CALL_BRD_df <- CALL_BRD_df[, IT2_SF := seq(.N), by = c("ITEM_NM_SEP")]
CALL_BRD_df <- CALL_BRD_df[, PG_SF := seq(.N), by = c("PRD_GRP_NM")]


PUMP_CMT_df <- CMT_df[PMO_STR_DTM >= ymd("2017-01-01") & SRCH == "CA" ,]
PUMP_CMT_df <- unique(PUMP_CMT_df[, .(PMO_NO, PMO_STR_DTM, PMO_END_DTM, ITEM_CD , ITEM_NM )])
PUMP_CMT_df <- PUMP_CMT_df[order(PMO_STR_DTM)]
PUMP_CMT_df <- PUMP_CMT_df[, SEQ := 1:nrow(PUMP_CMT_df)]

# 프로모션 상품평 테이블 
pump_cmt_all_df <- do.call(rbind, lapply(1:nrow(PUMP_CMT_df) , function(i) {
  # i <- 1
  print(i)
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
CALL_BRD_df <- merge(CALL_BRD_df, pump_df, by = c("BROAD_DT", "ITEM_CD", "ITEM_NM"), all.x = T)
CALL_BRD_df[is.na(CALL_BRD_df$PROMO_CNT), "PROMO_CNT"] <- 0
CALL_BRD_df$PROMO_CNT_G <- ifelse(CALL_BRD_df$PROMO_CNT > 0 , 1, 0 )




# 모델 생성 데이터 
MDL_DT <- "2017-01-01"
FIND_PAST_df <- CALL_BRD_df[BROAD_DT >= ymd(MDL_DT), ]

# 일자 갭 계산 
GRP <- data.table(data.frame(DG = 1:(360*10), DGG = rep(seq(30, (360*10), by = 30), each = 30)))
# 
DT_df <- data.frame(DT = seq(ymd("2016-01-01"), ymd(substr(SYSDATE,1,10)) + 365, by =  "1 month") )
DT_df$DT_VF <- c(ymd("2015-12-01"), lag(DT_df$DT)[-1])

# CALL_BRD_df %>% filter(BROAD_DT < ymd(STRT_DT) - 10 & is.na(상상담SR비))
# which(result_df$FIND_YN == "NO")
# 228   414  2735  4043 16285

# 직전, 이전 데이터 결합  nrow(FIND_PAST_df)
system.time(
  result_df <- do.call(rbind.fill, mclapply(1: nrow(FIND_PAST_df)  , function(i) { 
    
    # i <- 16285
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
    
    # 편성표
    day_seq <- data.frame(day = tmp1$BROAD_DT - seq(1,30, by = 1))
    day_seq$WEKPT <- wday(day_seq$day)
    (day_chk <- day_seq$day[10])
    
    if(PGM == "브랜드PGM") {
      # item 
      sub_df <- CALL_BRD_df[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM,]
      if (nrow(sub_df) == 0) {
        sub_df <- CALL_BRD_df[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == "일반PGM", ]
      } 
      sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
      sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
      # 과거말일 이전 데이터만 추출 
      sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    } else {
      # item 
      sub_df <- CALL_BRD_df[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM,]
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
    
    new_yo <- 0
    if (new_yo == 1) {
      # 이렇게 해도 못찾았다면! 아이템명(브랜드명제외) 시간대 매칭! 
      if (nrow(sub_df2) == 0) {
        
        if(PGM == "브랜드PGM") {
          # item 
          sub_df <- CALL_BRD_df[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
          if (nrow(sub_df) == 0) {
            sub_df <- CALL_BRD_df[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == "일반PGM", ]
          } 
          sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
          sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
          # 과거말일 이전 데이터만 추출 
          sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
        } else {
          # item 
          sub_df <- CALL_BRD_df[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
          sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
          sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
          # 과거말일 이전 데이터만 추출 
          sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
        }
        
        
        # 상품군 & 주중/주말 및 시간  일치 
        sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]  
        sub_df2$HHG <- 200 
        if (nrow(sub_df2) == 0) {  
          # +- 1시간 
          sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
          sub_df2$HHG <- 210
          if (nrow(sub_df2) == 0) {
            # +- 2시간 
            sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
            sub_df2$HHG <- 220
            if (nrow(sub_df2) == 0) {
              # +- 4시간 
              sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
              sub_df2$HHG <- 230
            } 
          }
        }
        
        
        # 주중/주말포기! 상품군 & 시간  일치 
        if (nrow(sub_df2) == 0) {
          
          # 시간  일치 
          sub_df2 <- sub_df[노출HH == hh  , ]
          sub_df2$HHG <- 2000
          if (nrow(sub_df2) == 0) {  
            # +- 1시간 
            sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
            sub_df2$HHG <- 2100
            if (nrow(sub_df2) == 0) {
              # +- 2시간 
              sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
              sub_df2$HHG <- 2200
              if (nrow(sub_df2) == 0) {
                # +- 4시간 
                sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
                sub_df2$HHG <- 2300
              } 
            }
          }
          
        }   
        
      }
      
      # 이렇게 해도 못찾았다면! 브랜드명 시간대 매칭! 
      if (nrow(sub_df2) == 0) {
        
        if(PGM == "브랜드PGM") {
          # item 
          sub_df <- CALL_BRD_df[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
          if (nrow(sub_df) == 0) {
            sub_df <- CALL_BRD_df[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == "일반PGM", ]
          } 
          sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
          sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
          # 과거말일 이전 데이터만 추출 
          sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
        } else {
          # item 
          sub_df <- CALL_BRD_df[BR_SF < tmp1$BR_SF & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
          sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
          sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
          # 과거말일 이전 데이터만 추출 
          sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
        }
        
        
        # 상품군 & 주중/주말 및 시간  일치 
        sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
        sub_df2$HHG <- 300
        if (nrow(sub_df2) == 0) {  
          # +- 1시간 
          sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
          sub_df2$HHG <- 310
          if (nrow(sub_df2) == 0) {
            # +- 2시간 
            sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
            sub_df2$HHG <- 320
            if (nrow(sub_df2) == 0) {
              # +- 4시간 
              sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
              sub_df2$HHG <- 330
            } 
          }
        }
        
        
        # 주중/주말포기! 상품군 & 시간  일치 
        if (nrow(sub_df2) == 0) {
          
          # 시간  일치 
          sub_df2 <- sub_df[노출HH == hh  , ]
          sub_df2$HHG <- 3000
          if (nrow(sub_df2) == 0) {  
            # +- 1시간 
            sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
            sub_df2$HHG <- 3100
            if (nrow(sub_df2) == 0) {
              # +- 2시간 
              sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
              sub_df2$HHG <- 3200
              if (nrow(sub_df2) == 0) {
                # +- 4시간 
                sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
                sub_df2$HHG <- 3300
              } 
            }
          }
          
        }   
        
      }
    }
    
    
    # 이렇게 해도 못찾았다면! 상품군 시간대 매칭! 
    if (nrow(sub_df2) == 0) {
      
      if(PGM == "브랜드PGM") {
        # item 
        sub_df <- CALL_BRD_df[PG_SF < tmp1$PG_SF & PRD_GRP_NM == tmp1$PRD_GRP_NM & PGM_GBN == PGM,]
        if (nrow(sub_df) == 0) {
          sub_df <- CALL_BRD_df[PG_SF < tmp1$PG_SF & PRD_GRP_NM == tmp1$PRD_GRP_NM & PGM_GBN == "일반PGM", ]
        } 
        sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
        sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
        # 과거말일 이전 데이터만 추출 
        sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
      } else {
        # item 
        sub_df <- CALL_BRD_df[PG_SF < tmp1$PG_SF & PRD_GRP_NM == tmp1$PRD_GRP_NM & PGM_GBN == PGM,]
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
        tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달시콜, 전달상담, 전달상담주문, 전달상담SR, 전달상담비)]
      } else {
        tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달시콜, 전달상담, 전달상담주문, 전달상담SR, 전달상담비)]  
      }
      # tmp1[,.(총인콜수_M, SUM_RUNTIME)]; tmp2 
      # 
      tmp <- cbind.data.frame(tmp1,  tmp2)
      tmp$FIND_YN <- "NO"
      
    } else {  
      
      # 기간 
      subset_df <- head(sub_df2[order(IT_SF, decreasing = T)],6)
      subset_df <- subset_df[,   .(
        평_노     = round(mean(SUM_RUNTIME, na.rm = T)), 
        평_총콜   = round(mean(총인콜수_M, na.rm = T)), 
        평_분콜   = round(mean(분콜, na.rm = T)), 
        평_모건비 = round(mean(모건비, na.rm = T) * 100), 
        평_상담비 = round(mean(상담비, na.rm = T) * 100), 
        평_상담SR비 = round(mean(상담SR비, na.rm = T) * 100), 
        평_상상담SR비 = round(mean(상상담SR비, na.rm = T) * 100), 
        평_가격   = round(mean(AVG_PRICE, na.rm = T)), 
        평_상품수 = round(mean(CNT_PRDCD, na.rm = T)),
        평_총건   = round(mean(T_CNT_ONAIR, na.rm = T)),
        #
        평_상콜   = round(mean(인입콜_상담_M, na.rm = T)),
        평_상주문콜   = round(mean(인입콜_주문_M, na.rm = T)),
        평_상SR콜 = round(mean(인입콜_SR_M, na.rm = T)),
        평_총미리비 = round(mean(미리비, na.rm = T)* 100),
        
        # 평_분상주콜   = round(mean(분상주콜, na.rm = T)),
        # 평_분상SR콜   = round(mean(분상SR콜, na.rm = T)),
        
        
        직_노     = round((SUM_RUNTIME[1])), 
        직_총콜   = round((총인콜수_M[1])), 
        직_분콜   = round((분콜[1])), 
        직_모건비 = round((모건비[1]) * 100), 
        직_상담비 = round((상담비[1]) * 100), 
        직_상담SR비 = round((상담SR비[1]) * 100), 
        직_상상담SR비 = round((상상담SR비[1]) * 100), 
        직_가격   = round((AVG_PRICE[1])), 
        직_상품수 = round((CNT_PRDCD[1])),
        직_총건   = round((T_CNT_ONAIR[1])),
        #
        직_상콜   = round(인입콜_상담_M[1]),
        직_상주문콜   = round((인입콜_주문_M[1])),
        직_상SR콜 = round(인입콜_SR_M[1]),
        직_총미리비 = round((미리비[1])* 100),
        
        # 직_분상주콜   = round((분상주콜[1])),
        # 직_분상SR콜   = round((분상SR콜[1])),
        
        
        HHG       = unique(HHG),
        최근방송일= as.numeric(day - BROAD_DT[1]) )] 
      
      # 전달 
      if ( substr(mnt,1,7) != substr(SYSDATE,1,7) ) {
        mnt_bf <- DT_df[which(DT_df$DT == mnt) - 1, ]$DT_VF # 방송전월  
        tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달시콜, 전달상담, 전달상담주문, 전달상담SR, 전달상담비)]
      } else {
        tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달시콜, 전달상담, 전달상담주문, 전달상담SR, 전달상담비)]  
      }
      
      
      # tmp1[,.(총인콜수_M, SUM_RUNTIME)]; tmp2 
      # 
      tmp <- cbind.data.frame(tmp1, subset_df, tmp2)
      tmp$FIND_YN <- "YES"
      
    }
    
    # print(paste(i, nrow(FIND_PAST_df), " // ",unique(sub_df2$HHG) , " // ",unique(tmp$FIND_YN) ))
    
    tmp 
    
  }, mc.cores = 10) )
)
rm(GRP, DT_df, FIND_PAST_df)
# user  system elapsed 
# 670.928  20.601  91.726 


# 0802일 기준 
# table(result_df$FIND_YN) ; # 5.8% ; table(result_df$FIND_YN)/sum(table(result_df$FIND_YN) )
# NO   YES 
# 1015 16458
# table(result_df$FIND_YN, result_df$HHG) ; # 94.2%
#        0    1    2    3   10   11   12   13  100  110  120  130 1000 1100 1200 1300
# NO     0    0    0    0    0    0    0    0    0    0    0    0    0    0    0    0
# YES 9728 2533  738  533  575  338  151  146 1356  180   57   32   56   24    8    3
# table(result_df$FIND_YN, result_df$신상)
#        NO   YES
# NO      2  1013
# YES    23 16435

# save.image(file = paste0("/home/DTA_CALL/HAND1.RData"))
# load(file = paste0("/home/DTA_CALL/HAND1.RData"))


names(result_df)

CALL_VAR_df <- result_df %>% dplyr::select(no, BROAD_DT, WEKDY_NM, WEK, BRD_DAYT, MIN_Q_START_DATE, MAX_Q_END_DATE,노출HH, HOLDY_YN,
                                           #
                                           IT_SF, BR_SF, IT2_SF, PG_SF, ITEM_CD, ITEM_NM, ITEM_NM_SEP, BRAND, PRD_GRP_NM,   
                                           # 
                                           PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                           # 
                                           CNT_PRDCD, AVG_PRICE, SUM_RUNTIME, 
                                           # 
                                           총인콜수_M, 인입콜_ARS_M, 인입콜_상담_M, 인입콜_주문_M, 인입콜_SR_M, 분콜, 
                                           #
                                           평_노, 평_총콜, 평_분콜, 평_모건비, 평_상담비, 평_가격, 평_상품수, 평_총건, 평_상담SR비, 평_상상담SR비, 평_상콜, 평_상주문콜, 평_상SR콜,
                                           #
                                           직_노, 직_총콜, 직_분콜, 직_모건비, 직_상담비, 직_가격, 직_상품수, 직_총건, 직_상담SR비, 직_상상담SR비, 직_상콜, 직_상주문콜, 직_상SR콜,
                                           #
                                           HHG, 최근방송일, 전달시콜, 전달상담, 전달상담주문, 전달상담SR, 전달상담비, FIND_YN, 상담, 신상,
                                           #
                                           모건비, 
                                           ## 추가변수 
                                           미리비, 
                                           평_총미리비, 
                                           직_총미리비,
                                           PROMO_CNT,
                                           PROMO_CNT_G
                                           
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

CALL_VAR_df <- CALL_VAR_df[, 노_전달시콜 := round((전달시콜/60) * SUM_RUNTIME) ]
CALL_VAR_df <- CALL_VAR_df[, 노_전달상담 := round((전달상담/60) * SUM_RUNTIME) ]          #
CALL_VAR_df <- CALL_VAR_df[, 노_전달상담주문 := round((전달상담주문/60) * SUM_RUNTIME) ]  #
CALL_VAR_df <- CALL_VAR_df[, 노_전달상담SR := round((전달상담SR/60) * SUM_RUNTIME) ]      #

CALL_VAR_df <- CALL_VAR_df[, 노_평_분콜 := round((평_분콜) * SUM_RUNTIME) ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분콜 := round((직_분콜) * SUM_RUNTIME) ]

#
CALL_VAR_df <- CALL_VAR_df[, 노_평_분상콜 := round((평_상콜/평_노) * SUM_RUNTIME) ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분상콜 := round((직_상콜/직_노) * SUM_RUNTIME) ]
#
CALL_VAR_df <- CALL_VAR_df[, 노_평_분상주문콜 := round((평_상주문콜/평_노) * SUM_RUNTIME) ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분상주문콜 := round((직_상주문콜/직_노) * SUM_RUNTIME) ]
#
CALL_VAR_df <- CALL_VAR_df[, 노_평_분상SR콜 := round((평_상SR콜/평_노) * SUM_RUNTIME) ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분상SR콜 := round((직_상SR콜/직_노) * SUM_RUNTIME) ]



# 브랜드 PGM 
CALL_VAR_df$BRAND_PGM_GRP <- ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("B special", "The Collection"   ), "1.B스페샬_더컬", 
                                    ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("최은경의 W", "왕영은의 톡톡톡", "SHOW me the Trend"), "2.왕톡_쇼미_최W",
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
CALL_VAR_df <- CALL_VAR_df[order(no)]
# CALL_VAR_df[is.na(CALL_VAR_df)] <- 0

# 휴일데이터 결합 
# HOLDY_DT <- ymd(paste0(HOLDY_DT$YEAR_MON_CD, "01")) 
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
CALL_VAR_df <- CALL_VAR_df[ , 직템__평_총콜 := c(평_총콜[1], lag(평_총콜)[-1] ) ] 
CALL_VAR_df <- CALL_VAR_df[ , 직템__평_상주문콜 := c(평_상주문콜[1], lag(평_상주문콜)[-1] ) ] 
CALL_VAR_df <- CALL_VAR_df[ , 직템__평_상SR콜 := c(평_상SR콜[1], lag(평_상SR콜)[-1] ) ] 

# par(mfrow = c(1,3))
# plot(CALL_VAR_df$총인콜수_M,CALL_VAR_df$직템__평_총콜, main = cor(as.matrix(na.omit(data.frame(y=CALL_VAR_df$총인콜수_M,x = CALL_VAR_df$직템__평_총콜))))); grid()
# plot(CALL_VAR_df$인입콜_주문_M,CALL_VAR_df$직템__평_상주문콜, main = cor(as.matrix(na.omit(data.frame(y=CALL_VAR_df$인입콜_주문_M,x = CALL_VAR_df$직템__평_상주문콜))))); grid()
# plot(CALL_VAR_df$인입콜_SR_M,CALL_VAR_df$직템__평_상SR콜, main = cor(as.matrix(na.omit(data.frame(y=CALL_VAR_df$인입콜_SR_M,x = CALL_VAR_df$직템__평_상SR콜))))); grid()

setDT(FESTA_YN)
FESTA_YN$BROAD_DT <- ymd(FESTA_YN$DT_CD)
CALL_VAR_df <- merge(CALL_VAR_df, FESTA_YN[,.(BROAD_DT, FESTA_YN)], by = c("BROAD_DT"), all.x = T)

CALL_VAR_df <- CALL_VAR_df[ , 날짜순서 := substr(BROAD_DT, 9, 10)]
CALL_VAR_df <- CALL_VAR_df[ , 날짜순서 := (ifelse(FESTA_YN != "Y" , "00", 날짜순서))]
CALL_VAR_df <- CALL_VAR_df[ , 날짜순서 := as.factor(ifelse(날짜순서 >= "11" , "11", 날짜순서))]



#----------------------------------------------------------------
print(paste(" EDA 마트생성종료 ", as.character(Sys.time())))


#----------------------------------------------------------------
# 3. 모델링 수행 
#----------------------------------------------------------------
# 실 테스트 
# wek <- ymd(STRT_DT)
wek <- ymd(substr(SYSDATE, 1, 10))
wek2 <- wek + 11

if (TEST_YN == "YES") {
  wek <- seq.Date(ymd( "2018-05-03"), ymd( "2018-08-15") , by = "1 days")
  wek2 <- wek + 11
}

#----------------------------------------------------------------
# 모델 공식 
#----------------------------------------------------------------
# CALL_VAR_df %>% filter(HOLDY_SEQ != "A.연휴아님") %>% group_by(BROAD_DT,HOLDY_SEQ) %>% summarise(n())
# CALL_VAR_df %>% filter(HOLDY_YN2 != "N") %>% group_by(BROAD_DT, HOLDY_YN, HOLDY_YN2) %>% summarise(n())

# 1.
var_nm <- c(
  "CNT_PRDCD", "AVG_PRICE", "SUM_RUNTIME",
  "평_노", "평_총콜", "평_분콜", "평_모건비", "평_상담비", "평_가격", "평_상품수", "평_총건",
  #
  "직_노", "직_총콜", "직_분콜", "직_모건비", "직_상담비", "직_가격", "직_상품수", "직_총건",
  #
  "최근방송일", "전달시콜", "전달상담비", 
  #
  "평_가격갭", "직_가격갭", "평_상품갭", "직_상품갭", "평_노출갭", "직_노출갭", "노_전달시콜", "노_평_분콜", "노_직_분콜",
  #
  "평_상담SR비",  "평_상콜", "평_상주문콜", "평_상SR콜", 
  "직_상담SR비",  "직_상콜", "직_상주문콜", "직_상SR콜",  
  "노_평_분상콜", "노_직_분상콜", "노_평_분상주문콜", "노_직_분상주문콜", "노_평_분상SR콜" , "노_직_분상SR콜", 
  #
  "FIND_YN", "상담", "신상", "WEK",  "BRAND_PGM_NM", "신상_IT", "노출HH",  
  # 
  "PRD_GRP_NM", "HOLDY_YN2", "HOLDY_SEQ",
  "직템__평_총콜", "직템__평_상주문콜", "직템__평_상SR콜",
  "평_총미리비", 
  "직_총미리비",
  "PROMO_CNT_G") # , "PROMO_CNT" "FESTA_YN", , "날짜순서"
# 
if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {
  var_nm <- c(var_nm, "FESTA_YN", "날짜순서")
}

# formula1 <- formula(paste("총인콜수_M ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)" ), collapse = "+")))
# formula2 <- formula(paste("log10(총인콜수_M) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)"), collapse = "+")))

formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)", "I(AVG_PRICE^3)","I(SUM_RUNTIME^2)", "I(노_전달시콜^2)",
                                                       "I(평_분콜^2)", "I(전달상담비^2)", "I(직_분콜^2)",  "I(평_상SR콜^2)",
                                                       "I(평_상담비^2)"), collapse = "+")))



fld_nm <- "error1"


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
# 
mdl_df <- data.frame(CALL_VAR_df)
mdl_df[is.na(mdl_df)] <- 0
# 

rm(HOLDY_DT, i)
# quantile(mdl_df$직_가격갭, prob = seq(0,1,0.01), na.rm = T)
# quantile(mdl_df$직_분콜, prob = seq(0,1,0.01), na.rm = T)
# quantile(mdl_df$평_분콜, prob = seq(0,1,0.01), na.rm = T)
# quantile(mdl_df$직_분콜, prob = seq(0,1,0.01), na.rm = T)
#----------------------------------------------------------------
head(mdl_df) 
# 
# 총인콜수_M 인입콜_ARS_M 인입콜_상담_M 인입콜_주문_M 인입콜_SR_M
# 총_인입콜수 ARS_인입콜수 상담원_인입콜수 상담원_주문_인입콜수 상담원_SR_인입콜수
MDL_GBN <- data.frame(GBN = 1:4, 
                      GBN_NM = as.character(c("총콜", "주문", "SR", "상담")),
                      MART = as.character(c("총인콜수_M", "인입콜_주문_M", "인입콜_SR_M", "인입콜_상담_M")), 
                      OZ = as.character(c("총_인입콜수", "상담원_주문_인입콜수", "상담원_SR_인입콜수", "상담원_인입콜수")))  
for(i in 2:4){MDL_GBN[,i] <- as.character(MDL_GBN[,i])}

if (TEST_YN == "YES") {
  wek_df <- data.frame(strt_day = wek, last_day = wek2) 
  wek_df$mdl_dt <- rep(seq.Date(wek_df$strt_day[1], last(wek_df$strt_day) , by = "7 days"), each = 7)[1:nrow(wek_df)]  
  
} else { 
  wek_df <- data.frame(strt_day = wek, 
                       last_day = wek2)
  if (wday(ymd(wek_df$strt_day) ) == 5) {
    wek_df$mdl_dt <- gsub( "-", "", wek_df$strt_day)
    
  } else {
    wek_df$mdl_dt <- max(substr(list.files( paste0("/home/DTA_CALL/WEK_MDL_BACKUP/") )[grep("ULTRA_", list.files( paste0("/home/DTA_CALL/WEK_MDL_BACKUP/") ))], 16, 23))
    
  }
  
  # wek_df$mdl_dt =  seq.Date(wek_df$strt_day - 6, last(wek_df$strt_day) , by = "1 days")[
  #                        which(wday(seq.Date(wek_df$strt_day - 6, last(wek_df$strt_day) , by = "1 days")) == 5)]   
}  
print(wek_df)

# 결과 백업2 
# save.image(file = paste0("/home/DTA_CALL/HAND1.RData" ))
# load(file = paste0("/home/DTA_CALL/HAND1.RData" ))

print("모델링 시작")

make_mdl <- 5
Final_df <- do.call(rbind, lapply(1:3, function(GBN) {  
  # GBN <- 1
  
  GBN_NM <- MDL_GBN$GBN_NM[MDL_GBN$GBN == GBN]
  GBN_MART <- MDL_GBN$MART[MDL_GBN$GBN == GBN]
  GBN_OZ <- MDL_GBN$OZ[MDL_GBN$GBN == GBN]
  names(mdl_df)[which(names(mdl_df) == GBN_MART)] <- "Y_REAL"
  
  print(paste( GBN_NM, GBN_MART   ))
  
  # lapply(1: length(wek), function(clu) {
  
  Predict_df <- do.call(rbind, lapply(1:length(wek), function(clu) {
    # clu <- 1
    
    #
    # mdl_df <- mdl_df[mdl_df$BROAD_DT < ymd("2018-08-08"),]
    # train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < "2018-08-02",] %>% filter(Y_REAL > 0))
    
    train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < wek[clu],] %>% filter(Y_REAL > 0))
    train_df <- train_df %>% filter(직_가격갭 > -200 & 직_가격갭 < 200 & 직_분콜 < 1000 )
    
    test_df <- mdl_df[mdl_df$BROAD_DT >= wek[clu] & mdl_df$BROAD_DT < wek2[clu],]
    
    Y_tr <- train_df$Y_REAL
    Y_tt <- test_df$Y_REAL
    # par(mfrow = c(1,2)) ; hist(Y_tr, xlim = c(0,10000), main = "ORD_CALL") ; hist(log10(Y_tr), main = "log(ORD_CALL)"); par(mfrow = c(1,1))
    Y_trl <- log10(train_df$Y_REAL )
    
    set.seed(123)
    mdl_mt   <- model.matrix(formula2, data= rbind.data.frame(train_df, test_df))[,-1]
    
    train_mt <- mdl_mt[1:nrow(train_df) , ]
    test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]
    
    set.seed(123)
    smp <- sample(dim(train_mt)[1], 400)
    
    # xgTrain <- xgb.DMatrix(data=train_mt[1:(dim(train_mt)[1] - 400 ),], label=Y_trl[1:(dim(train_mt)[1] - 400 )])
    # xgVal <- xgb.DMatrix(data=train_mt[(dim(train_mt)[1] - 400 + 1 ):(dim(train_mt)[1] ),], label=Y_trl[(dim(train_mt)[1] - 400 + 1 ):(dim(train_mt)[1] )])
    # xgTest <- xgb.DMatrix(data=test_mt)
    
    xgTrain <- xgb.DMatrix(data=train_mt[-smp,], label=Y_trl[-smp])
    xgVal <- xgb.DMatrix(data=train_mt[smp,], label=Y_trl[smp])
    xgTest <- xgb.DMatrix(data=test_mt)
    
    remodel_yn <- "Q"
    # 목요일이면 모델생성   
    # if ( wday(Sys.time()) == 5) {
    if ( wday(wek[clu]) != make_mdl ) {
      
      print(paste( clu, wek[clu] , wek2[clu ], " 목요일X 예측테스트 " ))
      
      # 목요일이 아니면 
      load(file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu] ] ) ,"__", GBN_NM, ".RData") )
      
      # 예측 테스트 실패시 NOT출현 // 재모델링 
      remodel_yn   <- tryCatch(predict(LS_MDL, newx = test_mt),
                               error = function(e) print("NOT"),
                               warning = function(w) print("NOT"))
    }
    
    # 목요일이거나 예측실패한경우 모델 재생성 
    if ( wday(wek[clu]) == make_mdl | remodel_yn == "NOT" ) {
      print(paste( clu, wek[clu] , wek2[clu ], " 목요일O 모델링 및 예측수행" ))
      
      print(paste(" XGB ", as.character(Sys.time())))
      # ###### 5.XGB 시작 ######
      
      if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {
        set.seed(123)
        xgbFitl <- xgb.train(
          data = xgTrain, nfold = 5, label = as.matrix(Y_trl),
          objective='reg:linear',
          nrounds=2200,
          eval_metric='rmse',
          watchlist=list(train=xgTrain, validate=xgVal),
          print_every_n=10,
          nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
          early_stopping_rounds=100 
        )
        
        png(file = paste0( "/home/DTA_CALL/VARPLOT/", fld_nm, "/",STRT_DT, GBN_NM,".png"), height = 1100, width = 700)
        xgbFitl %>% 
          xgb.importance(feature_names=colnames(xgTrain)) %>%  
          dplyr::slice(1:30) %>%
          xgb.plot.importance()
        dev.off()
        
        
      } else {
        
        # print(xgbFit)
        set.seed(123)
        xgbFitl <- xgboost(data = train_mt, nfold = 5, label = as.matrix(Y_trl),
                           nrounds = 2200, verbose = FALSE, objective = "reg:linear", eval_metric = "rmse",
                           nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
                           subsample = 0.5213, colsample_bytree = 0.4603)
      }
      
      
      
      ## print(xgbFitl)
      # ###### 5.XGB 시작 ######
      
      # save(LM_MDL, LMl_MDL, LS_MDL, LSl_MDL, RF.1, RF.1l, gbm.2, gbm.2l, file = paste0("/home/kimwh/KIMWH/DAT/IMAGE/CALL___", clu, ".RData") )
      save(xgbFitl, 
           file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu]] ), "__", GBN_NM, ".RData") )
      # rm(LM_MDL, LMl_MDL, LS_MDL, LSl_MDL, RF.1, RF.1l, gbm.2, gbm.2l, xgbFit, xgbFitl )
      
      # importance_matrix <- xgb.importance(colnames(train_mt), model = xgbFitl)
      # xgb.plot.importance(importance_matrix)
      # xgb.plot.importance(importance_matrix[1:30,])
      
      
      # xgbFitl %>% 
      #   xgb.importance(feature_names=colnames(xgTrain)) %>%  
      #   # dplyr::slice(1:30) %>% 
      #   xgb.plot.importance()
      
      # library(dygraphs)
      # library(coefplot)
      # dygraphs::dygraph(xgbFitl$evaluation_log, main = xgbFitl$best_iteration)
      
      
      
      
    } 
    
    # save.image(file = paste0("/home/DTA_CALL/HAND2.RData" ))
    # load(file = paste0("/home/DTA_CALL/HAND2.RData" ))
    
    #####---------------------------------- 예측 수행 ---------------------------------#####
    
    # clu <- 109
    if ( wday(wek[clu]) != make_mdl ) {
      print(paste( clu, wek[clu] , wek2[clu ], " 목요일X 예측수행" ))
    } else {
      print(paste( clu, wek[clu] , wek2[clu ], " 목요일O 예측수행" ))
    }
    
    
    # set.seed(123)
    # mdl_mt   <- model.matrix(formula1, data=rbind.data.frame(train_df, test_df))[,-1]
    # 
    # train_mt <- mdl_mt[1:nrow(train_df) , ]
    # test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]
    
    
    # ###### 5.XGB  ######
    ## Predictions
    preds2l <- predict(xgbFitl, newdata = test_mt)
    
    er_df <- data.frame(no_m = 1:nrow(test_df),
                        PGM_ID = test_df$PGM_ID,
                        DT = test_df$BROAD_DT, 
                        MIN_Q_START_DATE = test_df$MIN_Q_START_DATE, 
                        MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
                        ITEM_CD = test_df$ITEM_CD,
                        ITEM_NM = test_df$ITEM_NM,
                        PRD_GRP_NM = test_df$PRD_GRP_NM, 
                        SUM_RUNTIME = test_df$SUM_RUNTIME,
                        Y = Y_tt, XGBL = round(10^(as.numeric(preds2l))))
    
    # 
    # 
    # ###### XGB 끝 ######
    
    #####---------------------------------- 예측 수행 ---------------------------------#####
    
    data.frame(data.frame(MDL_GBN[MDL_GBN$GBN == GBN, ]), CLU = clu, er_df)
    
  }  ))
  
  print("예측 수행 ")
  # 예측 수행 
  Predict_df <- merge(Predict_df, data.frame(PREDICT_DT = wek[1:length(wek)], CLU = 1:length(wek)), by = "CLU", all.x = T) 
  
  print("예측 오차 ")
  # 예측 오차 
  Predict_gap_df <- do.call(rbind, lapply(1:length(wek) , function(clu) {
    # clu <- 1
    print(paste(clu, "/", length(wek),  gsub("-","",wek[clu])))
    
    tmp <- Predict_df %>% filter(CLU == clu)
    #
    ##### 스케쥴 채워넣기 시작 #####
    
    
    
    tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
    
    # tmp$STRT_END_GAP <- round(as.numeric(tmp$MIN_Q_START_DATE - tmp$MAX_Q_END_DATE_lag)/60)
    tmp$STRT_END_GAP <- round(as.numeric(base::difftime(tmp$MIN_Q_START_DATE, tmp$MAX_Q_END_DATE_lag, tz, units = c( "mins") )))
    IN_df <- tmp %>% filter(abs(STRT_END_GAP) > 5)
    
    which_pt <- which(tmp$STRT_END_GAP < 0)
    
    if ( length(which_pt) > 0) {
      # 음수 제거 
      tmp <- tmp[-c(which_pt -1),]
      tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
      tmp$STRT_END_GAP <- round(as.numeric(tmp$MIN_Q_START_DATE - tmp$MAX_Q_END_DATE_lag)/60)
      
      IN_df <- tmp %>% filter(abs(STRT_END_GAP) > 5)
      # 
    }
    
    
    # 스케쥴 생성 
    IN_df2 <- data.frame(
      data.frame(CLU = tmp$CLU[1],
                 GBN = tmp$GBN[1],
                 GBN_NM = tmp$GBN_NM[1],
                 MART = tmp$MART[1],
                 OZ = tmp$OZ[1])
      ,
      
      DT = substr(IN_df$MAX_Q_END_DATE_lag, 1, 10),
      MIN_Q_START_DATE = IN_df$MAX_Q_END_DATE_lag + 1,
      MAX_Q_END_DATE = IN_df$MIN_Q_START_DATE - 1,
      SUM_RUNTIME = as.numeric(IN_df$MIN_Q_START_DATE - 1 - IN_df$MAX_Q_END_DATE_lag),
      PREDICT_DT =  IN_df$PREDICT_DT)
    IN_df2 <- data.table(IN_df2)
    IN_df2 <- IN_df2[ , WEK := wday(ymd(DT))]
    IN_df2 <- IN_df2[ , WEK := WEK - 1 ]
    IN_df2$WEK <- ifelse(IN_df2$WEK == 0, 7, IN_df2$WEK)
    IN_df2 <- IN_df2[ , WEK_G := ifelse(WEK == 1, "1.월", 
                                        ifelse(WEK == 2, "2.화",
                                               ifelse(WEK == 3, "3.수",
                                                      ifelse(WEK == 4, "4.목",
                                                             ifelse(WEK == 5, "5.금",
                                                                    ifelse(WEK == 6, "6.토","7.일")))))) ]
    IN_df2 <- IN_df2[ , WEK := ifelse(WEK %in% c(1:5), "1.주중", 
                                      ifelse(WEK == 6, "2.토","3.일")) ]
    IN_df2$mon_bf <- mon_df$MON[which(mon_df$MON == paste0(substr(IN_df2$PREDICT_DT[1],1,8),"01")) - 1 ]
    
    null_df <- do.call(rbind, lapply(1:nrow(IN_df2), function(i){
      # i <- 2
      print(i)
      tmp <- IN_df2[i,]
      
      time_grp <- seq( ymd_hms(paste0(substr(tmp$MIN_Q_START_DATE,1,13), "0000" )), tmp$MAX_Q_END_DATE + 60*60, by = "1 hours")
      time_grp <- data.frame(MIN_Q_START_DATE = time_grp[-length(time_grp)], MAX_Q_END_DATE = time_grp[-1])
      time_grp$TIME_HH <- time_grp$MIN_Q_START_DATE
      time_grp$MIN_Q_START_DATE[1] <- tmp$MIN_Q_START_DATE
      time_grp$MAX_Q_END_DATE[length(time_grp$MAX_Q_END_DATE)] <- tmp$MAX_Q_END_DATE
      
      # time_grp$MIN_Q_START_DATE <- time_grp$MIN_Q_START_DATE + 60
      # time_grp$MIN_Q_START_DATE <- gsub(":00", ":01", time_grp$MIN_Q_START_DATE)
      # 
      # time_grp$MAX_Q_END_DATE <- gsub(":00", ":01", time_grp$MAX_Q_END_DATE)
      # time_grp$MIN_Q_START_DATE <- ymd_hms(time_grp$MIN_Q_START_DATE)
      # time_grp$MAX_Q_END_DATE <- ymd_hms(time_grp$MAX_Q_END_DATE)
      # 
      # time_grp$SUM_RUNTIME <- round(as.numeric(time_grp$MAX_Q_END_DATE - time_grp$MIN_Q_START_DATE))
      
      time_grp$SUM_RUNTIME <- round(as.numeric(base::difftime(time_grp$MAX_Q_END_DATE, time_grp$MIN_Q_START_DATE, tz, units = c( "mins") )))
      
      
      
      # time_grp$SUM_RUNTIME <- round(time_grp$SUM_RUNTIME/60)
      
      data.frame(tmp[,-c("MIN_Q_START_DATE", "MAX_Q_END_DATE", "SUM_RUNTIME")], time_grp)
      
    }))
    null_df$HH <- as.numeric(substr(null_df$TIME_HH, 12,13)  )
    null_df$HH <- ifelse(null_df$HH == 0 , 24, null_df$HH)
    null_df <- data.table(null_df)
    
    
    len <- c("XGBL")
    
    if (GBN_NM == "총콜") {
      
      null_df <- merge(null_df, CALL_df2 %>% dplyr::select("mon", "WEK_G", "HH", "전달시콜") ,
                       by.x = c("mon_bf", "WEK_G", "HH"),
                       by.y = c("mon", "WEK_G", "HH"),
                       all.x = T)
      null_df <- null_df[SUM_RUNTIME > 0 , ]
      null_df$PRED <- round((null_df$전달시콜/ 60) * null_df$SUM_RUNTIME )
      null_df <- data.frame(null_df %>% dplyr::select(-mon_bf, -전달시콜 , -PRED) ,
                            data.frame(matrix(rep(null_df$PRED,  length(len)), ncol = length(len), dimnames = list(NULL, len))))
      
    } else if (GBN_NM == "주문") {
      null_df <- merge(null_df, CALL_df2 %>% dplyr::select("mon", "WEK_G", "HH", "전달상담주문") ,
                       by.x = c("mon_bf", "WEK_G", "HH"),
                       by.y = c("mon", "WEK_G", "HH"),
                       all.x = T)
      null_df <- null_df[SUM_RUNTIME > 0 , ]
      null_df$PRED <- round((null_df$전달상담주문/ 60) * null_df$SUM_RUNTIME )
      null_df <- data.frame(null_df %>% dplyr::select(-mon_bf, -전달상담주문 , -PRED) , 
                            data.frame(matrix(rep(null_df$PRED,  length(len)), ncol = length(len), dimnames = list(NULL, len))))
      
    } else if (GBN_NM == "SR") { 
      null_df <- merge(null_df, CALL_df2 %>% dplyr::select("mon", "WEK_G", "HH", "전달상담SR") ,
                       by.x = c("mon_bf", "WEK_G", "HH"),
                       by.y = c("mon", "WEK_G", "HH"),
                       all.x = T)
      null_df <- null_df[SUM_RUNTIME > 0 , ]
      null_df$PRED <- round((null_df$전달상담SR/ 60) * null_df$SUM_RUNTIME )
      null_df <- data.frame(null_df %>% dplyr::select(-mon_bf, -전달상담SR , -PRED) , 
                            data.frame(matrix(rep(null_df$PRED,  length(len)), ncol = length(len), dimnames = list(NULL, len))))
    }
    
    
    # save.image(file = paste0("/home/DTA_CALL/HAND1_1.RData" ))
    # load(file = paste0("/home/DTA_CALL/HAND1_1.RData" ))
    
    
    ##### 스케쥴 채워넣기 끝 #####
    
    tmp$DT <- as.character(tmp$DT)
    null_df$DT <- as.character(null_df$DT)
    
    tmp <- rbind.fill(tmp, null_df[, names(null_df)[names(null_df) %in% names(tmp)]])
    
    #
    tmp$HH <- as.numeric(substr(tmp$MIN_Q_START_DATE, 12,13))
    names(tmp)[which(names(tmp) == "HH")] <- "노출HH"
    tmp$TIME_HH <- ymd_h(substr(tmp$MIN_Q_START_DATE, 1, 13))
    
    len <- c("XGBL")
    for(i in which(names(tmp) %in% len)) { tmp [,i] <- ifelse(tmp[,i]<0 , 0 , tmp[,i])}
    
    tmp$STRT_MIN <- as.numeric(substr(tmp$MIN_Q_START_DATE, 15,16))
    
    pwd <- paste0("/home/DTA_CALL/WEK_PREDICT_OUT/", gsub("-","",wek[clu]) )
    # dir 생성 
    if( sum(list.files(paste0("/home/DTA_CALL/WEK_PREDICT_OUT/" )) %in% c(gsub("-","",wek[clu]) )) == 0 ) {
      dir.create( pwd )
    }
    if(TEST_YN == "YES") {
      # 아이템 베이스 검증불가_예측결과 
      write.csv(tmp, paste0(pwd,"/","0.", GBN_NM, "검증X_예측결과XLS_ITEM__", gsub("-","",wek[clu]), ".csv"), row.names = F, fileEncoding = "CP949")
    } else {
      # 아이템 베이스 예측결과 
      write.csv(tmp, paste0(pwd,"/","0.", GBN_NM, "예측결과XLS_ITEM__", gsub("-","",wek[clu]), ".csv"), row.names = F, fileEncoding = "CP949")
    }
    
    
    # 
    # CALL_HH_df$TIME_HH <- ymd_h(paste(CALL_HH_df$기준일자, CALL_HH_df$기준시간))
    # names(CALL_HH_df)[which(names(CALL_HH_df) == GBN_OZ)] <- "Y"
    # dt_df <- CALL_HH_df %>% filter(ymd(기준일자) >= wek[clu] & ymd(기준일자)  < wek2[clu]) %>% dplyr::select(TIME_HH, Y)
    
    dt_df <- data.frame(TIME_HH = seq(ymd_hms(paste(wek[clu], "00:00:00")), ymd_hms(paste(wek2[clu], "00:00:00")), by = "1 hours"))
    # dt_df$YN <- 1
    tmp <- merge(dt_df, tmp, by = "TIME_HH", all.x = T)
    tmp$CLU <- clu
    tmp$노출HH      <- ifelse( is.na(tmp$노출HH)  , as.numeric(substr(tmp$TIME_HH, 12, 13)), tmp$노출HH)
    # tmp$SUM_RUNTIME <- ifelse( is.na(tmp$SUM_RUNTIME)  , 60, tmp$SUM_RUNTIME)
    tmp$SUM_RUNTIME <- round(as.numeric(tmp$MAX_Q_END_DATE - tmp$MIN_Q_START_DATE))
    tmp$SUM_RUNTIME <- ifelse( is.na(tmp$SUM_RUNTIME)  , 60, tmp$SUM_RUNTIME)
    
    tmp$STRT_MIN    <- ifelse( is.na(tmp$STRT_MIN)  , 0, tmp$STRT_MIN)
    for(i in which(names(tmp) %in% len)) { tmp [,i] <- ifelse(is.na(tmp[,i]) , 0 , tmp[,i])}
    
    tmp <- tmp[substr(tmp$TIME_HH,1,10) < wek2[clu],]
    
    
    tmp <- data.table(tmp)
    tmp <- tmp[ , `:=`(XGBLad = ifelse( SUM_RUNTIME + STRT_MIN <= 60, XGBL, round((XGBL/SUM_RUNTIME)*(60 - STRT_MIN ))   )) ]
    tmp <- tmp[ , `:=`(XGBLad2 = c(0, c(XGBL - XGBLad)[-length(XGBL)]  ) ) ]
    tmp <- tmp[ , `:=`(XGBL2 = XGBLad + XGBLad2    ) ]
    tmp_hh       <- tmp %>% group_by(TIME_HH) %>% summarise(XGBL = sum(XGBL2))
    tmp_hh       <- data.frame(tmp_hh)
    tmp_hh <- tmp_hh[-1,]
    
    
    error_df <- data.frame(clu = clu , GBN_TP = "예측완료", tmp_hh)
    # 검증불가
    write.csv(error_df, paste0(pwd,"/","7.", GBN_NM, "검증X_예측결과_시간ALL_XLS__", gsub("-","",wek[clu]), ".csv"), row.names = F,  fileEncoding = "CP949")
    error_df
  }))
  print("예측 완료 ")
  # 
  cbind.data.frame(MDL_GBN[MDL_GBN$GBN == GBN, c("GBN", "GBN_NM")],  Predict_gap_df)
}) ) 

print("모델링 완료")

if (TEST_YN == "YES") {
  
} else {
  
  print("예측결과 적재 시작 ")
  
  #####------- 요약최종 -------##### 
  # 예측결과 테이블 적재 
  melt_df          <- melt(Final_df, id.var = c("GBN", "GBN_NM", "clu", "GBN_TP", "TIME_HH")  )
  melt_df$variable <- as.character(melt_df$variable)
  melt_df$GRP      <- ifelse(melt_df$variable %in% c( "XGBL"), "5.XGboost(log)")
  melt_df$GRP      <- as.character(melt_df$GRP)
  melt_df          <- data.table(melt_df)
  dcast_df         <- dcast.data.table(melt_df, clu +GBN_TP +TIME_HH +variable + GRP ~ GBN + GBN_NM, var.value = "value", fill = 0)
  names(dcast_df)[which(names(dcast_df) %in% c("1_총콜", "2_주문", "3_SR") )] <- paste0("X", names(dcast_df)[which(names(dcast_df) %in% c("1_총콜", "2_주문", "3_SR") )])
  dcast_df$X4_상담콜 <- dcast_df$X2_주문 + dcast_df$X3_SR
  dcast_df$X1_총콜   <- round(ifelse(dcast_df$X1_총콜 < dcast_df$X4_상담콜, dcast_df$X4_상담콜 * (1/0.4), dcast_df$X1_총콜))
  dcast_df$X5_ARS콜  <- dcast_df$X1_총콜 - dcast_df$X4_상담콜
  names(dcast_df)[which(names(dcast_df) == "variable")] <- "GRP_CD"
  melt_df2          <- melt.data.table(dcast_df,  id.var = c("clu", "GBN_TP", "TIME_HH", "GRP", "GRP_CD") )
  melt_df2$variable <- as.character(melt_df2$variable)
  melt_df2$GUBUN    <- unlist(strsplit(melt_df2$variable, "_"))[seq(from = 2, to = length(melt_df2$variable)* 2 ,by = 2)]
  melt_df2$SEQ <- ifelse(melt_df2$GUBUN == "총콜", 1,
                         ifelse(melt_df2$GUBUN == "ARS콜", 2,
                                ifelse(melt_df2$GUBUN == "상담콜", 3,
                                       ifelse(melt_df2$GUBUN == "주문", 4, 5 ))))
  melt_df2$기준년 <- substr(melt_df2$TIME_HH,1,4)
  melt_df2$기준월 <- gsub("-", "",substr(melt_df2$TIME_HH,1,7))
  melt_df2$기준일 <- ymd(substr(melt_df2$TIME_HH,1,10))
  names(melt_df2)[which(names(melt_df2) == "value")] <- "PREDICT_VALUE"
  wek_df2         <- data.frame(기준일 = seq.Date(from = ymd(wek_df$strt_day), to = ymd(wek_df$last_day), by = "1 days"))
  wek_df2$SEQ_DAY <- 1:nrow(wek_df2)
  wek_df2         <- data.table(wek_df2)
  melt_df2        <- merge(melt_df2, wek_df2, by = "기준일", all.x = T)
  melt_df2        <- melt_df2[ , WEK := wday(ymd(기준일))]
  melt_df2        <- melt_df2[ , WEK := WEK - 1 ]
  melt_df2$WEK    <- ifelse(melt_df2$WEK == 0, 7, melt_df2$WEK)
  melt_df2        <- melt_df2[ , WEK_G := ifelse(WEK == 1, "1.월", 
                                                 ifelse(WEK == 2, "2.화",
                                                        ifelse(WEK == 3, "3.수",
                                                               ifelse(WEK == 4, "4.목",
                                                                      ifelse(WEK == 5, "5.금",
                                                                             ifelse(WEK == 6, "6.토","7.일")))))) ] 
  melt_df2$기준일  <- gsub("-", "", melt_df2$기준일)  
  last_df          <- melt_df2 %>% dplyr::select(기준년, 기준월, 기준일, WEK_G, TIME_HH, SEQ, GUBUN, SEQ_DAY,  GRP_CD, GRP, PREDICT_VALUE)
  last_df$TIME_HH  <- as.character(last_df$TIME_HH)
  last_df$SEQ_DAY  <- as.character(last_df$SEQ_DAY)
  last_df$PRD_DATE <- substr(SYSDATE, 1,10)
  last_df$ETL_DATE <- SYSDATE
  last_df$ETL_HOUR <- substr(SYSDATE, 12,13)
  last_df$ETL_WEK  <- c("일", "월", "화", "수", "목", "금", "토")[wday(SYSDATE)]
  last_df          <- last_df %>% dplyr::select(기준년, 기준월, 기준일, WEK_G, TIME_HH, SEQ, GUBUN, SEQ_DAY,  GRP_CD, GRP, PREDICT_VALUE, PRD_DATE, ETL_DATE,ETL_HOUR,ETL_WEK)
  last_df          <- last_df %>% arrange( TIME_HH, SEQ, GUBUN, GRP)
  #####------- 요약최종 -------##### 
  
  setDT(last_df)
  last_df <- last_df[ , 시간 := substr(TIME_HH, 12, 13) ]
  last_df$시간 <- as.numeric(last_df$시간)
  # last_df <- last_df %>% rename(Y = value)
  
  last_df <- last_df[ , H3 := ifelse(시간 <= 2, "1.0~2",
                                       ifelse(시간 <= 5, "2.3~5",
                                                ifelse(시간 <= 8, "3.6~8",
                                                         ifelse(시간 <= 11, "4.9~11",
                                                                  ifelse(시간 <= 14, "5.12~14",
                                                                           ifelse(시간 <= 17, "6.15~17", 
                                                                                    ifelse(시간 <= 20,  "7.18~20", "8.21~23")))))))]
  last_df <- last_df[ , H6 := ifelse(시간 <= 5, "1.0~5",
                                       ifelse(시간 <= 11, "2.6~11",
                                                ifelse(시간 <= 17, "3.12~17","4.18~23")))]
  
  rst <- 0
  if (rst == 1) {
    #####------- 오차 검증 -------##### 
    setDT(last_df)
    
    CALL_HH_df$TIME_HH <- ymd_hms(paste0(CALL_HH_df$기준일자, " ", CALL_HH_df$기준시간, ":00:00"))
    #
    melt_df <- melt.data.table(CALL_HH_df[,.(TIME_HH, 총_인입콜수, ARS_인입콜수, 상담원_인입콜수, 상담원_주문_인입콜수, 상담원_SR_인입콜수)],
                               id.var = c("TIME_HH"))
    # 
    melt_df$GUBUN <- ifelse(melt_df$variable == "총_인입콜수","총콜", 
                            ifelse(melt_df$variable == "ARS_인입콜수","ARS콜", 
                                   ifelse(melt_df$variable == "상담원_인입콜수","상담콜", 
                                          ifelse(melt_df$variable == "상담원_주문_인입콜수","주문", "SR" ))))
    melt_df$TIME_HH <- as.character(melt_df$TIME_HH)
    # 
    last_df2 <- merge(last_df, melt_df[,.(TIME_HH,GUBUN, value)], by = c("TIME_HH","GUBUN"), all.x= T)
    last_df2 <- last_df2[ , 시간 := substr(TIME_HH, 12, 13) ]
    last_df2$시간 <- as.numeric(last_df2$시간)
    last_df2 <- last_df2 %>% rename(Y = value)
    
    last_df2 <- last_df2[ , H3 := ifelse(시간 <= 2, "1.0~2",
                                           ifelse(시간 <= 5, "2.3~5",
                                                    ifelse(시간 <= 8, "3.6~8",
                                                             ifelse(시간 <= 11, "4.9~11",
                                                                      ifelse(시간 <= 14, "5.12~14",
                                                                               ifelse(시간 <= 17, "6.15~17", 
                                                                                        ifelse(시간 <= 20,  "7.18~20", "8.21~23")))))))]
    last_df2 <- last_df2[ , H6 := ifelse(시간 <= 5, "1.0~5",
                                           ifelse(시간 <= 11, "2.6~11",
                                                    ifelse(시간 <= 17, "3.12~17","4.18~23")))]
    
    
    # 
    error_h3_df <- last_df2[ , .(
      Y = sum(Y),
      PREDICT_VALUE = sum(PREDICT_VALUE)
    ), by = c("SEQ", "GRP", "GRP_CD", "GUBUN", "기준일", "H3")][, .(
      RMSE = round(RMSE(m = Y, o = PREDICT_VALUE)),
      MAPE = round(MAPE(m = Y, o = PREDICT_VALUE),1),
      MEDIAN갭 = median(round(abs(Y - PREDICT_VALUE)/Y, 3))*100,
      건수 = .N
    ), by = c("SEQ", "GRP", "GRP_CD", "GUBUN", "H3")] %>% arrange(SEQ,  H3)
    
    # 
    error_h6_df <- last_df2[ , .(
      Y = sum(Y),
      PREDICT_VALUE = sum(PREDICT_VALUE)
    ), by = c("SEQ", "GRP", "GRP_CD", "GUBUN", "기준일", "H6")][, .(
      RMSE = round(RMSE(m = Y, o = PREDICT_VALUE)),
      MAPE = round(MAPE(m = Y, o = PREDICT_VALUE),1),
      MEDIAN갭 = median(round(abs(Y - PREDICT_VALUE)/Y, 3))*100,
      건수 = .N
    ), by = c("SEQ", "GRP", "GRP_CD", "GUBUN", "H6")] %>% arrange(SEQ, H6)
    
    #  
    error_d_df <- last_df2[, .(
      RMSE = round(RMSE(m = Y, o = PREDICT_VALUE)),
      MAPE = round(MAPE(m = Y, o = PREDICT_VALUE),1),
      MEDIAN갭 = median(round(abs(Y - PREDICT_VALUE)/Y, 3))*100,
      건수 = .N
    ), by = c("SEQ", "GRP", "GRP_CD", "GUBUN", "SEQ_DAY")] %>% arrange(SEQ, SEQ_DAY)
    
    #  
    error_df <- last_df2[, .(
      RMSE = round(RMSE(m = Y, o = PREDICT_VALUE)),
      MAPE = round(MAPE(m = Y, o = PREDICT_VALUE),1),
      MEDIAN갭 = median(round(abs(Y - PREDICT_VALUE)/Y, 3))*100,
      건수 = .N
    ), by = c("SEQ", "GRP", "GRP_CD", "GUBUN")] %>% arrange(SEQ)
    
    error_h3_df$ER_GRP <- "1.3"
    error_h6_df$ER_GRP <- "2.6"
    error_d_df$ER_GRP <- "3.24"
    error_df$ER_GRP <- "4.99"
    
    error_df$ER_GRPNM <- "기존"
    
    er_a_df <- rbind.fill(error_h3_df %>% rename(ER_GRPNM = H3),
                          error_h6_df %>% rename(ER_GRPNM = H6),
                          error_d_df %>% rename(ER_GRPNM = SEQ_DAY),
                          error_df)
    #####------- 오차 검증 -------##### 
    er_a_df$MDL <- as.character(formula2)[3]
    write.csv(er_a_df, file = paste0("/home/DTA_CALL/VARPLOT/", fld_nm, "/", STRT_DT,  ".csv"), row.names = F, fileEncoding = "CP949")
  }
  # er_a_df db 저장 ~ 
  
  print("예측결과 DB 저장")
  #####------- 결과 저장 -------#####  
  
  source(file = "/home/DTA_CALL/DB_CON_etl.R")
  
  # nrow(last_df)
  for (i in 1:nrow(last_df)) { 
    
    # i <-1
    # print(paste(i, "//", nrow(last_df)))
    
    query.x <- paste0( "INSERT INTO DTA_OWN.CALL_WEEKLY_PRD_TB_ULTRA3 
                            (기준년, 기준월, 기준일,  WEK_G, TIME_HH, H3,H6,SEQ, GUBUN, SEQ_DAY,  GRP_CD, GRP, PREDICT_VALUE, PRD_DATE, ETL_DATE,ETL_HOUR,ETL_WEK) 
                            VALUES ('",
                       last_df$기준년[i], "','", 
                       last_df$기준월[i], "','", 
                       last_df$기준일[i], "','", 
                       last_df$WEK_G[i], "','",
                       last_df$TIME_HH[i], "','",
                       last_df$H3[i], "','",
                       last_df$H6[i], "',",
                       
                       last_df$SEQ[i], ",'", 
                       last_df$GUBUN[i], "','", 
                       last_df$SEQ_DAY[i], "','", 
                       last_df$GRP_CD[i], "','", 
                       last_df$GRP[i], "',", 
                       last_df$PREDICT_VALUE[i], ",'",
                       last_df$PRD_DATE[i], "','",
                       substr(SYSDATE, 1,10), "','",
                       substr(SYSDATE, 12,13), "','",
                       c("일", "월", "화", "수", "목", "금", "토")[wday(SYSDATE)], "')"  )  
    tryCatch(dbSendUpdate(conn, query.x), error = function(e) print("NOT"), warning = function(w) print("NOT"))
    
  }
  
  dbDisconnect(conn)
  rm(conn,drv)
  
  #####------- 결과 저장 -------##### 
  
  
  print("예측결과 DB적재완료")
  
  
  
  print("아이템 예측결과 DB 저장 시작")
  
  #####------- 요약최종 -------##### 
  # MDL_GBN <- data.frame(GBN = 1:4, 
  #                       GBN_NM = as.character(c("총콜", "주문", "SR", "상담")),
  #                       MART = as.character(c("총인콜수_M", "인입콜_주문_M", "인입콜_SR_M", "인입콜_상담_M")), 
  #                       OZ = as.character(c("총_인입콜수", "상담원_주문_인입콜수", "상담원_SR_인입콜수", "상담원_인입콜수")))  
  # for(i in 2:4){MDL_GBN[,i] <- as.character(MDL_GBN[,i])}
  
  # chk_dt <- STRT_DT
  # 
  check_df <- do.call(rbind.fill, lapply(1:3, function(GBN) { 
    
    # GBN <- 1
    
    GBN_NM <- MDL_GBN$GBN_NM[MDL_GBN$GBN == GBN]
    GBN_MART <- MDL_GBN$MART[MDL_GBN$GBN == GBN]
    # GBN_OZ <- MDL_GBN$OZ[MDL_GBN$GBN == GBN]
    # names(mdl_df)[which(names(mdl_df) == GBN_MART)] <- "Y_REAL"
    
    print(paste( GBN_NM, GBN_MART   ))
    # 
    pwd <- paste0("/home/DTA_CALL/WEK_PREDICT_OUT/" )
    list_nm <- list.files(pwd )
    list_nm <- list_nm[list_nm == STRT_DT]
    
    prd_df <- do.call(rbind.fill, lapply(1:length(list_nm), function(fold){
      
      print(paste0(fold, " // ",length(list_nm) ) )      
      # fold <- 1
      # list_nm[fold]
      list_fold_nm <- list.files(paste0(pwd, list_nm[fold] ) )
      
      if(length(list_fold_nm[grep( paste0("0." , GBN_NM, "검증X_예측결과XLS_ITEM__"), list_fold_nm)]) != 0 ) {
        list_fold_nm <- list_fold_nm[grep( paste0("0." , GBN_NM, "검증X_예측결과XLS_ITEM__"), list_fold_nm)]
      } else {
        list_fold_nm <- list_fold_nm[grep( paste0("0." , GBN_NM, "예측결과XLS_ITEM__"), list_fold_nm)]
        
      }
      tmp_df <- read.csv(paste0(pwd, list_nm[fold],"/",list_fold_nm), header = T, stringsAsFactors = FALSE, 
                         fileEncoding = "CP949")
      tmp_df$DAY <- substr(tmp_df$TIME_HH, 1, 10)
      
      tmp_df$GBN <- GBN
      tmp_df$GBN_NM <- GBN_NM
      tmp_df$TIME_HH <- ymd_hms(tmp_df$TIME_HH)
      tmp_df$DAY <- ymd(tmp_df$DAY)
      
      wek_df2 <- data.frame(DAY = seq.Date(from = ymd(min(tmp_df$DAY)), to = ymd(max(tmp_df$DAY)), by = "1 days"))
      wek_df2$SEQ_DAY <- 1:nrow(wek_df2)
      
      
      tmp_df <- merge(tmp_df, wek_df2, by = "DAY", all.x = T)
      tmp_df$PREDICT_DAY <- wek_df2$DAY[1] 
      tmp_df
      
    }))
    
    prd_df
  }))
  
  check_df <- check_df %>% dplyr::select(PREDICT_DAY, DAY, TIME_HH, GBN, GBN_NM, SEQ_DAY, PGM_ID,
                                         MIN_Q_START_DATE, MAX_Q_END_DATE,
                                         PRD_GRP_NM, ITEM_CD, ITEM_NM, SUM_RUNTIME, 
                                         노출HH, XGBL)
  
  
  melt_df <- melt(check_df, id.var = c("PREDICT_DAY", "DAY", "TIME_HH", "GBN", "GBN_NM", "SEQ_DAY","PGM_ID",
                                       "MIN_Q_START_DATE", "MAX_Q_END_DATE", 
                                       "PRD_GRP_NM", "ITEM_CD", "ITEM_NM", "SUM_RUNTIME", 
                                       "노출HH")  ) 
  melt_df$variable <- as.character(melt_df$variable)
  melt_df$GRP <- ifelse(melt_df$variable %in% c("LS"), "2.lassoReg.", ifelse(melt_df$variable %in% c("LSL"), "2.lassoReg.(log)",
                                                                             ifelse(melt_df$variable %in% c("LM"), "1.Reg.", ifelse(melt_df$variable %in% c("LML"), "1.Reg.(log)",
                                                                                                                                    ifelse(melt_df$variable %in% c("RF"), "3.RandomForest", ifelse(melt_df$variable %in% c(  "RFL"), "3.RandomForest(log)",  
                                                                                                                                                                                                   ifelse(melt_df$variable %in% c("GBM"), "4.GBM", ifelse(melt_df$variable%in% c( "GBML"), "4.GBM(log)", 
                                                                                                                                                                                                                                                          ifelse(melt_df$variable%in% c("XGB"), "5.XGboost", ifelse(melt_df$variable %in% c( "XGBL"), "5.XGboost(log)",
                                                                                                                                                                                                                                                                                                                    ifelse(melt_df$variable %in% c( "ENSB"), "6.Ensemble", "6.Ensemble(log)")))))))))))
  melt_df$GRP <- as.character(melt_df$GRP)
  melt_df <- data.table(melt_df)
  
  dcast_df <- dcast.data.table(data = melt_df, PREDICT_DAY +DAY +TIME_HH +SEQ_DAY+PGM_ID+MIN_Q_START_DATE+MAX_Q_END_DATE+
                                 PRD_GRP_NM + ITEM_CD+ ITEM_NM+ SUM_RUNTIME+노출HH+ variable + GRP ~ GBN + GBN_NM 
                               , var.value = "value", fill = 0)
  names(dcast_df)[which(names(dcast_df) %in% c("1_총콜", "2_주문", "3_SR") )] <- paste0("X", names(dcast_df)[which(names(dcast_df) %in% c("1_총콜", "2_주문", "3_SR") )])
  dcast_df$X4_상담콜 <- dcast_df$X2_주문 + dcast_df$X3_SR
  dcast_df$X1_총콜 <- round(ifelse(dcast_df$X1_총콜 < dcast_df$X4_상담콜, dcast_df$X4_상담콜 * (1/0.4), dcast_df$X1_총콜))
  dcast_df$X5_ARS콜 <- dcast_df$X1_총콜 - dcast_df$X4_상담콜
  
  
  dcast_df <- data.frame(dcast_df)
  for( i in grep("^X", names(dcast_df) ) ) {dcast_df[,i] <- as.numeric(dcast_df[,i])}
  
  names(dcast_df)[which(names(dcast_df) == "variable")] <- "GRP_CD"
  melt_df2 <- melt(dcast_df,  id.var = c("PREDICT_DAY", "DAY", "TIME_HH", "SEQ_DAY", "PGM_ID",
                                         "MIN_Q_START_DATE", "MAX_Q_END_DATE", 
                                         "PRD_GRP_NM", "ITEM_CD", "ITEM_NM", "SUM_RUNTIME", 
                                         "노출HH", "GRP", "GRP_CD") )
  melt_df2$variable <- as.character(melt_df2$variable)
  melt_df2$GUBUN <- unlist(strsplit(melt_df2$variable, "_"))[seq(from = 2, to = length(melt_df2$variable)* 2 ,by = 2)]
  
  
  melt_df2$SEQ <- ifelse(melt_df2$GUBUN == "총콜", 1,
                         ifelse(melt_df2$GUBUN == "ARS콜", 2,
                                ifelse(melt_df2$GUBUN == "상담콜", 3,
                                       ifelse(melt_df2$GUBUN == "주문", 4, 5 ))))
  
  melt_df2$기준년 <- substr(melt_df2$TIME_HH,1,4)
  melt_df2$기준월 <- gsub("-", "",substr(melt_df2$TIME_HH,1,7))
  melt_df2$기준일 <- ymd(substr(melt_df2$TIME_HH,1,10))
  names(melt_df2)[which(names(melt_df2) == "value")] <- "PREDICT_VALUE"
  
  melt_df2$SEQ_DAY <- as.numeric(melt_df2$기준일 - melt_df2$PREDICT_DAY) + 1  
  melt_df2 <- data.table(melt_df2)
  
  melt_df2 <- melt_df2[ , WEK := wday(ymd(기준일))]
  melt_df2 <- melt_df2[ , WEK := WEK - 1 ]
  melt_df2$WEK <- ifelse(melt_df2$WEK == 0, 7, melt_df2$WEK)
  melt_df2 <- melt_df2[ , WEK_G := ifelse(WEK == 1, "1.월", 
                                          ifelse(WEK == 2, "2.화",
                                                 ifelse(WEK == 3, "3.수",
                                                        ifelse(WEK == 4, "4.목",
                                                               ifelse(WEK == 5, "5.금",
                                                                      ifelse(WEK == 6, "6.토","7.일")))))) ] 
  
  
  
  melt_df2$기준일 <- gsub("-", "", melt_df2$기준일)  
  
  last_df <- melt_df2 %>% dplyr::select(기준년, 기준월, 기준일, WEK_G, TIME_HH, SEQ, GUBUN, SEQ_DAY, PGM_ID,
                                           PREDICT_DAY, MIN_Q_START_DATE, MAX_Q_END_DATE,
                                           PRD_GRP_NM, ITEM_CD, ITEM_NM, SUM_RUNTIME,노출HH,
                                           GRP_CD, GRP, PREDICT_VALUE)
  last_df$TIME_HH <- as.character(last_df$TIME_HH)
  last_df$SEQ_DAY <- as.character(last_df$SEQ_DAY)
  
  
  last_df$ETL_DATE <- SYSDATE
  last_df$ETL_HOUR <- substr(SYSDATE, 12,13)
  last_df$ETL_WEK <- c("일", "월", "화", "수", "목", "금", "토")[wday(SYSDATE)]
  
  # last_df$ETL_DATE <- last_df$PREDICT_DAY
  # last_df$ETL_HOUR <- "03"
  # last_df$ETL_WEK <- c("일", "월", "화", "수", "목", "금", "토")[wday(last_df$PREDICT_DAY)]
  
  last_df <- last_df %>% arrange( TIME_HH, SEQ, GUBUN, GRP)
  # last_df <- last_df %>% filter(GRP_CD %in% c( "XGBL") )
  
  last_df$PRD_GRP_NM <- ifelse(is.na(last_df$PRD_GRP_NM), "NULL", last_df$PRD_GRP_NM)
  last_df$ITEM_CD <- ifelse(is.na(last_df$ITEM_CD), 0, last_df$ITEM_CD)
  last_df$ITEM_NM <- ifelse(is.na(last_df$ITEM_NM), "NULL", last_df$ITEM_NM)
  #####------- 요약최종 -------##### 
  
  print("아이템 예측결과 DB 저장 시작2 ")
  #####------- 결과 저장 -------#####  
  
  source(file = "/home/DTA_CALL/DB_CON_etl.R")
  
  # nrow(last_df)
  for (i in 1:nrow(last_df)) {
    # mclapply(1:nrow(last_df), function(i) {  
    # i <-1
    # print(paste(i, "//", nrow(last_df)))
    
    query.x <- paste0( "INSERT INTO DTA_OWN.CALL_WEEKLY_ITEM_PRD_TB_ULTRA3 
                            (기준년, 기준월, 기준일, PGM_ID, WEK_G, TIME_HH, SEQ, GUBUN, SEQ_DAY,
                            PRD_DATE, MIN_Q_START_DATE, MAX_Q_END_DATE,
                            PRD_GRP_NM, ITEM_CD, ITEM_NM, SUM_RUNTIME, HH,
                            GRP_CD, GRP,
                            PREDICT_VALUE, ETL_DATE,ETL_HOUR,ETL_WEK) 
                            VALUES ('",
                       last_df$기준년[i], "','", 
                       last_df$기준월[i], "','", 
                       last_df$기준일[i], "','", 
                       last_df$PGM_ID[i], "','",
                       last_df$WEK_G[i], "','",
                       last_df$TIME_HH[i], "',", 
                       last_df$SEQ[i], ",'", 
                       last_df$GUBUN[i], "','", 
                       last_df$SEQ_DAY[i], "','", 
                       
                       last_df$PREDICT_DAY[i], "','",
                       last_df$MIN_Q_START_DATE[i], "','",
                       last_df$MAX_Q_END_DATE[i], "','",
                       last_df$PRD_GRP_NM[i], "',",
                       last_df$ITEM_CD[i], ",'",
                       last_df$ITEM_NM[i], "',",
                       last_df$SUM_RUNTIME[i], ",",
                       last_df$노출HH[i], ",'",
                       
                       last_df$GRP_CD[i], "','", 
                       last_df$GRP[i], "',", 
                       last_df$PREDICT_VALUE[i], ",'",
                       # last_df$ETL_DATE, "','",
                       # last_df$ETL_HOUR, "','",
                       # last_df$ETL_WEK, "')"  )  
                       substr(SYSDATE, 1,10), "','",
                       substr(SYSDATE, 12,13), "','",
                       c("일", "월", "화", "수", "목", "금", "토")[wday(SYSDATE)], "')"  )
    tryCatch(dbSendUpdate(conn, query.x), error = function(e) print("NOT"), warning = function(w) print("NOT"))
    
  }
  
  dbDisconnect(conn)
  rm(conn,drv)
  
  #####------- 결과 저장 -------##### 
  
  print("아이템 예측결과 DB적재완료")
  
}



save.image(file = "/home/DTA_CALL/AWS/DAT/WEK/CALL_WEK3.RData")
load(file = "/home/DTA_CALL/AWS/DAT/WEK/CALL_WEK3.RData") 



