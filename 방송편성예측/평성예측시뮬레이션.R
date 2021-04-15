#방송예측 시뮬레이션
# rm(list=ls())
# .rs.restartR()
##### 패키지 불러오기 ####
library(lightgbm)
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


library(stringdist)
library(text2vec)
library(magrittr)
library(proxy)


###### 패키지 #####

# source(file = "/home/DTA_CALL/DB_CON.R")
# source(file = "/home/jovyan/DB_con/DB_conn.R")
source(file = "/home/dio/DB_con/DB_conn.R")

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
# (SYSDATE <- as.character(Sys.time()) )
(SYSDATE <- as.character(paste0(ymd(STRT_DT), " 03:00:00") ))

print(STRT_DT); print(LAST_DT); print(SYSDATE)
# 


# SELECT
# PA1.PGM_ID
# ,PA1.ITEM_CD
# ,PA1.BROAD_DT
# ,SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.EXPCT_SAL_AMT else 0 end) AS EXPCT_SAL_AMT
# ,SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.CNF_FEE_AMT else 0 end) AS CNF_FEE_AMT
# ,sum(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.ADVR_SALE_AMT else 0 end) AS ADVR_SALE_AMT
# ,SUM(PA2.TGT_SAL_AMT) AS TGT_SAL_AMT
# , (CASE WHEN SUM(PA2.TGT_SAL_AMT) > 0  
#    THEN ROUND((SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.EXPCT_SAL_AMT else 0 end) + 
#                  SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.CNF_FEE_AMT else 0 end ) + 
#                  sum(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.ADVR_SALE_AMT else 0 end))/SUM(PA2.TGT_SAL_AMT),3)*100
#    ELSE 0 END) AS SALE_PERCENT_ONAIR

print("데이터1 호출")
BRD_df_bs_ontime <- dbGetQuery(conn, paste0("
                                            SELECT PA1.PGM_ID
                                            ,PA1.ITEM_CD
                                            ,PA1.BROAD_DT
                                            ,SUM(PA1.EXPCT_SAL_AMT) AS EXPCT_SAL_AMT
                                            ,SUM(PA1.CNF_FEE_AMT) AS CNF_FEE_AMT
                                            ,sum(PA1.ADVR_SALE_AMT) AS ADVR_SALE_AMT
                                            --,SUM(PA2.TGT_SAL_AMT) AS TGT_SAL_AMT
                                            ,(SUM(PA1.EXPCT_SAL_AMT) + SUM(PA1.CNF_FEE_AMT) + sum(PA1.ADVR_SALE_AMT)) AS T_EXPCCT_ASL_AMT
                                            
                                            
                                            FROM 
                                            (SELECT /*+ FULL(A1) FULL(A2) PARALLEL (A1 6) */ 
                                            A1.PGM_ID
                                            ,A2.ITEM_CD
                                            ,A1.BROAD_DT
                                            ,A1.BROAD_ORD_TIME_CD
                                            ,SUM(EXPCT_SAL_AMT) AS EXPCT_SAL_AMT
                                            ,SUM(CNF_FEE_AMT) AS CNF_FEE_AMT
                                            ,SUM(ADVR_SALE_AMT) AS ADVR_SALE_AMT
                                            
                                            FROM F_ORD_ORD_RLRSLT_S A1
                                            LEFT JOIN d_brd_broad_form_prd_m A2  ON A1.PGM_ID = A2.PGM_ID AND A1.PRD_CD = A2.PRD_CD
                                            --WHERE ROWNUM <= 100
                                            WHERE A1.BROAD_DT BETWEEN '20160101' AND '" , STRT_DT, "'
                                            
                                            GROUP BY
                                            A1.PGM_ID
                                            ,A2.ITEM_CD
                                            ,A1.BROAD_DT
                                            ,A1.BROAD_ORD_TIME_CD) PA1
                                            JOIN (
                                            SELECT 
                                            B1.PGM_ID
                                            ,B2.ITEM_CD
                                            ,B1.BROAD_DT
                                            --,B1.BROAD_ORD_TIME_CD
                                            ,SUM(B1.TGT_SAL_AMT) TGT_SAL_AMT
                                            
                                            FROM F_ORD_BROAD_TGT_D B1
                                            LEFT JOIN d_brd_broad_form_prd_m B2  ON B1.PGM_ID = B2.PGM_ID AND B1.PRD_CD = B2.PRD_CD
                                            
                                            AND B1.BROAD_RLRSLT_GBN_DTL_CD in ('CS', 'CM') --live방송 캐이블싱글,멀티
                                            
                                            GROUP BY
                                            B1.PGM_ID
                                            ,B2.ITEM_CD
                                            ,B1.BROAD_DT
                                            ,B1.BROAD_ORD_TIME_CD
                                            ) PA2 
                                            ON PA1.PGM_ID = PA2.PGM_ID AND
                                            PA1.ITEM_CD = PA2.ITEM_CD AND
                                            PA1.BROAD_DT = PA2.BROAD_DT
                                            -- PA1.BROAD_ORD_TIME_CD = PA2.BROAD_ORD_TIME_CD 
                                            
                                            GROUP BY 
                                            PA1.PGM_ID
                                            ,PA1.ITEM_CD
                                            ,PA1.BROAD_DT"))


print("데이터1 호출 완료")
###### 방송, 콜 데이터 실적 ###### 49483 - 157
# CALL_BRD_df_bs <- dbGetQuery(conn, paste0(" SELECT ITEM.ITEM_NM
#                                        , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1) AS BRAND
#                                        , PRD_GRP.PRD_GRP_NM
#                                        , A.*
#                                        , DT.WEKDY_NM
#                                        , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일
#                                        , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)  
#                                        FROM DTA_OWN.GSTS_PGM_ITEM_ORD A   /* PGM,ITEM별 주문*/
#                                        ,GSBI_OWN.D_PRD_ITEM_M ITEM
#                                        ,DHUB_OWN.DH_DT DT
#                                        ,GSBI_OWN.V_CMM_PRD_GRP_C PRD_GRP
#                                        WHERE -- A.PGM_ID = B.PGM_ID
#                                        A.ITEM_CD = ITEM.ITEM_CD
#                                        AND A.BROAD_DT = DT.DT_CD  
#                                        AND A.PRD_GRP_CD = PRD_GRP.PRD_GRP_CD
#                                        AND A.BROAD_DT < '" , STRT_DT,"' "))

print("데이터2 호출")
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

print("데이터2 호출 완료")


BRD_df_bs_ontime <- BRD_df_bs_ontime %>% filter(T_EXPCCT_ASL_AMT >0 ) 
CALL_BRD_df_bs <- merge(CALL_BRD_df_bs,BRD_df_bs_ontime, by=c("PGM_ID","ITEM_CD","BROAD_DT"))

# CALL_BRD_df_bs %>%
# filter(PGM_ID %in% c("373880","373890","373909","373916","373955","373964","373989","374000")) %>%
#   filter(ITEM_CD %in% c("511470","321217","601959","614541","308960","521955","422866","528153")) %>%
#   mutate(weght_value = EXPCT_SAL_AMT/(WEIHT_MI/60)) %>%
#   dplyr::select(PGM_ID,ITEM_CD,ITEM_NM,WEIHT_MI,EXPCT_SAL_AMT,weght_value,SUM_RUNTIME) %>%
#   mutate(WEIHT_MI =WEIHT_MI/60) %>%
#   mutate(SUM_RUNTIME = SUM_RUNTIME/60)

# CALL_BRD_df_bs %>%
#   filter(PGM_ID %in% c("374097")) %>%
#   mutate(weght_value = EXPCT_SAL_AMT/(WEIHT_MI/60)) %>%
#   dplyr::select(PGM_ID,ITEM_CD,ITEM_NM,WEIHT_MI,EXPCT_SAL_AMT,weght_value,SUM_RUNTIME) %>%
#   mutate(WEIHT_MI =WEIHT_MI/60) %>%
#   mutate(SUM_RUNTIME = SUM_RUNTIME/60)


#쿠쿠
#제이코닉

# ###### 편성 데이터 예측에 활용 #####
# CALL_SCH_df <- dbGetQuery(conn, paste0("SELECT * FROM DTA_OWN.PRE_SCHDL_TB WHERE ETL_DATE = '",STRT_DT,"'  "  ))

# 가중분
# CALL_SCH_df_w <- dbGetQuery(conn, paste0("
#                                          SELECT
#                                          BB.PGM_ID
#                                          ,ITEM_CD
#                                          ,ADVR_FEE_YN
#                                          ,CNF_FEE_YN
#                                          ,ROUND(SUM(TGT_EXPCT_SAL_AMT),0) AS TGT_EXPCT_SAL_AMT
#                                          ,ROUND(SUM(PLAN_WEIHT_SE/60),0) AS WEIHT_MI
#                                          
#                                          FROM d_brd_broad_form_prd_m AA
#                                          INNER JOIN d_brd_broad_form_M BB ON AA.PGM_ID = BB.PGM_ID 
#                                          WHERE ITEM_CD >0 AND BB.BROAD_DT BETWEEN '",STRT_DT,"' AND '",LAST_DT,"'
#                                          GROUP BY BB.PGM_ID
#                                          ,ITEM_CD
#                                          ,ADVR_FEE_YN
#                                          ,CNF_FEE_YN"))

print("데이터3 호출")

#방송 편성
CALL_SCH_df <- dbGetQuery(conn, paste0("
                                       SELECT 
                                       A.PULIN_GRP_NM,A.PULIN_GRP_SEQ,A.PGM_GBN_CD,A.PGM_ID,A.PGM_NM,A.TITLE_ID,A.TITLE_NAME,A.HH,A.HOPE_BROAD_STR_DTM,A.HOPE_BROAD_END_DTM,
                                       A.HOPE_BROAD_DY,A.HOLDY_YN,A.PRD_GRP_CD,A.PRD_GRP_NM,A.ITEM_CD,A.ITEM_NM,A.BRAND,
                                       A.PRD_CD,A.PRD_NM,A.PRD_SALE_PRC,A.NEW_PRD_YN,A.RUN_TIME,A.PROFIT_TYP_VAL,
                                       AVG(B.WEIHT_RT) AS WEIHT_RT, 
                                       ROUND(A.RUN_TIME*AVG(B.WEIHT_RT),0) AS WEIHT_TIME
                                       FROM
                                       (
                                       SELECT DISTINCT
                                       A.PULIN_GRP_NM 
                                       , A.PULIN_GRP_SEQ
                                       --, B.SEQ
                                       , (SELECT FN_GET_BI_CMM_DTL_CD('BI131', E.BROAD_TYPE) FROM DUAL) AS PGM_GBN_CD	
                                       , B.HOPE_BROAD_PGM_ID AS PGM_ID
                                       , (CASE WHEN I.TITLE_NAME IS NOT NULL THEN I.TITLE_NAME
                                       ELSE E.TITLE_NAME
                                       END) AS PGM_NM
                                       , E.TITLE_ID
                                       , E.TITLE_NAME
                                       , SUBSTR(TO_CHAR(B.HOPE_BROAD_STR_DTM, 'YYYYMMDDHH24MISS'),9,2) AS HH
                                       , TO_CHAR(B.HOPE_BROAD_STR_DTM, 'YYYYMMDDHH24MISS') AS HOPE_BROAD_STR_DTM
                                       , TO_CHAR(B.HOPE_BROAD_END_DTM, 'YYYYMMDDHH24MISS') AS HOPE_BROAD_END_DTM
                                       , TO_CHAR(B.HOPE_BROAD_STR_DTM, 'DY') AS HOPE_BROAD_DY
                                       , G.HOLDY_YN
                                       , B.PRD_GRP_CD
                                       , C.MCLASS_NAME AS PRD_GRP_NM
                                       , DECODE(B.ITEM_CD, 0, '', B.ITEM_CD) AS ITEM_CD
                                       , D.ITEM_NAME AS ITEM_NM
                                       , REGEXP_SUBSTR(D.ITEM_NAME, '[^:]+', 1, 1) AS BRAND
                                       , B.PRD_CD
                                       , H.PRD_NM
                                       , H.PRD_SALE_PRC
                                       , B.NEW_PRD_YN
                                       , round((B.HOPE_BROAD_END_DTM - B.HOPE_BROAD_STR_DTM)* 24 * 60) AS RUN_TIME 
                                       , DECODE(B.PROFIT_TYP_VAL, 'A', '광고매출', 'F', '확정수수료', 'N') AS PROFIT_TYP_VAL  --수익유형 (F : 확정수수료 / A : 광고매출)
                                       FROM   SDHUB_OWN.STG_TB_BY415 A  -- 풀인기본정보
                                       , SDHUB_OWN.STG_TB_BY416 B  -- 편성풀인아이템정보 
                                       , SDHUB_OWN.STG_TB_BY190 C  -- 상품군 조직 매핑 
                                       , SDHUB_OWN.STG_TB_BY049 D  -- 브랜드별 상품코드 매핑 
                                       , SDHUB_OWN.STG_TB_BY100 E  -- 프로그램 편성
                                       , DHUB_OWN.DH_DT         G  -- HOLDY_YN
                                       , GSBI_OWN.D_PRD_PRD_M   H
                                       , SDHUB_OWN.STG_TB_BY008 I  -- 프로그램 타이틀
                                       WHERE 1=1
                                       AND TO_CHAR(A.BROAD_STR_DTM, 'YYYYMMDD') >= '",STRT_DT,"'
                                       AND UPPER(A.PULIN_GRP_NM) LIKE '%<선편성 M-1> 3월%'
                                       
                                       AND A.PULIN_GRP_SEQ = B.PULIN_GRP_SEQ
                                       AND B.DEL_YN        = 'N'
                                       AND B.PRD_GRP_CD    = C.MCLASS_CODE(+)
                                       AND B.ITEM_CD       = D.ITEM_CODE(+)
                                       AND B.HOPE_BROAD_PGM_ID  = E.PGM_ID(+)
                                       AND TO_CHAR(B.HOPE_BROAD_STR_DTM, 'YYYYMMDD') = G.DT_CD(+)
                                       AND B.PRD_CD = H.PRD_CD(+)
                                       AND E.TITLE_ID  = I.TITLE_ID(+)
                                       
                                       AND D.ITEM_NAME NOT LIKE '%예비%'
                                       AND D.ITEM_NAME NOT LIKE '%상담%'
                                       
                                       ORDER BY 7
                                       ) A,
                                       (--가중율 추가--
                                       SELECT /*+ FULL(B)  PARALLEL(B 4) */
                                       B.STD_DT||B.STD_TM AS DT,
                                       B.WEIHT_RT  
                                       FROM GSBI_OWN.D_BRD_BROAD_WEIHT_RT_M B 
                                       WHERE B.CHANL_CD = 'C'
                                       ) B
                                       WHERE 1=1
                                       AND B.DT BETWEEN SUBSTR(A.HOPE_BROAD_STR_DTM,1,12) AND SUBSTR(A.HOPE_BROAD_END_DTM,1,12) 
                                       GROUP BY 
                                       A.PULIN_GRP_NM,A.PULIN_GRP_SEQ,A.PGM_GBN_CD,A.PGM_ID,A.PGM_NM,A.TITLE_ID,A.TITLE_NAME,A.HH,A.HOPE_BROAD_STR_DTM,A.HOPE_BROAD_END_DTM,
                                       A.HOPE_BROAD_DY,A.HOLDY_YN,PRD_GRP_CD,A.PRD_GRP_NM,A.ITEM_CD,A.ITEM_NM,A.BRAND,
                                       A.PRD_CD,A.PRD_NM,A.PRD_SALE_PRC,A.NEW_PRD_YN,A.RUN_TIME,A.PROFIT_TYP_VAL
                                       "))

print("데이터3 호출완료")

CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$PRD_GRP_NM), ]
CALL_SCH_df$TITLE_ID <- ifelse(is.na(CALL_SCH_df$TITLE_ID), 0, CALL_SCH_df$TITLE_ID)
CALL_SCH_df$FST_BROAD_DTM <- ifelse(CALL_SCH_df$ITEM_CD %in% CALL_BRD_df_bs$ITEM_CD, 20200101, 99999999)
CALL_SCH_df <- CALL_SCH_df[!CALL_SCH_df$PRD_GRP_NM %in% c("렌탈","보험","여행","핸드폰"),] #특정 카테고리 제외
# CALL_SCH_df <- CALL_SCH_df[TITLE_NM == c("렌탈."),] #특정 카테고리 제외


# CALL_SCH_df$AVG_PRICE <- ifelse(is.na(CALL_SCH_df$AVG_PRICE), 0, CALL_SCH_df$AVG_PRICE)
# CALL_SCH_df$MIN_PRICE <- ifelse(is.na(CALL_SCH_df$MIN_PRICE), 0, CALL_SCH_df$MIN_PRICE)
# CALL_SCH_df$MAX_PRICE <- ifelse(is.na(CALL_SCH_df$MAX_PRICE), 0, CALL_SCH_df$MAX_PRICE)
# CALL_SCH_df <- merge(CALL_SCH_df,CALL_SCH_df_w, by=c("PGM_ID","ITEM_CD"),all.x=T)
# rm(CALL_SCH_df_w)


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

print("데이터 모두 완료")


RMSE <- function(m, o){ sqrt(mean((m - o)^2))}
MAPE <- function(m, o){ n <- length(m)
res <- (100 / n) * sum(abs(m-o)/m)
res  }

dbDisconnect(conn)
rm(conn,drv)

range(CALL_BRD_df_bs$MIN_Q_START_DATE)
range(ymd_hms(CALL_SCH_df$HOPE_BROAD_STR_DTM))


### ITEM 중복제거
CALL_SCH_df <- merge(
  CALL_SCH_df %>%
    dplyr::group_by(PGM_ID,ITEM_CD) %>%
    dplyr::mutate(pgm_count = row_number()) %>%
    dplyr::filter(pgm_count == 1) %>%
    data.frame()
  ,
  CALL_SCH_df %>%
    dplyr::group_by(PGM_ID,ITEM_CD) %>%
    dplyr::summarise(AVG_BROAD_SALE_PRC = mean(PRD_SALE_PRC,na.rm=T)) %>%
    dplyr::mutate(AVG_BROAD_SALE_PRC = ifelse(is.nan(AVG_BROAD_SALE_PRC),0,AVG_BROAD_SALE_PRC)) %>%
    data.frame()
  , by = c("PGM_ID","ITEM_CD"),all.x=T) %>%
  dplyr::select(-pgm_count,-PRD_SALE_PRC,-PRD_CD,-PRD_NM)

# 
# A <- names(table(CALL_SCH_df$HOPE_BROAD_STR_DTM)[table(CALL_SCH_df$HOPE_BROAD_STR_DTM)>1])
# if(length(A) > 0) {
#   # tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
#   which_ptb <- which(CALL_SCH_df$HOPE_BROAD_STR_DTM %in% A)
#   diff_v <- diff(which_ptb)
#   check_df <- c(which(diff_v != 1) - 1, length(diff_v)) + 1
#   which_out <- which_ptb[-check_df]   
#   
#   if(length(which_out) == 0) {
#     CALL_SCH_df <- CALL_SCH_df[-which_ptb[2], ]
#   } else {
#     CALL_SCH_df <- CALL_SCH_df[-which_out, ]   
#   }
#   
# }
# 
# B <- names(table(CALL_SCH_df$MAX_Q_END_DATE)[table(CALL_SCH_df$MAX_Q_END_DATE)>1])
# A ; B
# if(length(B) > 0) {
#   # tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
#   which_ptb <- which(CALL_SCH_df$MAX_Q_END_DATE %in% B)
#   diff_v <- diff(which_ptb)
#   check_df <- c(which(diff_v != 1) - 1, length(diff_v)) + 1
#   which_out <- which_ptb[-check_df]
#   
#   if(length(which_out) == 0) {
#     CALL_SCH_df <- CALL_SCH_df[-which_ptb[2], ]
#   } else {
#     CALL_SCH_df <- CALL_SCH_df[-which_out, ]   
#   }
# }
# 
# CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$PRD_GRP_NM), ]
# # 가중분 결측값 처리 어떻게 할까....
# CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$WEIHT_MI), ] #40개 삭제


#----------------------------------------------------------------
# 주 편성표 결과 저장 2~5 시 돌리는 경우만 
#----------------------------------------------------------------

# end <- 1
# if (end == 1) { 


print(paste(" EDA 마트생성시작 ", as.character(Sys.time())))
#----------------------------------------------------------------
# 2. EDA 수행  
#----------------------------------------------------------------


## 필터 생성 ##

# CALL_BRD_df_bs <- CALL_BRD_df_bs %>% filter(PRD_GRP_NM != "핸드폰")
# CALL_BRD_df_bs <- CALL_BRD_df_bs %>% filter(PGM_GBN != "순환방송")
CALL_BRD_df_bs <- CALL_BRD_df_bs %>% filter(SUM_RUNTIME/60 > 10 , WEIHT_MI/60 >0)

##

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

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , ADVR_FEE_YN := ifelse(ADVR_SALE_AMT>0,"Y","N")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , CNF_FEE_YN := ifelse(CNF_FEE_AMT>0,"Y","N")]


############## 편성 분당 취급액 




#WEK = 1주중 , 2토, 3일
#WEK_G = 1~7 : 월~일


# 개월별 시간대별
CALL_df <- CALL_BRD_df_bs[ , .(
  전달취급액 = mean(EXPCT_SAL_AMT)),  #그 달에 주문 평균 수량
  by = c("mon", "WEK", "HH")]

CALL_df <- CALL_df[order(mon, WEK, HH)]

# CALL_df_FEE <- CALL_BRD_df_bs[ , .(
#   전달취급액 = mean(EXPCT_SAL_AMT)),  #그 달에 주문 평균 수량
#   by = c("mon", "WEK", "HH")]
# CALL_df_FEE <- CALL_df_FEE[order(mon, WEK, HH)]


# 개월별 시간대별 + 요일별 
CALL_df2 <- CALL_BRD_df_bs[ , .(
  전달취급액 = mean(EXPCT_SAL_AMT)),
  by = c("mon", "WEK_G", "HH")]
CALL_df2 <- CALL_df2[order(mon, WEK_G, HH)]


# 수량 인입 예측
CALL_BRD_df_bs <- data.table(CALL_BRD_df_bs)
CALL_SCH_df <- data.table(CALL_SCH_df)


####################
####################
# 시간 생성

CALL_SCH_df <- CALL_SCH_df[, BROAD_DT := substr(HOPE_BROAD_STR_DTM,1,8)]
CALL_SCH_df <- CALL_SCH_df[, BROAD_HH := substr(HOPE_BROAD_STR_DTM,9,10)]

CALL_SCH_df <- CALL_SCH_df[, HH := as.numeric(BROAD_HH) ]
CALL_SCH_df$HH <- ifelse(CALL_SCH_df$HH == 0, 24, CALL_SCH_df$HH)
# CALL_SCH_df <- na.omit(CALL_SCH_df)


######################
# CALL_SCH_df <- CALL_SCH_df[,HOPE_BROAD_STR_DTM := ymd_hms(HOPE_BROAD_STR_DTM)]
# CALL_SCH_df <- CALL_SCH_df[,HOPE_BROAD_END_DTM := ymd_hms(HOPE_BROAD_END_DTM)]


CALL_SCH_df <- CALL_SCH_df[ , WEK := wday(ymd(BROAD_DT))]
CALL_SCH_df <- CALL_SCH_df[ , WEK := WEK - 1 ]
CALL_SCH_df$WEK <- ifelse(CALL_SCH_df$WEK == 0, 7, CALL_SCH_df$WEK)
CALL_SCH_df <- CALL_SCH_df[ , WEK_G := ifelse(WEK == 1, "1.월",
                                              ifelse(WEK == 2, "2.화",
                                                     ifelse(WEK == 3, "3.수",
                                                            ifelse(WEK == 4, "4.목",
                                                                   ifelse(WEK == 5, "5.금",
                                                                          ifelse(WEK == 6, "6.토","7.일")))))) ]
CALL_SCH_df <- CALL_SCH_df[ , WEK := ifelse(WEK %in% c(1:5), "1.주중",
                                            ifelse(WEK == 6, "2.토","3.일")) ]
CALL_SCH_df <- CALL_SCH_df[ , mon := ymd(paste0(substr(BROAD_DT, 1, 6), "-01"))]


CALL_SCH_df <- CALL_SCH_df[ , ADVR_FEE_YN := ifelse(PROFIT_TYP_VAL=="광고매출","Y","N")]
CALL_SCH_df <- CALL_SCH_df[ , CNF_FEE_YN := ifelse(PROFIT_TYP_VAL=="확정수수료","Y","N")]


names(CALL_SCH_df)[which(names(CALL_SCH_df)=="HOPE_BROAD_STR_DTM")] <- "MIN_Q_START_DATE"
names(CALL_SCH_df)[which(names(CALL_SCH_df)=="HOPE_BROAD_END_DTM")] <- "MAX_Q_END_DATE"

# 순서 
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_SCH_df <- CALL_SCH_df[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_BRD_df_bs$no <- 1:nrow(CALL_BRD_df_bs)
CALL_SCH_df$no <- (nrow(CALL_BRD_df_bs) + 1 ):(nrow(CALL_BRD_df_bs) + nrow(CALL_SCH_df) )


#
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("BROAD_PRD_CNT"))] <- "CNT_PRDCD"
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("AVG_BROAD_SALE_PRC"))] <- "AVG_PRICE"
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("FST_BROAD_DT"))] <- "FST_BROAD_DTM"
CALL_BRD_df_bs$SUM_RUNTIME <- round(CALL_BRD_df_bs$SUM_RUNTIME/60)
CALL_BRD_df_bs$WEIHT_MI <- round(CALL_BRD_df_bs$WEIHT_MI/60)

CALL_BRD_df_bs <- CALL_BRD_df_bs[CN_RS_YN == "N"]


names(CALL_SCH_df)[which(names(CALL_SCH_df)=="RUN_TIME")] <- "SUM_RUNTIME"
names(CALL_SCH_df)[which(names(CALL_SCH_df)=="TITLE_NAME")] <- "TITLE_NM"
names(CALL_SCH_df)[which(names(CALL_SCH_df)=="AVG_BROAD_SALE_PRC")] <- "AVG_PRICE"

CALL_SCH_df <- CALL_SCH_df[,WEKDY_YN:=ifelse(WEK =="1.주중",0,1)]
CALL_SCH_df <- CALL_SCH_df[,WEKDY_NM:=as.character(wday(ymd(CALL_SCH_df$BROAD_DT),label=T))]


### 사전 구축

# ITEM_ID <- unique(CALL_SCH_df$ITEM_NM)
# ITEM_ID <- data.frame(id = 1:length(ITEM_ID), name = ITEM_ID)
# # 팩터를 케릭터형으로 
# ITEM_ID <- ITEM_ID %>% mutate_if(is.factor, as.character) %>% as.data.table
# setDT(ITEM_ID)
# setkey(ITEM_ID, id)
# # 단어형태 변경 
# prep_fun = tolower
# tok_fun = word_tokenizer
# train = ITEM_ID[J(ITEM_ID)]
# it_train = itoken(train$name,
#                   preprocessor = prep_fun,
#                   tokenizer = tok_fun,
#                   ids = train$id,
#                   progressbar = FALSE)
# vocab = create_vocabulary(it_train)
# # library(glmnet)
# vectorizer = vocab_vectorizer(vocab)
# dtm_train = create_dtm(it_train, vectorizer)
# ITEM_ID <- cbind.data.frame(ITEM_ID, t(apply(dtm_train, 1, function(x) names(which(x >= 1))[1:5] )))
# rm(dtm_train, vocab, train)
# CALL_SCH_df <- merge(CALL_SCH_df,ITEM_ID ,by.x = "ITEM_NM", by.y="name",all.x=T)


#46, 127, 211
# CALL_SCH_df2 <- CALL_SCH_df
# CALL_SCH_df <- CALL_SCH_df2

# i<-71
# CALL_SCH_df[CALL_SCH_df$AVG_PRICE==0,]


# ITEM기준 최신 가격 갖고오기
# 아이템이 없으면 같은체인룰 기반 평균가격 그대로 갖고오기
CALL_SCH_df <- do.call(rbind,mclapply(1:nrow(CALL_SCH_df),function(i){
  
  tmp <- CALL_SCH_df[i,]
  
  if(tmp$AVG_PRICE==0){
    itm <- tmp$ITEM_CD
    price <-CALL_BRD_df_bs[max(which(CALL_BRD_df_bs$ITEM_CD %in% itm)),"AVG_PRICE"]
    
    if(is.na(price)){
      price <- 0
    }
    
    tmp$AVG_PRICE <- price
    tmp
  } else{tmp}
},mc.cores = 4))


# 가중분당 광고매출 갖고오기
CALL_SCH_df <- do.call(rbind,mclapply(1:nrow(CALL_SCH_df),function(i){
  # i<-1
  tmp <- CALL_SCH_df[i,]
  
  if(tmp$ADVR_FEE_YN=="Y"){
    itm <- tmp$ITEM_CD
    
    ADVR_SALE_AMT_mi <- CALL_BRD_df_bs %>% dplyr::filter(BROAD_DT > gsub("-","",Sys.Date()-90),ITEM_CD==itm) %>%
      dplyr::filter(ADVR_SALE_AMT>0) %>%
      mutate(ADVR_SALE_AMT_mi = ADVR_SALE_AMT/WEIHT_MI) %>%
      dplyr::summarise(men = round(mean(ADVR_SALE_AMT_mi)))
    
    if(is.na(ADVR_SALE_AMT_mi)){
      ADVR_SALE_AMT_mi <- 0
    }
    
    tmp$ADVR_SALE_AMT_mi <- ADVR_SALE_AMT_mi
    tmp
  } else{tmp$ADVR_SALE_AMT_mi <- 0
  tmp}
},mc.cores = 4))


# 가중분당 확정수수료 갖고오기
CALL_SCH_df <- do.call(rbind,mclapply(1:nrow(CALL_SCH_df),function(i){
  # i<-503
  tmp <- CALL_SCH_df[i,]
  
  if(tmp$CNF_FEE_YN=="Y"){
    itm <- tmp$ITEM_CD
    
    CNF_FEE_YN_mi <- CALL_BRD_df_bs %>% dplyr::filter(BROAD_DT > gsub("-","",Sys.Date()-90),ITEM_CD==itm) %>%
      dplyr::filter(CNF_FEE_YN>0) %>%
      mutate(CNF_FEE_YN_mi = CNF_FEE_AMT/WEIHT_MI) %>%
      dplyr::summarise(men = round(mean(CNF_FEE_YN_mi)))
    
    if(is.na(CNF_FEE_YN_mi)){
      CNF_FEE_YN_mi <- 0
    }
    
    tmp$CNF_FEE_YN_mi <- CNF_FEE_YN_mi
    tmp
  } else{tmp$CNF_FEE_YN_mi <- 0
  tmp}
},mc.cores = 4))


# 달성율, 가중분 추가
# BRD_INFO$PGM_ID <- as.numeric(BRD_INFO$PGM_ID)
# BRD_INFO2 <- BRD_INFO[,c("BROAD_DT","BROAD_TIMERNG_CD","PGM_ID","TITLE_NM","PRD_GRP_NM","ITEM_CD","WEIHT_MI")]
# CALL_BRD_df_bs_t <- merge(CALL_BRD_df_bs,BRD_INFO2,by.x = c("BROAD_DT","BROAD_HH","PGM_ID","TITLE_NM","PRD_GRP_NM","ITEM_CD"), 
#                       by.y=c("BROAD_DT","BROAD_TIMERNG_CD","PGM_ID","TITLE_NM","PRD_GRP_NM","ITEM_CD"),all.x=T)


## PGM추가
BRAND_PGM_NM <- names(table(CALL_BRD_df_bs$BRAND_PGM_NM))
BRAND_PGM_NM <- c(BRAND_PGM_NM,"The Collection 2호")

CALL_SCH_df$BRAND_PGM_NM <- ifelse(CALL_SCH_df$PGM_NM %in% BRAND_PGM_NM,CALL_SCH_df$PGM_NM,"일반")
CALL_SCH_df$PGM_GBN <- ifelse(as.numeric(substr(CALL_SCH_df$MIN_Q_START_DATE,9,10)) %in% c(2,3,4,5),"순환방송",
                              ifelse(CALL_SCH_df$BRAND_PGM_NM=="일반","일반PGM","브랜드PGM"))


CALL_SCH_df$MIN_Q_START_DATE <- ymd_hms(CALL_SCH_df$MIN_Q_START_DATE)
CALL_SCH_df$MAX_Q_END_DATE <- ymd_hms(CALL_SCH_df$MAX_Q_END_DATE)

# 백업용 
CALL_BRD_BACK_df <- CALL_BRD_df_bs

names(CALL_SCH_df)[which(names(CALL_SCH_df)=="TGT_EXPCT_SAL_AMT")] <- "TGT_SAL_AMT"
# 편성예측때 SALE_PERCENT_ONAIR로 활용
CALL_BRD_df_bs    <- CALL_BRD_df_bs[, .(no, BROAD_DT, BROAD_HH, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME, 
                                        PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                        ITEM_CD, ITEM_NM, BRAND, 
                                        WEKDY_NM, WEKDY_YN, HOLDY_YN, 
                                        #CN_RS_YN, CNT_PRDCD, 
                                        FST_BROAD_DTM,
                                        AVG_PRICE, 
                                        PRD_GRP_NM, 
                                        #WEIHT_MI, 
                                        ADVR_FEE_YN, CNF_FEE_YN
                                        # ,TGT_SAL_AMT ,
                                        ,ADVR_SALE_AMT ,CNF_FEE_AMT
                                        # EXPCT_SAL_AMT, SALE_PERCENT_ONAIR, 
                                        ,T_AMT_ONAIR, T_QTY_ONAIR, T_QTY_ONAIR_MCPC, T_CNT_ONAIR, T_CNT_PRE, T_CNT_ONAIR_MCPC, EXPCT_SAL_AMT)] 


CALL_BRDSCH_df <- CALL_SCH_df[, .(no, BROAD_DT, BROAD_HH, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME, 
                                  PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                  ITEM_CD, ITEM_NM, BRAND, 
                                  WEKDY_NM, WEKDY_YN, HOLDY_YN, 
                                  #CN_RS_YN, CNT_PRDCD, 
                                  FST_BROAD_DTM,
                                  AVG_PRICE, 
                                  PRD_GRP_NM,
                                  #WEIHT_MI,
                                  ADVR_FEE_YN, CNF_FEE_YN)] #,TGT_SAL_AMT)]


# 
CALL_BRD_df_bs <- rbind.fill(CALL_BRD_df_bs, CALL_BRDSCH_df)
CALL_BRD_df_bs <- data.table(CALL_BRD_df_bs)


###### 제외 사항
CALL_BRD_df_bs <- CALL_BRD_df_bs[!PRD_GRP_NM %in% c("렌탈","보험","여행","핸드폰")] #특정 카테고리 제외
CALL_BRD_df_bs <- CALL_BRD_df_bs[PGM_GBN != "순환방송"]
# CALL_BRD_df_bs <- CALL_BRD_df_bs[WEIHT_MI > 5]
CALL_BRD_df_bs <- CALL_BRD_df_bs[SUM_RUNTIME > 10]

CALL_BRD_df_bs <- CALL_BRD_df_bs[CALL_BRD_df_bs$TITLE_NM!="오늘의 선택",]
CALL_BRD_df_bs <- CALL_BRD_df_bs[CALL_BRD_df_bs$TITLE_NM!="타임특가",]


# CALL_BRD_df_bs %>% filter(PRD_GRP_NM=="디지털기기") %>% dplyr::count(ITEM_NM)

#상품평
# CMT_df <- data.table(CMT_df)


# 표준화 총콜
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분취급액 := EXPCT_SAL_AMT / SUM_RUNTIME ]
#
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분수량 := round(T_QTY_ONAIR / SUM_RUNTIME) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분모수량 := round(T_QTY_ONAIR_MCPC / SUM_RUNTIME) ]

# CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분취급액 := EXPCT_SAL_AMT / WEIHT_MI ]
#
# CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분수량 := round(T_QTY_ONAIR / WEIHT_MI) ]
# CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분모수량 := round(T_QTY_ONAIR_MCPC / WEIHT_MI) ]


# 상품 시작시간
#35875

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 노출HH := substr(MIN_Q_START_DATE, 12, 13) ]
CALL_BRD_df_bs$노출HH <- as.numeric(CALL_BRD_df_bs$노출HH)
CALL_BRD_df_bs$노출HH <- ifelse(CALL_BRD_df_bs$노출HH == 0, 24, CALL_BRD_df_bs$노출HH)


# 날짜 
CALL_BRD_df_bs$BROAD_DT  <- ymd(CALL_BRD_df_bs$BROAD_DT)
CALL_BRD_df_bs$BRD_DAYT  <- ymd_h(substr(paste(CALL_BRD_df_bs$BROAD_DT, CALL_BRD_df_bs$BROAD_HH) , 1, 13))

CALL_BRD_df_bs$MIN_Q_START_DATE  <- ymd_hms(CALL_BRD_df_bs$MIN_Q_START_DATE)
CALL_BRD_df_bs$MAX_Q_END_DATE  <- ymd_hms(CALL_BRD_df_bs$MAX_Q_END_DATE)

### 회차 먹이기 
CALL_BRD_df_bs <- CALL_BRD_df_bs[SUM_RUNTIME != 0 ,]

# 순서 
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]

# 아이템, 브랜드, 상품분류
CALL_BRD_df_bs <- CALL_BRD_df_bs[, IT_SF := seq(.N), by = c("ITEM_NM")]

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 모_수량 := round(T_QTY_ONAIR_MCPC/T_QTY_ONAIR, 3) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 모_고객수 := round(T_CNT_ONAIR_MCPC/T_CNT_ONAIR, 3) ]

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , WEK := ifelse(WEKDY_NM == "Sun", "3.일",
                                                  ifelse(WEKDY_NM == "Sat", "2.토", "1.주중")) ]


# 상담여부 
# names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) == "CN_RS_YN")] <- "상담"
# CALL_BRD_df_bs <- CALL_BRD_df_bs[상담!="Y"] # 상담 방송 제외

# 신상여부
CALL_BRD_df_bs[is.na(FST_BROAD_DTM) | FST_BROAD_DTM == "99999999", "FST_BROAD_DTM"] <- "99991231"   
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 신상 := ifelse(BROAD_DT == ymd(substr(FST_BROAD_DTM,1,8)) | FST_BROAD_DTM=="99991231", "YES", "NO" )]


CALL_BRD_df_bs[ is.na(모_수량) , "모_수량"] <- 0
CALL_BRD_df_bs[ is.na(모_고객수) , "모_고객수"] <- 0

# CALL_BRD_df_bs <- CALL_BRD_df_bs[BROAD_HH %in% c(2,3,4,5)]

### 추가변수
# 아이템명 우측 생성 ITEM_NM_SEP, 좌측 BRAND 
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , ITEM_NM_SEP := unlist(strsplit(ITEM_NM, "[:]"))[2], by = "no" ]

# 순서 
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]

# 아이템, 브랜드, 상품분류
CALL_BRD_df_bs <- CALL_BRD_df_bs[, BR_SF := seq(.N), by = c("BRAND")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[, IT2_SF := seq(.N), by = c("ITEM_NM_SEP")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[, PG_SF := seq(.N), by = c("PRD_GRP_NM")]


# PUMP_CMT_df <- CMT_df[PMO_STR_DTM >= ymd("2017-01-01") & SRCH == "CA" ,]
# PUMP_CMT_df <- unique(PUMP_CMT_df[, .(PMO_NO, PMO_STR_DTM, PMO_END_DTM, ITEM_CD , ITEM_NM )])
# PUMP_CMT_df <- PUMP_CMT_df[order(PMO_STR_DTM)]
# PUMP_CMT_df <- PUMP_CMT_df[, SEQ := 1:nrow(PUMP_CMT_df)]
# 
# # 프로모션 상품평 테이블 
# pump_cmt_all_df <- do.call(rbind, lapply(1:nrow(PUMP_CMT_df) , function(i) {
#   # i <- 1
#   print(i)
#   TMP <- PUMP_CMT_df[i,]
#   
#   st <- ymd(substr(TMP$PMO_STR_DTM,1,10))
#   lt <- ymd(substr(TMP$PMO_END_DTM,1,10))
#   
#   day_df <- data.frame(DAY = seq.Date(st, lt, by = "days"  ))
#   result_df <- data.frame(SEQ = TMP$SEQ, day_df,
#                           SEQ_EVT = 1:nrow(day_df),
#                           # PRD_CD = TMP$PRD_CD,    PRD_NM = TMP$PRD_NM,
#                           ITEM_CD = TMP$ITEM_CD,  ITEM_NM = TMP$ITEM_NM,
#                           PMO_STR_DTM = TMP$PMO_STR_DTM,
#                           PMO_END_DTM = TMP$PMO_END_DTM)
#   result_df
#   
# }))
# 
# # 
# pump_cmt_all_df <- data.table(pump_cmt_all_df)
# 
# #    
# names(pump_cmt_all_df)[2] <- "BROAD_DT"
# pump_cmt_all_df$BROAD_DT <- ymd(pump_cmt_all_df$BROAD_DT)
# pump_df <- pump_cmt_all_df[ , .(
#   PROMO_CNT = .N
# ),  by = c("BROAD_DT", "ITEM_CD", "ITEM_NM")] 
# 
# 
# # 일별 아이템별 상품평 카운트 결합
# CALL_BRD_df_bs <- merge(CALL_BRD_df_bs, pump_df, by = c("BROAD_DT", "ITEM_CD", "ITEM_NM"), all.x = T)
# CALL_BRD_df_bs[is.na(CALL_BRD_df_bs$PROMO_CNT), "PROMO_CNT"] <- 0
# CALL_BRD_df_bs$PROMO_CNT_G <- ifelse(CALL_BRD_df_bs$PROMO_CNT > 0 , 1, 0 )


# 모델 생성 데이터 
MDL_DT <- "2017-01-01"
FIND_PAST_df <- CALL_BRD_df_bs[BROAD_DT >= ymd(MDL_DT), ]

# 일자 갭 계산 
GRP <- data.table(data.frame(DG = 1:(360*10), DGG = rep(seq(30, (360*10), by = 30), each = 30)))
# 
DT_df <- data.frame(DT = seq(ymd("2016-01-01"), ymd(substr(SYSDATE,1,10)) + 365, by =  "1 month") )
DT_df$DT_VF <- c(ymd("2015-12-01"), lag(DT_df$DT)[-1])


# 직전, 이전 데이터 결합  nrow(FIND_PAST_df)
# i<- 355

# 28338 28372 28378 28447 28456 28517 28529 28556
# 49003
# which(FIND_PAST_df$no=="49003")
system.time(
  result_df <- do.call(rbind.fill, mclapply(1 : nrow(FIND_PAST_df)  , function(i) { 
    # i <- 29317
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
    
    # 편성표
    if(tmp1$no %in% CALL_SCH_df$no){
      (day_chk <- Sys.Date()-1)
    } else {
      day_seq <- data.frame(day = tmp1$BROAD_DT - seq(1,30, by = 1))
      day_seq$WEKPT <- wday(day_seq$day)
      (day_chk <- day_seq$day[1]) 
    }
    
    
    if(PGM == "브랜드PGM") {
      # item 
      sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM,]
      if (nrow(sub_df) == 0) {
        sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == "일반PGM", ]
      } 
      sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
      sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
      # 과거말일 이전 데이터만 추출 
      sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    } else {
      # item 
      sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM,]
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
    # 이렇게 해도 못찾았다면! 아이템명(브랜드명제외) 시간대 매칭!
    # if (nrow(sub_df2) == 0) {
    # 
    #   if(PGM == "브랜드PGM") {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
    #     if (nrow(sub_df) == 0) {
    #       sub_df <- CALL_BRD_df_bs[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == "일반PGM", ]
    #     }
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   } else {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   }
    # 
    # 
    #   # 상품군 & 주중/주말 및 시간  일치
    #   sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #   sub_df2$HHG <- 200
    #   if (nrow(sub_df2) == 0) {
    #     # +- 1시간
    #     sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #     sub_df2$HHG <- 210
    #     if (nrow(sub_df2) == 0) {
    #       # +- 2시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #       sub_df2$HHG <- 220
    #       if (nrow(sub_df2) == 0) {
    #         # +- 4시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #         sub_df2$HHG <- 230
    #       }
    #     }
    #   }
    # 
    # 
    #   # 주중/주말포기! 상품군 & 시간  일치
    #   if (nrow(sub_df2) == 0) {
    # 
    #     # 시간  일치
    #     sub_df2 <- sub_df[노출HH == hh  , ]
    #     sub_df2$HHG <- 2000
    #     if (nrow(sub_df2) == 0) {
    #       # +- 1시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
    #       sub_df2$HHG <- 2100
    #       if (nrow(sub_df2) == 0) {
    #         # +- 2시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
    #         sub_df2$HHG <- 2200
    #         if (nrow(sub_df2) == 0) {
    #           # +- 4시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
    #           sub_df2$HHG <- 2300
    #         }
    #       }
    #     }
    # 
    #   }
    # 
    # }
    
    # # 이렇게 해도 못찾았다면! 브랜드명 시간대 매칭!
    # if (nrow(sub_df2) == 0) {
    # 
    #   if(PGM == "브랜드PGM") {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
    #     if (nrow(sub_df) == 0) {
    #       sub_df <- CALL_BRD_df_bs[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == "일반PGM", ]
    #     }
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   } else {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[BR_SF < tmp1$BR_SF & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   }
    # 
    # 
    #   # 상품군 & 주중/주말 및 시간  일치
    #   sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #   sub_df2$HHG <- 300
    #   if (nrow(sub_df2) == 0) {
    #     # +- 1시간
    #     sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #     sub_df2$HHG <- 310
    #     if (nrow(sub_df2) == 0) {
    #       # +- 2시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #       sub_df2$HHG <- 320
    #       if (nrow(sub_df2) == 0) {
    #         # +- 4시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #         sub_df2$HHG <- 330
    #       }
    #     }
    #   }
    # 
    # 
    #   # 주중/주말포기! 상품군 & 시간  일치
    #   if (nrow(sub_df2) == 0) {
    # 
    #     # 시간  일치
    #     sub_df2 <- sub_df[노출HH == hh  , ]
    #     sub_df2$HHG <- 3000
    #     if (nrow(sub_df2) == 0) {
    #       # +- 1시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
    #       sub_df2$HHG <- 3100
    #       if (nrow(sub_df2) == 0) {
    #         # +- 2시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
    #         sub_df2$HHG <- 3200
    #         if (nrow(sub_df2) == 0) {
    #           # +- 4시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
    #           sub_df2$HHG <- 3300
    #         }
    #       }
    #     }
    # 
    #   }
    # 
    # }
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
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        # ## 취급액
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
        
      } else {
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
      }
      
      tmp <- cbind.data.frame(tmp1,  tmp2)
      tmp$FIND_YN <- "NO"
      
    } else {  
      
      # 기간 
      subset_df <- head(sub_df2[order(IT_SF, decreasing = T)],6)
      subset_df <- subset_df[,   .(
        평_노     = round(mean(SUM_RUNTIME, na.rm = T)), 
        # 평_가     = round(mean(WEIHT_MI, na.rm = T)), 
        
        # 평_달성률   = mean(SALE_PERCENT_ONAIR, na.rm = T),
        #
        평_총수   = round(mean(T_QTY_ONAIR, na.rm = T)), 
        평_총고수   = round(mean(T_CNT_ONAIR, na.rm = T)), 
        
        
        평_분취급액   = mean(분취급액, na.rm = T),
        # 평_가중분취급액   = mean(가중분취급액, na.rm = T),
        #
        
        평_분수   = round(mean(분수량, na.rm = T)), 
        # 평_가분수   = round(mean(가중분수량, na.rm = T)), 
        
        평_총모수 = round(mean(T_QTY_ONAIR_MCPC, na.rm = T)), 
        평_총모고수 = round(mean(T_CNT_ONAIR_MCPC, na.rm = T)), 
        
        평_모비 = round(mean(모_수량, na.rm = T) * 100), 
        평_모고비 = round(mean(모_고객수, na.rm = T) * 100), 
        
        평_가격   = round(mean(AVG_PRICE, na.rm = T)), 
        # 평_상품수 = round(mean(CNT_PRDCD, na.rm = T)),
        
        
        ##
        
        직_노     = round((SUM_RUNTIME[1])), 
        직_총수   = round((T_QTY_ONAIR[1])), 
        직_분수   = round((분수량[1])), 
        직_총모수 = round((T_QTY_ONAIR_MCPC[1])), 
        직_모비 = round((모_수량[1]) * 100), 
        직_가격   = round((AVG_PRICE[1])), 
        # 직_상품수 = round((CNT_PRDCD[1])),
        
        # 직_가     = round((WEIHT_MI[1])), 
        직_총고수   = round((T_CNT_ONAIR[1])), 
        # 직_가분수   = round((가중분수량[1])), 
        직_총모고수 = round((T_CNT_ONAIR_MCPC[1])), 
        직_모고비 = round((모_고객수[1]) * 100), 
        
        #
        # 직_달성률   = (SALE_PERCENT_ONAIR[1]), 
        직_분취급액   = (분취급액[1]), 
        # 직_가취급액   = (가중분취급액[1]), 
        
        
        ##
        # 직_분상주콜   = round((분상주콜[1])),
        # 직_분상SR콜   = round((분상SR콜[1])),
        
        HHG       = unique(HHG),
        최근방송일= as.numeric(day - BROAD_DT[1]) )] 
      
      # 전달 
      
      if ( substr(mnt,1,7) != substr(SYSDATE,1,7) ) {
        mnt_bf <- DT_df[which(DT_df$DT == mnt) - 1, ]$DT_VF # 방송전월  
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        ## 취급액
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
        
      } else {
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
        
      }
      
      tmp <- cbind.data.frame(tmp1, subset_df, tmp2)
      tmp$FIND_YN <- "YES"
      
    }
    
    
    tmp
    
  }, mc.cores = 14))
)

# user   system  elapsed 
# 1011.810   43.665  145.879 
# rm(GRP, DT_df, FIND_PAST_df)
table(result_df$FIND_YN)

# 데이터 정리
CALL_VAR_df <- result_df %>% dplyr::select(no, BROAD_DT, WEKDY_NM, WEK, BRD_DAYT, MIN_Q_START_DATE, MAX_Q_END_DATE,노출HH, HOLDY_YN,
                                           #
                                           IT_SF, BR_SF, IT2_SF, PG_SF, ITEM_CD, ITEM_NM, ITEM_NM_SEP, BRAND, PRD_GRP_NM,   
                                           # 
                                           PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                           # 
                                           # CNT_PRDCD, 
                                           AVG_PRICE, SUM_RUNTIME, 
                                           # WEIHT_MI, 
                                           EXPCT_SAL_AMT,
                                           # 
                                           ADVR_SALE_AMT, CNF_FEE_AMT, ADVR_FEE_YN, CNF_FEE_YN,#TGT_SAL_AMT,
                                           # 
                                           # SALE_PERCENT_ONAIR,
                                           T_QTY_ONAIR, T_QTY_ONAIR_MCPC, T_CNT_ONAIR, T_CNT_PRE,
                                           #
                                           분취급액, 분수량, 분모수량, 
                                           # 가중분취급액, 가중분수량, 가중분모수량, 
                                           모_수량, 
                                           #
                                           평_노, 평_총수, 평_분수, 평_총모수, 평_모비, 평_가격, 
                                           # 평_상품수,
                                           #
                                           직_노, 직_총수, 직_분수, 직_총모수, 직_모비, 직_가격, 
                                           # 직_상품수,
                                           #
                                           # 평_가, 
                                           평_총고수, 
                                           # 평_가분수, 
                                           평_총모고수, 평_모고비,
                                           #
                                           # 직_가, 
                                           직_총고수, 
                                           # 직_가분수, 
                                           직_총모고수, 직_모고비,
                                           #
                                           HHG, 최근방송일, 전달취급액, 
                                           # 전달취급액, (달성률) 
                                           FIND_YN, 
                                           #상담, 
                                           신상,
                                           #
                                           # 평_달성률, 
                                           평_분취급액, 
                                           #평_가중분취급액, 직_달성률, 
                                           직_분취급액 #직_가취급액
                                           
                                           ## 추가변수 
                                           # PROMO_CNT,
                                           # PROMO_CNT_G
                                           
)

CALL_VAR_df <- data.table(CALL_VAR_df)
CALL_VAR_df <- CALL_VAR_df[,AVG_PRICE := ifelse(AVG_PRICE==0,평_가격,AVG_PRICE)]

### 파생 변수 생성 
CALL_VAR_df <- CALL_VAR_df[, 평_가격갭 := round( (AVG_PRICE - 평_가격)/AVG_PRICE , 3 ) * 100]
CALL_VAR_df <- CALL_VAR_df[, 직_가격갭 := round( (AVG_PRICE - 직_가격)/AVG_PRICE , 3 ) * 100]
CALL_VAR_df$평_가격갭 <- ifelse(CALL_VAR_df$평_가격갭 == "-Inf", 0, CALL_VAR_df$평_가격갭)
CALL_VAR_df$직_가격갭 <- ifelse(CALL_VAR_df$직_가격갭 == "-Inf", 0, CALL_VAR_df$직_가격갭)

# CALL_VAR_df <- CALL_VAR_df[, 평_상품갭 := CNT_PRDCD - 평_상품수]
# CALL_VAR_df <- CALL_VAR_df[, 직_상품갭 := CNT_PRDCD - 직_상품수]

CALL_VAR_df <- CALL_VAR_df[, 평_노출갭 := SUM_RUNTIME - 평_노]
CALL_VAR_df <- CALL_VAR_df[, 직_노출갭 := SUM_RUNTIME - 직_노]

# CALL_VAR_df <- CALL_VAR_df[, 평_가중노출갭 := WEIHT_MI - 평_가]
# CALL_VAR_df <- CALL_VAR_df[, 직_가중노출갭 := WEIHT_MI - 직_가]


CALL_VAR_df <- CALL_VAR_df[, 노_전달시취급액 := (전달취급액/60) * SUM_RUNTIME ]
# CALL_VAR_df <- CALL_VAR_df[, 노_전달가중시취급액 := (전달취급액/60) * WEIHT_MI ]


CALL_VAR_df <- CALL_VAR_df[, 노_평_분취급액 := (평_분취급액) * SUM_RUNTIME ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분취급액 := (직_분취급액) * SUM_RUNTIME ]

CALL_VAR_df <- CALL_VAR_df[, 노_평_분수량 := round((평_분수) * SUM_RUNTIME) ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분수량 := round((직_분수) * SUM_RUNTIME) ]

# CALL_VAR_df <- CALL_VAR_df[, 노_평_가중분수량 := (평_가분수) * WEIHT_MI ]
# CALL_VAR_df <- CALL_VAR_df[, 노_직_가중분수량 := (직_가분수) * WEIHT_MI ]

# CALL_VAR_df <- CALL_VAR_df[, 노_평_가중분취급액 := (평_가중분취급액) * WEIHT_MI ]
# CALL_VAR_df <- CALL_VAR_df[, 노_직_가중분취급액 := (직_가취급액) * WEIHT_MI ]


## 현재 가중분이 없는 상황이라 결측값이 나온다

# 브랜드 PGM 
CALL_VAR_df$BRAND_PGM_GRP <- ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("B special", "The Collection" ,"The Collection 2호"), "1.B스페샬_더컬", 
                                    ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("최은경의 W", "왕영은의 톡톡톡", "SHOW me the Trend"), 
                                           "2.왕톡_쇼미_최W",
                                           "3.똑소리_리뷰_일반"))

# 
CALL_VAR_df <- CALL_VAR_df[ , 신상_IT := ifelse(신상 == "YES" & IT_SF == min(IT_SF), "YES", "NO") , by = "ITEM_NM"]

#  
# CALL_VAR_df$상담 <- as.factor(CALL_VAR_df$상담)
CALL_VAR_df$신상 <- as.factor(CALL_VAR_df$신상)
CALL_VAR_df$신상_IT <- as.factor(CALL_VAR_df$신상_IT)
CALL_VAR_df$PGM_GBN <- as.factor(CALL_VAR_df$PGM_GBN)
CALL_VAR_df$BRAND_PGM_GRP <- as.factor(CALL_VAR_df$BRAND_PGM_GRP)

#
# CALL_VAR_df$PROMO_CNT_G <- as.factor(CALL_VAR_df$PROMO_CNT_G)
CALL_VAR_df$BRAND_PGM_NM <- as.factor(CALL_VAR_df$BRAND_PGM_NM)
# 
CALL_VAR_df <- CALL_VAR_df[order(no)]
# CALL_VAR_df[is.na(CALL_VAR_df)] <- 0

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
# wek <- ymd(substr(SYSDATE, 1, 10))
# wek2 <- wek + 11

# wek <- ymd(substr(SYSDATE, 1, 10)) -10# - 29
# wek2 <- wek + 7


# if (TEST_YN == "YES") {
#   wek <- seq.Date(ymd( "2018-05-03"), ymd( "2018-08-15") , by = "1 days")
#   wek2 <- wek + 11
# }

#----------------------------------------------------------------
# 모델 공식 
#----------------------------------------------------------------

# 2.
# var_nm <- c(
#   "CNT_PRDCD", "AVG_PRICE", "SUM_RUNTIME", "WEIHT_MI" ,
#   #"ADVR_SALE_AMT" , "CNF_FEE_AMT" , "TGT_SAL_AMT",
#   "평_노", "평_총수", "평_분수", "평_총모수", "평_모비", "평_가격", "평_상품수",
#   #
#   "평_가", "평_총고수", "평_가분수", "평_총모고수", "평_모고비",
#   #
#   "직_노", "직_총수", "직_분수", "직_총모수", "직_모비", "직_가격", "직_상품수",
#   #
#   "직_가", "직_총고수", "직_가분수", "직_총모고수", "직_모고비",
#   #
#   "최근방송일", "전달취급액", #"전달취급액" ,
#   #
#   "평_가격갭", "직_가격갭", "평_상품갭", "직_상품갭", "평_노출갭", "직_노출갭", "평_가중노출갭", "직_가중노출갭",
#   #
#   "노_전달시취급액" , "노_전달가중시취급액" , "노_평_분수량" , "노_직_분수량" ,        
#   "노_평_가중분수량" , "노_직_가중분수량" , "노_평_가중분취급액" ,"노_직_가중분취급액" , "노_평_분취급액","노_직_분취급액",
#   
#   #
#   "FIND_YN" , "신상", "WEK",  "BRAND_PGM_NM", "신상_IT", "노출HH",  
#   # 
#   "PRD_GRP_NM", "HOLDY_YN2", "HOLDY_SEQ",
#   "직템__평_총수"
#   ) # , "PROMO_CNT" "FESTA_YN", , "날짜순서"
# # 


# 
# ###########################################################################
# 가중분을 제거한담면???
var_nm <- c(
  # "CNT_PRDCD", 
  "AVG_PRICE", "SUM_RUNTIME",
  #"ADVR_SALE_AMT" , "CNF_FEE_AMT" , "TGT_SAL_AMT",
  "평_노", "평_총수", "평_분수", "평_총모수", "평_모비", "평_가격", 
  # "평_상품수",
  #
  "평_총고수", "평_총모고수", "평_모고비",
  #
  "직_노", "직_총수", "직_분수", "직_총모수", "직_모비", "직_가격", 
  # "직_상품수",
  #
  "직_총고수", "직_총모고수", "직_모고비",
  #
  "최근방송일", "전달취급액", #"전달취급액" ,
  #
  "평_가격갭", "직_가격갭", 
  # "평_상품갭", "직_상품갭", 
  "평_노출갭", "직_노출갭",
  #
  "노_전달시취급액"  , "노_평_분수량" , "노_직_분수량" ,
  "노_평_분취급액","노_직_분취급액",
  
  #
  "FIND_YN" , "신상", "WEK",  "BRAND_PGM_NM", "신상_IT", "노출HH",
  #
  "PRD_GRP_NM", "HOLDY_YN2", "HOLDY_SEQ",
  "직템__평_총수"
) # , "PROMO_CNT" "FESTA_YN", , "날짜순서"
#

###########################################################################


if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {
  var_nm <- c(var_nm, "FESTA_YN", "날짜순서")
}

# formula1 <- formula(paste("총인콜수_M ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)" ), collapse = "+")))
# formula2 <- formula(paste("log10(총인콜수_M) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)"), collapse = "+")))

# formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)", "I(노_평_가중분취급액^3)","I(SUM_RUNTIME^2)", "I(전달취급액^2)",
#                                                        "I(노_직_가중분취급액^2)","I(노_평_가중분취급액^2)"), collapse = "+")))

# formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)", "I(SUM_RUNTIME^2)", "I(전달취급액^2)", 
#                                                        "I(노_평_분취급액^2)", "I(노_직_분취급액^2)"),
#                                                       collapse = "+")))

formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[]),
                                                     collapse = "+")))


# formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(var_nm[],collapse = "+")))

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
# CALL_VAR_df$CNF_FEE_YN <- as.factor(CALL_VAR_df$CNF_FEE_YN)
# CALL_VAR_df$ADVR_FEE_YN <- as.factor(CALL_VAR_df$ADVR_FEE_YN)
# CALL_VAR_df$CNF_FEE_YN <- NULL

# CALL_VAR_df$PROMO_CNT_G <- as.factor(CALL_VAR_df$PROMO_CNT_G)
# 
mdl_df <- data.frame(CALL_VAR_df)
mdl_df[is.na(mdl_df)] <- 0
# 

# write.csv(mdl_df,file="mdl_df_0125.csv",row.names = F,fileEncoding = "CP949")
# rm(HOLDY_DT, i)

#----------------------------------------------------------------
print("모델링 시작")

# names(mdl_df)[which(names(mdl_df) == "T_QTY_ONAIR")] <- "Y_REAL"
names(mdl_df)[which(names(mdl_df) == "EXPCT_SAL_AMT")] <- "Y_REAL"



# # # eda
# # train_df %>%
# #   group_by(노출HH,PRD_GRP_NM) %>% dplyr::summarise(sales = mean(Y_REAL)) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=factor(노출HH),y=sales,group = PRD_GRP_NM, fill = PRD_GRP_NM)) + geom_line(aes(color = PRD_GRP_NM)) +
# #   facet_wrap(~PRD_GRP_NM)
# # 
# # train_df %>%
# #   group_by(노출HH,PRD_GRP_NM) %>% dplyr::summarise(count = n()) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=factor(노출HH),y=count , fill = PRD_GRP_NM)) + geom_bar(stat="identity") +
# #   facet_wrap(~PRD_GRP_NM)
# # 
# # train_df %>%
# #   mutate(month = factor(substr(BROAD_DT,1,7))) %>%
# #   group_by(month,PRD_GRP_NM) %>% dplyr::summarise(sales = mean(Y_REAL)) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=month,y=sales,group = PRD_GRP_NM, fill = PRD_GRP_NM)) + geom_line(aes(color = PRD_GRP_NM)) +
# #   facet_wrap(~PRD_GRP_NM) +
# #   ggplot2::ggtitle("시간별 취급액 현황") +
# #   theme(axis.title.x=element_blank(),
# #         axis.text.x=element_blank(),
# #         axis.ticks.x=element_blank())
# # 
# # train_df %>%
# #   group_by(노출HH,PRD_GRP_NM) %>% dplyr::summarise(sales = mean(Y_REAL),
# #                                                  sales2 = mean(노_평_가중분취급액)) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=factor(노출HH),y=sales,group = PRD_GRP_NM, fill = PRD_GRP_NM)) + geom_line(aes(color = PRD_GRP_NM)) +
# #   geom_line(aes(x=factor(노출HH),y=sales2),color = "red") +
# #   facet_wrap(~PRD_GRP_NM)
# 
#   
#   
# #### test 하기
# # wek <- ymd("20200423")
# # wek2 <- wek+2
# ##############
# 
# # mdl_df$상담 <- NULL
# train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < wek,] %>% filter(Y_REAL > 0))
# train_df <- train_df %>% filter(직_가격갭 > -200 & 직_가격갭 < 200 & 직_분수 < 1000 )
# 
# test_df <- mdl_df[mdl_df$BROAD_DT >= wek & mdl_df$BROAD_DT < wek2,]
# 
# Y_tr <- train_df$Y_REAL
# Y_tt <- test_df$Y_REAL
# Y_trl <- log10(train_df$Y_REAL )
# 
# set.seed(123)
# mdl_mt   <- model.matrix(formula2, data= rbind.data.frame(train_df, test_df))[,-1]
# 
# train_mt <- mdl_mt[1:nrow(train_df) , ]
# test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]
# 
# set.seed(123)
# smp <- sample(dim(train_mt)[1], round(dim(train_mt)[1]*0.25))
# 
# xgTrain <- xgb.DMatrix(data=train_mt[-smp,], label=Y_trl[-smp])
# xgVal <- xgb.DMatrix(data=train_mt[smp,], label=Y_trl[smp])
# xgTest <- xgb.DMatrix(data=test_mt)
# 
# remodel_yn <- "Q"
# 
# # 목요일이면 모델생성   
# # if ( wday(wek) != make_mdl ) {
# #   
# #   print(paste( wek , wek2, " 목요일X 예측테스트 " ))
# #   
# #   # 목요일이 아니면 
# #   load(file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu] ] ) ,"__", GBN_NM, ".RData") )
# #   
# #   # 예측 테스트 실패시 NOT출현 // 재모델링 
# #   remodel_yn   <- tryCatch(predict(LS_MDL, newx = test_mt),
# #                            error = function(e) print("NOT"),
# #                            warning = function(w) print("NOT"))
# # }
# # 
# # # 목요일이거나 예측실패한경우 모델 재생성 
# # if ( wday(wek[clu]) == make_mdl | remodel_yn == "NOT" ) {
# #   print(paste( wek , wek2, " 목요일O 모델링 및 예측수행" ))
# #   
# #   print(paste(" XGB ", as.character(Sys.time())))
# #   
# #   # ###### 5.XGB 시작 ######
# #   
# #   if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {
# #     
# #     
# 
#     set.seed(123)
#     xgbFitl <- xgb.train(
#       data = xgTrain, nfold = 5, label = as.matrix(Y_trl),
#       objective='reg:linear',
#       nrounds=1200,
#       eval_metric='rmse',
#       watchlist=list(train=xgTrain, validate=xgVal),
#       print_every_n=10,
#       nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
#       early_stopping_rounds=100
#     )
# 
#     preds2l <- predict(xgbFitl, newdata = test_mt)
#     RMSE(Y_tt,10^preds2l)
#     
#     data.frame(Y_tt,10^preds2l) %>%
#       ggplot(aes(x=1:length(Y_tt),y=Y_tt)) +
#       geom_line(color="blue") +
#       geom_line(aes(x=1:length(Y_tt),y=10^preds2l))
#       # geom_ribbon(aes(ymax=10^preds3l_up, ymin=10^preds3l_down),alpha=0.4,fill = "slategray4")
#   
#     # png(file = paste0( "/home/DTA_CALL/VARPLOT/", fld_nm, "/",STRT_DT, GBN_NM,".png"), height = 1100, width = 700)
#     
#     # xgbFitl %>% 
#     #   xgb.importance(feature_names=colnames(xgTrain)) %>%  
#     #   dplyr::slice(1:30) %>%
#     #   data.table() %>%
#     #   xgb.plot.importance()
#     # dev.off()
#     
#     dim(train_mt)
#     # } else {
#     
#     # print(xgbFit)
#     set.seed(123)
#     xgbFitl <- xgboost(data = train_mt, nfold = 5, label = as.matrix(Y_trl),
#                        nrounds = 2000, verbose = FALSE, eval_metric = "rmse",
#                        nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
#                        subsample = 0.5213, colsample_bytree = 0.4603)
#     
#     
#     preds2l <- predict(xgbFitl, newdata = test_mt)
#     RMSE(Y_tt,10^preds2l)
#     
#     data.frame(Y_tt,10^preds2l) %>%
#       ggplot(aes(x=1:length(Y_tt),y=Y_tt)) +
#       geom_line(color="blue") +
#       geom_line(aes(x=1:length(Y_tt),y=10^preds3l))+
#       geom_ribbon(aes(ymax=10^preds3l_up, ymin=10^preds3l_down),alpha=0.4,fill = "slategray4")
#     
#     # booster = "dart")
#     # }
#     
#     
#     
#     ## print(xgbFitl)
#     # ###### 5.XGB 시작 ######
#     
#     save(xgbFitl, 
#          file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu]] ), "__", GBN_NM, ".RData") )
#     
#   } 
#   
#   #####---------------------------------- 예측 수행 ---------------------------------#####
#   
#   # clu <- 109
#   if ( wday(wek[clu]) != make_mdl ) {
#     print(paste( clu, wek[clu] , wek2[clu ], " 목요일X 예측수행" ))
#   } else {
#     print(paste( clu, wek[clu] , wek2[clu ], " 목요일O 예측수행" ))
#   }
#   
#   
#   # set.seed(123)
#   # mdl_mt   <- model.matrix(formula1, data=rbind.data.frame(train_df, test_df))[,-1]
#   # 
#   # train_mt <- mdl_mt[1:nrow(train_df) , ]
#   # test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]
#   
#   
#   # ###### 5.XGB  ######
#   
#   preds2l <- predict(xgbFitl, newdata = test_mt)
#   
#   er_df <- data.frame(no_m = 1:nrow(test_df),
#                       PGM_ID = test_df$PGM_ID,
#                       DT = test_df$BROAD_DT, 
#                       MIN_Q_START_DATE = test_df$MIN_Q_START_DATE, 
#                       MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
#                       ITEM_CD = test_df$ITEM_CD,
#                       ITEM_NM = test_df$ITEM_NM,
#                       PRD_GRP_NM = test_df$PRD_GRP_NM, 
#                       SUM_RUNTIME = test_df$SUM_RUNTIME,
#                       Y = Y_tt, XGBL = round(10^(as.numeric(preds2l))))

# 
# 
# ###### XGB 끝 ######


# ###### lightgbm 시작 ######

wek <- ymd(substr(SYSDATE, 1, 10))
wek2 <- max(mdl_df$BROAD_DT) 
# wek + 7
# wek2 <- wek + 2


train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < wek,] %>% filter(Y_REAL > 0))
train_df <- train_df %>% filter(직_가격갭 > -200 & 직_가격갭 < 200 & 직_분수 < 1000 )
train_df <- train_df %>% filter(!TITLE_NM %in% c("오늘의 선택","타임특가"))
# train_df <- train_df %>% filter(WEIHT_MI > 10)  


test_df <- mdl_df[mdl_df$BROAD_DT >= wek & mdl_df$BROAD_DT < wek2,]
test_df <- test_df %>% filter(!TITLE_NM %in% c("오늘의 선택","타임특가"))
# test_df <- test_df %>% filter(WEIHT_MI > 10)  

Y_tr <- train_df$Y_REAL
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
smp <- sample(dim(train_mt)[1], round(dim(train_mt)[1]*0.25))

lgTrain <- lgb.Dataset(data=train_mt[-smp,], label=Y_trl[-smp])
lgVal <- lgb.Dataset(data=train_mt[smp,], label=Y_trl[smp])
lgTest <- lgb.Dataset(data=test_mt)


## 신뢰구간 확보

# lgb.model.cv = lgb.cv(data = lgb.train,
#                       objective = "regression",
#                       nrounds = 1000, 
#                       metric = "rmse",
#                       watchlist=list(train=lgTrain, validate=lgVal),
#                       feature_fraction = 0.8,
#                       # booster = "dart",
#                       nfold = 5)
# 
# best.iter = lgb.model.cv$best_iter
# best.iter = 525

set.seed(1234)
lightFitl <- lightgbm(data = lgTrain,
                      objective = "regression",
                      nrounds = 3000, 
                      metric = "rmse",
                      watchlist=list(train=lgTrain, validate=lgVal),
                      feature_fraction = 0.8,
                      booster = "dart")

# learning_rate = 0.01,
# num_iterations = ,
# num_leaves = 30)

set.seed(1234)
lightFitl_up <- lightgbm(data = lgTrain,
                         # nfold = 5, 
                         nrounds = 3000, 
                         eval_metric = "rmse",
                         watchlist=list(train=lgTrain, validate=lgVal),
                         feature_fraction = 0.8,
                         objective = 'quantile',
                         booster = "dart",
                         alpha = 0.8)


set.seed(1234)
lightFitl_down <- lightgbm(data = lgTrain,
                           nrounds = 3000, 
                           eval_metric = "rmse",
                           watchlist=list(train=lgTrain, validate=lgVal),
                           objective = 'quantile',
                           feature_fraction = 0.8,
                           booster = "dart",
                           alpha = 0.2)


# save(lightFitl, file = paste0("/home/dio/편성예측/lightFitl.RData"))
# save(lightFitl_up, file = paste0("/home/dio/편성예측/lightFitl_up.RData"))
# save(lightFitl_down, file = paste0("/home/dio/편성예측/lightFitl_down.RData"))


preds3l <- predict(lightFitl,test_mt)
## 신뢰구간
preds3l_up <- predict(lightFitl_up,test_mt)
preds3l_down <- predict(lightFitl_down,test_mt)
# head(train_df)
# RMSE(Y_tt,10^preds3l)
# MAPE(Y_tt,10^preds3l)


# mean(ifelse(10^preds3l-Y_tt>=0,Y_tt/10^preds3l,10^preds3l/Y_tt))

# data.frame(Y_tt,y_pred = 10^preds3l,y_up = 10^preds3l_up, y_down = 10^preds3l_down) %>%
#   mutate(rmse1 = ifelse(Y_tt <= y_up & Y_tt >= y_down,0,
#                         ifelse(Y_tt > y_up, Y_tt - y_up , y_down - Y_tt))) %>%
#   dplyr::summarise(rmse2 = mean(rmse1),
#                    mape = round(sum(rmse1/Y_tt),3),
#                    mape2 = mean(rmse1/Y_tt))

#10 5 ~ 12
# ac = 0.73
# range = 0.187

#10 6 ~ 13
# ac = 0.73
# range = 0.178

# RMSE(Y_tt,10^preds3l_up)
# RMSE(Y_tt,10^preds3l_down)
# 
# 1000, dart, fold5, log 531
# 4000, dart, fole5, log 581
# 4000, dart, fole5, log 550

# library(ggplot2)
# data.frame(Y_tt,10^preds3l) %>%
#   ggplot(aes(x=1:length(Y_tt),y=Y_tt)) +
#   geom_line(color="blue") +
#   geom_line(aes(x=1:length(Y_tt),y=10^preds3l,color="red"))
# 

# data.frame(Y_tt,10^preds3l) %>%
#   ggplot(aes(x=1:length(Y_tt),y=Y_tt)) +
#   geom_line(color="blue") +
#   geom_line(aes(x=1:length(Y_tt),y=10^preds3l))+
#   geom_ribbon(aes(ymax=10^preds3l_up, ymin=10^preds3l_down),alpha=0.4,fill = "slategray4")
# 


# 중요도
# tree_imp <- lgb.importance(lightFitl, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 30, measure = "Gain")


er_df <- data.frame(no = test_df$no,
                    PGM_ID = test_df$PGM_ID,
                    DT = test_df$BROAD_DT, 
                    MIN_Q_START_DATE = test_df$MIN_Q_START_DATE, 
                    MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
                    ITEM_CD = test_df$ITEM_CD,
                    ITEM_NM = test_df$ITEM_NM,
                    PRD_GRP_NM = test_df$PRD_GRP_NM, 
                    # WE_TIME = test_df$WEIHT_MI,
                    RUNTIME = test_df$SUM_RUNTIME,
                    # SUM_RUNTIME = test_df$SUM_RUNTIME,
                    # 신상 = test_df$신상,
                    Y = Y_tt, 
                    pred = round(10^(as.numeric(preds3l))),
                    preds3l_up = round(10^(as.numeric(preds3l_up))),
                    preds3l_down = round(10^(as.numeric(preds3l_down))),
                    BRAND_PGM_NM = test_df$BRAND_PGM_NM,
                    # Y_M = Y_tt/test_df$WEIHT_MI,
                    # pred_M = round(10^(as.numeric(preds3l)))/test_df$WEIHT_MI,
                    INSERT_DT = ymd(substr(SYSDATE, 1, 10)))

er_df2 <- er_df %>% 
  dplyr::group_by(PGM_ID,MIN_Q_START_DATE) %>%
  dplyr::mutate(row_n = dplyr::row_number()) %>%
  dplyr::mutate(row_n = max(row_n)) %>%
  dplyr::filter(row_n >1) %>% data.frame()


er_df2 <- merge(er_df2,CALL_SCH_df[,c("PGM_ID","ITEM_CD","ADVR_FEE_YN","CNF_FEE_YN","ADVR_SALE_AMT_mi","CNF_FEE_YN_mi")],by=c("PGM_ID","ITEM_CD"),all.x=T)
er_df2$preds3l_up <- ifelse(er_df2$preds3l_up>er_df2$pred,er_df2$preds3l_up,er_df2$pred)
er_df2$preds3l_down <- ifelse(er_df2$preds3l_down<er_df2$pred,er_df2$preds3l_down,er_df2$pred)

er_df2 <- merge(er_df2,CALL_SCH_df[,c("PGM_ID","ITEM_CD","WEIHT_TIME")],by=c("PGM_ID","ITEM_CD"),all.x=T)

# er_df2 <- er_df %>% mutate(gap = abs(Y-pred)) %>% arrange(desc(gap)) %>% dplyr::slice(round((nrow(er_df)*0.1)):nrow(er_df))
# MAPE(er_df2$Y,er_df2$pred)

# source(file = "/home/dio/DB_con/DB_conn.R")

# dbWriteTable(conn, "broad_test_21_2", er_df2 , row.names = F)
# dbSendUpdate(conn, paste0("DROP TABLE I_KIMDY9.broad_test_21_2"))

rm(list = ls()[-which(ls() %in% c("er_df2") )])
er_df_final <- er_df2



########### 온타임 ################


source(file = "/home/dio/DB_con/DB_conn.R")

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
# (SYSDATE <- as.character(Sys.time()) )
(SYSDATE <- as.character(paste0(ymd(STRT_DT), " 03:00:00") ))

print(STRT_DT); print(LAST_DT); print(SYSDATE)
# 


# SELECT
# PA1.PGM_ID
# ,PA1.ITEM_CD
# ,PA1.BROAD_DT
# ,SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.EXPCT_SAL_AMT else 0 end) AS EXPCT_SAL_AMT
# ,SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.CNF_FEE_AMT else 0 end) AS CNF_FEE_AMT
# ,sum(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.ADVR_SALE_AMT else 0 end) AS ADVR_SALE_AMT
# ,SUM(PA2.TGT_SAL_AMT) AS TGT_SAL_AMT
# , (CASE WHEN SUM(PA2.TGT_SAL_AMT) > 0
#    THEN ROUND((SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.EXPCT_SAL_AMT else 0 end) +
#                  SUM(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.CNF_FEE_AMT else 0 end ) +
#                  sum(case when PA1.BROAD_ORD_TIME_CD = 'O'  then PA1.ADVR_SALE_AMT else 0 end))/SUM(PA2.TGT_SAL_AMT),3)*100
#    ELSE 0 END) AS SALE_PERCENT_ONAIR

BRD_df_bs_ontime <- dbGetQuery(conn, paste0("
                                            SELECT /*+ FULL(A1) FULL(A2) PARALLEL (A1 4) */
                                            A1.PGM_ID
                                            ,A2.ITEM_CD
                                            ,A1.BROAD_DT
                                            --,A1.BROAD_ORD_TIME_CD
                                            ,SUM(EXPCT_SAL_AMT) AS EXPCT_SAL_AMT
                                            ,SUM(CNF_FEE_AMT) AS CNF_FEE_AMT
                                            ,SUM(ADVR_SALE_AMT) AS ADVR_SALE_AMT
                                            FROM F_ORD_ORD_RLRSLT_S A1
                                            LEFT JOIN d_brd_broad_form_prd_m A2  ON A1.PGM_ID = A2.PGM_ID AND A1.PRD_CD = A2.PRD_CD
                                            WHERE A1.BROAD_DT BETWEEN '20160101' AND '" , STRT_DT, "'
                                            
                                            AND BROAD_ORD_TIME_DTL_CD = 'O1' 
                                            GROUP BY
                                            A1.PGM_ID
                                            ,A2.ITEM_CD
                                            ,A1.BROAD_DT
                                            ,A1.BROAD_ORD_TIME_CD"))



###### 방송, 콜 데이터 실적 ###### 49483 - 157
# CALL_BRD_df_bs <- dbGetQuery(conn, paste0(" SELECT ITEM.ITEM_NM
#                                        , SUBSTR(REGEXP_SUBSTR(ITEM.ITEM_NM,'[^:]+',1,1),1) AS BRAND
#                                        , PRD_GRP.PRD_GRP_NM
#                                        , A.*
#                                        , DT.WEKDY_NM
#                                        , (CASE WHEN DT.WEKDY_NM IN ('Sat','Sun') THEN 1 ELSE 0 END) AS WEKDY_YN --1:주말 /  0:평일
#                                        , DT.HOLDY_YN --공휴일여부 (Y:공휴일&휴일 / N:기타)  
#                                        FROM DTA_OWN.GSTS_PGM_ITEM_ORD A   /* PGM,ITEM별 주문*/
#                                        ,GSBI_OWN.D_PRD_ITEM_M ITEM
#                                        ,DHUB_OWN.DH_DT DT
#                                        ,GSBI_OWN.V_CMM_PRD_GRP_C PRD_GRP
#                                        WHERE -- A.PGM_ID = B.PGM_ID
#                                        A.ITEM_CD = ITEM.ITEM_CD
#                                        AND A.BROAD_DT = DT.DT_CD  
#                                        AND A.PRD_GRP_CD = PRD_GRP.PRD_GRP_CD
#                                        AND A.BROAD_DT < '" , STRT_DT,"' "))


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


BRD_df_bs_ontime <- BRD_df_bs_ontime %>% filter(EXPCT_SAL_AMT >0 ) 
CALL_BRD_df_bs <- merge(CALL_BRD_df_bs,BRD_df_bs_ontime, by=c("PGM_ID","ITEM_CD","BROAD_DT"))

# ###### 편성 데이터 예측에 활용 #####
# CALL_SCH_df <- dbGetQuery(conn, paste0("SELECT * FROM DTA_OWN.PRE_SCHDL_TB WHERE ETL_DATE = '",STRT_DT,"'  "  ))

# 가중분
# CALL_SCH_df_w <- dbGetQuery(conn, paste0("
#                                          SELECT
#                                          BB.PGM_ID
#                                          ,ITEM_CD
#                                          ,ADVR_FEE_YN
#                                          ,CNF_FEE_YN
#                                          ,ROUND(SUM(TGT_EXPCT_SAL_AMT),0) AS TGT_EXPCT_SAL_AMT
#                                          ,ROUND(SUM(PLAN_WEIHT_SE/60),0) AS WEIHT_MI
#                                          
#                                          FROM d_brd_broad_form_prd_m AA
#                                          INNER JOIN d_brd_broad_form_M BB ON AA.PGM_ID = BB.PGM_ID 
#                                          WHERE ITEM_CD >0 AND BB.BROAD_DT BETWEEN '",STRT_DT,"' AND '",LAST_DT,"'
#                                          GROUP BY BB.PGM_ID
#                                          ,ITEM_CD
#                                          ,ADVR_FEE_YN
#                                          ,CNF_FEE_YN"))


#방송 편성
CALL_SCH_df <- dbGetQuery(conn, paste0("
                                       SELECT 
                                       A.PULIN_GRP_NM,A.PULIN_GRP_SEQ,A.PGM_GBN_CD,A.PGM_ID,A.PGM_NM,A.TITLE_ID,A.TITLE_NAME,A.HH,A.HOPE_BROAD_STR_DTM,A.HOPE_BROAD_END_DTM,
                                       A.HOPE_BROAD_DY,A.HOLDY_YN,A.PRD_GRP_CD,A.PRD_GRP_NM,A.ITEM_CD,A.ITEM_NM,A.BRAND,
                                       A.PRD_CD,A.PRD_NM,A.PRD_SALE_PRC,A.NEW_PRD_YN,A.RUN_TIME,A.PROFIT_TYP_VAL,
                                       AVG(B.WEIHT_RT) AS WEIHT_RT, 
                                       ROUND(A.RUN_TIME*AVG(B.WEIHT_RT),0) AS WEIHT_TIME
                                       FROM
                                       (
                                       SELECT DISTINCT
                                       A.PULIN_GRP_NM 
                                       , A.PULIN_GRP_SEQ
                                       --, B.SEQ
                                       , (SELECT FN_GET_BI_CMM_DTL_CD('BI131', E.BROAD_TYPE) FROM DUAL) AS PGM_GBN_CD	
                                       , B.HOPE_BROAD_PGM_ID AS PGM_ID
                                       , (CASE WHEN I.TITLE_NAME IS NOT NULL THEN I.TITLE_NAME
                                       ELSE E.TITLE_NAME
                                       END) AS PGM_NM
                                       , E.TITLE_ID
                                       , E.TITLE_NAME
                                       , SUBSTR(TO_CHAR(B.HOPE_BROAD_STR_DTM, 'YYYYMMDDHH24MISS'),9,2) AS HH
                                       , TO_CHAR(B.HOPE_BROAD_STR_DTM, 'YYYYMMDDHH24MISS') AS HOPE_BROAD_STR_DTM
                                       , TO_CHAR(B.HOPE_BROAD_END_DTM, 'YYYYMMDDHH24MISS') AS HOPE_BROAD_END_DTM
                                       , TO_CHAR(B.HOPE_BROAD_STR_DTM, 'DY') AS HOPE_BROAD_DY
                                       , G.HOLDY_YN
                                       , B.PRD_GRP_CD
                                       , C.MCLASS_NAME AS PRD_GRP_NM
                                       , DECODE(B.ITEM_CD, 0, '', B.ITEM_CD) AS ITEM_CD
                                       , D.ITEM_NAME AS ITEM_NM
                                       , REGEXP_SUBSTR(D.ITEM_NAME, '[^:]+', 1, 1) AS BRAND
                                       , B.PRD_CD
                                       , H.PRD_NM
                                       , H.PRD_SALE_PRC
                                       , B.NEW_PRD_YN
                                       , round((B.HOPE_BROAD_END_DTM - B.HOPE_BROAD_STR_DTM)* 24 * 60) AS RUN_TIME 
                                       , DECODE(B.PROFIT_TYP_VAL, 'A', '광고매출', 'F', '확정수수료', 'N') AS PROFIT_TYP_VAL  --수익유형 (F : 확정수수료 / A : 광고매출)
                                       FROM   SDHUB_OWN.STG_TB_BY415 A  -- 풀인기본정보
                                       , SDHUB_OWN.STG_TB_BY416 B  -- 편성풀인아이템정보 
                                       , SDHUB_OWN.STG_TB_BY190 C  -- 상품군 조직 매핑 
                                       , SDHUB_OWN.STG_TB_BY049 D  -- 브랜드별 상품코드 매핑 
                                       , SDHUB_OWN.STG_TB_BY100 E  -- 프로그램 편성
                                       , DHUB_OWN.DH_DT         G  -- HOLDY_YN
                                       , GSBI_OWN.D_PRD_PRD_M   H
                                       , SDHUB_OWN.STG_TB_BY008 I  -- 프로그램 타이틀
                                       WHERE 1=1
                                       AND TO_CHAR(A.BROAD_STR_DTM, 'YYYYMMDD') >= '",STRT_DT,"'
                                       AND UPPER(A.PULIN_GRP_NM) LIKE '%<선편성 M-1> 3월%'
                                       
                                       AND A.PULIN_GRP_SEQ = B.PULIN_GRP_SEQ
                                       AND B.DEL_YN        = 'N'
                                       AND B.PRD_GRP_CD    = C.MCLASS_CODE(+)
                                       AND B.ITEM_CD       = D.ITEM_CODE(+)
                                       AND B.HOPE_BROAD_PGM_ID  = E.PGM_ID(+)
                                       AND TO_CHAR(B.HOPE_BROAD_STR_DTM, 'YYYYMMDD') = G.DT_CD(+)
                                       AND B.PRD_CD = H.PRD_CD(+)
                                       AND E.TITLE_ID  = I.TITLE_ID(+)
                                       
                                       AND D.ITEM_NAME NOT LIKE '%예비%'
                                       AND D.ITEM_NAME NOT LIKE '%상담%'
                                       
                                       ORDER BY 7
                                       ) A,
                                       (--가중율 추가--
                                       SELECT /*+ FULL(B)  PARALLEL(B 4) */
                                       B.STD_DT||B.STD_TM AS DT,
                                       B.WEIHT_RT  
                                       FROM GSBI_OWN.D_BRD_BROAD_WEIHT_RT_M B 
                                       WHERE B.CHANL_CD = 'C'
                                       ) B
                                       WHERE 1=1
                                       AND B.DT BETWEEN SUBSTR(A.HOPE_BROAD_STR_DTM,1,12) AND SUBSTR(A.HOPE_BROAD_END_DTM,1,12) 
                                       GROUP BY 
                                       A.PULIN_GRP_NM,A.PULIN_GRP_SEQ,A.PGM_GBN_CD,A.PGM_ID,A.PGM_NM,A.TITLE_ID,A.TITLE_NAME,A.HH,A.HOPE_BROAD_STR_DTM,A.HOPE_BROAD_END_DTM,
                                       A.HOPE_BROAD_DY,A.HOLDY_YN,PRD_GRP_CD,A.PRD_GRP_NM,A.ITEM_CD,A.ITEM_NM,A.BRAND,
                                       A.PRD_CD,A.PRD_NM,A.PRD_SALE_PRC,A.NEW_PRD_YN,A.RUN_TIME,A.PROFIT_TYP_VAL
                                       "))

CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$PRD_GRP_NM), ]
CALL_SCH_df$TITLE_ID <- ifelse(is.na(CALL_SCH_df$TITLE_ID), 0, CALL_SCH_df$TITLE_ID)
CALL_SCH_df$FST_BROAD_DTM <- ifelse(CALL_SCH_df$ITEM_CD %in% CALL_BRD_df_bs$ITEM_CD, 20200101, 99999999)
CALL_SCH_df <- CALL_SCH_df[!CALL_SCH_df$PRD_GRP_NM %in% c("렌탈","보험","여행","핸드폰"),] #특정 카테고리 제외
# CALL_SCH_df <- CALL_SCH_df[TITLE_NM == c("렌탈."),] #특정 카테고리 제외


# CALL_SCH_df$AVG_PRICE <- ifelse(is.na(CALL_SCH_df$AVG_PRICE), 0, CALL_SCH_df$AVG_PRICE)
# CALL_SCH_df$MIN_PRICE <- ifelse(is.na(CALL_SCH_df$MIN_PRICE), 0, CALL_SCH_df$MIN_PRICE)
# CALL_SCH_df$MAX_PRICE <- ifelse(is.na(CALL_SCH_df$MAX_PRICE), 0, CALL_SCH_df$MAX_PRICE)
# CALL_SCH_df <- merge(CALL_SCH_df,CALL_SCH_df_w, by=c("PGM_ID","ITEM_CD"),all.x=T)
# rm(CALL_SCH_df_w)


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


RMSE <- function(m, o){ sqrt(mean((m - o)^2))}
MAPE <- function(m, o){ n <- length(m)
res <- (100 / n) * sum(abs(m-o)/m)
res  }

dbDisconnect(conn)
rm(conn,drv)

range(CALL_BRD_df_bs$MIN_Q_START_DATE)
range(ymd_hms(CALL_SCH_df$HOPE_BROAD_STR_DTM))


### ITEM 중복제거
CALL_SCH_df <- merge(
  CALL_SCH_df %>%
    dplyr::group_by(PGM_ID,ITEM_CD) %>%
    dplyr::mutate(pgm_count = row_number()) %>%
    dplyr::filter(pgm_count == 1) %>%
    data.frame()
  ,
  CALL_SCH_df %>%
    dplyr::group_by(PGM_ID,ITEM_CD) %>%
    dplyr::summarise(AVG_BROAD_SALE_PRC = mean(PRD_SALE_PRC,na.rm=T)) %>%
    dplyr::mutate(AVG_BROAD_SALE_PRC = ifelse(is.nan(AVG_BROAD_SALE_PRC),0,AVG_BROAD_SALE_PRC)) %>%
    data.frame()
  , by = c("PGM_ID","ITEM_CD"),all.x=T) %>%
  dplyr::select(-pgm_count,-PRD_SALE_PRC,-PRD_CD,-PRD_NM)

# 
# A <- names(table(CALL_SCH_df$HOPE_BROAD_STR_DTM)[table(CALL_SCH_df$HOPE_BROAD_STR_DTM)>1])
# if(length(A) > 0) {
#   # tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
#   which_ptb <- which(CALL_SCH_df$HOPE_BROAD_STR_DTM %in% A)
#   diff_v <- diff(which_ptb)
#   check_df <- c(which(diff_v != 1) - 1, length(diff_v)) + 1
#   which_out <- which_ptb[-check_df]   
#   
#   if(length(which_out) == 0) {
#     CALL_SCH_df <- CALL_SCH_df[-which_ptb[2], ]
#   } else {
#     CALL_SCH_df <- CALL_SCH_df[-which_out, ]   
#   }
#   
# }
# 
# B <- names(table(CALL_SCH_df$MAX_Q_END_DATE)[table(CALL_SCH_df$MAX_Q_END_DATE)>1])
# A ; B
# if(length(B) > 0) {
#   # tmp$MAX_Q_END_DATE_lag <- lag(tmp$MAX_Q_END_DATE,1)
#   which_ptb <- which(CALL_SCH_df$MAX_Q_END_DATE %in% B)
#   diff_v <- diff(which_ptb)
#   check_df <- c(which(diff_v != 1) - 1, length(diff_v)) + 1
#   which_out <- which_ptb[-check_df]
#   
#   if(length(which_out) == 0) {
#     CALL_SCH_df <- CALL_SCH_df[-which_ptb[2], ]
#   } else {
#     CALL_SCH_df <- CALL_SCH_df[-which_out, ]   
#   }
# }
# 
# CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$PRD_GRP_NM), ]
# # 가중분 결측값 처리 어떻게 할까....
# CALL_SCH_df <- CALL_SCH_df[!is.na(CALL_SCH_df$WEIHT_MI), ] #40개 삭제


#----------------------------------------------------------------
# 주 편성표 결과 저장 2~5 시 돌리는 경우만 
#----------------------------------------------------------------

# end <- 1
# if (end == 1) { 


print(paste(" EDA 마트생성시작 ", as.character(Sys.time())))
#----------------------------------------------------------------
# 2. EDA 수행  
#----------------------------------------------------------------


## 필터 생성 ##

# CALL_BRD_df_bs <- CALL_BRD_df_bs %>% filter(PRD_GRP_NM != "핸드폰")
# CALL_BRD_df_bs <- CALL_BRD_df_bs %>% filter(PGM_GBN != "순환방송")
CALL_BRD_df_bs <- CALL_BRD_df_bs %>% filter(SUM_RUNTIME/60 > 10 , WEIHT_MI/60 >0)

##

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

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , ADVR_FEE_YN := ifelse(ADVR_SALE_AMT>0,"Y","N")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , CNF_FEE_YN := ifelse(CNF_FEE_AMT>0,"Y","N")]


############## 편성 분당 취급액 




#WEK = 1주중 , 2토, 3일
#WEK_G = 1~7 : 월~일


# 개월별 시간대별
CALL_df <- CALL_BRD_df_bs[ , .(
  전달취급액 = mean(EXPCT_SAL_AMT)),  #그 달에 주문 평균 수량
  by = c("mon", "WEK", "HH")]

CALL_df <- CALL_df[order(mon, WEK, HH)]

# CALL_df_FEE <- CALL_BRD_df_bs[ , .(
#   전달취급액 = mean(EXPCT_SAL_AMT)),  #그 달에 주문 평균 수량
#   by = c("mon", "WEK", "HH")]
# CALL_df_FEE <- CALL_df_FEE[order(mon, WEK, HH)]


# 개월별 시간대별 + 요일별 
CALL_df2 <- CALL_BRD_df_bs[ , .(
  전달취급액 = mean(EXPCT_SAL_AMT)),
  by = c("mon", "WEK_G", "HH")]
CALL_df2 <- CALL_df2[order(mon, WEK_G, HH)]


# 수량 인입 예측
CALL_BRD_df_bs <- data.table(CALL_BRD_df_bs)
CALL_SCH_df <- data.table(CALL_SCH_df)


####################
####################
# 시간 생성

CALL_SCH_df <- CALL_SCH_df[, BROAD_DT := substr(HOPE_BROAD_STR_DTM,1,8)]
CALL_SCH_df <- CALL_SCH_df[, BROAD_HH := substr(HOPE_BROAD_STR_DTM,9,10)]

CALL_SCH_df <- CALL_SCH_df[, HH := as.numeric(BROAD_HH) ]
CALL_SCH_df$HH <- ifelse(CALL_SCH_df$HH == 0, 24, CALL_SCH_df$HH)
# CALL_SCH_df <- na.omit(CALL_SCH_df)


######################
# CALL_SCH_df <- CALL_SCH_df[,HOPE_BROAD_STR_DTM := ymd_hms(HOPE_BROAD_STR_DTM)]
# CALL_SCH_df <- CALL_SCH_df[,HOPE_BROAD_END_DTM := ymd_hms(HOPE_BROAD_END_DTM)]


CALL_SCH_df <- CALL_SCH_df[ , WEK := wday(ymd(BROAD_DT))]
CALL_SCH_df <- CALL_SCH_df[ , WEK := WEK - 1 ]
CALL_SCH_df$WEK <- ifelse(CALL_SCH_df$WEK == 0, 7, CALL_SCH_df$WEK)
CALL_SCH_df <- CALL_SCH_df[ , WEK_G := ifelse(WEK == 1, "1.월",
                                              ifelse(WEK == 2, "2.화",
                                                     ifelse(WEK == 3, "3.수",
                                                            ifelse(WEK == 4, "4.목",
                                                                   ifelse(WEK == 5, "5.금",
                                                                          ifelse(WEK == 6, "6.토","7.일")))))) ]
CALL_SCH_df <- CALL_SCH_df[ , WEK := ifelse(WEK %in% c(1:5), "1.주중",
                                            ifelse(WEK == 6, "2.토","3.일")) ]
CALL_SCH_df <- CALL_SCH_df[ , mon := ymd(paste0(substr(BROAD_DT, 1, 6), "-01"))]


CALL_SCH_df <- CALL_SCH_df[ , ADVR_FEE_YN := ifelse(PROFIT_TYP_VAL=="광고매출","Y","N")]
CALL_SCH_df <- CALL_SCH_df[ , CNF_FEE_YN := ifelse(PROFIT_TYP_VAL=="확정수수료","Y","N")]


names(CALL_SCH_df)[which(names(CALL_SCH_df)=="HOPE_BROAD_STR_DTM")] <- "MIN_Q_START_DATE"
names(CALL_SCH_df)[which(names(CALL_SCH_df)=="HOPE_BROAD_END_DTM")] <- "MAX_Q_END_DATE"

# 순서 
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_SCH_df <- CALL_SCH_df[order(BROAD_DT, MIN_Q_START_DATE)]
CALL_BRD_df_bs$no <- 1:nrow(CALL_BRD_df_bs)
CALL_SCH_df$no <- (nrow(CALL_BRD_df_bs) + 1 ):(nrow(CALL_BRD_df_bs) + nrow(CALL_SCH_df) )


#
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("BROAD_PRD_CNT"))] <- "CNT_PRDCD"
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("AVG_BROAD_SALE_PRC"))] <- "AVG_PRICE"
names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) %in% c("FST_BROAD_DT"))] <- "FST_BROAD_DTM"
CALL_BRD_df_bs$SUM_RUNTIME <- round(CALL_BRD_df_bs$SUM_RUNTIME/60)
CALL_BRD_df_bs$WEIHT_MI <- round(CALL_BRD_df_bs$WEIHT_MI/60)

CALL_BRD_df_bs <- CALL_BRD_df_bs[CN_RS_YN == "N"]


names(CALL_SCH_df)[which(names(CALL_SCH_df)=="RUN_TIME")] <- "SUM_RUNTIME"
names(CALL_SCH_df)[which(names(CALL_SCH_df)=="TITLE_NAME")] <- "TITLE_NM"
names(CALL_SCH_df)[which(names(CALL_SCH_df)=="AVG_BROAD_SALE_PRC")] <- "AVG_PRICE"

CALL_SCH_df <- CALL_SCH_df[,WEKDY_YN:=ifelse(WEK =="1.주중",0,1)]
CALL_SCH_df <- CALL_SCH_df[,WEKDY_NM:=as.character(wday(ymd(CALL_SCH_df$BROAD_DT),label=T))]


### 사전 구축

# ITEM_ID <- unique(CALL_SCH_df$ITEM_NM)
# ITEM_ID <- data.frame(id = 1:length(ITEM_ID), name = ITEM_ID)
# # 팩터를 케릭터형으로 
# ITEM_ID <- ITEM_ID %>% mutate_if(is.factor, as.character) %>% as.data.table
# setDT(ITEM_ID)
# setkey(ITEM_ID, id)
# # 단어형태 변경 
# prep_fun = tolower
# tok_fun = word_tokenizer
# train = ITEM_ID[J(ITEM_ID)]
# it_train = itoken(train$name,
#                   preprocessor = prep_fun,
#                   tokenizer = tok_fun,
#                   ids = train$id,
#                   progressbar = FALSE)
# vocab = create_vocabulary(it_train)
# # library(glmnet)
# vectorizer = vocab_vectorizer(vocab)
# dtm_train = create_dtm(it_train, vectorizer)
# ITEM_ID <- cbind.data.frame(ITEM_ID, t(apply(dtm_train, 1, function(x) names(which(x >= 1))[1:5] )))
# rm(dtm_train, vocab, train)
# CALL_SCH_df <- merge(CALL_SCH_df,ITEM_ID ,by.x = "ITEM_NM", by.y="name",all.x=T)


#46, 127, 211
# CALL_SCH_df2 <- CALL_SCH_df
# CALL_SCH_df <- CALL_SCH_df2

# i<-71
# CALL_SCH_df[CALL_SCH_df$AVG_PRICE==0,]


# ITEM기준 최신 가격 갖고오기
# 아이템이 없으면 같은체인룰 기반 평균가격 그대로 갖고오기
CALL_SCH_df <- do.call(rbind,mclapply(1:nrow(CALL_SCH_df),function(i){
  
  tmp <- CALL_SCH_df[i,]
  
  if(tmp$AVG_PRICE==0){
    itm <- tmp$ITEM_CD
    price <-CALL_BRD_df_bs[max(which(CALL_BRD_df_bs$ITEM_CD %in% itm)),"AVG_PRICE"]
    
    if(is.na(price)){
      price <- 0
    }
    
    tmp$AVG_PRICE <- price
    tmp
  } else{tmp}
},mc.cores = 4))


# 가중분당 광고매출 갖고오기
CALL_SCH_df <- do.call(rbind,mclapply(1:nrow(CALL_SCH_df),function(i){
  # i<-1
  tmp <- CALL_SCH_df[i,]
  
  if(tmp$ADVR_FEE_YN=="Y"){
    itm <- tmp$ITEM_CD
    
    ADVR_SALE_AMT_mi <- CALL_BRD_df_bs %>% dplyr::filter(BROAD_DT > gsub("-","",Sys.Date()-90),ITEM_CD==itm) %>%
      dplyr::filter(ADVR_SALE_AMT>0) %>%
      mutate(ADVR_SALE_AMT_mi = ADVR_SALE_AMT/WEIHT_MI) %>%
      dplyr::summarise(men = round(mean(ADVR_SALE_AMT_mi)))
    
    if(is.na(ADVR_SALE_AMT_mi)){
      ADVR_SALE_AMT_mi <- 0
    }
    
    tmp$ADVR_SALE_AMT_mi <- ADVR_SALE_AMT_mi
    tmp
  } else{tmp$ADVR_SALE_AMT_mi <- 0
  tmp}
},mc.cores = 4))


# 가중분당 확정수수료 갖고오기
CALL_SCH_df <- do.call(rbind,mclapply(1:nrow(CALL_SCH_df),function(i){
  # i<-503
  tmp <- CALL_SCH_df[i,]
  
  if(tmp$CNF_FEE_YN=="Y"){
    itm <- tmp$ITEM_CD
    
    CNF_FEE_YN_mi <- CALL_BRD_df_bs %>% dplyr::filter(BROAD_DT > gsub("-","",Sys.Date()-90),ITEM_CD==itm) %>%
      dplyr::filter(CNF_FEE_YN>0) %>%
      mutate(CNF_FEE_YN_mi = CNF_FEE_AMT/WEIHT_MI) %>%
      dplyr::summarise(men = round(mean(CNF_FEE_YN_mi)))
    
    if(is.na(CNF_FEE_YN_mi)){
      CNF_FEE_YN_mi <- 0
    }
    
    tmp$CNF_FEE_YN_mi <- CNF_FEE_YN_mi
    tmp
  } else{tmp$CNF_FEE_YN_mi <- 0
  tmp}
},mc.cores = 4))


# 달성율, 가중분 추가
# BRD_INFO$PGM_ID <- as.numeric(BRD_INFO$PGM_ID)
# BRD_INFO2 <- BRD_INFO[,c("BROAD_DT","BROAD_TIMERNG_CD","PGM_ID","TITLE_NM","PRD_GRP_NM","ITEM_CD","WEIHT_MI")]
# CALL_BRD_df_bs_t <- merge(CALL_BRD_df_bs,BRD_INFO2,by.x = c("BROAD_DT","BROAD_HH","PGM_ID","TITLE_NM","PRD_GRP_NM","ITEM_CD"), 
#                       by.y=c("BROAD_DT","BROAD_TIMERNG_CD","PGM_ID","TITLE_NM","PRD_GRP_NM","ITEM_CD"),all.x=T)


## PGM추가
BRAND_PGM_NM <- names(table(CALL_BRD_df_bs$BRAND_PGM_NM))
BRAND_PGM_NM <- c(BRAND_PGM_NM,"The Collection 2호")

CALL_SCH_df$BRAND_PGM_NM <- ifelse(CALL_SCH_df$PGM_NM %in% BRAND_PGM_NM,CALL_SCH_df$PGM_NM,"일반")
CALL_SCH_df$PGM_GBN <- ifelse(as.numeric(substr(CALL_SCH_df$MIN_Q_START_DATE,9,10)) %in% c(2,3,4,5),"순환방송",
                              ifelse(CALL_SCH_df$BRAND_PGM_NM=="일반","일반PGM","브랜드PGM"))


CALL_SCH_df$MIN_Q_START_DATE <- ymd_hms(CALL_SCH_df$MIN_Q_START_DATE)
CALL_SCH_df$MAX_Q_END_DATE <- ymd_hms(CALL_SCH_df$MAX_Q_END_DATE)

# 백업용 
CALL_BRD_BACK_df <- CALL_BRD_df_bs

names(CALL_SCH_df)[which(names(CALL_SCH_df)=="TGT_EXPCT_SAL_AMT")] <- "TGT_SAL_AMT"
# 편성예측때 SALE_PERCENT_ONAIR로 활용
CALL_BRD_df_bs    <- CALL_BRD_df_bs[, .(no, BROAD_DT, BROAD_HH, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME, 
                                        PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                        ITEM_CD, ITEM_NM, BRAND, 
                                        WEKDY_NM, WEKDY_YN, HOLDY_YN, 
                                        #CN_RS_YN, CNT_PRDCD, 
                                        FST_BROAD_DTM,
                                        AVG_PRICE, 
                                        PRD_GRP_NM, 
                                        #WEIHT_MI, 
                                        ADVR_FEE_YN, CNF_FEE_YN
                                        # ,TGT_SAL_AMT ,
                                        ,ADVR_SALE_AMT ,CNF_FEE_AMT
                                        # EXPCT_SAL_AMT, SALE_PERCENT_ONAIR, 
                                        ,T_AMT_ONAIR, T_QTY_ONAIR, T_QTY_ONAIR_MCPC, T_CNT_ONAIR, T_CNT_PRE, T_CNT_ONAIR_MCPC, EXPCT_SAL_AMT)] 


CALL_BRDSCH_df <- CALL_SCH_df[, .(no, BROAD_DT, BROAD_HH, MIN_Q_START_DATE, MAX_Q_END_DATE, SUM_RUNTIME, 
                                  PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                  ITEM_CD, ITEM_NM, BRAND, 
                                  WEKDY_NM, WEKDY_YN, HOLDY_YN, 
                                  #CN_RS_YN, CNT_PRDCD, 
                                  FST_BROAD_DTM,
                                  AVG_PRICE, 
                                  PRD_GRP_NM,
                                  #WEIHT_MI,
                                  ADVR_FEE_YN, CNF_FEE_YN)] #,TGT_SAL_AMT)]


# 
CALL_BRD_df_bs <- rbind.fill(CALL_BRD_df_bs, CALL_BRDSCH_df)
CALL_BRD_df_bs <- data.table(CALL_BRD_df_bs)


###### 제외 사항
CALL_BRD_df_bs <- CALL_BRD_df_bs[!PRD_GRP_NM %in% c("렌탈","보험","여행","핸드폰")] #특정 카테고리 제외
CALL_BRD_df_bs <- CALL_BRD_df_bs[PGM_GBN != "순환방송"]
# CALL_BRD_df_bs <- CALL_BRD_df_bs[WEIHT_MI > 5]
CALL_BRD_df_bs <- CALL_BRD_df_bs[SUM_RUNTIME > 10]

CALL_BRD_df_bs <- CALL_BRD_df_bs[CALL_BRD_df_bs$TITLE_NM!="오늘의 선택",]
CALL_BRD_df_bs <- CALL_BRD_df_bs[CALL_BRD_df_bs$TITLE_NM!="타임특가",]


# CALL_BRD_df_bs %>% filter(PRD_GRP_NM=="디지털기기") %>% dplyr::count(ITEM_NM)

#상품평
# CMT_df <- data.table(CMT_df)


# 표준화 총콜
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분취급액 := EXPCT_SAL_AMT / SUM_RUNTIME ]
#
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분수량 := round(T_QTY_ONAIR / SUM_RUNTIME) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 분모수량 := round(T_QTY_ONAIR_MCPC / SUM_RUNTIME) ]

# CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분취급액 := EXPCT_SAL_AMT / WEIHT_MI ]
#
# CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분수량 := round(T_QTY_ONAIR / WEIHT_MI) ]
# CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 가중분모수량 := round(T_QTY_ONAIR_MCPC / WEIHT_MI) ]


# 상품 시작시간
#35875

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 노출HH := substr(MIN_Q_START_DATE, 12, 13) ]
CALL_BRD_df_bs$노출HH <- as.numeric(CALL_BRD_df_bs$노출HH)
CALL_BRD_df_bs$노출HH <- ifelse(CALL_BRD_df_bs$노출HH == 0, 24, CALL_BRD_df_bs$노출HH)


# 날짜 
CALL_BRD_df_bs$BROAD_DT  <- ymd(CALL_BRD_df_bs$BROAD_DT)
CALL_BRD_df_bs$BRD_DAYT  <- ymd_h(substr(paste(CALL_BRD_df_bs$BROAD_DT, CALL_BRD_df_bs$BROAD_HH) , 1, 13))

CALL_BRD_df_bs$MIN_Q_START_DATE  <- ymd_hms(CALL_BRD_df_bs$MIN_Q_START_DATE)
CALL_BRD_df_bs$MAX_Q_END_DATE  <- ymd_hms(CALL_BRD_df_bs$MAX_Q_END_DATE)

### 회차 먹이기 
CALL_BRD_df_bs <- CALL_BRD_df_bs[SUM_RUNTIME != 0 ,]

# 순서 
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]

# 아이템, 브랜드, 상품분류
CALL_BRD_df_bs <- CALL_BRD_df_bs[, IT_SF := seq(.N), by = c("ITEM_NM")]

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 모_수량 := round(T_QTY_ONAIR_MCPC/T_QTY_ONAIR, 3) ]
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 모_고객수 := round(T_CNT_ONAIR_MCPC/T_CNT_ONAIR, 3) ]

CALL_BRD_df_bs <- CALL_BRD_df_bs[ , WEK := ifelse(WEKDY_NM == "Sun", "3.일",
                                                  ifelse(WEKDY_NM == "Sat", "2.토", "1.주중")) ]


# 상담여부 
# names(CALL_BRD_df_bs)[which(names(CALL_BRD_df_bs) == "CN_RS_YN")] <- "상담"
# CALL_BRD_df_bs <- CALL_BRD_df_bs[상담!="Y"] # 상담 방송 제외

# 신상여부
CALL_BRD_df_bs[is.na(FST_BROAD_DTM) | FST_BROAD_DTM == "99999999", "FST_BROAD_DTM"] <- "99991231"   
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , 신상 := ifelse(BROAD_DT == ymd(substr(FST_BROAD_DTM,1,8)) | FST_BROAD_DTM=="99991231", "YES", "NO" )]


CALL_BRD_df_bs[ is.na(모_수량) , "모_수량"] <- 0
CALL_BRD_df_bs[ is.na(모_고객수) , "모_고객수"] <- 0

# CALL_BRD_df_bs <- CALL_BRD_df_bs[BROAD_HH %in% c(2,3,4,5)]

### 추가변수
# 아이템명 우측 생성 ITEM_NM_SEP, 좌측 BRAND 
CALL_BRD_df_bs <- CALL_BRD_df_bs[ , ITEM_NM_SEP := unlist(strsplit(ITEM_NM, "[:]"))[2], by = "no" ]

# 순서 
CALL_BRD_df_bs <- CALL_BRD_df_bs[order(BROAD_DT, MIN_Q_START_DATE)]

# 아이템, 브랜드, 상품분류
CALL_BRD_df_bs <- CALL_BRD_df_bs[, BR_SF := seq(.N), by = c("BRAND")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[, IT2_SF := seq(.N), by = c("ITEM_NM_SEP")]
CALL_BRD_df_bs <- CALL_BRD_df_bs[, PG_SF := seq(.N), by = c("PRD_GRP_NM")]


# PUMP_CMT_df <- CMT_df[PMO_STR_DTM >= ymd("2017-01-01") & SRCH == "CA" ,]
# PUMP_CMT_df <- unique(PUMP_CMT_df[, .(PMO_NO, PMO_STR_DTM, PMO_END_DTM, ITEM_CD , ITEM_NM )])
# PUMP_CMT_df <- PUMP_CMT_df[order(PMO_STR_DTM)]
# PUMP_CMT_df <- PUMP_CMT_df[, SEQ := 1:nrow(PUMP_CMT_df)]
# 
# # 프로모션 상품평 테이블 
# pump_cmt_all_df <- do.call(rbind, lapply(1:nrow(PUMP_CMT_df) , function(i) {
#   # i <- 1
#   print(i)
#   TMP <- PUMP_CMT_df[i,]
#   
#   st <- ymd(substr(TMP$PMO_STR_DTM,1,10))
#   lt <- ymd(substr(TMP$PMO_END_DTM,1,10))
#   
#   day_df <- data.frame(DAY = seq.Date(st, lt, by = "days"  ))
#   result_df <- data.frame(SEQ = TMP$SEQ, day_df,
#                           SEQ_EVT = 1:nrow(day_df),
#                           # PRD_CD = TMP$PRD_CD,    PRD_NM = TMP$PRD_NM,
#                           ITEM_CD = TMP$ITEM_CD,  ITEM_NM = TMP$ITEM_NM,
#                           PMO_STR_DTM = TMP$PMO_STR_DTM,
#                           PMO_END_DTM = TMP$PMO_END_DTM)
#   result_df
#   
# }))
# 
# # 
# pump_cmt_all_df <- data.table(pump_cmt_all_df)
# 
# #    
# names(pump_cmt_all_df)[2] <- "BROAD_DT"
# pump_cmt_all_df$BROAD_DT <- ymd(pump_cmt_all_df$BROAD_DT)
# pump_df <- pump_cmt_all_df[ , .(
#   PROMO_CNT = .N
# ),  by = c("BROAD_DT", "ITEM_CD", "ITEM_NM")] 
# 
# 
# # 일별 아이템별 상품평 카운트 결합
# CALL_BRD_df_bs <- merge(CALL_BRD_df_bs, pump_df, by = c("BROAD_DT", "ITEM_CD", "ITEM_NM"), all.x = T)
# CALL_BRD_df_bs[is.na(CALL_BRD_df_bs$PROMO_CNT), "PROMO_CNT"] <- 0
# CALL_BRD_df_bs$PROMO_CNT_G <- ifelse(CALL_BRD_df_bs$PROMO_CNT > 0 , 1, 0 )


# 모델 생성 데이터 
MDL_DT <- "2017-01-01"
FIND_PAST_df <- CALL_BRD_df_bs[BROAD_DT >= ymd(MDL_DT), ]

# 일자 갭 계산 
GRP <- data.table(data.frame(DG = 1:(360*10), DGG = rep(seq(30, (360*10), by = 30), each = 30)))
# 
DT_df <- data.frame(DT = seq(ymd("2016-01-01"), ymd(substr(SYSDATE,1,10)) + 365, by =  "1 month") )
DT_df$DT_VF <- c(ymd("2015-12-01"), lag(DT_df$DT)[-1])


# 직전, 이전 데이터 결합  nrow(FIND_PAST_df)
# i<- 355

# 28338 28372 28378 28447 28456 28517 28529 28556
# 49003
# which(FIND_PAST_df$no=="49003")
system.time(
  result_df <- do.call(rbind.fill, mclapply(1 : nrow(FIND_PAST_df)  , function(i) { 
    # i <- 28342
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
    
    # 편성표
    if(tmp1$no %in% CALL_SCH_df$no){
      (day_chk <- Sys.Date()-1)
    } else {
      day_seq <- data.frame(day = tmp1$BROAD_DT - seq(1,30, by = 1))
      day_seq$WEKPT <- wday(day_seq$day)
      (day_chk <- day_seq$day[1]) 
    }
    
    
    if(PGM == "브랜드PGM") {
      # item 
      sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM,]
      if (nrow(sub_df) == 0) {
        sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == "일반PGM", ]
      } 
      sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
      sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
      # 과거말일 이전 데이터만 추출 
      sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    } else {
      # item 
      sub_df <- CALL_BRD_df_bs[IT_SF < tmp1$IT_SF & ITEM_NM == tmp1$ITEM_NM & PGM_GBN == PGM,]
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
    # 이렇게 해도 못찾았다면! 아이템명(브랜드명제외) 시간대 매칭!
    # if (nrow(sub_df2) == 0) {
    # 
    #   if(PGM == "브랜드PGM") {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
    #     if (nrow(sub_df) == 0) {
    #       sub_df <- CALL_BRD_df_bs[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == "일반PGM", ]
    #     }
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   } else {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[IT2_SF < tmp1$IT2_SF & ITEM_NM_SEP == tmp1$ITEM_NM_SEP & PGM_GBN == PGM,]
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   }
    # 
    # 
    #   # 상품군 & 주중/주말 및 시간  일치
    #   sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #   sub_df2$HHG <- 200
    #   if (nrow(sub_df2) == 0) {
    #     # +- 1시간
    #     sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #     sub_df2$HHG <- 210
    #     if (nrow(sub_df2) == 0) {
    #       # +- 2시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #       sub_df2$HHG <- 220
    #       if (nrow(sub_df2) == 0) {
    #         # +- 4시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #         sub_df2$HHG <- 230
    #       }
    #     }
    #   }
    # 
    # 
    #   # 주중/주말포기! 상품군 & 시간  일치
    #   if (nrow(sub_df2) == 0) {
    # 
    #     # 시간  일치
    #     sub_df2 <- sub_df[노출HH == hh  , ]
    #     sub_df2$HHG <- 2000
    #     if (nrow(sub_df2) == 0) {
    #       # +- 1시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
    #       sub_df2$HHG <- 2100
    #       if (nrow(sub_df2) == 0) {
    #         # +- 2시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
    #         sub_df2$HHG <- 2200
    #         if (nrow(sub_df2) == 0) {
    #           # +- 4시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
    #           sub_df2$HHG <- 2300
    #         }
    #       }
    #     }
    # 
    #   }
    # 
    # }
    
    # # 이렇게 해도 못찾았다면! 브랜드명 시간대 매칭!
    # if (nrow(sub_df2) == 0) {
    # 
    #   if(PGM == "브랜드PGM") {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
    #     if (nrow(sub_df) == 0) {
    #       sub_df <- CALL_BRD_df_bs[BR_SF  < tmp1$BR_SF  & BRAND == tmp1$BRAND & PGM_GBN == "일반PGM", ]
    #     }
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   } else {
    #     # item
    #     sub_df <- CALL_BRD_df_bs[BR_SF < tmp1$BR_SF & BRAND == tmp1$BRAND & PGM_GBN == PGM,]
    #     sub_df$DG <- as.numeric(day_chk - sub_df$BROAD_DT)
    #     sub_df <- merge(sub_df, GRP, by = "DG", all.x = T)
    #     # 과거말일 이전 데이터만 추출
    #     sub_df <- sub_df[BROAD_DT < day_chk & DG <= 366 , ]
    #   }
    # 
    # 
    #   # 상품군 & 주중/주말 및 시간  일치
    #   sub_df2 <- sub_df[노출HH == hh   & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #   sub_df2$HHG <- 300
    #   if (nrow(sub_df2) == 0) {
    #     # +- 1시간
    #     sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #     sub_df2$HHG <- 310
    #     if (nrow(sub_df2) == 0) {
    #       # +- 2시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #       sub_df2$HHG <- 320
    #       if (nrow(sub_df2) == 0) {
    #         # +- 4시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4)  & WEK == tmp1$WEK & HDY_YN == tmp1$HOLDY_YN, ]
    #         sub_df2$HHG <- 330
    #       }
    #     }
    #   }
    # 
    # 
    #   # 주중/주말포기! 상품군 & 시간  일치
    #   if (nrow(sub_df2) == 0) {
    # 
    #     # 시간  일치
    #     sub_df2 <- sub_df[노출HH == hh  , ]
    #     sub_df2$HHG <- 3000
    #     if (nrow(sub_df2) == 0) {
    #       # +- 1시간
    #       sub_df2 <- sub_df[노출HH >= (hh - 1) & 노출HH <= (hh + 1) , ]
    #       sub_df2$HHG <- 3100
    #       if (nrow(sub_df2) == 0) {
    #         # +- 2시간
    #         sub_df2 <- sub_df[노출HH >= (hh - 2) & 노출HH <= (hh + 2) , ]
    #         sub_df2$HHG <- 3200
    #         if (nrow(sub_df2) == 0) {
    #           # +- 4시간
    #           sub_df2 <- sub_df[노출HH >= (hh - 4) & 노출HH <= (hh + 4) , ]
    #           sub_df2$HHG <- 3300
    #         }
    #       }
    #     }
    # 
    #   }
    # 
    # }
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
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        # ## 취급액
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
        
      } else {
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
      }
      
      tmp <- cbind.data.frame(tmp1,  tmp2)
      tmp$FIND_YN <- "NO"
      
    } else {  
      
      # 기간 
      subset_df <- head(sub_df2[order(IT_SF, decreasing = T)],6)
      subset_df <- subset_df[,   .(
        평_노     = round(mean(SUM_RUNTIME, na.rm = T)), 
        # 평_가     = round(mean(WEIHT_MI, na.rm = T)), 
        
        # 평_달성률   = mean(SALE_PERCENT_ONAIR, na.rm = T),
        #
        평_총수   = round(mean(T_QTY_ONAIR, na.rm = T)), 
        평_총고수   = round(mean(T_CNT_ONAIR, na.rm = T)), 
        
        
        평_분취급액   = mean(분취급액, na.rm = T),
        # 평_가중분취급액   = mean(가중분취급액, na.rm = T),
        #
        
        평_분수   = round(mean(분수량, na.rm = T)), 
        # 평_가분수   = round(mean(가중분수량, na.rm = T)), 
        
        평_총모수 = round(mean(T_QTY_ONAIR_MCPC, na.rm = T)), 
        평_총모고수 = round(mean(T_CNT_ONAIR_MCPC, na.rm = T)), 
        
        평_모비 = round(mean(모_수량, na.rm = T) * 100), 
        평_모고비 = round(mean(모_고객수, na.rm = T) * 100), 
        
        평_가격   = round(mean(AVG_PRICE, na.rm = T)), 
        # 평_상품수 = round(mean(CNT_PRDCD, na.rm = T)),
        
        
        ##
        
        직_노     = round((SUM_RUNTIME[1])), 
        직_총수   = round((T_QTY_ONAIR[1])), 
        직_분수   = round((분수량[1])), 
        직_총모수 = round((T_QTY_ONAIR_MCPC[1])), 
        직_모비 = round((모_수량[1]) * 100), 
        직_가격   = round((AVG_PRICE[1])), 
        # 직_상품수 = round((CNT_PRDCD[1])),
        
        # 직_가     = round((WEIHT_MI[1])), 
        직_총고수   = round((T_CNT_ONAIR[1])), 
        # 직_가분수   = round((가중분수량[1])), 
        직_총모고수 = round((T_CNT_ONAIR_MCPC[1])), 
        직_모고비 = round((모_고객수[1]) * 100), 
        
        #
        # 직_달성률   = (SALE_PERCENT_ONAIR[1]), 
        직_분취급액   = (분취급액[1]), 
        # 직_가취급액   = (가중분취급액[1]), 
        
        
        ##
        # 직_분상주콜   = round((분상주콜[1])),
        # 직_분상SR콜   = round((분상SR콜[1])),
        
        HHG       = unique(HHG),
        최근방송일= as.numeric(day - BROAD_DT[1]) )] 
      
      # 전달 
      
      if ( substr(mnt,1,7) != substr(SYSDATE,1,7) ) {
        mnt_bf <- DT_df[which(DT_df$DT == mnt) - 1, ]$DT_VF # 방송전월  
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        ## 취급액
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp3_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
        
      } else {
        
        if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
          tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
          tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
            tmp2_1 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
            tmp2 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
              tmp2 <- CALL_df[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
            }
        
        # if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0 & hh>=23){
        #   tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-2), ][, .(전달취급액)]
        #   tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else if(nrow(CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ])==0){
        #     tmp3_1 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & (HH == hh+1 |  HH == hh-1), ][, .(전달취급액)]
        #     tmp3 <- data.table(전달취급액 = mean(tmp2_1$전달취급액))} else {
        #       tmp3 <- CALL_df_FEE[mon == mnt_bf & WEK == tmp1$WEK & HH == hh , ][, .(전달취급액)]
        #     }
        
      }
      
      tmp <- cbind.data.frame(tmp1, subset_df, tmp2)
      tmp$FIND_YN <- "YES"
      
    }
    
    
    tmp
    
  }, mc.cores = 14))
)

# user   system  elapsed 
# 1011.810   43.665  145.879 
# rm(GRP, DT_df, FIND_PAST_df)


# 데이터 정리
CALL_VAR_df <- result_df %>% dplyr::select(no, BROAD_DT, WEKDY_NM, WEK, BRD_DAYT, MIN_Q_START_DATE, MAX_Q_END_DATE,노출HH, HOLDY_YN,
                                           #
                                           IT_SF, BR_SF, IT2_SF, PG_SF, ITEM_CD, ITEM_NM, ITEM_NM_SEP, BRAND, PRD_GRP_NM,   
                                           # 
                                           PGM_ID, TITLE_NM, PGM_GBN, BRAND_PGM_NM, 
                                           # 
                                           # CNT_PRDCD, 
                                           AVG_PRICE, SUM_RUNTIME, 
                                           # WEIHT_MI, 
                                           # EXPCT_SAL_AMT,
                                           EXPCT_SAL_AMT,
                                           
                                           
                                           # 
                                           ADVR_SALE_AMT, CNF_FEE_AMT, ADVR_FEE_YN, CNF_FEE_YN,#TGT_SAL_AMT,
                                           # 
                                           # SALE_PERCENT_ONAIR,
                                           T_QTY_ONAIR, T_QTY_ONAIR_MCPC, T_CNT_ONAIR, T_CNT_PRE,
                                           #
                                           분취급액, 분수량, 분모수량, 
                                           # 가중분취급액, 가중분수량, 가중분모수량, 
                                           모_수량, 
                                           #
                                           평_노, 평_총수, 평_분수, 평_총모수, 평_모비, 평_가격, 
                                           # 평_상품수,
                                           #
                                           직_노, 직_총수, 직_분수, 직_총모수, 직_모비, 직_가격, 
                                           # 직_상품수,
                                           #
                                           # 평_가, 
                                           평_총고수, 
                                           # 평_가분수, 
                                           평_총모고수, 평_모고비,
                                           #
                                           # 직_가, 
                                           직_총고수, 
                                           # 직_가분수, 
                                           직_총모고수, 직_모고비,
                                           #
                                           HHG, 최근방송일, 전달취급액, 
                                           # 전달취급액, (달성률) 
                                           FIND_YN, 
                                           #상담, 
                                           신상,
                                           #
                                           # 평_달성률, 
                                           평_분취급액, 
                                           #평_가중분취급액, 직_달성률, 
                                           직_분취급액 #직_가취급액
                                           
                                           ## 추가변수 
                                           # PROMO_CNT,
                                           # PROMO_CNT_G
                                           
)

CALL_VAR_df <- data.table(CALL_VAR_df)
CALL_VAR_df <- CALL_VAR_df[,AVG_PRICE := ifelse(AVG_PRICE==0,평_가격,AVG_PRICE)]

### 파생 변수 생성 
CALL_VAR_df <- CALL_VAR_df[, 평_가격갭 := round( (AVG_PRICE - 평_가격)/AVG_PRICE , 3 ) * 100]
CALL_VAR_df <- CALL_VAR_df[, 직_가격갭 := round( (AVG_PRICE - 직_가격)/AVG_PRICE , 3 ) * 100]
CALL_VAR_df$평_가격갭 <- ifelse(CALL_VAR_df$평_가격갭 == "-Inf", 0, CALL_VAR_df$평_가격갭)
CALL_VAR_df$직_가격갭 <- ifelse(CALL_VAR_df$직_가격갭 == "-Inf", 0, CALL_VAR_df$직_가격갭)

# CALL_VAR_df <- CALL_VAR_df[, 평_상품갭 := CNT_PRDCD - 평_상품수]
# CALL_VAR_df <- CALL_VAR_df[, 직_상품갭 := CNT_PRDCD - 직_상품수]

CALL_VAR_df <- CALL_VAR_df[, 평_노출갭 := SUM_RUNTIME - 평_노]
CALL_VAR_df <- CALL_VAR_df[, 직_노출갭 := SUM_RUNTIME - 직_노]

# CALL_VAR_df <- CALL_VAR_df[, 평_가중노출갭 := WEIHT_MI - 평_가]
# CALL_VAR_df <- CALL_VAR_df[, 직_가중노출갭 := WEIHT_MI - 직_가]


CALL_VAR_df <- CALL_VAR_df[, 노_전달시취급액 := (전달취급액/60) * SUM_RUNTIME ]
# CALL_VAR_df <- CALL_VAR_df[, 노_전달가중시취급액 := (전달취급액/60) * WEIHT_MI ]


CALL_VAR_df <- CALL_VAR_df[, 노_평_분취급액 := (평_분취급액) * SUM_RUNTIME ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분취급액 := (직_분취급액) * SUM_RUNTIME ]

CALL_VAR_df <- CALL_VAR_df[, 노_평_분수량 := round((평_분수) * SUM_RUNTIME) ]
CALL_VAR_df <- CALL_VAR_df[, 노_직_분수량 := round((직_분수) * SUM_RUNTIME) ]

# CALL_VAR_df <- CALL_VAR_df[, 노_평_가중분수량 := (평_가분수) * WEIHT_MI ]
# CALL_VAR_df <- CALL_VAR_df[, 노_직_가중분수량 := (직_가분수) * WEIHT_MI ]

# CALL_VAR_df <- CALL_VAR_df[, 노_평_가중분취급액 := (평_가중분취급액) * WEIHT_MI ]
# CALL_VAR_df <- CALL_VAR_df[, 노_직_가중분취급액 := (직_가취급액) * WEIHT_MI ]


## 현재 가중분이 없는 상황이라 결측값이 나온다

# 브랜드 PGM 
CALL_VAR_df$BRAND_PGM_GRP <- ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("B special", "The Collection" ,"The Collection 2호"), "1.B스페샬_더컬", 
                                    ifelse(CALL_VAR_df$BRAND_PGM_NM %in% c("최은경의 W", "왕영은의 톡톡톡", "SHOW me the Trend"), 
                                           "2.왕톡_쇼미_최W",
                                           "3.똑소리_리뷰_일반"))

# 
CALL_VAR_df <- CALL_VAR_df[ , 신상_IT := ifelse(신상 == "YES" & IT_SF == min(IT_SF), "YES", "NO") , by = "ITEM_NM"]

#  
# CALL_VAR_df$상담 <- as.factor(CALL_VAR_df$상담)
CALL_VAR_df$신상 <- as.factor(CALL_VAR_df$신상)
CALL_VAR_df$신상_IT <- as.factor(CALL_VAR_df$신상_IT)
CALL_VAR_df$PGM_GBN <- as.factor(CALL_VAR_df$PGM_GBN)
CALL_VAR_df$BRAND_PGM_GRP <- as.factor(CALL_VAR_df$BRAND_PGM_GRP)

#
# CALL_VAR_df$PROMO_CNT_G <- as.factor(CALL_VAR_df$PROMO_CNT_G)
CALL_VAR_df$BRAND_PGM_NM <- as.factor(CALL_VAR_df$BRAND_PGM_NM)
# 
CALL_VAR_df <- CALL_VAR_df[order(no)]
# CALL_VAR_df[is.na(CALL_VAR_df)] <- 0

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
# wek <- ymd(substr(SYSDATE, 1, 10))
# wek2 <- wek + 11

# wek <- ymd(substr(SYSDATE, 1, 10)) -10# - 29
# wek2 <- wek + 7


# if (TEST_YN == "YES") {
#   wek <- seq.Date(ymd( "2018-05-03"), ymd( "2018-08-15") , by = "1 days")
#   wek2 <- wek + 11
# }

#----------------------------------------------------------------
# 모델 공식 
#----------------------------------------------------------------

# 2.
# var_nm <- c(
#   "CNT_PRDCD", "AVG_PRICE", "SUM_RUNTIME", "WEIHT_MI" ,
#   #"ADVR_SALE_AMT" , "CNF_FEE_AMT" , "TGT_SAL_AMT",
#   "평_노", "평_총수", "평_분수", "평_총모수", "평_모비", "평_가격", "평_상품수",
#   #
#   "평_가", "평_총고수", "평_가분수", "평_총모고수", "평_모고비",
#   #
#   "직_노", "직_총수", "직_분수", "직_총모수", "직_모비", "직_가격", "직_상품수",
#   #
#   "직_가", "직_총고수", "직_가분수", "직_총모고수", "직_모고비",
#   #
#   "최근방송일", "전달취급액", #"전달취급액" ,
#   #
#   "평_가격갭", "직_가격갭", "평_상품갭", "직_상품갭", "평_노출갭", "직_노출갭", "평_가중노출갭", "직_가중노출갭",
#   #
#   "노_전달시취급액" , "노_전달가중시취급액" , "노_평_분수량" , "노_직_분수량" ,        
#   "노_평_가중분수량" , "노_직_가중분수량" , "노_평_가중분취급액" ,"노_직_가중분취급액" , "노_평_분취급액","노_직_분취급액",
#   
#   #
#   "FIND_YN" , "신상", "WEK",  "BRAND_PGM_NM", "신상_IT", "노출HH",  
#   # 
#   "PRD_GRP_NM", "HOLDY_YN2", "HOLDY_SEQ",
#   "직템__평_총수"
#   ) # , "PROMO_CNT" "FESTA_YN", , "날짜순서"
# # 


# 
# ###########################################################################
# 가중분을 제거한담면???
var_nm <- c(
  # "CNT_PRDCD", 
  "AVG_PRICE", "SUM_RUNTIME",
  #"ADVR_SALE_AMT" , "CNF_FEE_AMT" , "TGT_SAL_AMT",
  "평_노", "평_총수", "평_분수", "평_총모수", "평_모비", "평_가격", 
  # "평_상품수",
  #
  "평_총고수", "평_총모고수", "평_모고비",
  #
  "직_노", "직_총수", "직_분수", "직_총모수", "직_모비", "직_가격", 
  # "직_상품수",
  #
  "직_총고수", "직_총모고수", "직_모고비",
  #
  "최근방송일", "전달취급액", #"전달취급액" ,
  #
  "평_가격갭", "직_가격갭", 
  # "평_상품갭", "직_상품갭", 
  "평_노출갭", "직_노출갭",
  #
  "노_전달시취급액"  , "노_평_분수량" , "노_직_분수량" ,
  "노_평_분취급액","노_직_분취급액",
  
  #
  "FIND_YN" , "신상", "WEK",  "BRAND_PGM_NM", "신상_IT", "노출HH",
  #
  "PRD_GRP_NM", "HOLDY_YN2", "HOLDY_SEQ",
  "직템__평_총수"
) # , "PROMO_CNT" "FESTA_YN", , "날짜순서"
#

###########################################################################


if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {
  var_nm <- c(var_nm, "FESTA_YN", "날짜순서")
}

# formula1 <- formula(paste("총인콜수_M ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)" ), collapse = "+")))
# formula2 <- formula(paste("log10(총인콜수_M) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)"), collapse = "+")))

# formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)", "I(노_평_가중분취급액^3)","I(SUM_RUNTIME^2)", "I(전달취급액^2)",
#                                                        "I(노_직_가중분취급액^2)","I(노_평_가중분취급액^2)"), collapse = "+")))

# formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[], "I(AVG_PRICE^2)", "I(SUM_RUNTIME^2)", "I(전달취급액^2)", 
#                                                        "I(노_평_분취급액^2)", "I(노_직_분취급액^2)"),
#                                                       collapse = "+")))

formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(c(var_nm[]),
                                                     collapse = "+")))


# formula2 <- formula(paste("log10(Y_REAL) ~ " , paste(var_nm[],collapse = "+")))

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
# CALL_VAR_df$CNF_FEE_YN <- as.factor(CALL_VAR_df$CNF_FEE_YN)
# CALL_VAR_df$ADVR_FEE_YN <- as.factor(CALL_VAR_df$ADVR_FEE_YN)
# CALL_VAR_df$CNF_FEE_YN <- NULL

# CALL_VAR_df$PROMO_CNT_G <- as.factor(CALL_VAR_df$PROMO_CNT_G)
# 
mdl_df <- data.frame(CALL_VAR_df)
mdl_df[is.na(mdl_df)] <- 0
# 

# rm(HOLDY_DT, i)

#----------------------------------------------------------------
print("모델링 시작")

# names(mdl_df)[which(names(mdl_df) == "T_QTY_ONAIR")] <- "Y_REAL"
names(mdl_df)[which(names(mdl_df) == "EXPCT_SAL_AMT")] <- "Y_REAL"

# # # eda
# # train_df %>%
# #   group_by(노출HH,PRD_GRP_NM) %>% dplyr::summarise(sales = mean(Y_REAL)) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=factor(노출HH),y=sales,group = PRD_GRP_NM, fill = PRD_GRP_NM)) + geom_line(aes(color = PRD_GRP_NM)) +
# #   facet_wrap(~PRD_GRP_NM)
# # 
# # train_df %>%
# #   group_by(노출HH,PRD_GRP_NM) %>% dplyr::summarise(count = n()) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=factor(노출HH),y=count , fill = PRD_GRP_NM)) + geom_bar(stat="identity") +
# #   facet_wrap(~PRD_GRP_NM)
# # 
# # train_df %>%
# #   mutate(month = factor(substr(BROAD_DT,1,7))) %>%
# #   group_by(month,PRD_GRP_NM) %>% dplyr::summarise(sales = mean(Y_REAL)) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=month,y=sales,group = PRD_GRP_NM, fill = PRD_GRP_NM)) + geom_line(aes(color = PRD_GRP_NM)) +
# #   facet_wrap(~PRD_GRP_NM) +
# #   ggplot2::ggtitle("시간별 취급액 현황") +
# #   theme(axis.title.x=element_blank(),
# #         axis.text.x=element_blank(),
# #         axis.ticks.x=element_blank())
# # 
# # train_df %>%
# #   group_by(노출HH,PRD_GRP_NM) %>% dplyr::summarise(sales = mean(Y_REAL),
# #                                                  sales2 = mean(노_평_가중분취급액)) %>%
# #   filter(PRD_GRP_NM != "디지털기기") %>%
# #   ggplot(aes(x=factor(노출HH),y=sales,group = PRD_GRP_NM, fill = PRD_GRP_NM)) + geom_line(aes(color = PRD_GRP_NM)) +
# #   geom_line(aes(x=factor(노출HH),y=sales2),color = "red") +
# #   facet_wrap(~PRD_GRP_NM)
# 
#   
#   
# #### test 하기
# # wek <- ymd("20200423")
# # wek2 <- wek+2
# ##############
# 
# # mdl_df$상담 <- NULL
# train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < wek,] %>% filter(Y_REAL > 0))
# train_df <- train_df %>% filter(직_가격갭 > -200 & 직_가격갭 < 200 & 직_분수 < 1000 )
# 
# test_df <- mdl_df[mdl_df$BROAD_DT >= wek & mdl_df$BROAD_DT < wek2,]
# 
# Y_tr <- train_df$Y_REAL
# Y_tt <- test_df$Y_REAL
# Y_trl <- log10(train_df$Y_REAL )
# 
# set.seed(123)
# mdl_mt   <- model.matrix(formula2, data= rbind.data.frame(train_df, test_df))[,-1]
# 
# train_mt <- mdl_mt[1:nrow(train_df) , ]
# test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]
# 
# set.seed(123)
# smp <- sample(dim(train_mt)[1], round(dim(train_mt)[1]*0.25))
# 
# xgTrain <- xgb.DMatrix(data=train_mt[-smp,], label=Y_trl[-smp])
# xgVal <- xgb.DMatrix(data=train_mt[smp,], label=Y_trl[smp])
# xgTest <- xgb.DMatrix(data=test_mt)
# 
# remodel_yn <- "Q"
# 
# # 목요일이면 모델생성   
# # if ( wday(wek) != make_mdl ) {
# #   
# #   print(paste( wek , wek2, " 목요일X 예측테스트 " ))
# #   
# #   # 목요일이 아니면 
# #   load(file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu] ] ) ,"__", GBN_NM, ".RData") )
# #   
# #   # 예측 테스트 실패시 NOT출현 // 재모델링 
# #   remodel_yn   <- tryCatch(predict(LS_MDL, newx = test_mt),
# #                            error = function(e) print("NOT"),
# #                            warning = function(w) print("NOT"))
# # }
# # 
# # # 목요일이거나 예측실패한경우 모델 재생성 
# # if ( wday(wek[clu]) == make_mdl | remodel_yn == "NOT" ) {
# #   print(paste( wek , wek2, " 목요일O 모델링 및 예측수행" ))
# #   
# #   print(paste(" XGB ", as.character(Sys.time())))
# #   
# #   # ###### 5.XGB 시작 ######
# #   
# #   if (FESTA_YN$FESTA_YN[FESTA_YN$DT_CD == STRT_DT] == "Y") {
# #     
# #     
# 
#     set.seed(123)
#     xgbFitl <- xgb.train(
#       data = xgTrain, nfold = 5, label = as.matrix(Y_trl),
#       objective='reg:linear',
#       nrounds=1200,
#       eval_metric='rmse',
#       watchlist=list(train=xgTrain, validate=xgVal),
#       print_every_n=10,
#       nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
#       early_stopping_rounds=100
#     )
# 
#     preds2l <- predict(xgbFitl, newdata = test_mt)
#     RMSE(Y_tt,10^preds2l)
#     
#     data.frame(Y_tt,10^preds2l) %>%
#       ggplot(aes(x=1:length(Y_tt),y=Y_tt)) +
#       geom_line(color="blue") +
#       geom_line(aes(x=1:length(Y_tt),y=10^preds2l))
#       # geom_ribbon(aes(ymax=10^preds3l_up, ymin=10^preds3l_down),alpha=0.4,fill = "slategray4")
#   
#     # png(file = paste0( "/home/DTA_CALL/VARPLOT/", fld_nm, "/",STRT_DT, GBN_NM,".png"), height = 1100, width = 700)
#     
#     # xgbFitl %>% 
#     #   xgb.importance(feature_names=colnames(xgTrain)) %>%  
#     #   dplyr::slice(1:30) %>%
#     #   data.table() %>%
#     #   xgb.plot.importance()
#     # dev.off()
#     
#     dim(train_mt)
#     # } else {
#     
#     # print(xgbFit)
#     set.seed(123)
#     xgbFitl <- xgboost(data = train_mt, nfold = 5, label = as.matrix(Y_trl),
#                        nrounds = 2000, verbose = FALSE, eval_metric = "rmse",
#                        nthread = 8, eta = 0.01, gamma = 0.0468, max_depth = 6, min_child_weight = 1.7817,
#                        subsample = 0.5213, colsample_bytree = 0.4603)
#     
#     
#     preds2l <- predict(xgbFitl, newdata = test_mt)
#     RMSE(Y_tt,10^preds2l)
#     
#     data.frame(Y_tt,10^preds2l) %>%
#       ggplot(aes(x=1:length(Y_tt),y=Y_tt)) +
#       geom_line(color="blue") +
#       geom_line(aes(x=1:length(Y_tt),y=10^preds3l))+
#       geom_ribbon(aes(ymax=10^preds3l_up, ymin=10^preds3l_down),alpha=0.4,fill = "slategray4")
#     
#     # booster = "dart")
#     # }
#     
#     
#     
#     ## print(xgbFitl)
#     # ###### 5.XGB 시작 ######
#     
#     save(xgbFitl, 
#          file = paste0("/home/DTA_CALL/WEK_MDL_BACKUP/MDL_DATE_ULTRA_", gsub("-", "", wek_df$mdl_dt[wek_df$strt_day == wek[clu]] ), "__", GBN_NM, ".RData") )
#     
#   } 
#   
#   #####---------------------------------- 예측 수행 ---------------------------------#####
#   
#   # clu <- 109
#   if ( wday(wek[clu]) != make_mdl ) {
#     print(paste( clu, wek[clu] , wek2[clu ], " 목요일X 예측수행" ))
#   } else {
#     print(paste( clu, wek[clu] , wek2[clu ], " 목요일O 예측수행" ))
#   }
#   
#   
#   # set.seed(123)
#   # mdl_mt   <- model.matrix(formula1, data=rbind.data.frame(train_df, test_df))[,-1]
#   # 
#   # train_mt <- mdl_mt[1:nrow(train_df) , ]
#   # test_mt  <- mdl_mt[(nrow(train_df) + 1 ):dim(mdl_mt)[1] , ]
#   
#   
#   # ###### 5.XGB  ######
#   
#   preds2l <- predict(xgbFitl, newdata = test_mt)
#   
#   er_df <- data.frame(no_m = 1:nrow(test_df),
#                       PGM_ID = test_df$PGM_ID,
#                       DT = test_df$BROAD_DT, 
#                       MIN_Q_START_DATE = test_df$MIN_Q_START_DATE, 
#                       MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
#                       ITEM_CD = test_df$ITEM_CD,
#                       ITEM_NM = test_df$ITEM_NM,
#                       PRD_GRP_NM = test_df$PRD_GRP_NM, 
#                       SUM_RUNTIME = test_df$SUM_RUNTIME,
#                       Y = Y_tt, XGBL = round(10^(as.numeric(preds2l))))

# 
# 
# ###### XGB 끝 ######


# ###### lightgbm 시작 ######

# wek <- ymd(substr(SYSDATE, 1, 10)) -10
# wek2 <- wek+30

wek <- ymd(substr(SYSDATE, 1, 10))
wek2 <- max(mdl_df$BROAD_DT) # wek + 7


train_df <- data.frame(mdl_df[mdl_df$BROAD_DT < wek,] %>% filter(Y_REAL > 0))
train_df <- train_df %>% filter(직_가격갭 > -200 & 직_가격갭 < 200 & 직_분수 < 1000 )
train_df <- train_df %>% filter(!TITLE_NM %in% c("오늘의 선택","타임특가"))
# train_df <- train_df %>% filter(WEIHT_MI > 10)  


test_df <- mdl_df[mdl_df$BROAD_DT >= wek & mdl_df$BROAD_DT < wek2,]
test_df <- test_df %>% filter(!TITLE_NM %in% c("오늘의 선택","타임특가"))
# test_df <- test_df %>% filter(WEIHT_MI > 10)  

Y_tr <- train_df$Y_REAL
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
smp <- sample(dim(train_mt)[1], round(dim(train_mt)[1]*0.25))

lgTrain <- lgb.Dataset(data=train_mt[-smp,], label=Y_trl[-smp])
lgVal <- lgb.Dataset(data=train_mt[smp,], label=Y_trl[smp])
lgTest <- lgb.Dataset(data=test_mt)


## 신뢰구간 확보

# lgb.model.cv = lgb.cv(data = lgb.train,
#                       objective = "regression",
#                       nrounds = 1000, 
#                       metric = "rmse",
#                       watchlist=list(train=lgTrain, validate=lgVal),
#                       feature_fraction = 0.8,
#                       # booster = "dart",
#                       nfold = 5)
# 
# best.iter = lgb.model.cv$best_iter
# best.iter = 525

set.seed(1234)
lightFitl <- lightgbm(data = lgTrain,
                      objective = "regression",
                      nrounds = 3000, 
                      metric = "rmse",
                      watchlist=list(train=lgTrain, validate=lgVal),
                      feature_fraction = 0.8,
                      booster = "dart")

# learning_rate = 0.01,
# num_iterations = ,
# num_leaves = 30)

set.seed(1234)
lightFitl_up <- lightgbm(data = lgTrain,
                         # nfold = 5, 
                         nrounds = 3000, 
                         eval_metric = "rmse",
                         watchlist=list(train=lgTrain, validate=lgVal),
                         feature_fraction = 0.8,
                         objective = 'quantile',
                         booster = "dart",
                         alpha = 0.8)


set.seed(1234)
lightFitl_down <- lightgbm(data = lgTrain,
                           nrounds = 3000, 
                           eval_metric = "rmse",
                           watchlist=list(train=lgTrain, validate=lgVal),
                           objective = 'quantile',
                           feature_fraction = 0.8,
                           booster = "dart",
                           alpha = 0.2)



preds3l <- predict(lightFitl,test_mt)
## 신뢰구간
preds3l_up <- predict(lightFitl_up,test_mt)
preds3l_down <- predict(lightFitl_down,test_mt)





er_df <- data.frame(no = test_df$no,
                    PGM_ID = test_df$PGM_ID,
                    DT = test_df$BROAD_DT, 
                    MIN_Q_START_DATE = test_df$MIN_Q_START_DATE, 
                    MAX_Q_END_DATE = test_df$MAX_Q_END_DATE,
                    ITEM_CD = test_df$ITEM_CD,
                    ITEM_NM = test_df$ITEM_NM,
                    PRD_GRP_NM = test_df$PRD_GRP_NM, 
                    # WE_TIME = test_df$WEIHT_MI,
                    RUNTIME = test_df$SUM_RUNTIME,
                    # SUM_RUNTIME = test_df$SUM_RUNTIME,
                    # 신상 = test_df$신상,
                    Y = Y_tt, 
                    pred = round(10^(as.numeric(preds3l))),
                    preds3l_up = round(10^(as.numeric(preds3l_up))),
                    preds3l_down = round(10^(as.numeric(preds3l_down))),
                    BRAND_PGM_NM = test_df$BRAND_PGM_NM,
                    # Y_M = Y_tt/test_df$WEIHT_MI,
                    # pred_M = round(10^(as.numeric(preds3l)))/test_df$WEIHT_MI,
                    INSERT_DT = ymd(substr(SYSDATE, 1, 10)))


er_df$preds3l_up <- ifelse(er_df$preds3l_up>er_df$pred,er_df$preds3l_up,er_df$pred)
er_df$preds3l_down <- ifelse(er_df$preds3l_down<er_df$pred,er_df$preds3l_down,er_df$pred)

# er_df2 <- er_df %>% 
#   dplyr::group_by(PGM_ID,MIN_Q_START_DATE) %>%
#   dplyr::mutate(row_n = dplyr::row_number()) %>%
#   dplyr::mutate(row_n = max(row_n)) %>%
#   dplyr::filter(row_n >1) %>% data.frame()
# 
# 
# er_df2 <- merge(er_df2,CALL_SCH_df[,c("PGM_ID","ITEM_CD","ADVR_FEE_YN","CNF_FEE_YN","ADVR_SALE_AMT_mi","CNF_FEE_YN_mi")],by=c("PGM_ID","ITEM_CD"),all.x=T)
# er_df2$preds3l_up <- ifelse(er_df2$preds3l_up>er_df2$pred,er_df2$preds3l_up,er_df2$pred)
# er_df2$preds3l_down <- ifelse(er_df2$preds3l_down<er_df2$pred,er_df2$preds3l_down,er_df2$pred)
# 
# er_df2 <- merge(er_df2,CALL_SCH_df[,c("PGM_ID","ITEM_CD","WEIHT_TIME")],by=c("PGM_ID","ITEM_CD"),all.x=T)
names(er_df)[11:13] <- c("PRED_ON","PREDS3L_UP_ON","PREDS3L_DOWN_ON")


zz2 <- merge(er_df_final,er_df[,c("PGM_ID","ITEM_CD","PRED_ON","PREDS3L_UP_ON","PREDS3L_DOWN_ON")],by=c("PGM_ID","ITEM_CD"))
zz2$INSERT_DT

source(file = "/home/dio/DB_con/DB_conn.R")

# dbWriteTable(conn, "I_KIMDY9.broad_update", zz2 , row.names = F)
# dbSendUpdate(conn, paste0("DROP TABLE I_KIMDY9.broad_test_21_2"))
for (i in 1:nrow(zz2)) {
  # i <-1
  # print(paste(i, "//", nrow(last_df)))
  print(i)
  query.x <- paste0( "INSERT INTO I_KIMDY9.broad_update
                     (PGM_ID,ITEM_CD,no,DT,MIN_Q_START_DATE,MAX_Q_END_DATE,ITEM_NM,PRD_GRP_NM,RUNTIME,Y,pred,preds3l_up,preds3l_down,BRAND_PGM_NM,INSERT_DT,row_n,
                     ADVR_FEE_YN,CNF_FEE_YN,ADVR_SALE_AMT_mi,CNF_FEE_YN_mi,WEIHT_TIME,PRED_ON,PREDS3L_UP_ON,PREDS3L_DOWN_ON)
                     VALUES (",
                     zz2$PGM_ID[i], ",'",
                     zz2$ITEM_CD[i], "',",
                     zz2$no[i], ",'",
                     zz2$DT[i], "','",
                     zz2$MIN_Q_START_DATE[i], "','",
                     zz2$MAX_Q_END_DATE[i], "','",
                     zz2$ITEM_NM[i], "','",
                     zz2$PRD_GRP_NM[i], "',",
                     zz2$RUNTIME[i], ",",
                     zz2$Y[i], ",",
                     zz2$pred[i], ",",
                     zz2$preds3l_up[i], ",",
                     zz2$preds3l_down[i], ",'",
                     zz2$BRAND_PGM_NM[i], "','",
                     zz2$INSERT_DT[i], "',",
                     zz2$row_n[i], ",'",
                     zz2$ADVR_FEE_YN[i], "','",
                     zz2$CNF_FEE_YN[i], "',",
                     zz2$ADVR_SALE_AMT_mi[i], ",",
                     zz2$CNF_FEE_YN_mi[i], ",",
                     zz2$WEIHT_TIME[i], ",",
                     zz2$PRED_ON[i], ",",
                     zz2$PREDS3L_UP_ON[i], ",",
                     zz2$PREDS3L_DOWN_ON[i], ")")
  tryCatch(dbSendUpdate(conn, query.x), error = function(e) print("NOT"), warning = function(w) print("NOT"))
}
dbSendUpdate(conn, paste0("GRANT SELECT ON I_KIMDY9.broad_update TO MSTR_APP"))
dbSendUpdate(conn, paste0("GRANT SELECT ON I_KIMDY9.broad_update TO I_KANGMA"))

dbSendUpdate(conn, paste0("COMMIT"))

dbDisconnect(conn)
rm(conn,drv)
