# 00 22 01 * * /home/kimwh3/BAT/REORD_BAT3_식품.sh
# 35 22 01 * * /home/kimwh3/BAT/REORD_BAT3_의류.sh
# 00 23 01 * * /home/kimwh3/BAT/REORD_BAT3_미용_용품.sh
# 00 01 * * * /home/kimwh3/BAT/CUST_LOG_BAT0100.sh
# 00 08 * * * /home/kimwh3/BAT/CSP_RST_BAT.sh
# 00 20 * * * /home/kimwh3/BAT/tune1_tt.sh
# 05 12 * * * /home/kimwh3/BAT/tune1.sh
# 05 17 * * * /home/kimwh3/BAT/tune2.sh
# 05 23 * * * /home/kimwh3/BAT/tune2.sh

# .rs.restartR()

# vi /home/kimwh3/BAT/tune1.sh
# nohup /usr/lib64/R/bin/Rscript /home/kimwh3/RSC/REORD_CUST/tune1.R > /home/kimwh3/BAT/LOG/tune1.log &
# chmod 754 /home/kimwh3/BAT/tune1.sh
# 05 07 * * * /home/kimwh3/BAT/tune1.sh
# [분 시 일 월 요일(0~7)]
# /home/kimwh3/BAT/tune1.sh

#----------------------------------------------------------------#
# 패키지 불러오기 ####
#----------------------------------------------------------------#
library(data.table)
library(plyr)
library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)
library(MASS)
library(parallel)
library(doSNOW)
library(party)
library(glmnet)
library(quantreg)
library(gclus)
library(caret)
library(gbm)
library(doParallel)
library(kernlab)
library(xgboost)
library(RcppRoll)
library(NMF)
library(pROC) 


library(DBI)
library(RJDBC)
#----------------------------------------------------------------#
options(scipen = 15)

# "Y" 인 경우 같은세션에서 로그데이터 돌림  
DIRECT <- "N"
# 1.5년 의미
DAYD <- round(365 * 1.5) + 5
# 시간 갭 의미 
GAP <- 3
TIMED <- 60*24*(GAP+1)

# DB접근  
# source(file = "/home/kimwh3/DB_CON.R")
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ") 
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")

# dbDisconnect(conn)
# rm(conn,drv)
#----------------------------------------------------------------#
# 1. 데이터 호출 ####
#----------------------------------------------------------------#
gc(T)
(TODAY_DT <- ymd(substr(as.character(Sys.time()), 1, 10)))
(SYSDATE <- ymd_hms(Sys.time()))
(START_SYSDATE <- ymd_hms(Sys.time()))
(SYSDATE <- ymd_hms(paste0(substr(SYSDATE,1,13), "0000")))

# 마트구축 테스트중 
# (SYSDATE  <- ymd_hms("20190912000000"))
# (SYSDATE  <- DATE_SEQ[r])
(SYSDATE1 <- SYSDATE + 60 * 60)
(TODAY_DT <- ymd(substr(SYSDATE,1,10)))
(NEXT_DT <- gsub("-", "", TODAY_DT + 1))



# 고객등급 시작 년월 
STRT_REALMB <- gsub("-", "", substr(TODAY_DT, 1, 7))
# # 마이샵 시작날짜 
# STRT_MYSHOP <- TODAY_DT - 7
# 시작날짜 
STRT_ORD_DT <- ymd(paste0(substr(TODAY_DT - 365 * 2 , 1, 4), "0101"))
STRT_ORD_DT_Y <- TODAY_DT - 365 * 1
STRT_ORD_DT_1.5Y <- TODAY_DT - round(365 * 1.5)
STRT_ORD_DT_2Y <- TODAY_DT - 365 * 2
STRT_ORD_DT_2M <- TODAY_DT - 60
# STRT_ORD_DT_3Y <- TODAY_DT - 365 * 3
# STRT_ORD_DT_4Y <- TODAY_DT - 365 * 4
# 종료날짜 
LAST_ORD_DT <- TODAY_DT - 1
gc(T)

RMSE <- function(m, o){ sqrt(mean((m - o)^2))}  
MAPE <- function(m, o){ n <- length(m)
res <- (100 / n) * sum(abs(m-o)/m)
res  }


print(paste0(" 1.  데이터 호출   // 시간 : ", Sys.time())) 


##### 1.1 주문 대상아이템기준 RE_ORD ######
system.time(
  RE_ORD <- dbGetQuery(conn, paste0(
    "SELECT /*+ FULL(ORD) FULL(PRD) FULL(ITEM) PARALLEL (ORD 4) */
     ORD.CUST_NO
     --, ORD.PGM_ID
     --, ORD.ORD_NO
     , PRD.ENTPR_PRD_GRP_CD
     , PRD_GRP.PRD_GRP_NM
     , CLS1.PRD_CLS_NM
     , CLS1.PRD_SML_CLS_CD
     , CLS2.PRD_SML_CLS_NM
     , PRD.ITEM_CD
     , ITEM.ITEM_NM
     , ORD.PRD_CD
     , PRD.PRD_NM
     , PRD.BRAND_CD
     , BRA.BRAND_NM
     , ORD.ORD_DT
     , ORD.ORD_TIME
     , ORD.BROAD_ORD_TIME_DTL_CD
     , ORD.SRCNG_GBN_CD
     , CASE WHEN ORD.CHANL_CD IN ('C','H','U','D') THEN '1.CALL' ELSE '2.MC_PC' END AS CHANL_GBN
     , ORD.ORD_THETIME_AGE_VAL  -- 주문당시 연령값
    --, H.GENDR_CD               -- 성별 
    , ORD.ORD_THETIME_SALE_PRC -- 주문당시 판매가
    --, ORD.ORD_CHANL_DTL_CD     -- 주문 유입채널 
    --, H.EC_CUST_GRD_CD -- EC등급코드      
     --, TOT_ORD_NO
     --, TOT_ORD_QTY
     --, TOT_ORD_AMT + SUP_SHR_TOT_ORD_DC_AMT + CMM_CPN_DC_AMT * TOT_ORD_QTY + TOT_ORD_NACCM_SALE_AMT AS TOT_ORD_AMT
     
     --, NET_ORD_NO
     --, NET_ORD_QTY AS NET_ORD_QTY
     --, NET_ORD_AMT + SUP_SHR_NET_ORD_DC_AMT + CMM_CPN_DC_AMT * NET_ORD_QTY + NET_ORD_NACCM_SALE_AMT AS NET_ORD_AMT
     
     FROM GSBI_OWN.F_ORD_ORD_D ORD
     LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON ORD.PRD_CD = PRD.PRD_CD
     LEFT JOIN GSBI_OWN.D_PRD_ITEM_M ITEM ON PRD.ITEM_CD = ITEM.ITEM_CD
     LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C PRD_GRP ON PRD.ENTPR_PRD_GRP_CD = PRD_GRP.PRD_GRP_CD
     LEFT JOIN GSBI_OWN.D_PRD_PRD_CLS_M CLS1 ON ITEM.PRD_CLS_CD = CLS1.PRD_CLS_CD
     LEFT JOIN GSBI_OWN.D_PRD_PRD_SML_CLS_M CLS2 ON CLS1.PRD_SML_CLS_CD = CLS2.PRD_SML_CLS_CD
     LEFT JOIN GSBI_OWN.D_PRD_BRAND_M BRA ON PRD.BRAND_CD = BRA.BRAND_CD
     
     
     WHERE ORD.ORD_DT >= '" ,  gsub("-", "", STRT_ORD_DT_1.5Y) ,"' AND ORD.SRCNG_GBN_CD IN ('CA', 'EC') 
     AND NET_ORD_NO > 0
     AND PRD_GRP.PRD_GRP_NM IN ('건강식품','일반식품', '헤어/미용기구','이미용', '생활용품', '주방용품', '레포츠용품','속옷', '아동의류', '아동')
     --AND PRD_GRP.PRD_GRP_NM IN ('건강식품','일반식품')
     "
  ))
)

##### 1.1 주문 #####

setDT(RE_ORD)
RE_ORD$ORD_HMS <- ymd_hm(paste(RE_ORD$ORD_DT,  RE_ORD$ORD_TIME))

# saveRDS(RE_ORD, file = paste0("/home/kimwh3/DAT/REORD/DAY_DATA/","재구매" ,".RDS "))
# RE_ORD <-readRDS(paste0("/home/kimwh3/DAT/REORD/DAY_DATA/","재구매" ,".RDS "))

##### 1.1.1 주문 실시간 RE_TEST ######
system.time(
  RE_TEST <- dbGetQuery(conn, paste0(
    "
     SELECT /*+ FULL(ORD) FULL(PRD) FULL(ITEM) PARALLEL (ORD 4) */
     ORD.CUST_NO
     --, ORD.PGM_ID
     --, ORD.ORD_NO
     , PRD.ENTPR_PRD_GRP_CD
     , PRD_GRP.PRD_GRP_NM
     , CLS1.PRD_CLS_NM
     , CLS1.PRD_SML_CLS_CD
     , CLS2.PRD_SML_CLS_NM
     , PRD.ITEM_CD
     , ITEM.ITEM_NM
     , ORD.PRD_CD
     , PRD.PRD_NM
     , PRD.BRAND_CD
     , BRA.BRAND_NM
     , ORD.ORD_DT
     , ORD.ORD_TIME
     , ORD.BROAD_ORD_TIME_DTL_CD
     , ORD.SRCNG_GBN_CD
     , CASE WHEN ORD.CHANL_CD IN ('C','H','U','D') THEN '1.CALL' ELSE '2.MC_PC' END AS CHANL_GBN
     , CST.AGE AS ORD_THETIME_AGE_VAL  -- 주문당시 연령값
    --, H.GENDR_CD               -- 성별 
    , ORD.ORD_THETIME_SALE_PRC -- 주문당시 판매가
    --, ORD.ORD_CHANL_DTL_CD     -- 주문 유입채널 
    --, H.EC_CUST_GRD_CD -- EC등급코드      
     --, TOT_ORD_NO
     --, TOT_ORD_QTY
     --, TOT_ORD_AMT + SUP_SHR_TOT_ORD_DC_AMT + CMM_CPN_DC_AMT * TOT_ORD_QTY + TOT_ORD_NACCM_SALE_AMT AS TOT_ORD_AMT
     
     --, NET_ORD_NO
     --, NET_ORD_QTY AS NET_ORD_QTY
     --, NET_ORD_AMT + SUP_SHR_NET_ORD_DC_AMT + CMM_CPN_DC_AMT * NET_ORD_QTY + NET_ORD_NACCM_SALE_AMT AS NET_ORD_AMT
     
     FROM GSBI_OWN.F_ORD_RTM_ORD_D ORD
     LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON ORD.PRD_CD = PRD.PRD_CD
     LEFT JOIN GSBI_OWN.D_PRD_ITEM_M ITEM ON PRD.ITEM_CD = ITEM.ITEM_CD
     LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C PRD_GRP ON PRD.ENTPR_PRD_GRP_CD = PRD_GRP.PRD_GRP_CD
     LEFT JOIN GSBI_OWN.D_PRD_PRD_CLS_M CLS1 ON ITEM.PRD_CLS_CD = CLS1.PRD_CLS_CD
     LEFT JOIN GSBI_OWN.D_PRD_PRD_SML_CLS_M CLS2 ON CLS1.PRD_SML_CLS_CD = CLS2.PRD_SML_CLS_CD
     LEFT JOIN GSBI_OWN.D_PRD_BRAND_M BRA ON PRD.BRAND_CD = BRA.BRAND_CD
     LEFT JOIN DHUB_OWN.CST_CUST_M CST ON ORD.CUST_NO = CST.CUST_NO
     
     WHERE ORD.ORD_DT >= '", gsub("-","",TODAY_DT - 1), "' AND ORD.SRCNG_GBN_CD IN ('CA', 'EC') 
     AND NET_ORD_NO > 0
     --AND PRD.ITEM_CD IN ('313335','455585')
     
     AND PRD_GRP.PRD_GRP_NM IN ('건강식품','일반식품', '헤어/미용기구','이미용', '생활용품', '주방용품', '레포츠용품','속옷', '아동의류', '아동')
     
     --AND PRD_GRP.PRD_GRP_NM IN ('건강식품','일반식품')
     --AND PRD_GRP.PRD_GRP_NM IN ('헤어/미용기구','이미용', '생활용품', '주방용품')
     --AND PRD_GRP.PRD_GRP_NM IN ('레포츠용품','속옷', '아동의류', '아동')
     "
  ))
)
gc(T);gc(T)
##### 1.1.1 주문 실시간 ######
print("준리얼 실시간주문데이터 dim")
dim(RE_TEST)
setDT(RE_TEST)
RE_TEST$ORD_HMS <- ymd_hms(paste(RE_TEST$ORD_DT,  RE_TEST$ORD_TIME))
RE_TEST <- RE_TEST[ORD_HMS < SYSDATE]
range(RE_TEST$ORD_HMS)

RE_ORD <- rbind.fill(RE_ORD, RE_TEST) %>% distinct()
gc(T)

setDT(RE_ORD)
setkeyv(RE_ORD, c("CUST_NO", "ITEM_CD", "ITEM_NM"))
RE_ORD_cust <- RE_ORD[ ,  .(CNT = .N), by = c("CUST_NO", "ITEM_CD", "ITEM_NM")]
RE_ORD_cust <- unique(RE_ORD_cust[CNT>1]$CUST_NO)
print("2회이상구매한 고객 수 ")
print(length(RE_ORD_cust))

# 리얼멤버십 호출 
list_grd <- list.files("/home/kimwh3/DAT/REORD/GRD_DATA/")
# which(list_grd == paste0(gsub("-","",substr(TODAY_DT - 180, 1, 7)),".RDS"))

# 180일 전부터 호출
RE_GRD <- do.call(rbind, mclapply(which(substr(list_grd,1,6) %in% gsub("-","",substr(TODAY_DT - 180, 1, 7))):length(list_grd), function(i){
  # i <- 1
  print(i)
  tmp <- readRDS(paste0("/home/kimwh3/DAT/REORD/GRD_DATA/",list_grd[i] ))
  tmp
}, mc.cores = 11))

## 180일 전 vip, vvip 경험이 있는 고객 평균 200만명 대상 예측 
setDT(RE_GRD)
GOLDUP6 <- RE_GRD[STD_YM > gsub("-","",substr(TODAY_DT - 180, 1, 7)) & EC_CUST_GRD_CD %in% c("VIP","VVIP")]$CUST_NO  %>% unique()
GOLDUP6 <- unique(RE_GRD$CUST_NO)
GOLDUP6 <- unique(c(RE_ORD_cust, GOLDUP6))

print(length(GOLDUP6))

setDT(RE_ORD)


print(length(unique(RE_ORD$CUST_NO)))
RE_ORD <- RE_ORD[CUST_NO %in% GOLDUP6] 
print(length(unique(RE_ORD$CUST_NO)))


TEST_CUST_NO <- unique(RE_ORD$CUST_NO)
print(length(TEST_CUST_NO))
saveRDS(TEST_CUST_NO, file = paste0("/home/kimwh3/DAT/REORD/LOG_RST_CUST/","CUST_", SYSDATE ,".RDS"))



# 매월 1일, 2일, 3일 배치 적재 
##### 1.2 등급  RE_GRD ######
if (substr(TODAY_DT,9,10) %in% c("01", "02", "03")) {
  print("1.5 등급")
  RE_GRD <- dbGetQuery(conn, paste0(
    "
     SELECT /*+ FULL (A) PARALLEL (A 4) */CUST_NO, STD_YM
     , CASE WHEN EC_CUST_GRD_CD IN ('SS') THEN 'VVIP'
     WHEN EC_CUST_GRD_CD IN ('CC') THEN 'VIP'
     WHEN EC_CUST_GRD_CD IN ('DD') THEN 'GOLD' ELSE 'ETC' END AS EC_CUST_GRD_CD  -- 고객등급구분
    FROM GSBI_OWN.D_CST_MM_CUST_M A
     WHERE STD_YM = '",  substr(gsub("-","",TODAY_DT),1,6),"' AND EC_CUST_GRD_CD IN ('CC', 'SS') 
     "
  ))
  saveRDS(RE_GRD, file = paste0("/home/kimwh3/DAT/REORD/GRD_DATA/", substr(gsub("-","",TODAY_DT),1,6) ,".RDS "))
}
##### 1.2 등급 ######


if (DIRECT == "Y") {
  gc(T);gc(T);gc(T)
  DT <- gsub("-","",ymd(LAST_ORD_DT))
  ##### 1.3 행동 데이터 RE_LOG0 ####
  for ( i in 1:length(DT)) {
    print(i)
    # 로그 
    system.time(
      RE_LOG0 <- dbGetQuery(conn, paste0(
        "SELECT /*+ FULL (A) PARALLEL (A 4) */ 
         CUST_NO,
         CONCT_DT, 
         CONCT_TIME, 
         SES_ID, 
         B.BRAND_CD,
         BRA.BRAND_NM,
         B.ITEM_CD,
         ITEM.ITEM_NM,
         A.PRD_CD,
         B.PRD_NM
         --REGEXP_SUBSTR(REGEXP_replace(QUERY,'&mseq=', '!'),'[^!]+',2,2) AS MSEQ,
         --REGEXP_SUBSTR(REGEXP_SUBSTR(REGEXP_replace(QUERY,'&mseq=', '!'),'[^!]+',2,2), '[^&]+',1,1) AS MNG_ITM_VAL_1
         --, QUERY
         FROM SDHUB_OWN.STG_MPC_LOG_CLCT_M A
         LEFT JOIN GSBI_OWN.D_PRD_PRD_M B ON A.PRD_CD = TO_CHAR(B.PRD_CD)
         LEFT JOIN GSBI_OWN.D_PRD_ITEM_M ITEM ON B.ITEM_CD = ITEM.ITEM_CD
         LEFT JOIN GSBI_OWN.D_PRD_BRAND_M BRA ON B.BRAND_CD = BRA.BRAND_CD
         WHERE A.CONCT_DT = '", DT[i],"'
         AND (QUERY LIKE '%tq=%' OR QUERY LIKE '%kwd=%' OR QUERY LIKE '%prdid=%') "
      ))
    )
    
    gc(T);gc(T)
    # 검색 
    system.time(
      RE_KEY0 <- dbGetQuery(conn, paste0(
        "SELECT /*+ FULL (C) PARALLEL (C 4) */ 
         C.CONCT_DT, C.CONCT_TIME, C.SES_ID, C.PARAM_VAL 
         FROM SDHUB_OWN.STG_MPC_LOG_CLCT_D C 
         WHERE CONCT_DT = '", DT[i],"'
         AND PARAM_KEY = 'SRCH_KEYWD'"
      ))
    )
    
    setDT(RE_LOG0)
    setDT(RE_KEY0)
    
    RE_LOG0 <- RE_LOG0[!is.na(CUST_NO) | nchar(CUST_NO) < 10]
    
    gc(T);gc(T)
    
  }
  
  ##### 1.3 행동 데이터 ####
} else {
  # 고객 타겟 시작 
  # source(file = "/home/kimwh3/RSC/REORD_CUST/CUST_TARGET_LOG_REAL.R") 
  system("/home/kimwh3/BAT/tune1_1.sh")
  
}



###### 휴일데이터 활용 HOLDY_DT ###### 
HOLDY_DT <- dbGetQuery(conn, paste0("SELECT * 
                                     FROM DTA_OWN.CMM_STD_DT
                                     WHERE DT_CD >= '" , gsub("-", "", STRT_ORD_DT_2Y), "'
                                     ORDER BY DT_CD")
)

###### 휴일데이터 활용 #####

###### 편성 데이터 예측에 활용 CALL_SCH_df #####
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
                                        WHERE A.BROAD_DT BETWEEN '",gsub("-","",LAST_ORD_DT),"' AND '",NEXT_DT,"'
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
                                        
                                        
                                        --PGM_ID PGM_NM TITLE_ID TITLE_NM PGM_GBN BRAND_PGM_NM BROAD_DT BROAD_STR_DTM BROAD_END_DTM
                                        --PRD_GRP_NM ITEM_CD BRAND ITEM_NM WEKDY_NM WEKDY_YN HOLDY_YN CN_RS_YN PRD_FORM_SEQ SORT_SEQ RUN_TIME
                                        --CNT_PRDCD FST_BROAD_DTM AVG_PRICE MIN_PRICE MAX_PRICE
                                        
                                        
                                        
                                        
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
                                        WHERE A.BROAD_DT BETWEEN '",gsub("-","",LAST_ORD_DT),"' AND '",NEXT_DT,"'
                                        AND A.PGM_ID = B.PGM_ID
                                        AND A.USE_YN = 'Y' 
                                        AND A.CHANL_CD = 'C'
                                        AND B.USE_YN = 'Y'
                                        AND A.MNFC_GBN_CD = '1')
                                        AND B.ITEM_CODE = C.ITEM_CD
                                        AND C.PRD_GRP_CD = PRDGRP.PRD_GRP_CD
                                        AND A.BROAD_DATE BETWEEN TO_DATE('",gsub("-","",LAST_ORD_DT),"', 'YYYYMMDD') AND TO_DATE('",NEXT_DT,"', 'YYYYMMDD') + 0.99999
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

###### 편성 데이터 예측에 활용 #####

dbDisconnect(conn)
rm(conn,drv)

range(CALL_SCH_df$MIN_Q_START_DATE)

# 편성표 정제 
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

range(CALL_SCH_df$MIN_Q_START_DATE)


#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE1.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE1.RData"))



#----------------------------------------------------------------#
# 2. 주기 도출 ####
#----------------------------------------------------------------#

# 데이터 테이블화 

setDT(RE_ORD); setkey(RE_ORD, keycols = "CUST_NO")
setDT(RE_TEST); setkey(RE_TEST, keycols = "CUST_NO")
setDT(RE_GRD); setkey(RE_GRD, keycols = "CUST_NO")
gc(T)

print(paste0(" 2.  주기도출 시작 // 시간 : ", Sys.time())) 

# 2.0 주기도출 시작 
print(paste0(" 주기도출 기간 : ", STRT_ORD_DT_2Y, " ~ ", LAST_ORD_DT)) 
# 년월 생성 
RE_ORD$ORD_YM    <- substr(RE_ORD$ORD_DT,1,6) 
# 고객 등급 합치기 
print(dim(RE_ORD))
RE_ORD <- merge(RE_ORD, RE_GRD, by.x = c("CUST_NO", "ORD_YM"), by.y = c("CUST_NO", "STD_YM"), all.x = T)
print(dim(RE_ORD))


# 고객번호 기준 최근 6개월 GOLD 이상 대상 타겟 마케팅 
# GOLDUP6 <- RE_ORD[ymd(ORD_DT) > (TODAY_DT - 180) & EC_CUST_GRD_CD %in% c("GOLD","VIP","VVIP")]$CUST_NO  %>% unique()
# # GOLDUP6 <- RE_GRD[STD_YM > gsub("-","",substr(TODAY_DT - 180, 1, 7)) & EC_CUST_GRD_CD %in% c("GOLD","VIP","VVIP")]$CUST_NO  %>% unique()
# 
# print(length(unique(RE_ORD$CUST_NO)))
# RE_ORD <- RE_ORD[CUST_NO %in% GOLDUP6]
# print(length(unique(RE_ORD$CUST_NO)))

rm(RE_GRD);gc(T);gc(T)

gc(T);gc(T)
# DB접근  
# source(file = "/home/kimwh3/DB_CON.R") 
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ") 
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")
## 주기 정보 호출 
PERIOD_df <- dbGetQuery(conn, paste0(" SELECT DISTINCT * FROM DTA_OWN.RE_TMP_TB WHERE 주기생성일 = (SELECT max(주기생성일) FROM DTA_OWN.RE_TMP_TB)
                                      UNION ALL
                                      SELECT DISTINCT * FROM DTA_OWN.RE_HAIR_TB WHERE 주기생성일 = (SELECT max(주기생성일) FROM DTA_OWN.RE_HAIR_TB)
                                      UNION ALL
                                      SELECT DISTINCT * FROM DTA_OWN.RE_CLTH_TB WHERE 주기생성일 = (SELECT max(주기생성일) FROM DTA_OWN.RE_CLTH_TB)"))
dbDisconnect(conn)
rm(conn,drv) 

# 대상 리스트  
setDT(PERIOD_df)
PERIOD_df_prd <- PERIOD_df[재구매고객수 > 10][order(재구매고객수)]
dim(PERIOD_df_prd)
# 대상 리스트 코드만 
VAR_LIST_ID <- PERIOD_df_prd$구분코드; length(VAR_LIST_ID)

# 대상 리스트 매칭
VARLIST    <- c("PRD_SML_CLS_CD", "ENTPR_PRD_GRP_CD", "BRAND_CD", "ITEM_CD", "PRD_CD")
VARLIST_NM <- c("PRD_SML_CLS_NM", "PRD_GRP_NM", "BRAND_NM", "ITEM_NM", "PRD_NM")
NUM_LIST <- data.frame(VAR = VARLIST, VAR_NM = VARLIST_NM)
setkeyv(RE_ORD, c( "CUST_NO"))

gc(T)
print("주기도래 고객 추출 ")
NUM_LIST$VAR    <- as.character(NUM_LIST$VAR)
NUM_LIST$VAR_NM <- as.character(NUM_LIST$VAR_NM)

#
PERIOD_df_prd <- PERIOD_df_prd[order(구분코드, VAR_NM, 재구매건수)]
system.time(PERIOD_df_prd <- PERIOD_df_prd[, 순서rev := rev(seq(.N)), by=c("구분코드",  "VAR_NM")])
PERIOD_df_prd <- PERIOD_df_prd[순서rev == 1]

# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_테스트고객.RData"))
# load(paste0("/home/kimwh3/DAT/REORD/IMAGE_테스트고객.RData"))




# 300  고객 주기 데이터 붙여주기 
system.time(test_result <- do.call(rbind.fill, lapply(c(4), function(i){
  # i <- 4
  print(i)
  num <- NUM_LIST[i,]
  # 상품 
  prd <- PERIOD_df_prd[구분1 == num$VAR,.(구분코드,VAR_NM, Q1, P40, 중앙값,  P60, Q3 ) ]
  # if(!num$VAR %in% c("PRD_SML_CLS_CD", "ENTPR_PRD_GRP_CD")) {prd$구분코드 <- as.numeric(prd$구분코드)}
  RE_ORD0 <- RE_ORD %>% dplyr::rename(VAR = num$VAR, VAR_NM = num$VAR_NM)
  RE_ORD0 <- RE_ORD0[VAR %in% prd$구분코드]
  
  prd$구분코드 <- as.numeric(prd$구분코드)
  prd <- merge(RE_ORD0, prd, by.x = c("VAR","VAR_NM"), by.y = c("구분코드", "VAR_NM"), all.x= T)
  prd <- prd[!is.na(Q1)]
  system.time(prd <- prd[ order(CUST_NO, VAR, VAR_NM, -ORD_DT  )])
  # system.time(prd$KEY  <- paste0(prd$CUST_NO,prd$VAR,prd$VAR_NM))
  
  setkeyv(prd, c("CUST_NO","VAR","VAR_NM"))
  system.time(prd <- prd[, 주문순서rev := seq_len(.N), by=c("CUST_NO","VAR","VAR_NM")]) 
  
  system.time(prd <- prd[주문순서rev == 1])
  system.time(prd <- prd[ ,현재일차이 := as.numeric(TODAY_DT - ymd(ORD_DT) ) ])
  prd$중앙값차이 <- abs(prd$현재일차이 - prd$중앙값)
  
  
  test00 <- prd[현재일차이 >= P40 & 현재일차이 <= P60][ , .(CUST_NO, VAR, VAR_NM,  ORD_DT, 현재일차이, 중앙값차이, Q1, P40, 중앙값,  P60, Q3   )]
  test00 <- test00[ order(VAR, VAR_NM, 중앙값차이  )]
  # system.time(test00$KEY <- paste0(test00$VAR,test00$VAR_NM))
  setkeyv(test00, c("VAR","VAR_NM"))
  if(nrow(test00) != 0) {system.time(test00 <- test00[, 주문순서 := (seq_len(.N)), by=c("VAR","VAR_NM")])}
  test00 <- test00[ ,주기그룹 := "근접"]
  gc(T)
  
  test0 <- prd[(현재일차이 < P40 & 현재일차이 >= Q1) | (현재일차이 > P60 & 현재일차이 <= Q3)][ , .(CUST_NO, VAR, VAR_NM,  ORD_DT, 현재일차이, 중앙값차이, Q1, P40, 중앙값,  P60, Q3   )]
  test0 <- test0[ order(VAR, VAR_NM, 중앙값차이  )]
  # system.time(test0$KEY <- paste0(test0$VAR,test0$VAR_NM))
  # setkey(test0, "KEY")
  setkeyv(test0, c("VAR","VAR_NM"))
  if(nrow(test0) != 0) {system.time(test0 <- test0[, 주문순서 := (seq_len(.N)), by=c("VAR","VAR_NM")])}
  test0 <- test0[ ,주기그룹 := "도달"]
  gc(T)
  
  test1 <- prd[현재일차이 < Q1 ][ , .(CUST_NO, VAR, VAR_NM,  ORD_DT, 현재일차이, 중앙값차이, Q1, P40, 중앙값,  P60, Q3   )]
  test1 <- test1[ order(VAR, VAR_NM, 중앙값차이  )]
  # system.time(test1$KEY <- paste0(test1$VAR,test1$VAR_NM))
  # setkey(test1, "KEY")
  setkeyv(test1, c("VAR","VAR_NM"))
  if(nrow(test1) != 0) {system.time(test1 <- test1[, 주문순서 := (seq_len(.N)), by=c("VAR","VAR_NM")])}
  test1 <- test1[ ,주기그룹 := "미달"]
  gc(T)
  
  test2 <- prd[현재일차이 > Q3 ][ , .(CUST_NO, VAR, VAR_NM,  ORD_DT, 현재일차이, 중앙값차이, Q1, P40, 중앙값,  P60, Q3   )]
  test2 <- test2[ order(VAR, VAR_NM, 중앙값차이  )]
  # system.time(test2$KEY <- paste0(test2$VAR,test00$VAR_NM))
  # setkey(test2, "KEY")
  setkeyv(test2, c("VAR","VAR_NM"))
  if(nrow(test2) != 0) {system.time(test2 <- test2[, 주문순서 := (seq_len(.N)), by=c("VAR","VAR_NM")])}
  test2 <- test2[ ,주기그룹 := "이탈"]
  gc(T)
  
  
  test0 <- rbind.fill(test2, test00, test0, test1 )
  
  test0$구분1 <- num$VAR
  test0$구분2 <- num$VAR_NM
  
  
  test0
  
})))
dim(test_result)
length(unique(test_result$CUST_NO ))

setDT(test_result)
test_result[is.na(test_result)] <- 0
# rm(P_TMP, prd_tmp, test)

#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE2.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE2.RData"))


# 2. 주기 도출 ####
#----------------------------------------------------------------#

print(paste0(" 2.  주기도출 완료 // 시간 : ", Sys.time())) 

#----------------------------------------------------------------#
# 3. 고객 주기 정보 ####
#----------------------------------------------------------------#

setkeyv(RE_ORD, c("CUST_NO", "ITEM_CD", "ITEM_NM"))
## 상품군, 상품세분류, 브랜드, 아이템, 상품구매 건수
## 상품군, 상품세분류, 브랜드, 아이템, 상품구매 본인주기 
system.time(RE_ORD <- RE_ORD[!is.na(ITEM_CD) & !is.na(ITEM_NM)  , 아이템_주문건수 := .N, by=c("CUST_NO", "ITEM_CD", "ITEM_NM")])
# RE_ORD <- RE_ORD[!is.na(PRD_CD) & !is.na(PRD_NM)                , 상품_주문건수 := .N, by=c("CUST_NO", "PRD_CD", "PRD_NM")]


# ## 아이템
gc(T)
print(paste0(" 3.1  아이템 개인주기 시작 // 시간 : ", Sys.time())) 

# 주문 순서
TMP <- RE_ORD[ order(CUST_NO, ITEM_CD, ITEM_NM, ORD_DT, ORD_TIME  )][ITEM_CD %in% test_result$VAR , ]
system.time(setkeyv(TMP, c("CUST_NO", "ITEM_CD", "ITEM_NM", "ORD_DT")))
system.time(TMP <- TMP[!is.na(ITEM_CD) & !is.na(ITEM_NM) , 일자순서 := seq_len(.N), by=c("CUST_NO", "ITEM_CD", "ITEM_NM", "ORD_DT")])

TMP <- TMP[ order(CUST_NO, ITEM_CD, ITEM_NM, -ORD_DT  )]
system.time(TMP <- TMP[, 아이템순서 := (seq_len(.N)), by=c("CUST_NO", "ITEM_CD", "ITEM_NM")])
TMP <- TMP[ order(CUST_NO, ITEM_CD, ITEM_NM, ORD_DT  )]

# 주기찾기 31
system.time(TMP <- TMP[일자순서 == 1, 이전구매일 :=ORD_DT[아이템순서 == 2], by = c("CUST_NO", "ITEM_CD", "ITEM_NM")])

system.time(TMP <- TMP[일자순서 == 1, 이전구매일 := ifelse(is.na(이전구매일), ORD_DT, 이전구매일) ])
system.time(TMP <- TMP[일자순서 == 1, 구매일차이 := as.numeric(ymd(ORD_DT) - ymd(이전구매일) )])
# 주기입력
system.time(TMP <- TMP[일자순서 == 1, 주기_평균   := round(sum(구매일차이, na.rm = T)/(아이템_주문건수[1] - 1)), by = c("CUST_NO",  "ITEM_CD", "ITEM_NM")])
system.time(TMP <- TMP[일자순서 == 1, 주기_중앙값 := round(median(구매일차이[-1], na.rm = T)), by = c("CUST_NO",  "ITEM_CD", "ITEM_NM")])
gc(T);
# TMP[CUST_NO == "47464313"]

# 파생변수 추가
system.time(TMP <- TMP[, 이전등급 := EC_CUST_GRD_CD[아이템순서 == 2], by = c("CUST_NO", "ITEM_CD", "ITEM_NM")])
system.time(TMP <- TMP[, 이전주문시점 := BROAD_ORD_TIME_DTL_CD[아이템순서 == 2], by = c("CUST_NO", "ITEM_CD", "ITEM_NM")])
system.time(TMP <- TMP[, 이전주문채널 := CHANL_GBN[아이템순서 == 2], by = c("CUST_NO", "ITEM_CD", "ITEM_NM")])
system.time(TMP <- TMP[, 이전주문채널2 := CHANL_GBN[아이템순서 == 3], by = c("CUST_NO", "ITEM_CD", "ITEM_NM")])

system.time(TMP <- TMP[, 구매일차이_직전1 := 구매일차이[아이템순서 == 2], by = c("CUST_NO", "ITEM_CD", "ITEM_NM")])
system.time(TMP <- TMP[, 구매일차이_직전2 := 구매일차이[아이템순서 == 3] , by = c("CUST_NO", "ITEM_CD", "ITEM_NM")])

TMP <- TMP[ order(CUST_NO, -ORD_DT, -ORD_TIME  )]
system.time(TMP <- TMP[, 고객순서 := (seq_len(.N)), by=c("CUST_NO")])
system.time(TMP <- TMP[, PRD_GRP_NM_직전1 := PRD_GRP_NM[고객순서 == 2], by = c("CUST_NO")])
system.time(TMP <- TMP[, PRD_GRP_NM_직전2 := PRD_GRP_NM[고객순서 == 3] , by = c("CUST_NO")])
system.time(TMP <- TMP[, PRD_CLS_NM_직전1 := PRD_CLS_NM[고객순서 == 2], by = c("CUST_NO")])
system.time(TMP <- TMP[, PRD_CLS_NM_직전2 := PRD_CLS_NM[고객순서 == 3] , by = c("CUST_NO")])
system.time(TMP <- TMP[, PRICE_직전1 := ORD_THETIME_SALE_PRC[고객순서 == 2], by = c("CUST_NO")])
system.time(TMP <- TMP[, PRICE_직전2 := ORD_THETIME_SALE_PRC[고객순서 == 3] , by = c("CUST_NO")])



TMP$주문시간 <- as.numeric(substr(TMP$ORD_TIME,1,2))
TMP$주문시간 <- ifelse(TMP$주문시간 == 0, 24, TMP$주문시간)
TMP <- TMP[ , 주문시간G := ifelse(주문시간 >= 2 & 주문시간 < 8, "4.새벽", 
                                  ifelse(주문시간 >= 8 & 주문시간 < 13, "1.낮",
                                             ifelse(주문시간 >= 13 & 주문시간 < 19, "2.오후", "3.저녁"))) ] 

TMP <- TMP[ , WEK := wday(ymd(ORD_DT)) - 1 ]
TMP$WEK <- ifelse(TMP$WEK == 0, 7, TMP$WEK)
TMP <- TMP[ , WEK_G := ifelse(WEK == 1, "1.월", 
                              ifelse(WEK == 2, "2.화",
                                     ifelse(WEK == 3, "3.수",
                                            ifelse(WEK == 4, "4.목",
                                                   ifelse(WEK == 5, "5.금",
                                                          ifelse(WEK == 6, "6.토","7.일")))))) ] 
# 최빈값 
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

setkey(TMP, CUST_NO)
system.time(TMP <- TMP[, 선호요일 := getmode(WEK_G) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 선호쇼핑타임 := getmode(주문시간G) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 평균소비가격대 := round(mean(ORD_THETIME_SALE_PRC[ORD_THETIME_SALE_PRC>0], na.rm = T)) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 선호아이템명 := getmode(ITEM_NM) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 선호상품군명 := getmode(PRD_GRP_NM) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 아이템주문다양성 := length(unique(ITEM_NM)) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 상품군주문다양성 := length(unique(PRD_GRP_NM)) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 선호CAEC소싱 := getmode(SRCNG_GBN_CD) , by = c("CUST_NO")])

TMP <- TMP[ , 주문시점 := ifelse(BROAD_ORD_TIME_DTL_CD %in% c( "E1", "E2", "Z", "ETC", "NULL"), "4.기타",
                             ifelse(BROAD_ORD_TIME_DTL_CD == "P1", "1.미리주문",
                                    ifelse(BROAD_ORD_TIME_DTL_CD == "O1", "2.온타임", "3.방송후12시간")))]

system.time(TMP <- TMP[, 선호주문시점 := getmode(주문시점) , by = c("CUST_NO")])

DTL <- data.table(ORD_DT = gsub("-","",seq.Date(from = ymd(TODAY_DT - 90), to = ymd(TODAY_DT), by = "days")),
                  SEQ_DT = 90:0)
TMP <- merge(TMP, DTL, by="ORD_DT", all.x= T)

system.time(TMP <- TMP[, 최근일주일주문건수 := sum(SEQ_DT <= 7 , na.rm = T) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 최근한달주문건수   := sum(SEQ_DT <= 30, na.rm = T) , by = c("CUST_NO")])
system.time(TMP <- TMP[, 최근세달주문건수   := sum(SEQ_DT <= 90, na.rm = T) , by = c("CUST_NO")])

system.time(TMP <- TMP[, 최근세달평균구매주기   := abs(round(mean(diff(unique(SEQ_DT[SEQ_DT <= 90])), na.rm = T))) , by = c("CUST_NO")])






TMP$ITEM_CD <- as.character(TMP$ITEM_CD)

TMP <- TMP[아이템순서 == 1 ,.(
  PRD_GRP_NM, PRD_CLS_NM, CUST_NO, ITEM_CD, ITEM_NM, BROAD_ORD_TIME_DTL_CD, CHANL_GBN, ORD_THETIME_AGE_VAL, EC_CUST_GRD_CD,
  아이템_주문건수, 구매일차이, 주기_평균, 주기_중앙값, 이전등급, 이전주문시점, 이전주문채널,이전주문채널2,
  구매일차이_직전1, 구매일차이_직전2,
  PRD_GRP_NM_직전1, PRD_GRP_NM_직전2, PRD_CLS_NM_직전1, PRD_CLS_NM_직전2 , PRICE_직전1, PRICE_직전2,
  
  주문시간, WEK, WEK_G, 선호요일, 주문시간G, 선호쇼핑타임, 평균소비가격대,     선호아이템명, 선호상품군명, 
  아이템주문다양성, 상품군주문다양성, 선호CAEC소싱,       주문시점, 선호주문시점, 최근일주일주문건수,
  최근한달주문건수, 최근세달주문건수, 최근세달평균구매주기
)]

# TMP[CUST_NO == "47464313"]

setDT(test_result)
setkeyv(test_result, c("CUST_NO", "VAR", "VAR_NM"))
# 아이템 합치기
test_result <- test_result[구분1 == "ITEM_CD"] ; dim(test_result)
test_result$VAR <- as.character(test_result$VAR)
test_result <- merge(test_result, TMP, by.x = c("CUST_NO", "VAR", "VAR_NM"), by.y = c("CUST_NO", "ITEM_CD", "ITEM_NM"), all.x = T)
test_result <- test_result %>% dplyr::rename(주문건수 = 아이템_주문건수)
rm(TMP);gc(T);gc(T)


# ## 상품
# gc(T)
# print(paste0(" 3.2  상품 개인주기 시작 // 시간 : ", Sys.time())) 
# 
# # 주문 순서
# TMP <- RE_ORD[ order(CUST_NO, PRD_CD, PRD_NM, ORD_DT  )][PRD_CD %in% test_result$VAR ]
# TMP <- TMP[!is.na(PRD_CD) & !is.na(PRD_NM) , 일자순서 := seq(.N), by=c("CUST_NO", "PRD_CD", "PRD_NM", "ORD_DT")]
# gc(T); 
# gc(T); 
# # TMP[CUST_NO == "47464313"]
# 
# # 파생변수 추가
# TMP <- TMP[일자순서 == 1, 이전구매일 := lag(ORD_DT, 1), by = c("CUST_NO", "PRD_CD", "PRD_NM")]
# TMP <- TMP[일자순서 == 1, 이전구매일 := ifelse(is.na(이전구매일), ORD_DT, 이전구매일) ]
# TMP <- TMP[일자순서 == 1, 구매일차이 := as.numeric(ymd(ORD_DT) - ymd(이전구매일) )]
# # 주기입력
# TMP <- TMP[일자순서 == 1, 주기_평균   := round(sum(구매일차이, na.rm = T)/(상품_주문건수[1] - 1)), 
#                by = c("CUST_NO",   "PRD_CD", "PRD_NM", "상품_주문건수")]
# TMP <- TMP[일자순서 == 1, 주기_중앙값 := round(median(구매일차이[-1], na.rm = T)), 
#                by = c("CUST_NO",  "PRD_CD", "PRD_NM", "상품_주문건수")]
# 
# # 파생변수 추가
# TMP <- TMP[일자순서 == 1, 이전등급 := lag(EC_CUST_GRD_CD, 1), by = c("CUST_NO",  "PRD_CD", "PRD_NM")]
# TMP <- TMP[일자순서 == 1, 이전주문시점 := lag(BROAD_ORD_TIME_DTL_CD, 1), by = c("CUST_NO",  "PRD_CD", "PRD_NM")]
# TMP <- TMP[일자순서 == 1, 이전주문채널 := lag(CHANL_GBN, 1), by = c("CUST_NO", "PRD_CD", "PRD_NM")]
# 
# TMP <- TMP[일자순서 == 1, 이전주문채널 := lag(CHANL_GBN, 1), by = c("CUST_NO",  "PRD_CD", "PRD_NM")]
# TMP <- TMP[일자순서 == 1, 구매일차이_직전1 := lag(구매일차이, 1), by = c("CUST_NO",  "PRD_CD", "PRD_NM")]
# TMP <- TMP[일자순서 == 1, 구매일차이_직전2 := lag(구매일차이, 2), by = c("CUST_NO",  "PRD_CD", "PRD_NM")]
# 
# TMP <- TMP[ order(CUST_NO, PRD_CD, PRD_NM, ORD_DT  )]
# TMP <- TMP[, 상품순서 := rev(seq(.N)), by=c("CUST_NO",  "PRD_CD", "PRD_NM")]
# TMP$PRD_CD <- as.character(TMP$PRD_CD) 
# 
# TMP <- TMP[상품순서 == 1 ,.(
#   CUST_NO, PRD_CD, PRD_NM, BROAD_ORD_TIME_DTL_CD, CHANL_GBN, ORD_THETIME_AGE_VAL, EC_CUST_GRD_CD,
#   상품_주문건수, 구매일차이, 주기_평균, 주기_중앙값, 이전등급, 이전주문시점, 이전주문채널,
#   구매일차이_직전1, 구매일차이_직전2
# )]  
# # TMP[CUST_NO == "47464313"]
# 
# TMP$PRD_CD <- as.character(TMP$PRD_CD) 
# 
# setDT(test_result)
# # 상품 합치기
# test_result3 <- test_result[구분1 == "PRD_CD"] ; dim(test_result3) 
# test_result3$VAR <- as.character(test_result3$VAR) 
# test_result3 <- merge(test_result3, TMP, by.x = c("CUST_NO", "VAR", "VAR_NM"), by.y = c("CUST_NO", "PRD_CD", "PRD_NM"), all.x = T)
# test_result3 <- test_result3 %>% dplyr::rename(주문건수 = 상품_주문건수)
# rm(TMP);gc(T);gc(T)

#
# test_result <- rbind.fill(test_result3, test_result2)
# gc(T);gc(T); rm(test_result2, test_result3) ; gc(T);gc(T);

# test_result <- test_result3
# gc(T);gc(T); rm(test_result3) ; gc(T);gc(T);

## na 0 처리 
test_result <- test_result[ , 주기_평균 := ifelse(is.na(주기_평균), 0 , 주기_평균)]
test_result <- test_result[ , 주기_중앙값 := ifelse(is.na(주기_중앙값), 0 , 주기_중앙값)]

test_result <- test_result[ , 이전등급 := ifelse(is.na(이전등급), "ETC" , 이전등급)]
test_result <- test_result[ , EC_CUST_GRD_CD := ifelse(is.na(EC_CUST_GRD_CD), "ETC" , EC_CUST_GRD_CD)]
test_result <- test_result[ , 이전주문시점 := ifelse(is.na(이전주문시점), "NULL" , 이전주문시점)]
test_result <- test_result[ , 이전주문채널 := ifelse(is.na(이전주문채널), "NULL" , 이전주문채널)]
test_result <- test_result[ , 이전주문채널2 := ifelse(is.na(이전주문채널2), "NULL" , 이전주문채널2)]

test_result <- test_result[ , 구매일차이_직전1 := ifelse(is.na(구매일차이_직전1), 0 , 구매일차이_직전1)]
test_result <- test_result[ , 구매일차이_직전2 := ifelse(is.na(구매일차이_직전2), 0 , 구매일차이_직전2)]

test_result <- test_result[ , PRD_GRP_NM_직전1 := ifelse(is.na(PRD_GRP_NM_직전1), "미생성" , PRD_GRP_NM_직전1)]
test_result <- test_result[ , PRD_GRP_NM_직전2 := ifelse(is.na(PRD_GRP_NM_직전2), "미생성" , PRD_GRP_NM_직전2)]

test_result <- test_result[ , PRD_GRP_NM := ifelse(is.na(PRD_GRP_NM), "미생성" , PRD_GRP_NM)]
test_result <- test_result[ , PRD_CLS_NM := ifelse(is.na(PRD_CLS_NM), "미생성" , PRD_CLS_NM)]

test_result <- test_result[ , PRD_CLS_NM_직전1 := ifelse(is.na(PRD_CLS_NM_직전1), "미생성" , PRD_CLS_NM_직전1)]
test_result <- test_result[ , PRD_CLS_NM_직전2 := ifelse(is.na(PRD_CLS_NM_직전2), "미생성" , PRD_CLS_NM_직전2)]

test_result <- test_result[ , PRICE_직전1 := ifelse(is.na(PRICE_직전1), 0 , PRICE_직전1)]
test_result <- test_result[ , PRICE_직전2 := ifelse(is.na(PRICE_직전2), 0 , PRICE_직전2)]


#
# test_result[is.na(test_result)] <- 0
rm(RE_ORD);gc(T);gc(T);gc(T);gc(T)


#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE3.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE3.RData"))

# 3. 고객 주기 정보 ####
#----------------------------------------------------------------#
print(paste0(" 3.3  상품 개인주기 완료  // 시간 : ", Sys.time())) 

#----------------------------------------------------------------#
# 4. 행동 정보 ####
#----------------------------------------------------------------#
repeat_start <- Sys.time()
print(paste("repeat_start! : " , repeat_start))

i <- 0
repeat { 
  i <- i+1
  print(paste0(" waiting log data set ", i, " // repeat_start! ", repeat_start))
  Sys.sleep(30)
  
  if (sum(list.files("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/") %in% c(paste0("ITEM_LOG_", SYSDATE ,".RDS"))) == 1) { 
    
    print(paste0(" starting log data set ", i))
    Sys.sleep(90)
    
    # RE_LOG_PG_PRD  <- readRDS(file = paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/","PRD_LOG_", SYSDATE ,".RDS"))
    RE_LOG_PG_ITEM <- readRDS(file = paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/","ITEM_LOG_", SYSDATE ,".RDS"))
    
    print(paste0(" 행동데이터 호출완료  // 시간 : ", Sys.time())) 
    
    file.remove(paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/","ITEM_LOG_", SYSDATE ,".RDS"))
    
    
    # 4. 3 소스코드 받아서 돌리기 ####
    
    setDT(test_result)
    setkeyv(test_result, c("CUST_NO","VAR", "VAR_NM"))
    setDT(RE_LOG_PG_ITEM)
    setkeyv(RE_LOG_PG_ITEM, c("CUST_NO","ITEM_CD", "ITEM_NM"))
    
    
    
    test_result$CUST_NO <- as.character(test_result$CUST_NO)
    RE_LOG_PG_ITEM$ITEM_CD <- as.character(RE_LOG_PG_ITEM$ITEM_CD)
    test_result$VAR <- as.character(test_result$VAR)
    # RE_LOG_PG_BRAD$BRAND_CD <- as.character(RE_LOG_PG_BRAD$BRAND_CD)
    
    ## 상품 관점
    # RE_LOG_PG_PRD <- merge(RE_LOG_PG_PRD, test_result[구분1 == "PRD_CD"], 
    #                        by.x = c("CUST_NO", "PRD_CD", "PRD_NM"),
    #                        by.y = c("CUST_NO", "VAR",    "VAR_NM"),
    #                        all = T)
    ## 아이템 관점
    RE_LOG_PG_ITEM <- merge(RE_LOG_PG_ITEM, test_result[구분1 == "ITEM_CD"], 
                            by.x = c("CUST_NO", "ITEM_CD", "ITEM_NM"),
                            by.y = c("CUST_NO", "VAR",    "VAR_NM"),
                            all = T)
    # ## 브랜드 관점 
    # RE_LOG_PG_BRAD <- merge(RE_LOG_PG_BRAD, test_result[구분1 == "BRAND_CD"], 
    #                         by.x = c("CUST_NO", "BRAND_CD", "BRAND_NM"),
    #                         by.y = c("CUST_NO", "VAR",    "VAR_NM"),
    #                         all = T)
    # 
    
    
    ### 추가된 부분 ###
    rm(test_result)
    gc(T);gc(T);gc(T);gc(T)
    tmp2 <- RE_LOG_PG_ITEM[,c("CUST_NO","ITEM_CD")] %>% distinct()
    
    print(dim(tmp2))
    print("추가된 부분 ")
    print(Sys.time())
    
    
    seq_df <- c(seq(1,nrow(tmp2), by = 1000000), nrow(tmp2))
    # # DB접근
    # source(file = "/home/kimwh3/DB_CON.R")
    drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
    conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")
    
    
    # dbWriteTable(conn, "ITEM_TMP001", tmp2, row.names = FALSE)
    
    for( i in 1:(length(seq_df) -1)) {
      print(paste0(i, " ", seq_df[(i+1)], " // ", nrow(tmp2)))
      gc(T)
      tmp <- tmp2[seq_df[i]:seq_df[(i+1)], ]
      if ( i == 1 ) {
        dbWriteTable(conn, "ITEM_TMP001", tmp, row.names = FALSE)
      } else {
        dbWriteTable(conn, "ITEM_TMP002", tmp, row.names = FALSE)
        dbSendUpdate(conn, paste0("
                               INSERT INTO DTA_OWN.ITEM_TMP001 A (
                               A.CUST_NO, A.ITEM_CD
                               )
                               SELECT B.CUST_NO, B.ITEM_CD
                               FROM DTA_OWN.ITEM_TMP002 B
                               "))
        dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.ITEM_TMP002 PURGE"))
      }
    }
    gc(T);gc(T)
    
    dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.ITEM_TMP001 TO DTA_APP"))
    
    dbDisconnect(conn)
    rm(conn,drv)
    
    # 고객별 마지막 주문일자 가져오기 위함 
    # source(file = "/home/kimwh3/DB_CON.R")
    drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
    conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")
    
    RE_ORD_MAXDT <- dbGetQuery(conn, paste0(
      "
       SELECT /*+ FULL (A) PARALLEL (A 4) */  A.CUST_NO, PRD.ITEM_CD, MAX(A.ORD_DT) AS MAX_ORD_DT
       FROM GSBI_OWN.F_ORD_ORD_D A 
         LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON A.PRD_CD = PRD.PRD_CD 
       WHERE A.CUST_NO IN (SELECT B.CUST_NO FROM DTA_OWN.ITEM_TMP001 B)
         AND A.ORD_DT >= TO_CHAR(sysdate - 553, 'YYYYMMDD')
       GROUP BY A.CUST_NO, PRD.ITEM_CD
       "
    )) 
    
    # 접속 종료 
    dbDisconnect(conn)
    rm(conn,drv)
    
    rm(tmp2)
    gc(T); gc(T) 
    print(Sys.time())
    
    setDT(RE_ORD_MAXDT)
    RE_ORD_MAXDT$CUST_NO <- as.character(RE_ORD_MAXDT$CUST_NO)
    RE_ORD_MAXDT$ITEM_CD <- as.character(RE_ORD_MAXDT$ITEM_CD)
    RE_ORD_MAXDT <- RE_ORD_MAXDT[ , MAX_ORD_DT := round(as.numeric(difftime(ymd(substr(as.character(Sys.time()), 1, 10)), ymd(MAX_ORD_DT), tz, units = c( "days"))))]
    
    dim(RE_LOG_PG_ITEM)
    RE_LOG_PG_ITEM <- merge(RE_LOG_PG_ITEM, RE_ORD_MAXDT, by = c("CUST_NO","ITEM_CD"), all.x= T)
    dim(RE_LOG_PG_ITEM)
    
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 현재일차이 := ifelse(주기그룹 == "신규" , MAX_ORD_DT,   현재일차이 )]
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 현재일차이 := ifelse(is.na(현재일차이), 553 , 현재일차이)] )
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , MAX_ORD_DT := NULL]
    
    
    ### 추가된 부분 ###
    
    
    #---# 설명을 위한 데이터 저장 #---#
    save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE4.RData"))
    # load(paste0("/home/kimwh3/DAT/EXP/IMAGE4.RData"))
    
    
    
    
    print(paste0(" 4. 상품페이지 데이터 가공 완료  // 시간 : ", Sys.time())) 
    # rm(RE_LOG_PG)
    gc(T);gc(T);gc(T)
    
    # RE_LOG_PG_PRD[CUST_NO == "47464313" & !is.na(현재일차이)]
    
    
    
    # setDT(RE_TEST)
    # RE_TEST_P <- RE_TEST[ , .(구매여부 = "Y", 최초구매시간 = min(ORD_HMS)), by = c("CUST_NO", "PRD_CD")]
    # RE_TEST_I <- RE_TEST[ , .(구매여부 = "Y", 최초구매시간 = min(ORD_HMS)), by = c("CUST_NO", "ITEM_CD")]
    # RE_TEST_B <- RE_TEST[ , .(구매여부 = "Y", 최초구매시간 = min(ORD_HMS)), by = c("CUST_NO", "BRAND_CD")]
    # 
    # RE_TEST_P$CUST_NO <- as.character(RE_TEST_P$CUST_NO)
    # RE_TEST_I$CUST_NO <- as.character(RE_TEST_I$CUST_NO)
    # RE_TEST_B$CUST_NO <- as.character(RE_TEST_B$CUST_NO)
    
    # RE_TEST_P$PRD_CD <- as.character(RE_TEST_P$PRD_CD)
    # RE_TEST_I$ITEM_CD <- as.character(RE_TEST_I$ITEM_CD)
    # RE_TEST_B$BRAND_CD <- as.character(RE_TEST_B$BRAND_CD)
    
    ## 정답셋 합치기 
    ## 상품 관점
    # RE_LOG_PG_PRD <- merge(RE_LOG_PG_PRD, RE_TEST_P, 
    #                        by = c("CUST_NO", "PRD_CD"),
    #                        all.x = T)
    # ## 아이템 관점
    # RE_LOG_PG_ITEM <- merge(RE_LOG_PG_ITEM, RE_TEST_I, 
    #                         by = c("CUST_NO", "ITEM_CD"),
    #                         all.x = T)
    ## 브랜드 관점 
    # RE_LOG_PG_BRAD <- merge(RE_LOG_PG_BRAD, RE_TEST_B, 
    #                         by = c("CUST_NO", "BRAND_CD"),
    #                         all.x = T)
    
    # RE_LOG_PG_PRD$CUST_NO  <- as.character(RE_LOG_PG_PRD$CUST_NO)
    RE_LOG_PG_ITEM$CUST_NO <- as.character(RE_LOG_PG_ITEM$CUST_NO)
    # RE_LOG_PG_PRD$PRD_CD   <- as.character(RE_LOG_PG_PRD$PRD_CD)
    RE_LOG_PG_ITEM$ITEM_CD <- as.character(RE_LOG_PG_ITEM$ITEM_CD)
    
    
    # RE_LOG_PG_PRD  <- RE_LOG_PG_PRD[ , 구매여부 := NA]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구매여부 := NA]
    # RE_LOG_PG_PRD  <- RE_LOG_PG_PRD[ , 최초구매시간 := NA]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최초구매시간 := NA]
    
    
    
    print(paste0(" 4. 상품페이지 데이터 가공 파생변수 가공시작  // 시간 : ", Sys.time())) 
    
    
    # RE_LOG_PG_PRD$최근첫탐색시간P <- ifelse(!is.na(RE_LOG_PG_PRD$최초구매시간)  , 
    #                                  round(as.numeric(difftime(RE_LOG_PG_PRD$최초구매시간, ymd_hms(RE_LOG_PG_PRD$최근첫탐색시간P), tz, units = c( "mins")))),
    #                                  round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_PRD$최근첫탐색시간P), tz, units = c( "mins")))))
    # RE_LOG_PG_ITEM$최근첫탐색시간I <- ifelse(!is.na(RE_LOG_PG_ITEM$최초구매시간)  , 
    #                                   round(as.numeric(difftime(RE_LOG_PG_ITEM$최초구매시간, ymd_hms(RE_LOG_PG_ITEM$최근첫탐색시간I), tz, units = c( "mins")))),
    #                                   round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_ITEM$최근첫탐색시간I), tz, units = c( "mins")))))
    
    # RE_LOG_PG_PRD$최근2탐색시간P <- ifelse(!is.na(RE_LOG_PG_PRD$최초구매시간)  , 
    #                                  round(as.numeric(difftime(RE_LOG_PG_PRD$최초구매시간, ymd_hms(RE_LOG_PG_PRD$최근2탐색시간P), tz, units = c( "mins")))),
    #                                  round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_PRD$최근2탐색시간P), tz, units = c( "mins")))))
    # RE_LOG_PG_ITEM$최근2탐색시간I <- ifelse(!is.na(RE_LOG_PG_ITEM$최초구매시간)  , 
    #                                   round(as.numeric(difftime(RE_LOG_PG_ITEM$최초구매시간, ymd_hms(RE_LOG_PG_ITEM$최근2탐색시간I), tz, units = c( "mins")))),
    #                                   round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_ITEM$최근2탐색시간I), tz, units = c( "mins")))))
    
    
    # RE_LOG_PG_PRD$최근3탐색시간P <- ifelse(!is.na(RE_LOG_PG_PRD$최초구매시간)  , 
    #                                  round(as.numeric(difftime(RE_LOG_PG_PRD$최초구매시간, ymd_hms(RE_LOG_PG_PRD$최근3탐색시간P), tz, units = c( "mins")))),
    #                                  round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_PRD$최근3탐색시간P), tz, units = c( "mins")))))
    # RE_LOG_PG_ITEM$최근3탐색시간I <- ifelse(!is.na(RE_LOG_PG_ITEM$최초구매시간)  , 
    #                                   round(as.numeric(difftime(RE_LOG_PG_ITEM$최초구매시간, ymd_hms(RE_LOG_PG_ITEM$최근3탐색시간I), tz, units = c( "mins")))),
    #                                   round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_ITEM$최근3탐색시간I), tz, units = c( "mins")))))
    
    setkeyv(RE_LOG_PG_ITEM, c("최초구매시간", "최근첫탐색시간I"))
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근첫탐색시간I:= ifelse(!is.na(최초구매시간)  , 
                                                                      round(as.numeric(difftime(최초구매시간, ymd_hms(최근첫탐색시간I), tz, units = c( "mins")))),
                                                                      round(as.numeric(difftime(SYSDATE, ymd_hms(최근첫탐색시간I), tz, units = c( "mins")))))]
    )
    
    setkeyv(RE_LOG_PG_ITEM, c("최초구매시간", "최근2탐색시간I"))
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근2탐색시간I:= ifelse(!is.na(최초구매시간)  , 
                                                                      round(as.numeric(difftime(최초구매시간, ymd_hms(최근2탐색시간I), tz, units = c( "mins")))),
                                                                      round(as.numeric(difftime(SYSDATE, ymd_hms(최근2탐색시간I), tz, units = c( "mins")))))]
    )
    setkeyv(RE_LOG_PG_ITEM, c("최초구매시간", "최근3탐색시간I"))
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근3탐색시간I:= ifelse(!is.na(최초구매시간)  , 
                                                                      round(as.numeric(difftime(최초구매시간, ymd_hms(최근3탐색시간I), tz, units = c( "mins")))),
                                                                      round(as.numeric(difftime(SYSDATE, ymd_hms(최근3탐색시간I), tz, units = c( "mins")))))]
    )
    
    
    # summary(RE_LOG_PG_PRD$최근첫탐색시간P)
    # summary(RE_LOG_PG_ITEM$최근첫탐색시간I)
    # summary(RE_LOG_PG_BRAD$최근첫탐색시간B)
    
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 탐색여부 := ifelse(is.na(최근첫탐색P), "N", "Y")]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 구매이력여부 := ifelse(is.na(현재일차이), "N", "Y")]
    # 
    # # RE_LOG_PG_PRD[CUST_NO == "9943868"]
    # # summary(RE_LOG_PG_PRD$최근첫탐색시간P)
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근첫탐색시간P := ifelse(is.na(최근첫탐색시간P), TIMED, 최근첫탐색시간P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근2탐색시간P := ifelse(is.na(최근2탐색시간P), TIMED, 최근2탐색시간P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근3탐색시간P := ifelse(is.na(최근3탐색시간P), TIMED, 최근3탐색시간P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근첫탐색P := ifelse(is.na(최근첫탐색P), 0, 최근첫탐색P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근1일탐색P := ifelse(is.na(최근1일탐색P), 0, 최근1일탐색P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근3일탐색P := ifelse(is.na(최근3일탐색P), 0, 최근3일탐색P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근7일탐색P := ifelse(is.na(최근7일탐색P), 0, 최근7일탐색P)]
    # # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근14일탐색P := ifelse(is.na(최근14일탐색P), 0, 최근14일탐색P)]
    # # summary(RE_LOG_PG_PRD$현재일차이)
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 현재일차이 := ifelse(is.na(현재일차이), DAYD, 현재일차이)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 중앙값차이 := ifelse(is.na(중앙값차이), DAYD, 중앙값차이)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 주문순서 := ifelse(is.na(주문순서), 0, 주문순서)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 주기그룹 := ifelse(is.na(주기그룹), "신규", 주기그룹)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 구분1 := ifelse(is.na(구분1), "PRD_CD", 구분1)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 구분2 := ifelse(is.na(구분2), "PRD_NM", 구분2)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , BROAD_ORD_TIME_DTL_CD := ifelse(is.na(BROAD_ORD_TIME_DTL_CD), "ETC", BROAD_ORD_TIME_DTL_CD)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , CHANL_GBN := ifelse(is.na(CHANL_GBN), "ETC", CHANL_GBN)]
    # # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , ORD_THETIME_AGE_VAL := ifelse(is.na(ORD_THETIME_AGE_VAL), 50, ORD_THETIME_AGE_VAL)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , EC_CUST_GRD_CD := ifelse(is.na(EC_CUST_GRD_CD), "ETC2", EC_CUST_GRD_CD)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 주문건수 := ifelse(is.na(주문건수), 0, 주문건수)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 구매일차이 := ifelse(is.na(구매일차이), DAYD, 구매일차이)]
    # 
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 주기_평균 := ifelse(is.na(주기_평균), 0, 주기_평균)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 주기_중앙값 := ifelse(is.na(주기_중앙값), 0, 주기_중앙값)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 이전등급 := ifelse(is.na(이전등급), "ETC2", 이전등급)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 이전주문시점 := ifelse(is.na(이전주문시점), "ETC", 이전주문시점)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 이전주문채널 := ifelse(is.na(이전주문채널), "ETC", 이전주문채널)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 구매일차이_직전1 := ifelse(is.na(구매일차이_직전1), DAYD, 구매일차이_직전1)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 구매일차이_직전2 := ifelse(is.na(구매일차이_직전2), DAYD, 구매일차이_직전2)]
    # 
    # # RE_LOG_PG_PRD$구매여부 <- NA
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 구매여부 := ifelse(is.na(구매여부), "N", 구매여부)]
    
    
    
    ## 아이템 
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 탐색여부 := ifelse(is.na(최근첫탐색I), "N", "Y")]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구매이력여부 := ifelse(is.na(현재일차이), "N", "Y")]
    
    # summary(RE_LOG_PG_ITEM$최근첫탐색시간I)
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근첫탐색시간I := ifelse(is.na(최근첫탐색시간I), TIMED, 최근첫탐색시간I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근2탐색시간I := ifelse(is.na(최근2탐색시간I), TIMED, 최근2탐색시간I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근3탐색시간I := ifelse(is.na(최근3탐색시간I), TIMED, 최근3탐색시간I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근첫탐색I := ifelse(is.na(최근첫탐색I), 0, 최근첫탐색I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근1일탐색I := ifelse(is.na(최근1일탐색I), 0, 최근1일탐색I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근3일탐색I := ifelse(is.na(최근3일탐색I), 0, 최근3일탐색I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근7일탐색I := ifelse(is.na(최근7일탐색I), 0, 최근7일탐색I)]
    # RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근14일탐색I := ifelse(is.na(최근14일탐색I), 0, 최근14일탐색I)]
    # summary(RE_LOG_PG_ITEM$현재일차이)
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 현재일차이 := ifelse(is.na(현재일차이), DAYD, 현재일차이)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 중앙값차이 := ifelse(is.na(중앙값차이), DAYD, 중앙값차이)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 주문순서 := ifelse(is.na(주문순서), 0, 주문순서)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 주기그룹 := ifelse(is.na(주기그룹), "신규", 주기그룹)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구분1 := ifelse(is.na(구분1), "ITEM_CD", 구분1)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구분2 := ifelse(is.na(구분2), "ITEM_NM", 구분2)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BROAD_ORD_TIME_DTL_CD := ifelse(is.na(BROAD_ORD_TIME_DTL_CD), "ETC", BROAD_ORD_TIME_DTL_CD)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , CHANL_GBN := ifelse(is.na(CHANL_GBN), "ETC", CHANL_GBN)]
    # RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , ORD_THETIME_AGE_VAL := ifelse(is.na(ORD_THETIME_AGE_VAL), 50, ORD_THETIME_AGE_VAL)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , EC_CUST_GRD_CD := ifelse(is.na(EC_CUST_GRD_CD), "ETC2", EC_CUST_GRD_CD)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 주문건수 := ifelse(is.na(주문건수), 0, 주문건수)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구매일차이 := ifelse(is.na(구매일차이), DAYD, 구매일차이)]
    
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 주기_평균 := ifelse(is.na(주기_평균), 0, 주기_평균)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 주기_중앙값 := ifelse(is.na(주기_중앙값), 0, 주기_중앙값)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 이전등급 := ifelse(is.na(이전등급), "ETC2", 이전등급)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 이전주문시점 := ifelse(is.na(이전주문시점), "ETC", 이전주문시점)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 이전주문채널 := ifelse(is.na(이전주문채널), "ETC", 이전주문채널)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 이전주문채널2 := ifelse(is.na(이전주문채널2), "ETC", 이전주문채널2)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구매일차이_직전1 := ifelse(is.na(구매일차이_직전1), DAYD, 구매일차이_직전1)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구매일차이_직전2 := ifelse(is.na(구매일차이_직전2), DAYD, 구매일차이_직전2)]
    
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRD_GRP_NM_직전1 := ifelse(is.na(PRD_GRP_NM_직전1), "미생성", PRD_GRP_NM_직전1)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRD_GRP_NM_직전2 := ifelse(is.na(PRD_GRP_NM_직전2), "미생성", PRD_GRP_NM_직전2)]
    
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRD_GRP_NM := ifelse(is.na(PRD_GRP_NM), "미생성", PRD_GRP_NM)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRD_CLS_NM := ifelse(is.na(PRD_CLS_NM), "미생성", PRD_CLS_NM)]
    
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRD_CLS_NM_직전1 := ifelse(is.na(PRD_CLS_NM_직전1), "미생성", PRD_CLS_NM_직전1)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRD_CLS_NM_직전2 := ifelse(is.na(PRD_CLS_NM_직전2), "미생성", PRD_CLS_NM_직전2)]
    
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRICE_직전1 := ifelse(is.na(PRICE_직전1),0, PRICE_직전1)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , PRICE_직전2 := ifelse(is.na(PRICE_직전2),0, PRICE_직전2)]
    
    
    # RE_LOG_PG_ITEM$구매여부 <- NA
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 구매여부 := ifelse(is.na(구매여부), "N", 구매여부)]
    
    
    
    ## 브랜드 
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 탐색여부 := ifelse(is.na(최근첫탐색B), "N", "Y")]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 구매이력여부 := ifelse(is.na(현재일차이), "N", "Y")]
    # 
    # # summary(RE_LOG_PG_BRAD$최근첫탐색시간B)
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 최근첫탐색시간B := ifelse(is.na(최근첫탐색시간B), TIMED, 최근첫탐색시간B)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 최근첫탐색B := ifelse(is.na(최근첫탐색B), 0, 최근첫탐색B)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 최근1일탐색B := ifelse(is.na(최근1일탐색B), 0, 최근1일탐색B)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 최근3일탐색B := ifelse(is.na(최근3일탐색B), 0, 최근3일탐색B)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 최근7일탐색B := ifelse(is.na(최근7일탐색B), 0, 최근7일탐색B)]
    # # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 최근14일탐색B := ifelse(is.na(최근14일탐색B), 0, 최근14일탐색B)]
    # # summary(RE_LOG_PG_BRAD$현재일차이)
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 현재일차이 := ifelse(is.na(현재일차이), DAYD, 현재일차이)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 중앙값차이 := ifelse(is.na(중앙값차이), DAYD, 중앙값차이)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 주문순서 := ifelse(is.na(주문순서), 0, 주문순서)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 주기그룹 := ifelse(is.na(주기그룹), "신규", 주기그룹)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 구분1 := ifelse(is.na(구분1), "BRAND_CD", 구분1)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 구분2 := ifelse(is.na(구분2), "BRAND_NM", 구분2)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , BROAD_ORD_TIME_DTL_CD := ifelse(is.na(BROAD_ORD_TIME_DTL_CD), "ETC", BROAD_ORD_TIME_DTL_CD)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , CHANL_GBN := ifelse(is.na(CHANL_GBN), "ETC", CHANL_GBN)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , ORD_THETIME_AGE_VAL := ifelse(is.na(ORD_THETIME_AGE_VAL), 50, ORD_THETIME_AGE_VAL)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , EC_CUST_GRD_CD := ifelse(is.na(EC_CUST_GRD_CD), "ETC2", EC_CUST_GRD_CD)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 주문건수 := ifelse(is.na(주문건수), 0, 주문건수)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 구매일차이 := ifelse(is.na(구매일차이), DAYD, 구매일차이)]
    # 
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 주기_평균 := ifelse(is.na(주기_평균), 0, 주기_평균)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 주기_중앙값 := ifelse(is.na(주기_중앙값), 0, 주기_중앙값)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 이전등급 := ifelse(is.na(이전등급), "ETC2", 이전등급)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 이전주문시점 := ifelse(is.na(이전주문시점), "ETC", 이전주문시점)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 이전주문채널 := ifelse(is.na(이전주문채널), "ETC", 이전주문채널)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 구매일차이_직전1 := ifelse(is.na(구매일차이_직전1), DAYD, 구매일차이_직전1)]
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 구매일차이_직전2 := ifelse(is.na(구매일차이_직전2), DAYD, 구매일차이_직전2)]
    # 
    # RE_LOG_PG_BRAD <- RE_LOG_PG_BRAD[ , 구매여부 := ifelse(is.na(구매여부), "N", 구매여부)]
    
    
    print(paste0(" 4. 상품페이지 데이터 가공 파생변수 가공완료  // 시간 : ", Sys.time())) 
    
    
    
    
    
    # RE_LOG_PG_PRD$최근첫검색시간P <- round(as.numeric(RE_LOG_PG_PRD$최초구매시간 - ymd_hms(RE_LOG_PG_PRD$최근첫검색시간P))/3600)
    # RE_LOG_PG_ITEM$최근첫검색시간I <- round(as.numeric(RE_LOG_PG_ITEM$최초구매시간 - ymd_hms(RE_LOG_PG_ITEM$최근첫검색시간I))/3600)
    
    # RE_LOG_PG_PRD$최근첫검색시간P <- ifelse(!is.na(RE_LOG_PG_PRD$최초구매시간)  , 
    #                                  round(as.numeric(difftime(RE_LOG_PG_PRD$최초구매시간, ymd_hms(RE_LOG_PG_PRD$최근첫검색시간P), tz, units = c( "mins")))),
    #                                  round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_PRD$최근첫검색시간P), tz, units = c( "mins")))))
    # RE_LOG_PG_ITEM$최근첫검색시간I <- ifelse(!is.na(RE_LOG_PG_ITEM$최초구매시간)  , 
    #                                   round(as.numeric(difftime(RE_LOG_PG_ITEM$최초구매시간, ymd_hms(RE_LOG_PG_ITEM$최근첫검색시간I), tz, units = c( "mins")))),
    #                                   round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_ITEM$최근첫검색시간I), tz, units = c( "mins")))))
    # 
    # RE_LOG_PG_ITEM$최근2검색시간I <- ifelse(!is.na(RE_LOG_PG_ITEM$최초구매시간)  , 
    #                                   round(as.numeric(difftime(RE_LOG_PG_ITEM$최초구매시간, ymd_hms(RE_LOG_PG_ITEM$최근2검색시간I), tz, units = c( "mins")))),
    #                                   round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_ITEM$최근2검색시간I), tz, units = c( "mins")))))
    # 
    # 
    # RE_LOG_PG_ITEM$최근3검색시간I <- ifelse(!is.na(RE_LOG_PG_ITEM$최초구매시간)  , 
    #                                   round(as.numeric(difftime(RE_LOG_PG_ITEM$최초구매시간, ymd_hms(RE_LOG_PG_ITEM$최근3검색시간I), tz, units = c( "mins")))),
    #                                   round(as.numeric(difftime(SYSDATE, ymd_hms(RE_LOG_PG_ITEM$최근3검색시간I), tz, units = c( "mins")))))
    
    setkeyv(RE_LOG_PG_ITEM, c("최초구매시간", "최근첫검색시간I"))
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근첫검색시간I:= ifelse(!is.na(최초구매시간)  , 
                                                                      round(as.numeric(difftime(최초구매시간, ymd_hms(최근첫검색시간I), tz, units = c( "mins")))),
                                                                      round(as.numeric(difftime(SYSDATE, ymd_hms(최근첫검색시간I), tz, units = c( "mins")))))]
    )
    setkeyv(RE_LOG_PG_ITEM, c("최초구매시간", "최근2검색시간I"))
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근2검색시간I:= ifelse(!is.na(최초구매시간)  , 
                                                                      round(as.numeric(difftime(최초구매시간, ymd_hms(최근2검색시간I), tz, units = c( "mins")))),
                                                                      round(as.numeric(difftime(SYSDATE, ymd_hms(최근2검색시간I), tz, units = c( "mins")))))]
    )
    setkeyv(RE_LOG_PG_ITEM, c("최초구매시간", "최근3검색시간I"))
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근3검색시간I:= ifelse(!is.na(최초구매시간)  , 
                                                                      round(as.numeric(difftime(최초구매시간, ymd_hms(최근3검색시간I), tz, units = c( "mins")))),
                                                                      round(as.numeric(difftime(SYSDATE, ymd_hms(최근3검색시간I), tz, units = c( "mins")))))]
    )
    
    
    
    
    #
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 검색여부 := ifelse(is.na(최근첫검색P), "N", "Y")]
    # # summary(RE_LOG_PG_PRD$최근첫탐색시간P)
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근첫검색시간P := ifelse(is.na(최근첫검색시간P), 24*7 + 20, 최근첫검색시간P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근첫검색P := ifelse(is.na(최근첫검색P), 0, 최근첫검색P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근1일검색P := ifelse(is.na(최근1일검색P), 0, 최근1일검색P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근3일검색P := ifelse(is.na(최근3일검색P), 0, 최근3일검색P)]
    # RE_LOG_PG_PRD <- RE_LOG_PG_PRD[ , 최근7일검색P := ifelse(is.na(최근7일검색P), 0, 최근7일검색P)]
    
    ## 아이템 
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 검색여부 := ifelse(is.na(최근첫검색I), "N", "Y")]
    # summary(RE_LOG_PG_ITEM$최근첫검색시간I)
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근첫검색시간I := ifelse(is.na(최근첫검색시간I), TIMED, 최근첫검색시간I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근2검색시간I := ifelse(is.na(최근2검색시간I), TIMED, 최근2검색시간I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근3검색시간I := ifelse(is.na(최근3검색시간I), TIMED, 최근3검색시간I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근첫검색I := ifelse(is.na(최근첫검색I), 0, 최근첫검색I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근1일검색I := ifelse(is.na(최근1일검색I), 0, 최근1일검색I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근3일검색I := ifelse(is.na(최근3일검색I), 0, 최근3일검색I)]
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , 최근7일검색I := ifelse(is.na(최근7일검색I), 0, 최근7일검색I)]
    
    #---# 설명을 위한 데이터 저장 #---#
    save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE5.RData"))
    # load(paste0("/home/kimwh3/DAT/EXP/IMAGE5.RData"))
    
    # 4. 3 소스코드 받아서 돌리기 ####
    #----------------------------------------------------------------#
    print(paste0(" 4. 검색어 데이터 가공 파생변수 가공완료  // 시간 : ", Sys.time())) 
    
    #----------------------------------------------------------------#
    # 4. 행동 정보 ####
    #----------------------------------------------------------------#
    
    #----------------------------------------------------------------#
    # 5. 방송 정보 ####
    #----------------------------------------------------------------#
    
    print(paste0(" 5. 방송 데이터 호출   // 시간 : ", Sys.time())) 
    setDT(CALL_SCH_df)
    head(CALL_SCH_df,2 )
    CALL_SCH_df$MIN_Q_START_DATE <- ymd_hms(CALL_SCH_df$MIN_Q_START_DATE)
    CALL_SCH_df <- CALL_SCH_df[MIN_Q_START_DATE >= (SYSDATE  - 13 * 60 * 60) & MIN_Q_START_DATE <  (SYSDATE  + 4 * 60 * 60)]
    
    # ## 적재 시작
    # tmp2 <- unique(RE_LOG_PG_PRD[,c("PRD_CD", "PRD_NM")] )
    # seq_df <- c(seq(1,nrow(tmp2), by = 300000), nrow(tmp2))
    # # DB접근
    # source(file = "/home/kimwh3/DB_CON.R")
    # 
    # for( i in 1:(length(seq_df) -1)) {
    #   print(paste0(i, " ", seq_df[(i+1)], " // ", nrow(tmp2)))
    #   gc(T)
    #   tmp <- tmp2[seq_df[i]:seq_df[(i+1)], ]
    #   if ( i == 1 ) {
    #     dbWriteTable(conn, "PRD_TMP9", tmp, row.names = FALSE)
    #   } else {
    #     dbWriteTable(conn, "PRD_TMP92", tmp, row.names = FALSE)
    #     dbSendUpdate(conn, paste0("
    #                               INSERT INTO DTA_OWN.PRD_TMP9 A (
    #                               A.PRD_CD, A.PRD_NM
    #                               )
    #                               SELECT B.PRD_CD, B.PRD_NM
    #                               FROM DTA_OWN.PRD_TMP92 B
    #                               "))
    #     dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.PRD_TMP92 PURGE"))
    #   }
    # }
    # 
    # 
    # ##### 상품아이템코드 #
    # ITEM_CD <- dbGetQuery(conn, paste0("SELECT DISTINCT PRD.ITEM_CD
    #                                    , B.PRD_CD
    #                                    FROM DTA_OWN.PRD_TMP9 B
    #                                    LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON TO_CHAR(B.PRD_CD) = TO_CHAR(PRD.PRD_CD)
    #                                    WHERE SALE_PSBL_YN = 'Y' 
    #                                    AND PRD_GBN_CD <> 88
    #                                    "))
    # 
    # dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.PRD_TMP9 PURGE"))
    # 
    # # 접속 종료 
    # dbDisconnect(conn)
    # rm(conn,drv)
    
    gc(T);gc(T);gc(T);gc(T)
    print(paste0(" 5. 방송 데이터 호출 완료   // 시간 : ", Sys.time())) 
    # 555858
    ITEM_LIST <- unique(CALL_SCH_df[ITEM_CD %in% RE_LOG_PG_ITEM$ITEM_CD]$ITEM_CD) 
    system.time(
      RE_BRD_CUST_I <- do.call(rbind.fill, mclapply(1:length(ITEM_LIST), function(i) {
        
        # i <- 1
        print(i)
        (tmp <- RE_LOG_PG_ITEM[ITEM_CD == ITEM_LIST[i]])
        #
        (tmp_brd <- CALL_SCH_df[ITEM_CD  == ITEM_LIST[i]])
        #
        (tmp_brd$BRD_AFT1 <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE ) & MIN_Q_START_DATE <  (SYSDATE  + 1 * 60 * 60) , "Y", "N" )) ) 
        (tmp_brd$BRD_AFT3 <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE ) & MIN_Q_START_DATE <  (SYSDATE  + 3 * 60 * 60) , "Y", "N" )) ) 
        
        (tmp_brd$BRD_BF1  <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE  - 1 * 60 * 60) & MIN_Q_START_DATE <  (SYSDATE  ) , "Y", "N" )) )
        (tmp_brd$BRD_BF3  <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE  - 3 * 60 * 60) & MIN_Q_START_DATE <  (SYSDATE  ) , "Y", "N" )) ) 
        (tmp_brd$BRD_BF12 <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE  - 12 * 60 * 60) & MIN_Q_START_DATE <  (SYSDATE  ) , "Y", "N" )) ) 
        # (tmp_brd)
        
        tmp$BRD_AFT1 <- ifelse(sum(tmp_brd$BRD_AFT1 == "Y")>0, "Y", "N")
        tmp$BRD_AFT3 <- ifelse(sum(tmp_brd$BRD_AFT3 == "Y")>0, "Y", "N")
        tmp$BRD_BF1  <- ifelse(sum(tmp_brd$BRD_BF1 == "Y")>0, "Y", "N")
        tmp$BRD_BF3  <- ifelse(sum(tmp_brd$BRD_BF3 == "Y")>0, "Y", "N")
        tmp$BRD_BF12 <- ifelse(sum(tmp_brd$BRD_BF12 == "Y")>0, "Y", "N")
        
        BRD_AFT1 <- tmp_brd[BRD_AFT1 == "Y" , .(BRD_AFT1_RUNTIME = sum(SUM_RUNTIME),
                                                BRD_AFT1_RUNCNT  = .N) ]
        BRD_AFT3 <- tmp_brd[BRD_AFT3 == "Y" , .(BRD_AFT3_RUNTIME = sum(SUM_RUNTIME),
                                                BRD_AFT3_RUNCNT  = .N) ]
        BRD_BF1 <- tmp_brd[BRD_BF1 == "Y" , .(BRD_BF1_RUNTIME = sum(SUM_RUNTIME),
                                              BRD_BF1_RUNCNT  = .N) ]
        BRD_BF3 <- tmp_brd[BRD_BF3 == "Y" , .(BRD_BF3_RUNTIME = sum(SUM_RUNTIME),
                                              BRD_BF3_RUNCNT  = .N) ]
        BRD_BF12 <- tmp_brd[BRD_BF12 == "Y" , .(BRD_BF12_RUNTIME = sum(SUM_RUNTIME),
                                                BRD_BF12_RUNCNT  = .N) ]
        
        tmp$BRD_AFT1_RUNTIME <- ifelse(sum(BRD_AFT1$BRD_AFT1_RUNTIME) != 0, BRD_AFT1$BRD_AFT1_RUNTIME, 0)
        tmp$BRD_AFT3_RUNTIME <- ifelse(sum(BRD_AFT1$BRD_AFT3_RUNTIME) != 0, BRD_AFT1$BRD_AFT3_RUNTIME, 0)
        tmp$BRD_BF1_RUNTIME  <- ifelse(sum(BRD_AFT1$BRD_BF1_RUNTIME) != 0, BRD_AFT1$BRD_BF1_RUNTIME, 0)
        tmp$BRD_BF3_RUNTIME  <- ifelse(sum(BRD_AFT1$BRD_BF3_RUNTIME) != 0, BRD_AFT1$BRD_BF3_RUNTIME, 0)
        tmp$BRD_BF12_RUNTIME <- ifelse(sum(BRD_AFT1$BRD_BF12_RUNTIME) != 0, BRD_AFT1$BRD_BF12_RUNTIME, 0)
        
        tmp$BRD_AFT1_RUNCNT <- ifelse(sum(BRD_AFT1$BRD_AFT1_RUNCNT) != 0, BRD_AFT1$BRD_AFT1_RUNCNT, 0)
        tmp$BRD_AFT3_RUNCNT <- ifelse(sum(BRD_AFT1$BRD_AFT3_RUNCNT) != 0, BRD_AFT1$BRD_AFT3_RUNCNT, 0)
        tmp$BRD_BF1_RUNCNT  <- ifelse(sum(BRD_AFT1$BRD_BF1_RUNCNT) != 0, BRD_AFT1$BRD_BF1_RUNCNT, 0)
        tmp$BRD_BF3_RUNCNT  <- ifelse(sum(BRD_AFT1$BRD_BF3_RUNCNT) != 0, BRD_AFT1$BRD_BF3_RUNCNT, 0)
        tmp$BRD_BF12_RUNCNT <- ifelse(sum(BRD_AFT1$BRD_BF12_RUNCNT) != 0, BRD_AFT1$BRD_BF12_RUNCNT, 0)
        
        tmp[,.(
          CUST_NO, ITEM_CD, ITEM_NM,
          BRD_AFT1, BRD_AFT3, BRD_BF1, BRD_BF3, BRD_BF12, BRD_AFT1_RUNTIME, BRD_AFT3_RUNTIME, BRD_BF1_RUNTIME,
          BRD_BF3_RUNTIME, BRD_BF12_RUNTIME, BRD_AFT1_RUNCNT, BRD_AFT3_RUNCNT, BRD_BF1_RUNCNT, BRD_BF3_RUNCNT, BRD_BF12_RUNCNT
        )]
      }, mc.cores = 10))
    ) 
    
    #---# 설명을 위한 데이터 저장 #---#
    save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE6.RData"))
    # load(paste0("/home/kimwh3/DAT/EXP/IMAGE6.RData"))
    
    # ITEM_CD$PRD_CD <- as.character(ITEM_CD$PRD_CD)
    # ITEM_CD <- na.omit(ITEM_CD)
    # setDT(ITEM_CD)
    # RE_LOG_PG_PRD <- merge(RE_LOG_PG_PRD, ITEM_CD , by = c("PRD_CD"), all.x = T)
    
    # system.time(
    #   RE_BRD_CUST_P <- do.call(rbind.fill, mclapply(1:length(ITEM_LIST), function(i) {
    #     
    #     # i <- 1
    #     print(i)
    #     (tmp <- RE_LOG_PG_PRD[ITEM_CD == ITEM_LIST[i]])
    #     #
    #     (tmp_brd <- CALL_SCH_df[ITEM_CD  == ITEM_LIST[i]])
    #     #
    #     (tmp_brd$BRD_AFT1 <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE ) & MIN_Q_START_DATE <  (SYSDATE  + 1 * 60 * 60) , "Y", "N" )) ) 
    #     (tmp_brd$BRD_AFT3 <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE ) & MIN_Q_START_DATE <  (SYSDATE  + 3 * 60 * 60) , "Y", "N" )) ) 
    #     
    #     (tmp_brd$BRD_BF1  <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE  - 1 * 60 * 60) & MIN_Q_START_DATE <  (SYSDATE  ) , "Y", "N" )) )
    #     (tmp_brd$BRD_BF3  <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE  - 3 * 60 * 60) & MIN_Q_START_DATE <  (SYSDATE  ) , "Y", "N" )) ) 
    #     (tmp_brd$BRD_BF12 <- with(tmp_brd, ifelse( MIN_Q_START_DATE >= (SYSDATE  - 12 * 60 * 60) & MIN_Q_START_DATE <  (SYSDATE  ) , "Y", "N" )) ) 
    #     # (tmp_brd)
    #     
    #     tmp$BRD_AFT1 <- ifelse(sum(tmp_brd$BRD_AFT1 == "Y")>0, "Y", "N")
    #     tmp$BRD_AFT3 <- ifelse(sum(tmp_brd$BRD_AFT3 == "Y")>0, "Y", "N")
    #     tmp$BRD_BF1  <- ifelse(sum(tmp_brd$BRD_BF1 == "Y")>0, "Y", "N")
    #     tmp$BRD_BF3  <- ifelse(sum(tmp_brd$BRD_BF3 == "Y")>0, "Y", "N")
    #     tmp$BRD_BF12 <- ifelse(sum(tmp_brd$BRD_BF12 == "Y")>0, "Y", "N")
    #     
    #     BRD_AFT1 <- tmp_brd[BRD_AFT1 == "Y" , .(BRD_AFT1_RUNTIME = sum(SUM_RUNTIME),
    #                                             BRD_AFT1_RUNCNT  = .N) ]
    #     BRD_AFT3 <- tmp_brd[BRD_AFT3 == "Y" , .(BRD_AFT3_RUNTIME = sum(SUM_RUNTIME),
    #                                             BRD_AFT3_RUNCNT  = .N) ]
    #     BRD_BF1 <- tmp_brd[BRD_BF1 == "Y" , .(BRD_BF1_RUNTIME = sum(SUM_RUNTIME),
    #                                           BRD_BF1_RUNCNT  = .N) ]
    #     BRD_BF3 <- tmp_brd[BRD_BF3 == "Y" , .(BRD_BF3_RUNTIME = sum(SUM_RUNTIME),
    #                                           BRD_BF3_RUNCNT  = .N) ]
    #     BRD_BF12 <- tmp_brd[BRD_BF12 == "Y" , .(BRD_BF12_RUNTIME = sum(SUM_RUNTIME),
    #                                             BRD_BF12_RUNCNT  = .N) ]
    #     
    #     tmp$BRD_AFT1_RUNTIME <- ifelse(sum(BRD_AFT1$BRD_AFT1_RUNTIME) != 0, BRD_AFT1$BRD_AFT1_RUNTIME, 0)
    #     tmp$BRD_AFT3_RUNTIME <- ifelse(sum(BRD_AFT1$BRD_AFT3_RUNTIME) != 0, BRD_AFT1$BRD_AFT3_RUNTIME, 0)
    #     tmp$BRD_BF1_RUNTIME  <- ifelse(sum(BRD_AFT1$BRD_BF1_RUNTIME) != 0, BRD_AFT1$BRD_BF1_RUNTIME, 0)
    #     tmp$BRD_BF3_RUNTIME  <- ifelse(sum(BRD_AFT1$BRD_BF3_RUNTIME) != 0, BRD_AFT1$BRD_BF3_RUNTIME, 0)
    #     tmp$BRD_BF12_RUNTIME <- ifelse(sum(BRD_AFT1$BRD_BF12_RUNTIME) != 0, BRD_AFT1$BRD_BF12_RUNTIME, 0)
    #     
    #     tmp$BRD_AFT1_RUNCNT <- ifelse(sum(BRD_AFT1$BRD_AFT1_RUNCNT) != 0, BRD_AFT1$BRD_AFT1_RUNCNT, 0)
    #     tmp$BRD_AFT3_RUNCNT <- ifelse(sum(BRD_AFT1$BRD_AFT3_RUNCNT) != 0, BRD_AFT1$BRD_AFT3_RUNCNT, 0)
    #     tmp$BRD_BF1_RUNCNT  <- ifelse(sum(BRD_AFT1$BRD_BF1_RUNCNT) != 0, BRD_AFT1$BRD_BF1_RUNCNT, 0)
    #     tmp$BRD_BF3_RUNCNT  <- ifelse(sum(BRD_AFT1$BRD_BF3_RUNCNT) != 0, BRD_AFT1$BRD_BF3_RUNCNT, 0)
    #     tmp$BRD_BF12_RUNCNT <- ifelse(sum(BRD_AFT1$BRD_BF12_RUNCNT) != 0, BRD_AFT1$BRD_BF12_RUNCNT, 0)
    #     
    #     tmp[,.(
    #       CUST_NO, PRD_CD, PRD_NM,
    #       BRD_AFT1, BRD_AFT3, BRD_BF1, BRD_BF3, BRD_BF12, BRD_AFT1_RUNTIME, BRD_AFT3_RUNTIME, BRD_BF1_RUNTIME,
    #       BRD_BF3_RUNTIME, BRD_BF12_RUNTIME, BRD_AFT1_RUNCNT, BRD_AFT3_RUNCNT, BRD_BF1_RUNCNT, BRD_BF3_RUNCNT, BRD_BF12_RUNCNT
    #     )]
    #   }, mc.cores = 10))
    # )
    
    
    RE_LOG_PG_ITEM <- merge(RE_LOG_PG_ITEM, RE_BRD_CUST_I , by = c("CUST_NO", "ITEM_CD", "ITEM_NM"), all.x= T)
    
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_AFT1 := ifelse(!is.na(BRD_AFT1), BRD_AFT1, "N")] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_AFT3 := ifelse(!is.na(BRD_AFT3), BRD_AFT3, "N")] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF1 := ifelse(!is.na(BRD_BF1), BRD_BF1, "N")] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF3 := ifelse(!is.na(BRD_BF3), BRD_BF3, "N")] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF12 := ifelse(!is.na(BRD_BF12), BRD_BF12, "N")] )
    
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_AFT1_RUNTIME := ifelse(!is.na(BRD_AFT1_RUNTIME), BRD_AFT1_RUNTIME, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_AFT3_RUNTIME := ifelse(!is.na(BRD_AFT3_RUNTIME), BRD_AFT3_RUNTIME, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF1_RUNTIME := ifelse(!is.na(BRD_BF1_RUNTIME), BRD_BF1_RUNTIME, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF3_RUNTIME := ifelse(!is.na(BRD_BF3_RUNTIME), BRD_BF3_RUNTIME, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF12_RUNTIME := ifelse(!is.na(BRD_BF12_RUNTIME), BRD_BF12_RUNTIME, 0)] )
    
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_AFT1_RUNCNT := ifelse(!is.na(BRD_AFT1_RUNCNT), BRD_AFT1_RUNCNT, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_AFT3_RUNCNT := ifelse(!is.na(BRD_AFT3_RUNCNT), BRD_AFT3_RUNCNT, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF1_RUNCNT := ifelse(!is.na(BRD_BF1_RUNCNT), BRD_BF1_RUNCNT, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF3_RUNCNT := ifelse(!is.na(BRD_BF3_RUNCNT), BRD_BF3_RUNCNT, 0)] )
    system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , BRD_BF12_RUNCNT := ifelse(!is.na(BRD_BF12_RUNCNT), BRD_BF12_RUNCNT, 0)] )
    
    
    
    # RE_LOG_PG_PRD <- merge(RE_LOG_PG_PRD, RE_BRD_CUST_P , by = c("CUST_NO", "PRD_CD", "PRD_NM"), all.x= T)
    # 
    # RE_LOG_PG_PRD$BRD_AFT1 <- ifelse(!is.na(RE_LOG_PG_PRD$BRD_AFT1), RE_LOG_PG_PRD$BRD_AFT1, "N")
    # RE_LOG_PG_PRD$BRD_AFT3 <- ifelse(!is.na(RE_LOG_PG_PRD$BRD_AFT3 ), RE_LOG_PG_PRD$BRD_AFT3, "N")
    # RE_LOG_PG_PRD$BRD_BF1  <- ifelse(!is.na(RE_LOG_PG_PRD$BRD_BF1), RE_LOG_PG_PRD$BRD_BF1, "N")
    # RE_LOG_PG_PRD$BRD_BF3  <- ifelse(!is.na(RE_LOG_PG_PRD$BRD_BF3), RE_LOG_PG_PRD$BRD_BF3, "N")
    # RE_LOG_PG_PRD$BRD_BF12 <- ifelse(!is.na(RE_LOG_PG_PRD$BRD_BF12), RE_LOG_PG_PRD$BRD_BF12, "N")
    # 
    # RE_LOG_PG_PRD$BRD_AFT1_RUNTIME <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_AFT1_RUNTIME), BRD_AFT1_RUNTIME, 0))
    # RE_LOG_PG_PRD$BRD_AFT3_RUNTIME <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_AFT3_RUNTIME), BRD_AFT3_RUNTIME, 0))
    # RE_LOG_PG_PRD$BRD_BF1_RUNTIME  <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_BF1_RUNTIME), BRD_BF1_RUNTIME, 0))
    # RE_LOG_PG_PRD$BRD_BF3_RUNTIME  <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_BF3_RUNTIME), BRD_BF3_RUNTIME, 0))
    # RE_LOG_PG_PRD$BRD_BF12_RUNTIME <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_BF12_RUNTIME), BRD_BF12_RUNTIME, 0))
    # 
    # RE_LOG_PG_PRD$BRD_AFT1_RUNCNT <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_AFT1_RUNCNT), BRD_AFT1_RUNCNT, 0))
    # RE_LOG_PG_PRD$BRD_AFT3_RUNCNT <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_AFT3_RUNCNT), BRD_AFT3_RUNCNT, 0))
    # RE_LOG_PG_PRD$BRD_BF1_RUNCNT  <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_BF1_RUNCNT), BRD_BF1_RUNCNT, 0))
    # RE_LOG_PG_PRD$BRD_BF3_RUNCNT  <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_BF3_RUNCNT), BRD_BF3_RUNCNT, 0))
    # RE_LOG_PG_PRD$BRD_BF12_RUNCNT <- with(RE_LOG_PG_PRD, ifelse(!is.na(BRD_BF12_RUNCNT), BRD_BF12_RUNCNT, 0))
    
    # 5. 방송 정보 ####
    #----------------------------------------------------------------#
    print(paste0(" 5. 방송 데이터 가공완료   // 시간 : ", Sys.time())) 
    
    #----------------------------------------------------------------#
    # 6. 시간, 날짜 ####
    #----------------------------------------------------------------#
    print(paste0(" 6. 날짜 데이터 호출   // 시간 : ", Sys.time())) 
    head(HOLDY_DT)
    setDT(HOLDY_DT)
    HOLDY_DT <- HOLDY_DT[ymd(DT_CD) == TODAY_DT, .(WEKDAY_NM, HOLDY_NM, HOLDY_YN, FESTA_YN, SPC_HOLDY_YN)]
    
    RE_LOG_PG_ITEM <- cbind(RE_LOG_PG_ITEM, HOLDY_DT)
    RE_LOG_PG_ITEM$TIME_HOUR <- substr(SYSDATE+1, 12,13)
    
    # RE_LOG_PG_PRD <- cbind(RE_LOG_PG_PRD, HOLDY_DT)
    # RE_LOG_PG_PRD$TIME_HOUR <- substr(SYSDATE+1, 12,13)
    
    # 6. 시간, 날짜 ####
    #----------------------------------------------------------------#
    
    # 
    RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM %>% dplyr::select(-ORD_DT, -주문순서, -id)
    # RE_LOG_PG_PRD  <- RE_LOG_PG_PRD %>% dplyr::select(-ORD_DT, -주문순서, -id)
    
    print(paste0(" 7. 최종 데이터 적재시작   // 시간 : ", Sys.time())) 
    
    # saveRDS(RE_LOG_PG_ITEM, file = paste0("/home/kimwh3/DAT/REORD/ZMART_DATA_REAL/","ITEM_", SYSDATE ,".RDS"))
    # saveRDS(RE_LOG_PG_PRD, file = paste0("/home/kimwh3/DAT/REORD/ZMART_DATA_REAL/PRD/","PRD_", SYSDATE ,".RDS"))
    #----------------------------------------------------------------#
    print(paste0(" 7. 최종 데이터 적재완료   // 시간 : ", Sys.time())) 
    #----------------------------------------------------------------#
    print("아이템 디멘션")
    print(table(RE_LOG_PG_ITEM$구매여부))
    # print("상품 디멘션")
    # print(table(RE_LOG_PG_PRD$구매여부))
    
    #---# 설명을 위한 데이터 저장 #---#
    save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE7.RData"))
    # load(paste0("/home/kimwh3/DAT/EXP/IMAGE7.RData"))
    
    
    # rm(list = ls()[-which(ls() %in% c("RE_LOG_PG_ITEM","RE_LOG_PG_PRD","SYSDATE", "SYSDATE1", "TODAY_DT") )])
    rm(list = ls()[-which(ls() %in% c("RE_LOG_PG_ITEM","SYSDATE", "SYSDATE1", "TODAY_DT") )])
    
    ls()
    
    gc(T);gc(T);gc(T);gc(T);gc(T);
    break
  }}   

gc(T);gc(T);gc(T);

setDT(RE_LOG_PG_ITEM)
setkeyv(RE_LOG_PG_ITEM, c("CUST_NO"))
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, PRD_GRP_NM_직전1 := ifelse(is.na(PRD_GRP_NM_직전1), PRD_GRP_NM_직전1 , PRD_GRP_NM_직전1 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, PRD_GRP_NM_직전2 := ifelse(is.na(PRD_GRP_NM_직전2), PRD_GRP_NM_직전2 , PRD_GRP_NM_직전2 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, PRD_CLS_NM_직전1 := ifelse(is.na(PRD_CLS_NM_직전1), PRD_CLS_NM_직전1 , PRD_CLS_NM_직전1 ), by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, PRD_CLS_NM_직전2 := ifelse(is.na(PRD_CLS_NM_직전2), PRD_CLS_NM_직전2 , PRD_CLS_NM_직전2 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, PRICE_직전1 := ifelse(is.na(PRICE_직전1), PRICE_직전1 , PRICE_직전1 ), by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, PRICE_직전2 := ifelse(is.na(PRICE_직전2), PRICE_직전2 , PRICE_직전2 ) , by = c("CUST_NO")])

system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 선호요일 := ifelse(is.na(선호요일), 선호요일 , 선호요일 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 선호쇼핑타임 := ifelse(is.na(선호쇼핑타임), 선호쇼핑타임 , 선호쇼핑타임 ) , by =c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 평균소비가격대 := ifelse(is.na(평균소비가격대), 평균소비가격대 , 평균소비가격대 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 선호아이템명 := ifelse(is.na(선호아이템명), 선호아이템명 , 선호아이템명 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 선호상품군명 := ifelse(is.na(선호상품군명), 선호상품군명 , 선호상품군명 ), by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 아이템주문다양성 := ifelse(is.na(아이템주문다양성), 아이템주문다양성 , 아이템주문다양성 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 상품군주문다양성 := ifelse(is.na(상품군주문다양성), 상품군주문다양성 , 상품군주문다양성 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 선호CAEC소싱 := ifelse(is.na(선호CAEC소싱), 선호CAEC소싱 , 선호CAEC소싱 ) , by = c("CUST_NO")])


system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 선호주문시점 := ifelse(is.na(선호주문시점), 선호주문시점 , 선호주문시점 ) , by = c("CUST_NO")])

system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 최근일주일주문건수 := ifelse(is.na(최근일주일주문건수), 최근일주일주문건수 , 최근일주일주문건수 ), by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 최근한달주문건수   := ifelse(is.na(최근한달주문건수), 최근한달주문건수 , 최근한달주문건수 ) , by = c("CUST_NO")])
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 최근세달주문건수   := ifelse(is.na(최근세달주문건수), 최근세달주문건수 , 최근세달주문건수 ) , by = c("CUST_NO")])

system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 최근세달평균구매주기   := ifelse(is.na(최근세달평균구매주기), 최근세달평균구매주기 , 최근세달평균구매주기 ), by = c("CUST_NO")])


#----------------------------------------------------------------#
# 8. 모델링 적용 시작 
#----------------------------------------------------------------#

#----------------------------------------------------------------#
# 8.1 기존데이터 및 모델링 호출 및 변수리스트 ####
#----------------------------------------------------------------#

# 기존 데이터 호출 
# saveRDS(RE_IMART2, file = paste0("/home/kimwh3/DAT/REORD/ZMDL/","ITEM_FINAL",".RDS"))
RE_IMART0 <- readRDS(file = paste0("/home/kimwh3/DAT/REORD/ZMDL/","ITEM_FINAL", ".RDS"))
setDT(RE_IMART0)
setkeyv(RE_IMART0, c("CUST_NO","ITEM_CD", "ITEM_NM"))
# 저장된 모델 호출 #
# save(xgbFitl, file = paste0("/home/kimwh3/DAT/REORD/ZMDL/XGB_MDLI_FINAL.RData") )
load(file = paste0("/home/kimwh3/DAT/REORD/ZMDL/XGB_MDLI_FINAL.RData") )

# names_df 변수리스트 #
names_df <- c("PRD_GRP_NM", "PRD_CLS_NM", 
              "최근첫탐색시간I", "최근2탐색시간I",  "최근3탐색시간I", 
              "최근첫탐색I",     "최근1일탐색I",    "최근3일탐색I",    "최근7일탐색I",    "현재일차이","현재일차이2","중앙값차이","주기그룹", 
              "Q1G", "Q1", "P40", "중앙값" , "P60" ,  "Q3",
              
              "CHANL_GBN", "ORD_THETIME_AGE_VAL", "GENDR_CD",
              
              "EC_CUST_GRD_CD",  "주문건수",  "구매일차이","주기_평균", "주기_중앙값",     "이전등급", 
              "이전주문시점",    "이전주문채널",   "이전주문채널2" ,  "구매일차이_직전1","구매일차이_직전2", 
              "PRD_GRP_NM_직전1", "PRD_GRP_NM_직전2","PRD_CLS_NM_직전1" ,"PRD_CLS_NM_직전2", "PRICE_직전1" , "PRICE_직전2",
              
              "WEK_G" , "선호요일",  "주문시간G"  ,"선호쇼핑타임","평균소비가격대", "선호상품군명",  "상품군주문다양성", "아이템주문다양성", 
              "선호CAEC소싱", "주문시점", "선호주문시점", "최근일주일주문건수", "최근한달주문건수" , "최근세달주문건수", "최근세달평균구매주기", 
              
              "탐색여부",  #"구매이력여부",    
              
              "최근첫검색시간I", "최근2검색시간I", "최근3검색시간I", "최근첫검색I",     "최근1일검색I",    "최근3일검색I",    "최근7일검색I",
              "검색여부",
              "BRD_AFT1",  "BRD_AFT3",  "BRD_BF1","BRD_BF3",   "BRD_BF12", 
              "BRD_AFT1_RUNTIME","BRD_AFT3_RUNTIME","BRD_BF1_RUNTIME", "BRD_BF3_RUNTIME", "BRD_BF12_RUNTIME","BRD_AFT1_RUNCNT",
              "BRD_AFT3_RUNCNT", "BRD_BF1_RUNCNT",  "BRD_BF3_RUNCNT",  "BRD_BF12_RUNCNT", 
              "WEKDAY_NM",  
              # "HOLDY_YN",                                                     # "HOLDY_NM",  "FESTA_YN",  "SPC_HOLDY_YN",    
              "TIME_HOUR",   "주문건수구분",  "GENDR_CD",  
              "최근일주일주문건수G", "최근한달주문건수G" , "최근세달주문건수G", "최근세달평균구매주기G", 
              "선호아이템명동일", "주기유무",
              
              "현재_주기차이A",  "현재_주기차이M",  "구매_주기차이A",  "구매_주기차이M",  "중앙_주기차이A",  "중앙_주기차이M" ,
              "탐색주기시간평균", "검색주기시간평균",  "탐색일주일평균",    "검색일주일평균" , "현재일그룹"
)
# 변수리스트 #


RE_LOG_PG_ITEM$SYSDATE <- SYSDATE
RE_IMART <- RE_LOG_PG_ITEM
rm(RE_LOG_PG_ITEM); gc(T); gc(T); gc(T) ; gc(T)
## 적재 시작
tmp2 <- RE_IMART[,c("CUST_NO","ITEM_CD")] %>% distinct()
# tmp3 <- RE_IMARTT[,c("CUST_NO","ITEM_CD")] %>% distinct()
# tmp2 <- rbind.fill(tmp2, tmp3) ; rm(tmp3)

seq_df <- c(seq(1,nrow(tmp2), by = 1000000), nrow(tmp2))
# DB접근
# source(file = "/home/kimwh3/DB_CON.R")
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")

# for( i in 1:(length(seq_df) -1)) {
#   print(paste0(i, " ", seq_df[(i+1)], " // ", nrow(tmp2)))
#   gc(T)
#   tmp <- tmp2[seq_df[i]:seq_df[(i+1)], ]
#   if ( i == 1 ) {
#     dbWriteTable(conn, "ITEM_TMP001", tmp, row.names = FALSE)
#   } else {
#     dbWriteTable(conn, "ITEM_TMP002", tmp, row.names = FALSE)
#     dbSendUpdate(conn, paste0("
#                               INSERT INTO DTA_OWN.ITEM_TMP001 A (
#                               A.CUST_NO, A.ITEM_CD
#                               )
#                               SELECT B.CUST_NO, B.ITEM_CD
#                               FROM DTA_OWN.ITEM_TMP002 B
#                               "))
#     dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.ITEM_TMP002 PURGE"))
#   }
# }
# dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.ITEM_TMP001 TO DTA_APP"))

# 상품아이템코드 #
GRP_df <- dbGetQuery(conn, paste0("SELECT DISTINCT 
                                   B.ITEM_CD
                                   , ITEM.ITEM_NM
                                   
                                   , ITEM.PRD_GRP_CD
                                   , PRD_GRP.PRD_GRP_NM
                                   , ITEM.PRD_CLS_CD
                                   
                                   , CLS1.PRD_CLS_NM
                                   , CLS1.PRD_SML_CLS_CD
                                   , CLS2.PRD_SML_CLS_NM
                                   , CLS3.PRD_MID_CLS_CD
                                   , CLS3.PRD_MID_CLS_NM
                                   , CLS4.PRD_LRG_CLS_CD
                                   , CLS4.PRD_LRG_CLS_NM
                                   
                                   
                                   FROM DTA_OWN.ITEM_TMP001 B
                                   LEFT JOIN GSBI_OWN.D_PRD_ITEM_M ITEM         ON TO_CHAR(B.ITEM_CD) = TO_CHAR(ITEM.ITEM_CD)
                                   --LEFT JOIN GSBI_OWN.D_PRD_BRAND_M BRA         ON PRD.BRAND_CD = BRA.BRAND_CD
                                   LEFT JOIN GSBI_OWN.V_CMM_PRD_GRP_C PRD_GRP   ON ITEM.PRD_GRP_CD = PRD_GRP.PRD_GRP_CD
                                   LEFT JOIN GSBI_OWN.D_PRD_PRD_CLS_M CLS1      ON ITEM.PRD_CLS_CD = CLS1.PRD_CLS_CD
                                   LEFT JOIN GSBI_OWN.D_PRD_PRD_SML_CLS_M CLS2  ON CLS1.PRD_SML_CLS_CD = CLS2.PRD_SML_CLS_CD
                                   LEFT JOIN GSBI_OWN.D_PRD_PRD_MID_CLS_M CLS3  ON CLS2.PRD_MID_CLS_CD = CLS3.PRD_MID_CLS_CD
                                   LEFT JOIN GSBI_OWN.D_PRD_PRD_LRG_CLS_M CLS4  ON CLS3.PRD_LRG_CLS_CD = CLS4.PRD_LRG_CLS_CD
                                   "))

# 성별 RE_GEN #
RE_GEN <- dbGetQuery(conn, paste0(
  "
   SELECT /*+ FULL (A) PARALLEL (A 4) */ A.CUST_NO 
   , CASE WHEN GENDR IN ('1','3','5','7') THEN 'M' 
   WHEN GENDR IN ('2','4','6','8') THEN 'F' ELSE 'ETC' END AS GENDR_CD  
   , AGE
   FROM  DHUB_OWN.CST_CUST_M A
   WHERE A.CUST_NO IN (SELECT B.CUST_NO FROM DTA_OWN.ITEM_TMP001 B)
   "
)) 
# 성별 #

# RE_ORD_MAXDT <- dbGetQuery(conn, paste0(
#   "
#   SELECT /*+ FULL (A) PARALLEL (A 4) */  A.CUST_NO, PRD.ITEM_CD, MAX(A.ORD_DT) AS MAX_ORD_DT
#   FROM GSBI_OWN.F_ORD_ORD_D A 
#   LEFT JOIN GSBI_OWN.D_PRD_PRD_M PRD ON A.PRD_CD = PRD.PRD_CD 
#   WHERE A.CUST_NO IN (SELECT B.CUST_NO FROM DTA_OWN.ITEM_TMP001 B)
#   AND A.ORD_DT >= TO_CHAR(sysdate - 553, 'YYYYMMDD')
#   GROUP BY A.CUST_NO, PRD.ITEM_CD
#   "
# )) 

# source(file = "/home/kimwh3/DB_CON.R")
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")

dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.ITEM_TMP001 PURGE"))

dbDisconnect(conn)
rm(conn,drv)
# source(file = "/home/kimwh3/DB_CON.R")
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")

PERIOD_df <- dbGetQuery(conn, paste0(" SELECT DISTINCT * FROM DTA_OWN.RE_TMP_TB WHERE 주기생성일 = (SELECT max(주기생성일) FROM DTA_OWN.RE_TMP_TB)
                                      UNION ALL
                                      SELECT DISTINCT * FROM DTA_OWN.RE_HAIR_TB WHERE 주기생성일 = (SELECT max(주기생성일) FROM DTA_OWN.RE_HAIR_TB)
                                      UNION ALL
                                      SELECT DISTINCT * FROM DTA_OWN.RE_CLTH_TB WHERE 주기생성일 = (SELECT max(주기생성일) FROM DTA_OWN.RE_CLTH_TB)"))




# 접속 종료 
dbDisconnect(conn)
rm(conn,drv)
rm(tmp,tmp2)
gc(T); gc(T) 

setDT(RE_IMART) 
setkeyv(RE_IMART, c("CUST_NO","ITEM_CD", "ITEM_NM"))


#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE8.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE8.RData"))

#----------------------------------------------------------------#
# 8.1 기존데이터 및 모델링 호출 및 변수리스트 ####
#----------------------------------------------------------------#
gc(T);gc(T);gc(T);gc(T)
#----------------------------------------------------------------#
# 8.2 데이터 가공 ####
#----------------------------------------------------------------#

# 아이템 단위 #
setDT(RE_GEN)
setDT(GRP_df)
setDT(PERIOD_df)
# setDT(RE_ORD_MAXDT)

setkeyv(RE_GEN, c("CUST_NO"))
setkeyv(GRP_df, c("ITEM_CD",  "ITEM_NM"))
# setkeyv(RE_ORD_MAXDT, c("CUST_NO",  "ITEM_CD"))

RE_IMART$CUST_NO <- as.numeric(RE_IMART$CUST_NO)
# RE_IMART$ITEM_CD <- as.numeric(RE_IMART$ITEM_CD)
# 성별, 연령 붙이기 
RE_IMART <- merge(RE_IMART, RE_GEN, by = "CUST_NO", all.x= T)
RE_IMART <- RE_IMART[ , ORD_THETIME_AGE_VAL := ifelse(is.na(ORD_THETIME_AGE_VAL), AGE,   ORD_THETIME_AGE_VAL )]
RE_IMART <- RE_IMART[ , ORD_THETIME_AGE_VAL := ifelse(ORD_THETIME_AGE_VAL > 70, 70,
                                                      ifelse(ORD_THETIME_AGE_VAL< 15 , 15,   ORD_THETIME_AGE_VAL ))]
rm(RE_GEN); gc(T); gc(T)
# RE_ORD_MAXDT$ITEM_CD <- as.character(RE_ORD_MAXDT$ITEM_CD)
# RE_ORD_MAXDT <- RE_ORD_MAXDT[ , MAX_ORD_DT := round(as.numeric(difftime(ymd(substr(as.character(Sys.time()), 1, 10)), ymd(MAX_ORD_DT), tz, units = c( "days"))))]
# 
# dim(RE_IMART)
# RE_IMART <- merge(RE_IMART, RE_ORD_MAXDT, by = c("CUST_NO","ITEM_CD"), all.x= T)
# dim(RE_IMART)
#  
# RE_IMART <- RE_IMART[ , 현재일차이 := ifelse(주기그룹 == "신규" , MAX_ORD_DT,   현재일차이 )]
# system.time(RE_IMART <- RE_IMART[ , 현재일차이 := ifelse(is.na(현재일차이), 553 , 현재일차이)] )
# RE_IMART <- RE_IMART[ , MAX_ORD_DT := NULL]

# 상품에 대한 카테고리 정보 붙이기 
RE_IMART2 <- RE_IMART[is.na(PRD_GRP_NM)]
RE_IMART2 <- RE_IMART2[ , c("PRD_GRP_NM", "PRD_CLS_NM", "Q1", "P40", "중앙값", "P60", "Q3"):= NULL]
PERIOD_df2 <- PERIOD_df[구분1 == 'ENTPR_PRD_GRP_CD'][,.(VAR_NM,  Q1, P40, 중앙값, 평균, P60,  Q3 )]

dim(RE_IMART2)
RE_IMART2 <- merge(RE_IMART2, GRP_df[,.(ITEM_CD,ITEM_NM, PRD_GRP_NM, PRD_CLS_NM)], by = c("ITEM_CD", "ITEM_NM"), all.x= T)
dim(RE_IMART2)
rm(GRP_df); gc(T); gc(T)
dim(RE_IMART2)
RE_IMART2 <- merge(RE_IMART2, PERIOD_df2, by.x = c("PRD_GRP_NM"), by.y = "VAR_NM", all.x= T)
dim(RE_IMART2)
rm(PERIOD_df2)


RE_IMART2 <- rbind.fill(RE_IMART0, RE_IMART[!is.na(PRD_GRP_NM)], RE_IMART2)
rm(RE_IMART);  gc(T); gc(T); gc(T); gc(T) # rm(RE_ORD_MAXDT);

setDT(RE_IMART2); setkeyv(RE_IMART2,c("CUST_NO","ITEM_CD", "ITEM_NM"))

# 8.2 데이터 가공 ####
#----------------------------------------------------------------#
gc(T);gc(T);gc(T);gc(T)
#----------------------------------------------------------------#

#----------------------------------------------------------------#
# 8.3 데이터 가공2 ####
system.time(RE_IMART2 <- RE_IMART2[ , 구매일차이_직전1 := ifelse(is.na(구매일차이_직전1), 0, 구매일차이_직전1)] )
system.time(RE_IMART2 <- RE_IMART2[ , 구매일차이_직전2 := ifelse(is.na(구매일차이_직전2), 0, 구매일차이_직전2)] )

system.time(RE_IMART2 <- RE_IMART2[ , PRD_GRP_NM_직전1 := ifelse(is.na(PRD_GRP_NM_직전1), "미생성", PRD_GRP_NM_직전1)] )
system.time(RE_IMART2 <- RE_IMART2[ , PRD_GRP_NM_직전2 := ifelse(is.na(PRD_GRP_NM_직전2), "미생성", PRD_GRP_NM_직전2)] )

system.time(RE_IMART2 <- RE_IMART2[ , PRD_GRP_NM := ifelse(is.na(PRD_GRP_NM), "미생성", PRD_GRP_NM)] )
system.time(RE_IMART2 <- RE_IMART2[ , PRD_CLS_NM := ifelse(is.na(PRD_CLS_NM), "미생성", PRD_CLS_NM)] )

system.time(RE_IMART2 <- RE_IMART2[ , PRD_CLS_NM_직전1 := ifelse(is.na(PRD_CLS_NM_직전1), "미생성", PRD_CLS_NM_직전1)] )
system.time(RE_IMART2 <- RE_IMART2[ , PRD_CLS_NM_직전2 := ifelse(is.na(PRD_CLS_NM_직전2), "미생성", PRD_CLS_NM_직전2)] )

system.time(RE_IMART2 <- RE_IMART2[ , PRICE_직전1 := ifelse(is.na(PRICE_직전1),  0, PRICE_직전1)] )
system.time(RE_IMART2 <- RE_IMART2[ , PRICE_직전2 := ifelse(is.na(PRICE_직전2),  0, PRICE_직전2)] )


system.time(RE_IMART2 <- RE_IMART2[ , WEK_G := ifelse(is.na(WEK_G),  "미생성", WEK_G)] )
system.time(RE_IMART2 <- RE_IMART2[ , 주문시간G := ifelse(is.na(주문시간G),  "미생성", 주문시간G)] )
system.time(RE_IMART2 <- RE_IMART2[ , 선호요일 := ifelse(is.na(선호요일),  "미생성", 선호요일)] )
system.time(RE_IMART2 <- RE_IMART2[ , 선호쇼핑타임 := ifelse(is.na(선호쇼핑타임),  "미생성", 선호쇼핑타임)] )
system.time(RE_IMART2 <- RE_IMART2[ , 평균소비가격대 := ifelse(is.na(평균소비가격대),  0, 평균소비가격대)] )
system.time(RE_IMART2 <- RE_IMART2[ , 선호상품군명 := ifelse(is.na(선호상품군명),  "미생성", 선호상품군명)] )

system.time(RE_IMART2 <- RE_IMART2[ , 상품군주문다양성 := ifelse(is.na(상품군주문다양성),  "미생성", 상품군주문다양성)] )
system.time(RE_IMART2 <- RE_IMART2[ , 선호CAEC소싱 := ifelse(is.na(선호CAEC소싱),  "미생성", 선호CAEC소싱)] )

system.time(RE_IMART2 <- RE_IMART2[ , 주문시점 := ifelse(is.na(주문시점),  "미생성", 주문시점)] )
system.time(RE_IMART2 <- RE_IMART2[ , 선호주문시점 := ifelse(is.na(선호주문시점),  "미생성", 선호주문시점)] )

system.time(RE_IMART2 <- RE_IMART2[ , 이전주문채널 := ifelse(is.na(이전주문채널),  "미생성", 이전주문채널)] )
system.time(RE_IMART2 <- RE_IMART2[ , 이전주문채널2 := ifelse(is.na(이전주문채널2),  "미생성", 이전주문채널2)] )

system.time(RE_IMART2 <- RE_IMART2[ , 최근일주일주문건수G := ifelse(is.na(최근일주일주문건수),  "Y" , "N")] )
system.time(RE_IMART2 <- RE_IMART2[ , 최근일주일주문건수 := ifelse(is.na(최근일주일주문건수), 0 , 최근일주일주문건수)] )

system.time(RE_IMART2 <- RE_IMART2[ , 최근한달주문건수G := ifelse(is.na(최근한달주문건수),  "Y" , "N")] )
system.time(RE_IMART2 <- RE_IMART2[ , 최근한달주문건수 := ifelse(is.na(최근한달주문건수), 0 , 최근한달주문건수)] )
system.time(RE_IMART2 <- RE_IMART2[ , 최근세달주문건수G := ifelse(is.na(최근세달주문건수),  "Y" , "N")] )
system.time(RE_IMART2 <- RE_IMART2[ , 최근세달주문건수 := ifelse(is.na(최근세달주문건수), 0 , 최근세달주문건수)] )
system.time(RE_IMART2 <- RE_IMART2[ , 최근세달평균구매주기G := ifelse(is.na(최근세달평균구매주기),  "Y" , "N")] )
system.time(RE_IMART2 <- RE_IMART2[ , 최근세달평균구매주기 := ifelse(is.na(최근세달평균구매주기), 0 , 최근세달평균구매주기)] )

system.time(RE_IMART2 <- RE_IMART2[ , 선호아이템명 := ifelse(is.na(선호아이템명),  "미생성", 선호아이템명)] )
system.time(RE_IMART2 <- RE_IMART2[ , 선호아이템명동일 := ifelse(선호아이템명 == ITEM_NM, "0.동일" , "1.다름")] )
system.time(RE_IMART2 <- RE_IMART2[ , 아이템주문다양성 := ifelse(is.na(아이템주문다양성), 0 , 아이템주문다양성)] )

system.time(RE_IMART2 <- RE_IMART2[ , 주기유무 := ifelse(is.na(Q1), "N", "Y")] )
system.time(RE_IMART2 <- RE_IMART2[ , Q1 := ifelse(is.na(Q1), 40 , Q1)] )
system.time(RE_IMART2 <- RE_IMART2[ , P40 := ifelse(is.na(P40), 70 , P40)] )
system.time(RE_IMART2 <- RE_IMART2[ , 중앙값 := ifelse(is.na(중앙값), 90 , 중앙값)] )
system.time(RE_IMART2 <- RE_IMART2[ , P60 := ifelse(is.na(P60), 120 , P60)] )
system.time(RE_IMART2 <- RE_IMART2[ , Q3 := ifelse(is.na(Q3), 170 , Q3)] )



setDT(RE_IMART2)
setkeyv(RE_IMART2, c("CUST_NO", "ITEM_CD", "ITEM_NM"))

# RE_IMART2$아이템주문다양성
# apply(RE_IMART2,2, function(x) sum(is.na(x)))
RE_IMART2 <- RE_IMART2[!is.na(선호아이템명동일)]
# RE_IMART2[is.na(Q1)]

system.time(RE_IMART2 <- RE_IMART2[ , 주문건수구분 := ifelse(주문건수 == 0, "0",
                                                           ifelse(주문건수 == 1, "1",
                                                                      ifelse(주문건수 ==2, "2",
                                                                                 ifelse(주문건수 ==3, "3",
                                                                                            ifelse(주문건수 ==4, "4", 
                                                                                                       ifelse(주문건수 ==5, "5", "5up" ))))))] )


#
RE_IMART2 <- RE_IMART2[ , 현재_주기차이A := (현재일차이 - 주기_평균)]
RE_IMART2 <- RE_IMART2[ , 현재_주기차이M := (현재일차이 - 주기_중앙값)]
#
RE_IMART2 <- RE_IMART2[ , 구매_주기차이A := (구매일차이 - 주기_평균)]
RE_IMART2 <- RE_IMART2[ , 구매_주기차이M := (구매일차이 - 주기_중앙값)]
#
RE_IMART2 <- RE_IMART2[ , 중앙_주기차이A := (중앙값차이 - 주기_평균)]
RE_IMART2 <- RE_IMART2[ , 중앙_주기차이M := (중앙값차이 - 주기_중앙값)]
#

RE_IMART2 <- RE_IMART2[ , HOLDY_NM := ifelse(is.na(HOLDY_NM), "일반",   HOLDY_NM )]
RE_IMART2 <- RE_IMART2[ , 구매여부 := ifelse(구매여부 == "Y", 1,   0 )]

RE_IMART2 <- RE_IMART2[ , 탐색주기시간평균 := round(((최근첫탐색시간I - 최근2탐색시간I) + (최근2탐색시간I - 최근3탐색시간I))/2 )   ]
RE_IMART2 <- RE_IMART2[ , 검색주기시간평균 := round(((최근첫검색시간I - 최근2검색시간I) + (최근2검색시간I - 최근3검색시간I))/2 )   ]
RE_IMART2 <- RE_IMART2[ , 탐색일주일평균 := round(최근7일탐색I/7 )   ]
RE_IMART2 <- RE_IMART2[ , 검색일주일평균 := round(최근7일검색I/7 )   ]

RE_IMART2 <- RE_IMART2[ , 현재일그룹 := ifelse(현재일차이 == 0, "0",
                                               ifelse(현재일차이 <= 1, "1",
                                                           ifelse(현재일차이 <=3, "3",
                                                                       ifelse(현재일차이 <=5, "5",
                                                                                   ifelse(현재일차이 <=7, "7", 
                                                                                               ifelse(현재일차이 <=10, "10", "10up" ))))))   ]

RE_IMART2 <- RE_IMART2[ , Q1G := ifelse(Q1 == 0, "0",
                                        ifelse(Q1 <= 3, "3",
                                               ifelse(Q1 <=7, "7",
                                                      ifelse(Q1 <=15, "15",
                                                             ifelse(Q1 <=30, "30", 
                                                                    ifelse(Q1 <=50, "50", 
                                                                           ifelse(Q1 <=90, "90", "90up" )))))))   ]

RE_IMART2 <- RE_IMART2[ , ORD_THETIME_AGE_VAL2 := ORD_THETIME_AGE_VAL^2   ]
RE_IMART2 <- RE_IMART2[ , 현재일차이2 := 현재일차이^2   ]
RE_IMART2 <- RE_IMART2[ , 평균소비가격대 := ifelse(평균소비가격대 >= 100000,  100000, 평균소비가격대)   ]

RE_IMART2 <- RE_IMART2[RE_IMART2$최근첫탐색시간I >= 0]
RE_IMART2 <- RE_IMART2[RE_IMART2$최근첫검색시간I >= 0]
# RE_IMART2 <- RE_IMART2[RE_IMART2$주기_평균 >= 0]
# RE_IMART2 <- RE_IMART2[RE_IMART2$주기_중앙값 >= 0]
# RE_IMART2 <- RE_IMART2[RE_IMART2$구매일차이_직전1 >= 0]
# RE_IMART2 <- RE_IMART2[RE_IMART2$구매일차이_직전2 >= 0]
# 
# SYSDATE2 <-SYSDATE
# system.time(train_df <- RE_IMART2[!SYSDATE == SYSDATE2, ])
# gc(T);gc(T)
# system.time(test_df  <- RE_IMART2[SYSDATE >= SYSDATE2, ])
# SYSDATE <-SYSDATE2; rm(SYSDATE2)
# gc(T);gc(T)
# 
# test_df <- test_df[최근첫탐색시간I >= 0]
# test_df <- test_df[최근첫검색시간I >= 0]
# test_df <- test_df[주기_평균 >= 0]
# test_df <- test_df[주기_중앙값 >= 0]
# test_df <- test_df[구매일차이_직전1 >= 0]
# test_df <- test_df[구매일차이_직전2 >= 0]

RE_IMART2 <- RE_IMART2[ , 주기_평균 := ifelse(주기_평균 < 0, 0,   주기_평균 )]
RE_IMART2 <- RE_IMART2[ , 주기_중앙값 := ifelse(주기_중앙값 < 0, 0,   주기_중앙값 )]
RE_IMART2 <- RE_IMART2[ , 구매일차이_직전1 := ifelse(구매일차이_직전1 < 0, 0,   구매일차이_직전1 )]
RE_IMART2 <- RE_IMART2[ , 구매일차이_직전2 := ifelse(구매일차이_직전2 < 0, 0,   구매일차이_직전2 )]



table(RE_IMART2$구매여부 )
# table(RE_IMART2$현재일그룹, RE_IMART2$구매여부 )

# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL5I.RData"))
# load(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL5I.RData"))


system.time(RE_IMART2 <- RE_IMART2[ , PRD_GRP_NM := ifelse(is.na(PRD_GRP_NM),  "미생성", PRD_GRP_NM)] )
# system.time(RE_IMART2 <- RE_IMART2[ , PRD_LRG_CLS_NM := ifelse(is.na(PRD_LRG_CLS_NM),  "미생성", PRD_LRG_CLS_NM)] )
system.time(RE_IMART2 <- RE_IMART2[ , PRD_CLS_NM := ifelse(is.na(PRD_CLS_NM),  "미생성", PRD_CLS_NM)] )


# 팩터화 
RE_IMART2 <- RE_IMART2 %>% mutate_if(is.character, as.factor) %>% as.data.table



#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE9.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE9.RData"))



# 8.3 데이터 가공2 ####
#----------------------------------------------------------------#
gc(T);gc(T);gc(T);gc(T)
#----------------------------------------------------------------#
# 8.4. 모델 적용 ####
#----------------------------------------------------------------#
RE_IMART2 <- RE_IMART2 %>% as.data.frame
# RE_IMART2$SYSDATE <- as.character(RE_IMART2$SYSDATE)
train_df <- RE_IMART2[!RE_IMART2$SYSDATE == SYSDATE, c("CUST_NO", "ITEM_CD", "ITEM_NM", "구매여부", "SYSDATE", names_df)]
test_df  <- RE_IMART2[RE_IMART2$SYSDATE == SYSDATE, c("CUST_NO", "ITEM_CD", "ITEM_NM", "구매여부", "SYSDATE", names_df)]



# 구매여부 비중 맞추기 
# table(train_df$구매여부) 
# table(test_df$구매여부) 

Y_tr <- (train_df$구매여부)
Y_tt <- (test_df$구매여부 )

set.seed(123)
formula2 <- formula( paste(" 구매여부 ~ ", paste(names_df, collapse = "+")) )
gc(T);gc(T);gc(T);gc(T)


rm(RE_IMART0, RE_IMART2)
gc(T);  gc(T);  gc(T);  gc(T)

save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL_SPEED_tmp.RData"))
# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL_SPEED_",SYSDATE,".RData"))
# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL_SPEED20191230.RData"))
# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL_SPEED20191218.RData"))
# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL_SPEED20191206.RData"))
# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL_SPEED20191211.RData"))
# load(paste0("/home/kimwh3/DAT/REORD/IMAGE_MDL_SPEED_tmp.RData"))

REO <- "N"
# 분할 예측
CUST_LISTI <- unique(test_df$CUST_NO)
seq_df <- unique(c(round(seq(1, c(length(CUST_LISTI)), length.out = 15)), length(CUST_LISTI)))
gc(T);  gc(T);  gc(T);  gc(T)

# c(length(seq_df) - 1)
system.time(
  CSV <- do.call(rbind.fill, lapply(1:c(length(seq_df) - 1), function(j) {
    # j <- 1
    print(j)
    
    gc(T)
    # test_dft <- test_df[seq_df[j]:seq_df[(j+1)],]
    test_dft <- test_df[test_df$CUST_NO %in% c(CUST_LISTI[seq_df[j]:(seq_df[j+1])] ) & test_df$현재일차이 > 2 ,]
    
    set.seed(123)
    mdl_mt   <- model.matrix(formula2, data= rbind.fill(train_df, test_dft))[,-1] ; print(dim(mdl_mt))
    
    gc(T)
    
    set.seed(123)
    smp <- sample(dim(train_df)[1],30000)
    
    test_mt  <- mdl_mt[(dim(train_df)[1] + 1) : dim(mdl_mt)[1],]  ; dim(test_mt)
    mdl_mt <- mdl_mt[-((dim(train_df)[1] + 1) : dim(mdl_mt)[1]),]
    
    train_mt <- mdl_mt[-smp , ]
    val_mt  <- mdl_mt[smp , ]
    
    rm(mdl_mt)
    gc(T);gc(T)
    # xgTrain <- xgb.DMatrix(data=train_mt, label=Y_tr[-smp])
    # xgVal <- xgb.DMatrix(data=val_mt, label=Y_tr[smp])
    # xgTest <- xgb.DMatrix(data=test_mt)
    
    # set.seed(123)
    # xgbFitl <- xgb.train(
    #   data = xgTrain, nfold = 5, # label = as.matrix(Y_tr[-smp]),
    #   objective='binary:logistic',
    #   nrounds=600,
    #   eval_metric='logloss', # 'auc'
    #   watchlist=list(train=xgTrain, validate=xgVal),
    #   print_every_n=50,
    #   nthread = 7, eta = 0.02, max_depth = 6, min_child_weight = 1.7817
    #   ,early_stopping_rounds=100
    # )
    
    
    # CV 저장경로  ###
    # save(xgbFitl, file = paste0("/home/kimwh3/DAT/REORD/ZMDL/XGB_MDLI_FINAL.RData") )
    # load(file = paste0("/home/kimwh3/DAT/REORD/ZMDL/XGB_MDLI_FINAL.RData") )
    gc(T)
    
    # preds2l <- predict(xgbFitl, newdata = test_mt[, c(xgbFitl$feature_names)]) 
    # preds2lyn <- ifelse(preds2l> 0.5, "구매", "미구매")
    # test_dft$PROB <- preds2l
    
    gc(T)
    # 예측 
    load(file = paste0("/home/kimwh3/DAT/REORD/ZMDL/XGB_MDLI_FINAL.RData") )
    preds2l <- tryCatch(predict(xgbFitl, newdata = test_mt),
                        error = function(e) print("NOT"),
                        warning = function(w) print("NOT"))
    if (preds2l == "NOT") {
      print("모델 피쳐로만 예측 수행")
      preds2l <- tryCatch(predict(xgbFitl, newdata =  test_mt[, c(xgbFitl$feature_names)]),
                          error = function(e) print("NOT"),
                          warning = function(w) print("NOT"))
      
      if (preds2l == "NOT") {
        print("모델 재생성 후 예측 수행")
        xgTrain <- xgb.DMatrix(data=train_mt, label=Y_tr[-smp])
        xgVal <- xgb.DMatrix(data=val_mt, label=Y_tr[smp])
        xgTest <- xgb.DMatrix(data=test_mt)
        gc(T)
        set.seed(123)
        xgbFitl <- xgb.train(
          data = xgTrain, nfold = 5, # label = as.matrix(Y_tr[-smp]),
          objective='binary:logistic',
          nrounds=300,
          eval_metric='logloss', # 'auc'
          watchlist=list(train=xgTrain, validate=xgVal),
          print_every_n=50,
          nthread = 20, eta = 0.05, max_depth = 6, min_child_weight = 1.7817
          ,early_stopping_rounds=100
        )
        print("모델 새로 저장 완료 ")
        save(xgbFitl, file = paste0("/home/kimwh3/DAT/REORD/ZMDL/XGB_MDLI_FINAL.RData") )
        
        preds2l <- predict(xgbFitl, newdata = test_mt) 
      }
      
    }
    preds2lyn <- ifelse(preds2l> 0.5, "구매", "미구매")
    test_dft$PROB <- preds2l
    
    #   test_dft
    #     
    #   }, mc.cores = 15))
    # )
    
    test_df <- test_dft
    rm(test_dft)
    
    rm(train_mt, test_mt, val_mt)
    gc(T);gc(T);gc(T);gc(T)
    #----------------------------------------------------------------#
    # 8.4. 모델 적용 ####
    #----------------------------------------------------------------#
    
    
    
    #----------------------------------------------------------------#
    # 9. 결과정리 및 적재 ####
    #----------------------------------------------------------------#
    
    # 모델 예측 결과에 기본 정보 붙이기 
    
    # DB접근
    # source(file = "/home/kimwh3/DB_CON.R")
    drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
    conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")
    
    
    dbWriteTable(conn, "RE_TEST_FINAL", test_df[,c("CUST_NO", "PROB")], row.names = FALSE)
    dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.RE_TEST_FINAL TO DTA_APP"))
    dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.PRD_TARGET TO DTA_APP"))
    
    dbDisconnect(conn)
    rm(conn,drv)
    #
    # source(file = "/home/kimwh3/DB_CON.R")
    drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
    conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")
    
    
    REALMEM <- dbGetQuery(conn, paste0("SELECT /*+ FULL (A) PARALLEL (A 4) */DISTINCT CUST_NO
                                    , CASE WHEN EC_CUST_GRD_CD IN ('SS') THEN 'VVIP' 
                                    WHEN EC_CUST_GRD_CD IN ('CC') THEN 'VIP'
                                    WHEN EC_CUST_GRD_CD IN ('DD') THEN 'GOLD' ELSE 'ETC' END AS EC_CUST_GRD_CD  -- 고객등급구분
                                   FROM GSBI_OWN.D_CST_MM_CUST_M A
                                    WHERE CUST_NO IN (SELECT DISTINCT CUST_NO FROM DTA_OWN.RE_TEST_FINAL)
                                    AND STD_YM = '", substr(gsub("-","", TODAY_DT - 2),1,6) ,"'"))
    
    MYSHOP <- dbGetQuery(conn, paste0("SELECT /*+ FULL (A) PARALLEL (A 4) */ 
                                   A.CUST_NO
                                   , COUNT(*) as 마이쇼핑_방문건수
                                  FROM DHUB_OWN.PBR_MPC_INTG A
                                   WHERE CUST_NO IN (SELECT DISTINCT CUST_NO FROM DTA_OWN.RE_TEST_FINAL)
                                   AND BHR_DT BETWEEN '", gsub("-","", TODAY_DT- 7),"' AND '", gsub("-","", TODAY_DT),"'
                                   AND BHR_LVL_4_CD = '1020403'
                                   GROUP BY CUST_NO"))
    
    # 동의여부 SMS_YN 
    SMS_YN <- dbGetQuery(conn, paste0("SELECT /*+ FULL (A) PARALLEL (A 4) */ A.CUST_NO, 
                                   A.APP_PUSH_RCV_AGREE_YN
                                   FROM GSBI_OWN.D_CST_MM_CUST_M A
                                   WHERE A.STD_YM = TO_CHAR(sysdate - 1, 'YYYYMM') AND A.CUST_NO IN ( SELECT DISTINCT B.CUST_NO FROM DTA_OWN.RE_TEST_FINAL B)
                                 "))
    # 동의여부 #
    dbDisconnect(conn)
    rm(conn,drv)
    
    # source(file = "/home/kimwh3/DB_CON.R")
    drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
    conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")
    
    
    
    # PRD_TARGET 
    PRD_TARGET <- dbGetQuery(conn, paste0("SELECT * FROM DTA_OWN.PRD_TARGET"))
    
    dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.RE_TEST_FINAL PURGE"))
    
    # 접속 종료 
    dbDisconnect(conn)
    rm(conn,drv)
    
    
    #
    
    head(REALMEM,2)
    head(MYSHOP,2)
    head(PRD_TARGET,2)
    
    
    
    setDT(REALMEM)
    setDT(MYSHOP)
    setDT(SMS_YN)
    setDT(test_df)
    setDT(PRD_TARGET)
    
    # 256406 
    test_df2 <- merge(test_df, REALMEM %>% dplyr::rename(EC_CUST_GRD_CD_NOW = EC_CUST_GRD_CD), by = "CUST_NO", all.x= T)
    test_df2 <- merge(test_df2, MYSHOP, by = "CUST_NO", all.x= T)
    test_df2 <- merge(test_df2, SMS_YN, by = "CUST_NO", all.x= T)
    test_df2$마이쇼핑_방문건수 <- ifelse(is.na(test_df2$마이쇼핑_방문건수), 0 , test_df2$마이쇼핑_방문건수)
    test_df2$EC_CUST_GRD_CD_NOW <- ifelse(is.na(test_df2$EC_CUST_GRD_CD_NOW), "ETC" , test_df2$EC_CUST_GRD_CD_NOW)
    setDT(test_df2)
    setkeyv(test_df2, c("CUST_NO", "PROB"))
    # 확률 순서 정렬
    test_df2 <- test_df2[ITEM_NM != "미생성",]
    
    if(REO == "Y") {test_df2 <- test_df2[test_df2$주기그룹 != "신규", ] } # 재구매 대상 아이템만 
    
    test_df2 <- test_df2[order(CUST_NO, -PROB)]
    setkeyv(test_df2, c("CUST_NO"))
    system.time(test_df2 <- test_df2[, Probability_Sort := seq_len(.N), by=c("CUST_NO")])
    test_df2$PROB <- round(test_df2$PROB, 3)
    setnames(test_df2, c("PROB", "마이쇼핑_방문건수", "현재일차이", "주기그룹", "주기_평균", "SYSDATE", "EC_CUST_GRD_CD_NOW", "APP_PUSH_RCV_AGREE_YN"),
             c("Probability", "Myshopping_VisitCnt", "Last_BuyGap", "PeriodG", "Period_AVG", "SysdateT", "RealMembership", "App_Push_Agree"))
    
    
    # 10
    test_df3 <- test_df2[Probability_Sort <= 50][,.(CUST_NO, PRD_GRP_NM, ITEM_CD, ITEM_NM, Probability_Sort, Probability, RealMembership, 
                                                    Myshopping_VisitCnt, App_Push_Agree, Last_BuyGap, PeriodG, Period_AVG, SysdateT, 선호CAEC소싱  ) ]
    test_df3 <- test_df3[order(CUST_NO, Probability_Sort)]
    test_df3$선호CAEC소싱 <- as.character(test_df3$선호CAEC소싱)
    test_df3 <- test_df3[ ,선호CAEC소싱 := ifelse( 선호CAEC소싱 == "미생성", "CA", 선호CAEC소싱)]
    
    ## 시작 
    
    
    PRD_TARGET$ITEM_CD <- as.character(PRD_TARGET$ITEM_CD)
    dim(test_df3)
    # test_df3 <- merge(test_df3, PRD_TARGET[FAMOUS_SORT == 1,.(ITEM_CD,ITEM_NM,SRCNG_GBN_CD,PRD_CD,PRD_NM,FAMOUS_SORT)],
    #                   by.x = c("ITEM_CD","ITEM_NM", "선호CAEC소싱"),
    #                   by.y = c("ITEM_CD","ITEM_NM", "SRCNG_GBN_CD"), all.x= T)
    
    if(REO == "N") {
      famous_prd <- PRD_TARGET[FAMOUS_SORT == 1,.(ITEM_CD,ITEM_NM,SRCNG_GBN_CD,PRD_CD,PRD_NM,FAMOUS_SORT)]
      test_df31 <- merge(test_df3, famous_prd[SRCNG_GBN_CD == "CA"], by = c("ITEM_CD","ITEM_NM"), all.x= T)
      test_df32 <- merge(test_df3, famous_prd[SRCNG_GBN_CD == "EC"], by = c("ITEM_CD","ITEM_NM"), all.x= T)
      test_df3 <- rbind.fill(test_df31, test_df32)
      
    }
    
    if(REO == "Y") {
      famous_prd <- PRD_TARGET[FAMOUS_SORT == 1,.(ITEM_CD,ITEM_NM,SRCNG_GBN_CD,PRD_CD,PRD_NM,FAMOUS_SORT)]
      test_df31 <- merge(test_df3, famous_prd[SRCNG_GBN_CD == "CA"], by = c("ITEM_CD","ITEM_NM"), all.x= T)
      test_df32 <- merge(test_df3, famous_prd[SRCNG_GBN_CD == "EC"], by = c("ITEM_CD","ITEM_NM"), all.x= T)
      
      famous_prd2 <- PRD_TARGET[FAMOUS_SORT == 2,.(ITEM_CD,ITEM_NM,SRCNG_GBN_CD,PRD_CD,PRD_NM,FAMOUS_SORT)]
      test_df33 <- merge(test_df3, famous_prd2[SRCNG_GBN_CD == "CA"], by = c("ITEM_CD","ITEM_NM"), all.x= T)
      test_df34 <- merge(test_df3, famous_prd2[SRCNG_GBN_CD == "EC"], by = c("ITEM_CD","ITEM_NM"), all.x= T)
      
      
      test_df31 <- na.omit(test_df31)
      test_df32 <- na.omit(test_df32)
      test_df33 <- na.omit(test_df33)
      test_df34 <- na.omit(test_df34)
      test_df3 <- rbind.fill(test_df31, test_df32, test_df33, test_df34) 
      
    } # 재구매 대상 아이템만 
    
    
    
    dim(test_df3)
    
    # 
    setDT(test_df3)
    test_df3 <- test_df3 %>% mutate_if(is.factor, as.character)
    test_df3 <- na.omit(test_df3)
    #
    setDT(test_df3)
    setkeyv(test_df3, c("CUST_NO"))
    test_df3 <- test_df3[order(CUST_NO, -Probability, SRCNG_GBN_CD)]
    system.time(test_df3 <- test_df3[ , Probability_Sort := seq_len(.N), by=c("CUST_NO")])
    #
    test_df3 <- test_df3 %>% filter(Probability_Sort <= 50) %>% dplyr::select(-FAMOUS_SORT)  # 20
    
    test_df3$DT <- substr(test_df3$SysdateT,1,10)
    test_df3$HH <- substr(test_df3$SysdateT,12,13)
    
    ## 적재 시작
    print("파이널. 적재시작")
    seq_df <- unique(c(seq(1,nrow(test_df3), by = 100000), nrow(test_df3)))
    
    print("대상 고객수")
    print(seq_df)
    # DB접근
    # source(file = "/home/kimwh3/DB_CON.R")
    drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
    conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")
    
    
    for( i in 1:(length(seq_df) -1)) {
      print(paste0(i, " ", seq_df[(i+1)], " // ", nrow(test_df3)))
      gc(T)
      tmp <- test_df3[seq_df[i]:seq_df[(i+1)], ]
      # if ( !dbExistsTable(conn, "CUST_TARGET") ) {
      if ( j == 1 ) {
        dbWriteTable(conn, "CUST_TARGET", tmp, row.names = FALSE)
      } else {
        dbWriteTable(conn, "CUST_TARGET2", tmp, row.names = FALSE)
        dbSendUpdate(conn, paste0("
                               INSERT INTO DTA_OWN.CUST_TARGET A (
                               A.SysdateT, A.ITEM_CD, A.ITEM_NM, A.CUST_NO,  A.Probability, A.Probability_Sort,A.PRD_GRP_NM, A.Last_BuyGap,
                               A.App_Push_Agree, A.Myshopping_VisitCnt, A.RealMembership, A.PeriodG, A.Period_AVG, A.선호CAEC소싱, A.PRD_CD, A.PRD_NM, A.DT, A.HH
                               )
                               SELECT B.SysdateT, B.ITEM_CD, B.ITEM_NM, B.CUST_NO,  B.Probability, B.Probability_Sort,B.PRD_GRP_NM, B.Last_BuyGap,
                               B.App_Push_Agree, B.Myshopping_VisitCnt, B.RealMembership, B.PeriodG, B.Period_AVG, B.선호CAEC소싱, B.PRD_CD, B.PRD_NM, B.DT, B.HH
                               FROM DTA_OWN.CUST_TARGET2 B
                               "))
        dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.CUST_TARGET2 PURGE"))
      }
    }
    dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET TO MSTR_APP"))
    dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET TO i_kimdy9"))
    dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET TO i_sungsa"))
    dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET TO DTA_APP"))
    
    
    
    dbDisconnect(conn)
    rm(conn,drv)
    
    
    print("최종 적재 완료 ")
    print(Sys.time())
    print(paste(" 적재될 테이블 행 : "    , nrow(test_df3)))
    
    
    
    head(test_df3, 2 )
    setDT(test_df3)
    setkeyv(test_df3, c("CUST_NO","PRD_CD","PRD_NM","Probability_Sort"))
    test_df4 <- test_df3[, .(CUST_NO,Probability_Sort,PRD_CD,Probability,  Myshopping_VisitCnt,RealMembership)]
    
    dim(test_df4)
    test_df4 <- test_df4[Probability >= 0.01]
    print("20%이상 확률 고객 대상 데이터 포맷 변환")
    dim(test_df4)
    length(unique(test_df4$CUST_NO))
    
    test_df4 <- test_df4[ , Probability := round(Probability *100)]
    test_df4 <- test_df4[ , PP := paste0(PRD_CD, "_", Probability)]
    test_df4 <- test_df4[order(CUST_NO, -Probability)]
    test_df5 <- test_df4[ , .(PPP = paste(PP, collapse = "|"), CNT = .N ), by = c("CUST_NO" , "Myshopping_VisitCnt", "RealMembership" ) ]
    test_df6 <- test_df5[ , CUST_PPP := paste0(CUST_NO, ",", PPP)][,.(CUST_PPP, CUST_NO, Myshopping_VisitCnt,RealMembership, CNT)]
    print(table(test_df6$CNT))
    test_df6
    
  }))
) 


print(paste("최종 마무리 완료 ", Sys.time()))
print(seq_df)
#----------------------------------------------------------------#
# 9. 결과정리 및 적재 #### 
#----------------------------------------------------------------#  

print(dim(CSV))
head(CSV)
print(table(CSV$CNT ))
print(table(CSV$CNT )/sum(table(CSV$CNT )))





#  print("최종 테이블 적재") 
#  print(seq_df)
# # DB접근 
#  # source(file = "/home/kimwh3/DB_CON.R")
#  drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
#  conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")
#  
#  dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.CUST_TARGET_FINAL PURGE "))
#  dbSendUpdate(conn, paste0("CREATE TABLE CUST_TARGET_FINAL AS SELECT * FROM DTA_OWN.CUST_TARGET "))
#  
#  dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET_FINAL TO MSTR_APP"))
#  dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET_FINAL TO DTA_APP"))
#  
#  dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET_FINAL TO DTA_OWN"))
#  dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET_FINAL TO i_kimdy9"))
#  dbSendUpdate(conn, paste0("GRANT SELECT ON DTA_OWN.CUST_TARGET_FINAL TO i_sungsa"))
# # 접속 종료 
# dbDisconnect(conn)
# rm(conn,drv)
# 
# print("최종 테이블 적재 DTA_OWN ") 
# drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
# conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_OWN","gs#dta!@34")
# 
# dbSendUpdate(conn, paste0("DROP TABLE DTA_OWN.CUST_TARGET_FINAL PURGE "))
# dbSendUpdate(conn, paste0("CREATE TABLE CUST_TARGET_FINAL AS SELECT * FROM DTA_OWN.CUST_TARGET_FINAL "))
# 
# dbDisconnect(conn)
# rm(conn,drv)


# CSV2 <- CSV %>% filter(CNT >= 3) %>% dplyr::select(CUST_PPP)
print(table(CSV2$CNT[CSV2$Myshopping_VisitCnt>0] ))
# write.csv(CSV2, file = "/home/kimwh3/OUT/TEST/TEST_CSV20191031.csv", row.names = F)
# write.csv(CSV2, file = "/home/kimwh3/OUT/TEST/TEST_CSV20191113.csv", row.names = F)
# write.csv(CSV2, file = "/home/kimwh3/OUT/TEST/TEST_CSV20200109.csv", row.names = F)


# CSV21 <- CSV2[1:100000,]
# write.csv(CSV21, file = "/home/kimwh3/OUT/TEST/TEST_CSV20191031_v1.csv", row.names = F)
# CSV22 <- CSV2[100001:200000,]
# write.csv(CSV22, file = "/home/kimwh3/OUT/TEST/TEST_CSV20191031_v2.csv", row.names = F)
# CSV23 <- CSV2[200001:300000,]
# write.csv(CSV23, file = "/home/kimwh3/OUT/TEST/TEST_CSV20191031_v3.csv", row.names = F)
# CSV24 <- CSV2[300001:400000,]
# write.csv(CSV24, file = "/home/kimwh3/OUT/TEST/TEST_CSV20191031_v4.csv", row.names = F)
# CSV25 <- CSV2[400001:530233,]
# write.csv(CSV25, file = "/home/kimwh3/OUT/TEST/TEST_CSV20191031_v5.csv", row.names = F)

# 마이쇼핑 페이지 하단 테스트 모수 
# save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_TEST191031.RData"))
# load(paste0("/home/kimwh3/DAT/REORD/IMAGE_TEST191031.RData"))


# > tail(CSV)
# CUST_PPP CNT
# 290884                                                 53317862,34669653_64|50394849_43|30354938_42|35024766_38|36504177_37   5
# 290885                                                             53325150,35893730_39|37166368_37|36215559_33|17450821_31   4
# 290886 53339983,35860205_77|37613425_67|38189500_67|37735174_60|35593255_53|35674218_52|38210815_42|35944368_38|36842850_36   9
# 290887                                                             53375762,33587776_36|31696133_35|14367999_34|31693399_31   4
# 290888                                     53410050,27517420_71|50641594_42|35107435_34|31538959_33|50708108_33|36017438_32   6
# 290889                                                                                                 53412001,35107435_33   1
# > table(CSV$CNT)
# 
# 1      2      3      4      5      6      7      8      9     10 
# 133410  80126  38359  16937   7764   4151   2492   1657   1351   4642 
# > 


