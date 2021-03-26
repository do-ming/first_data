# .rs.restartR()

# vi /home/kimwh3/BAT/tune1_1.sh
# nohup /usr/lib64/R/bin/Rscript /home/kimwh3/RSC/REORD_CUST/tune1_1.R > /home/kimwh3/BAT/LOG/tune1_1.log &
# chmod 754 /home/kimwh3/BAT/tune1_1.sh
# 00 01 * * * /home/kimwh3/BAT/tune1_1.sh
# [분 시 일 월 요일(0~7)]
# /home/kimwh3/BAT/tune1_1.sh

#----------------------------------------------------------------#
# 패키지 불러오기 ####
#----------------------------------------------------------------#
library(data.table)
library(plyr)
library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)
library(doParallel)
library(stringdist)
library(text2vec)
library(magrittr)
library(proxy)


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


print(paste0(" 1.  준실시간 데이터 호출 시작  // 시간 : ", Sys.time())) 


# DB접근  
# source(file = "/home/kimwh3/DB_CON.R")
drv  <- JDBC("oracle.jdbc.OracleDriver", classPath="/usr/lib/oracle/11.2/client64/lib/ojdbc5.jar", " ")
conn <- dbConnect(drv,"jdbc:oracle:thin:@//10.52.244.32:1522/DHUB1","DTA_APP","gs#dta_app!@34")


gc(T);gc(T);gc(T)
DT <- gsub("-","",ymd(TODAY_DT))
##### 1.3 행동 데이터 RE_LOG0 ####

# 로그 
system.time(
  RE_LOG0 <- dbGetQuery(conn, paste0(
    "SELECT /*+ FULL (A) PARALLEL (A 6) */ 
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
     WHERE A.CONCT_DT >= '", DT,"'
     AND (QUERY LIKE '%tq=%' OR QUERY LIKE '%kwd=%' OR QUERY LIKE '%prdid=%') "
  ))
)

gc(T);gc(T)

# 검색 
system.time(
  RE_KEY0 <- dbGetQuery(conn, paste0(
    "SELECT /*+ FULL (C) PARALLEL (C 6) */ 
     C.CONCT_DT, C.CONCT_TIME, C.SES_ID, C.PARAM_VAL 
     FROM SDHUB_OWN.STG_MPC_LOG_CLCT_D C 
     WHERE CONCT_DT >= '", DT,"'
     AND PARAM_KEY = 'SRCH_KEYWD'"
  ))
)

setDT(RE_LOG0)
setDT(RE_KEY0)

RE_LOG0 <- RE_LOG0[!is.na(CUST_NO) | nchar(CUST_NO) < 10]

gc(T);gc(T)

print(paste0(" 1.  준실시간 데이터 호출 완료  // 시간 : ", Sys.time())) 

print("준실시간 로그데이터 dim ")
print(dim(RE_LOG0))
print("준실시간 검색어데이터 dim ")
print(dim(RE_KEY0))



##### 1.3 행동 데이터 ####

dbDisconnect(conn)
rm(conn,drv)

# 저장된 그룹 데이터 호출 
list_grd <- list.files("/home/kimwh3/DAT/REORD/GRD_DATA/")
RE_GRD <- do.call(rbind, mclapply(which(substr(list_grd,1,6) %in% gsub("-","",substr(TODAY_DT - 180, 1, 7))):length(list_grd), function(i){
  # i <- 1
  print(i)
  tmp <- readRDS(paste0("/home/kimwh3/DAT/REORD/GRD_DATA/",list_grd[i] ))
  tmp
}, mc.cores = 10))







#----------------------------------------------------------------#
# 4. 행동 정보 ####
#----------------------------------------------------------------#


# 4. 1 소스코드로 돌리기 ####

print(paste0(" 4. 행동 데이터 호출   // 시간 : ", Sys.time())) 
#### 행동 #
# DTL <- gsub("-","",seq.Date(from = ymd(TODAY_DT - 7), to = ymd(TODAY_DT), by = "days"))
DTL <- gsub("-","",seq.Date(from = ymd(TODAY_DT - GAP), to = ymd(TODAY_DT), by = "days"))

DTL
# 120
list_grd <- list.files("/home/kimwh3/DAT/REORD/LOG_DATA/")
list_grd <- list_grd[substr(list_grd,1,8) %in% DTL]
system.time(
  RE_LOG <- do.call(rbind, mclapply(1:length(list_grd), function(i){
    # i <- 1
    print(i)
    tmp <- readRDS(paste0("/home/kimwh3/DAT/REORD/LOG_DATA/",list_grd[i] ))
    tmp
  }, mc.cores = 10))
)
gc(T);gc(T)

# 17 
list_grd <- list.files("/home/kimwh3/DAT/REORD/KEY_DATA/")
list_grd <- list_grd[substr(list_grd,1,8) %in% DTL]
system.time(
  RE_KEY <- do.call(rbind, mclapply(1:length(list_grd), function(i){
    # i <- 1
    print(i)
    tmp <- readRDS(paste0("/home/kimwh3/DAT/REORD/KEY_DATA/",list_grd[i] ))
    tmp
  }, mc.cores = 10))
)
gc(T);gc(T)

print(paste0(" 4. 행동 데이터 호출 완료  // 시간 : ", Sys.time())) 

## 추가 행동 데이터 ##

RE_LOG <- rbind.fill(RE_LOG, RE_LOG0)
RE_KEY <- rbind.fill(RE_KEY, RE_KEY0)
rm(RE_LOG0, RE_KEY0)
## 추가 행동 데이터 ##


setDT(RE_LOG)
setDT(RE_KEY)

setkeyv(RE_LOG, c("CUST_NO", "SES_ID", "CONCT_DT", "CONCT_TIME"))
setkeyv(RE_KEY, c( "SES_ID", "CONCT_DT", "CONCT_TIME"))

#
setDT(RE_GRD)
GOLDUP6 <- RE_GRD[STD_YM > gsub("-","",substr(TODAY_DT - 180, 1, 7)) & EC_CUST_GRD_CD %in% c("VIP","VVIP")]$CUST_NO  %>% unique()
# GOLDUP6 <- unique(RE_GRD$CUST_NO)
print(length(GOLDUP6))

print(length(unique(RE_LOG$CUST_NO)))
RE_LOG <- RE_LOG[CUST_NO %in% GOLDUP6]  %>% distinct()
print(length(unique(RE_LOG$CUST_NO)))

RE_LOG_PG <- RE_LOG[!is.na(PRD_CD)]%>% distinct() %>% dplyr::select(-SES_ID)
rm(RE_GRD); gc(T);gc(T);gc(T)



RE_KEY <- RE_KEY %>% distinct()
RE_KEY <- merge(RE_KEY, RE_LOG[,.(CUST_NO, CONCT_DT, CONCT_TIME, SES_ID )], by = c("SES_ID", "CONCT_DT", "CONCT_TIME"), all.x = T) 

# # save.image(paste0("/home/kimwh3/DAT/REORD/IMAGE_테스트고객90.RData")) # 2019-09-29 03:59:55
# # load(paste0("/home/kimwh3/DAT/REORD/IMAGE_테스트고객90.RData"))

# 테스트 대상 고객 호출 
TEST_CUST_NO <- readRDS(file = paste0("/home/kimwh3/DAT/REORD/LOG_RST_CUST/","CUST_", SYSDATE ,".RDS"))
# TEST_CUST_NO <- c(unique(RE_LOG_PG$CUST_NO)[1:50000])
RE_KEY <- RE_KEY[CUST_NO %in% GOLDUP6] 
# 테스트 대상 고객 매칭시켜주기
RE_KEY <- RE_KEY[CUST_NO %in% TEST_CUST_NO]
RE_LOG_PG <- RE_LOG_PG[CUST_NO %in% TEST_CUST_NO]

print(length(unique(RE_LOG_PG$CUST_NO)))

# 테스트 

#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE1_10.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE1_10.RData"))

RE_KEY <- RE_KEY[!is.na(CUST_NO)]
RE_KEY <- RE_KEY[order(CUST_NO, CONCT_DT, CONCT_TIME, SES_ID)] %>% dplyr::select(-SES_ID)
rm(RE_LOG)
gc(T);gc(T);gc(T)
# RE_KEY2[CUST_NO == "47464313"][, .(CONCT_DT, CONCT_TIME,  CUST_NO,  PARAM_VAL)]

if(DIRECT != "Y") {
  saveRDS(RE_KEY, file = paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/","ITEM_LOG_" ,SYSDATE,".RDS"))
  saveRDS(RE_LOG_PG[,.(CUST_NO,ITEM_CD,ITEM_NM)] %>% distinct(), file = paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/","ITEM_LOGPG_" ,SYSDATE,".RDS"))
  print("save 중... ")
  Sys.sleep(5)
  system("/home/kimwh3/BAT/tune1_1K.sh")
  gc(T);gc(T)
  rm(RE_KEY)
  gc(T);gc(T)
} 



# 상품, 아이템, 브랜드 관점
RE_LOG_PG$TODAY    <- TODAY_DT
RE_LOG_PG$CONCT_DT <- ymd(RE_LOG_PG$CONCT_DT)

#  155.603 
# system.time(RE_LOG_PG$탐색시간 <- ymd_hms(paste0(gsub("-","",RE_LOG_PG$CONCT_DT), RE_LOG_PG$CONCT_TIME)))

#  155.603 
system.time(RE_LOG_PG <- RE_LOG_PG[ , 탐색시간 := ymd_hms(paste0(gsub("-","",CONCT_DT), CONCT_TIME))])
RE_LOG_PG <- RE_LOG_PG[탐색시간 < SYSDATE]
# 9943868
# 122
DTL_df <- data.table(CONCT_DT = ymd(DTL), 일차이 = as.numeric(TODAY_DT - ymd(DTL)) )
RE_LOG_PG <- merge(RE_LOG_PG, DTL_df, by = "CONCT_DT" , all.x = T)
RE_LOG_PG <- RE_LOG_PG[order(CUST_NO, CONCT_DT, CONCT_TIME, PRD_CD, PRD_NM )]
# 392.015 
# system.time(RE_LOG_PG <- RE_LOG_PG[, 상품탐색순서 := rev(seq(.N)), by=c("CUST_NO", "PRD_CD", "PRD_NM")])
# RE_LOG_PG <- RE_LOG_PG[order(CUST_NO, CONCT_DT, CONCT_TIME, ITEM_CD, ITEM_NM )]
# 74.770 
RE_LOG_PG <- RE_LOG_PG[order(CUST_NO, -CONCT_DT, -CONCT_TIME, ITEM_CD, ITEM_NM )]
system.time(RE_LOG_PG <- RE_LOG_PG[, 아이템탐색순서 := (seq_len(.N)), by=c("CUST_NO", "ITEM_CD", "ITEM_NM")])
# RE_LOG_PG <- RE_LOG_PG[order(CUST_NO, CONCT_DT, CONCT_TIME, BRAND_CD, BRAND_NM )]
# 79
# system.time(RE_LOG_PG <- RE_LOG_PG[, 브랜드탐색순서 := rev(seq(.N)), by=c("CUST_NO", "BRAND_CD", "BRAND_NM")])


# RE_LOG_PG <- RE_LOG_PG[ , 상품탐색일별 := .N, by = c("CUST_NO", "일차이", "PRD_CD", "PRD_NM")]
# RE_LOG_PG <- RE_LOG_PG[ , 아이템탐색일별 := .N, by = c("CUST_NO", "일차이", "ITEM_CD", "ITEM_NM")]
# RE_LOG_PG <- RE_LOG_PG[ , 브랜드탐색일별 := .N, by = c("CUST_NO", "일차이", "BRAND_CD", "BRAND_NM")]

# 최근첫탐색_일차이, 최근두번째탐색_일차이, 최근세번째탐색_일차이, 하루, 3일, 7일, 14일간 탐색횟수
# 첫탐색_시간차이, 두번째탐색_시간차이, 세번째탐색_시간차이 

# RE_LOG_PG[CUST_NO == "9943868"]

print(paste0(" 4. 탐색 데이터 요약 시작   // 시간 : ", Sys.time())) 

# RE_LOG_PG1 <- RE_LOG_PG[ , .(CUST_NO, PRD_CD, PRD_NM, 탐색시간, 상품탐색순서, 일차이)]
# # 759.531
# system.time(
#   RE_LOG_PG_PRD <- RE_LOG_PG1[ , .(
#     최근첫탐색시간P = 탐색시간[상품탐색순서 == 1],
#     최근2탐색시간P = 탐색시간[상품탐색순서 == 2],
#     최근3탐색시간P = 탐색시간[상품탐색순서 == 3],
#     최근첫탐색P = min(일차이),
#     최근1일탐색P = sum(일차이 == 1),
#     최근3일탐색P = sum(일차이 <= 3),
#     최근7일탐색P = sum(일차이 <= 7)#,
#     # 최근14일탐색P = sum(일차이 <= 14)
#   ), by = c("CUST_NO", "PRD_CD", "PRD_NM")]
# )
# rm(RE_LOG_PG1); gc(T); gc(T); gc(T)

# RE_LOG_PG2 <- RE_LOG_PG[ , .(CUST_NO, ITEM_CD, ITEM_NM, 탐색시간, 아이템탐색순서, 일차이)]
setkeyv(RE_LOG_PG, c("CUST_NO", "ITEM_CD", "ITEM_NM" ))
# 559.908
system.time(
  RE_LOG_PG_ITEM <- RE_LOG_PG[ , .(
    최근첫탐색시간I = 탐색시간[아이템탐색순서 == 1],
    최근2탐색시간I = 탐색시간[아이템탐색순서 == 2],
    최근3탐색시간I = 탐색시간[아이템탐색순서 == 3],
    최근첫탐색I = min(일차이),
    최근1일탐색I = sum(일차이 == 1),
    최근3일탐색I = sum(일차이 <= 3),
    최근7일탐색I = sum(일차이 <= 7)#,
    # 최근14일탐색I = sum(일차이 <= 14)
  ), by = c("CUST_NO", "ITEM_CD", "ITEM_NM")]
)
rm(RE_LOG_PG); gc(T); gc(T); gc(T)

head(RE_LOG_PG_ITEM)

print(paste0(" 4. 탐색 데이터 요약 완료  // 시간 : ", Sys.time())) 



print(paste0(" 4.로그 데이터 아이템 웨이팅 시작   // 시간 : ", Sys.time())) 

# 검색어 데이터 결과 완료되어 호출 때까지 리핏 실행! 
repeat_start <- Sys.time()
print(paste("repeat_start! : " , repeat_start))

i <- 0
repeat { 
  i <- i+1
  print(paste0(" waiting log data set ", i, " // repeat_start! ", repeat_start))
  Sys.sleep(30)
  
  
  if (sum(list.files("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/") %in% c(paste0("ITEM_LOG_KEYFINAL_", SYSDATE ,".RDS"))) == 1) {
    
    print("starting read ")
    Sys.sleep(80)
    RE_KEY_CUST_I <- readRDS(paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/","ITEM_LOG_KEYFINAL_", SYSDATE ,".RDS"))
    print(dim(RE_KEY_CUST_I))
    # 호출 완료 후 삭제 
    file.remove(paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/","ITEM_LOG_KEYFINAL_", SYSDATE ,".RDS"))
    
    
    setDT(RE_LOG_PG_ITEM)
    setDT(RE_KEY_CUST_I)
    
    print(dim(RE_LOG_PG_ITEM))
    RE_LOG_PG_ITEM <- merge(RE_LOG_PG_ITEM, RE_KEY_CUST_I , by = c("CUST_NO", "ITEM_CD", "ITEM_NM"), all.x = T)
    print(dim(RE_LOG_PG_ITEM))
    
    print(paste0(" 4. 로그 데이터 아이템 완료   // 시간 : ", Sys.time())) 
    saveRDS(RE_LOG_PG_ITEM, file = paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/","ITEM_LOG_", SYSDATE ,".RDS"))
    break
    
  }
}

#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE1_11.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE1_11.RData"))
head(RE_LOG_PG_ITEM)
