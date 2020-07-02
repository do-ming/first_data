

# .rs.restartR()

# vi /home/kimwh3/BAT/tune1_1K.sh
# nohup /usr/lib64/R/bin/Rscript /home/kimwh3/RSC/REORD_CUST/tune/tune1_1K.R > /home/kimwh3/BAT/LOG/tune1_1K.log &
# chmod 754 /home/kimwh3/BAT/tune1_1K.sh
# 00 01 * * * /home/kimwh3/BAT/tune1_1K.sh
# [분 시 일 월 요일(0~7)]
# /home/kimwh3/BAT/tune1_1K.sh
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

# DTL <- gsub("-","",seq.Date(from = ymd(TODAY_DT - 7), to = ymd(TODAY_DT), by = "days"))
DTL <- gsub("-","",seq.Date(from = ymd(TODAY_DT - GAP), to = ymd(TODAY_DT), by = "days"))
DTL


print(paste0(" 1.  데이터 호출   // 시간 : ", Sys.time())) 


## 시간 넘어가는거 방지하기!
# idx1 <- which(list.files("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/") %in%
#         c(paste0("ITEM_LOG_",SYSDATE-60*60*2,".RDS"),
#           paste0("ITEM_LOG_",SYSDATE-60*60*1,".RDS"),
#           paste0("ITEM_LOG_",SYSDATE,".RDS"),
#           paste0("ITEM_LOG_",SYSDATE+60*60*1,".RDS"),
#           paste0("ITEM_LOG_",SYSDATE-60*60*2,".RDS")))
# 
# idx2 <- which(list.files("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/") %in%
#                 c(paste0("ITEM_LOGPG_",SYSDATE-60*60*2,".RDS"),
#                   paste0("ITEM_LOGPG_",SYSDATE-60*60*1,".RDS"),
#                   paste0("ITEM_LOGPG_",SYSDATE,".RDS"),
#                   paste0("ITEM_LOGPG_",SYSDATE+60*60*1,".RDS"),
#                   paste0("ITEM_LOGPG_",SYSDATE-60*60*2,".RDS")))
# 
# test <- readRDS(paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/",
#                        list.files("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/")[idx2]))


RE_KEY <- readRDS(paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/","ITEM_LOG_",SYSDATE,".RDS"))
RE_LOG_PG_ITEM <- readRDS(paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/","ITEM_LOGPG_" ,SYSDATE,".RDS"))


print(paste0(" 1.  데이터 호출 완료  // 시간 : ", Sys.time())) 




#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE1_1K0.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE1_1K0.RData"))


setDT(RE_KEY)
setDT(RE_LOG_PG_ITEM)


ITEM_ID <- unique(RE_LOG_PG_ITEM$ITEM_NM)
ITEM_ID <- data.frame(id = 1:length(ITEM_ID), name = ITEM_ID)
# 팩터를 케릭터형으로 
ITEM_ID <- ITEM_ID %>% mutate_if(is.factor, as.character) %>% as.data.table
setDT(ITEM_ID)
setkey(ITEM_ID, id)
# 단어형태 변경 
prep_fun = tolower
tok_fun = word_tokenizer
train = ITEM_ID[J(ITEM_ID)]
it_train = itoken(train$name,
                  preprocessor = prep_fun,
                  tokenizer = tok_fun,
                  ids = train$id,
                  progressbar = FALSE)
vocab = create_vocabulary(it_train)
# library(glmnet)
vectorizer = vocab_vectorizer(vocab)
dtm_train = create_dtm(it_train, vectorizer)
ITEM_ID <- cbind.data.frame(ITEM_ID, t(apply(dtm_train, 1, function(x) names(which(x >= 1))[1:5] )))
rm(dtm_train, vocab, train)

RE_LOG_PG_ITEM <- merge(RE_LOG_PG_ITEM, ITEM_ID, by.x = c("ITEM_NM"), by.y = c("name"), all.x = T)
gc(T);gc(T);gc(T);gc(T)
DTL_df <- data.table(CONCT_DT = ymd(DTL), 일차이 = as.numeric(TODAY_DT - ymd(DTL)) )
DTL_df$CONCT_DT <- gsub("-","", DTL_df$CONCT_DT)
system.time(RE_KEY$검색시간 <- ymd_hms(paste0(RE_KEY$CONCT_DT, RE_KEY$CONCT_TIME)))
RE_KEY <- RE_KEY[검색시간 < SYSDATE]
RE_KEY <- merge(RE_KEY, DTL_df, by = "CONCT_DT" , all.x = T)
RE_KEY <- RE_KEY[order(CUST_NO, CONCT_DT, CONCT_TIME )]
RE_KEY <- RE_KEY[!is.na(PARAM_VAL)]
RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ , ITEM_NM := ifelse(is.na(ITEM_NM), "미생성", ITEM_NM)]
RE_KEY$검색시간    <- as.character(RE_KEY$검색시간)

print(paste0(" 4. 키워드 데이터 시작   // 시간 : ", Sys.time())) 

gc(T);gc(T);gc(T)
RE_KEY <- RE_KEY[ , CONCT_DT := NULL][ , CONCT_TIME := NULL]
setkeyv(RE_KEY, c("CUST_NO", "PARAM_VAL"))
gc(T);gc(T);gc(T)
print(paste0(" 4. 키워드 데이터 상품 시작   // 시간 : ", Sys.time())) 




RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ order(CUST_NO, ITEM_CD, ITEM_NM)]
setkey(RE_LOG_PG_ITEM, "CUST_NO")
system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 고객순서 := (seq_len(.N)), by=c("CUST_NO")])
RE_LOG_PG_ITEM$고객순서 <- as.character(RE_LOG_PG_ITEM$고객순서)

CUST_LISTI <- unique(RE_LOG_PG_ITEM$고객순서)
setkeyv(RE_LOG_PG_ITEM, c("CUST_NO", "고객순서"))
print(length(CUST_LISTI))


print(paste0(" 4. 로그키워드 데이터 아이템 시작   // 시간 : ", Sys.time())) 
# 2000 
# length(CUST_LISTI)
system.time(
  RE_KEY_CUST_I <- do.call(rbind.fill, mclapply(1:length(CUST_LISTI), function(i){
    
    # i <- 655
    print(paste0(i, " // ", length(CUST_LISTI)))
    
    RE_LOG_PG_ITEMT <- RE_LOG_PG_ITEM[고객순서 %in% CUST_LISTI[i] ]
    RE_KEYT         <- RE_KEY[CUST_NO %in% RE_LOG_PG_ITEMT$CUST_NO ]
    
    key_c <- RE_LOG_PG_ITEMT[,.(CUST_NO, ITEM_NM, ITEM_CD, V1,V2,V3,V4,V5)]
    key_c$SYSDATE <- SYSDATE
    
    RE_KEYT$dts0 <- ymd(substr(RE_KEYT$검색시간,1,10))
    key_c$dts0 <- ymd(substr(key_c$SYSDATE,1,10))
    key_c$dts1 <- ymd(substr(key_c$SYSDATE,1,10)) - 1
    key_c$dts2 <- ymd(substr(key_c$SYSDATE,1,10)) - 2
    key_c$dts3 <- ymd(substr(key_c$SYSDATE,1,10)) - 3
    # key_c$dts4 <- ymd(substr(key_c$SYSDATE,1,10)) - 4
    # key_c$dts5 <- ymd(substr(key_c$SYSDATE,1,10)) - 5
    # key_c$dts6 <- ymd(substr(key_c$SYSDATE,1,10)) - 6
    # key_c$dts7 <- ymd(substr(key_c$SYSDATE,1,10)) - 7
    
    key_cm <- melt.data.table(key_c, id = c("CUST_NO", "ITEM_NM", "ITEM_CD", "SYSDATE", "V1","V2","V3","V4","V5"))
    system.time(tot_key <- merge(RE_KEYT, key_cm, by.x = c("CUST_NO", "dts0"), by.y = c("CUST_NO", "value"),all = T))
    
    tot_key <- tot_key[!is.na(PARAM_VAL) & !is.na(SYSDATE)]
    tot_key$diff_min <- round(as.numeric(difftime(tot_key$SYSDATE, tot_key$검색시간,  units = "secs")))
    tot_key <- tot_key[diff_min >=0][, diff_min := round(diff_min/60)]
    tot_key$no <- as.character(1:nrow( tot_key))
    tot_key <- tot_key[ , find_yn := grep(paste(c(V1, V2,V3,V4,V5), collapse = "|"), PARAM_VAL), by ="no"]
    tot_key <- tot_key[!is.na(find_yn),]
    
    
    if (nrow(tot_key) == 0 ) {
      
      tot_key2 <- data.table(CUST_NO = RE_LOG_PG_ITEMT$CUST_NO,
                             ITEM_CD = RE_LOG_PG_ITEMT$ITEM_CD,
                             ITEM_NM = RE_LOG_PG_ITEMT$ITEM_NM,
                             최근첫검색시간I = NA,
                             최근2검색시간I = NA,
                             최근3검색시간I = NA,
                             최근첫검색I = NA,
                             최근1일검색I = NA,
                             최근3일검색I = NA,
                             최근7일검색I = NA)
    } else {
      
      tot_key <- tot_key[order(CUST_NO, ITEM_NM, ITEM_CD, SYSDATE, variable, diff_min)]
      tot_key <- tot_key[ , seqn := seq(.N), by = c("CUST_NO", "ITEM_NM", "ITEM_CD", "SYSDATE")]
      tot_key$variable <- as.numeric(gsub("dts","", tot_key$variable))
      
      
      tot_key2 <- tot_key[ , .(최근첫검색시간I = 검색시간[seqn == 1],
                                      최근2검색시간I = 검색시간[seqn == 2],
                                      최근3검색시간I = 검색시간[seqn == 3],
                                      
                                      최근첫검색I = min(variable),
                                      최근1일검색I = sum(variable == 1),
                                      최근3일검색I = sum(variable <= 3),
                                      최근7일검색I = sum(variable <= 7)), by = .(CUST_NO, ITEM_CD, ITEM_NM)]
    }
    
    tot_key2
    
  }, mc.cores = 20))
)




print(dim(RE_LOG_PG_ITEM))
RE_LOG_PG_ITEM <- merge(RE_LOG_PG_ITEM, RE_KEY_CUST_I , by = c("CUST_NO", "ITEM_CD", "ITEM_NM"), all.x = T)
print(dim(RE_LOG_PG_ITEM))


print(paste0(" 4. 로그키워드 데이터 아이템 완료   // 시간 : ", Sys.time())) 
saveRDS(RE_LOG_PG_ITEM, file = paste0("/home/kimwh3/DAT/REORD/ZLOG_RST_DATA/KEY/","ITEM_LOG_KEYFINAL_" , SYSDATE,".RDS"))






#---# 설명을 위한 데이터 저장 #---#
save.image(paste0("/home/kimwh3/DAT/EXP/IMAGE1_1K1.RData"))
# load(paste0("/home/kimwh3/DAT/EXP/IMAGE1_1K1.RData"))

















# if (nrow(RE_LOG_PG_ITEM) > 9999999999) {
# 
#   
# RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[ order(CUST_NO, ITEM_CD, ITEM_NM)]
# setkey(RE_LOG_PG_ITEM, "CUST_NO")
# system.time(RE_LOG_PG_ITEM <- RE_LOG_PG_ITEM[, 고객순서 := (seq_len(.N)), by=c("CUST_NO")])
# RE_LOG_PG_ITEM$고객순서 <- as.character(RE_LOG_PG_ITEM$고객순서)
# 
# 
# 
# custlist <- unique(RE_LOG_PG_ITEM$CUST_NO)
# cust_seq <- seq(1,length(custlist), length.out = 20)
# 
# 
# # length(cust_seq)
# system.time(
#   RE_KEY_CUST_I <- do.call(rbind.fill, lapply(1, function(i) {
#   # i <- 1
#     print(Sys.time())
#   TMP <- RE_LOG_PG_ITEM[CUST_NO %in% custlist[cust_seq[i]:cust_seq[(i+1)]] ]
#   
#   CUST_LISTI <- unique(TMP$고객순서)
#   setkeyv(TMP, c("CUST_NO", "고객순서"))
#   print(length(CUST_LISTI))
#   
#   print(paste0(i, "//  4. 로그키워드 데이터 아이템 시작   // 시간 : ", Sys.time())) 
#   # 2000 
#   # length(CUST_LISTI)
#     system.time(
#       RE_KEY_CUST_I <- do.call(rbind.fill, mclapply(1:length(CUST_LISTI), function(i){
#         
#         # i <- 655
#         print(paste0(i, " // ", length(CUST_LISTI)))
#         
#         gc(T);
#         
#         RE_LOG_PG_ITEMT <- TMP[고객순서 %in% CUST_LISTI[i] ]
#         RE_KEYT         <- RE_KEY[CUST_NO %in% RE_LOG_PG_ITEMT$CUST_NO ]
#         
#         key_c <- RE_LOG_PG_ITEMT[,.(CUST_NO, ITEM_NM, ITEM_CD, V1,V2,V3,V4,V5)]
#         key_c$SYSDATE <- SYSDATE
#         
#         RE_KEYT$dts0 <- ymd(substr(RE_KEYT$검색시간,1,10))
#         key_c$dts0 <- ymd(substr(key_c$SYSDATE,1,10))
#         key_c$dts1 <- ymd(substr(key_c$SYSDATE,1,10)) - 1
#         key_c$dts2 <- ymd(substr(key_c$SYSDATE,1,10)) - 2
#         key_c$dts3 <- ymd(substr(key_c$SYSDATE,1,10)) - 3
#         key_c$dts4 <- ymd(substr(key_c$SYSDATE,1,10)) - 4
#         key_c$dts5 <- ymd(substr(key_c$SYSDATE,1,10)) - 5
#         key_c$dts6 <- ymd(substr(key_c$SYSDATE,1,10)) - 6
#         key_c$dts7 <- ymd(substr(key_c$SYSDATE,1,10)) - 7
#         
#         key_cm <- melt.data.table(key_c, id = c("CUST_NO", "ITEM_NM", "ITEM_CD", "SYSDATE", "V1","V2","V3","V4","V5"))
#         system.time(tot_key <- merge(RE_KEYT, key_cm, by.x = c("CUST_NO", "dts0"), by.y = c("CUST_NO", "value"),all = T))
#         
#         tot_key <- tot_key[!is.na(PARAM_VAL) & !is.na(SYSDATE)]
#         tot_key$diff_min <- round(as.numeric(difftime(tot_key$SYSDATE, tot_key$검색시간,  units = "secs")))
#         tot_key <- tot_key[diff_min >=0][, diff_min := round(diff_min/60)]
#         tot_key$no <- as.character(1:nrow( tot_key))
#         tot_key <- tot_key[ , find_yn := grep(paste(c(V1, V2,V3,V4,V5), collapse = "|"), PARAM_VAL), by ="no"]
#         tot_key <- tot_key[!is.na(find_yn),]
#         
#         
#         if (nrow(tot_key) == 0 ) {
#           
#           tot_key2 <- data.table(CUST_NO = RE_LOG_PG_ITEMT$CUST_NO,
#                                  ITEM_CD = RE_LOG_PG_ITEMT$ITEM_CD,
#                                  ITEM_NM = RE_LOG_PG_ITEMT$ITEM_NM,
#                                  최근첫검색시간I = NA,
#                                  최근2검색시간I = NA,
#                                  최근3검색시간I = NA,
#                                  최근첫검색I = NA,
#                                  최근1일검색I = NA,
#                                  최근3일검색I = NA,
#                                  최근7일검색I = NA)
#         } else {
#           
#           tot_key <- tot_key[order(CUST_NO, ITEM_NM, ITEM_CD, SYSDATE, variable, diff_min)]
#           tot_key <- tot_key[ , seqn := seq(.N), by = c("CUST_NO", "ITEM_NM", "ITEM_CD", "SYSDATE")]
#           tot_key$variable <- as.numeric(gsub("dts","", tot_key$variable))
#           
#           
#           tot_key2 <- tot_key[ , .(최근첫검색시간I = 검색시간[seqn == 1],
#                                           최근2검색시간I = 검색시간[seqn == 2],
#                                           최근3검색시간I = 검색시간[seqn == 3],
#                                           
#                                           최근첫검색I = min(variable),
#                                           최근1일검색I = sum(variable == 1),
#                                           최근3일검색I = sum(variable <= 3),
#                                           최근7일검색I = sum(variable <= 7)), by = .(CUST_NO, ITEM_CD, ITEM_NM)]
#         }
#         gc(T);
#         
#         tot_key2
#         
#       }, mc.cores = 20))
#     )
#   
#   RE_KEY_CUST_I
# 
#  }))
# )
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# print(paste0(" 4. 로그키워드 데이터 아이템 시작   // 시간 : ", Sys.time())) 
# # 2000 
# # length(CUST_LISTI)
# system.time(
#   RE_KEY_CUST_I <- do.call(rbind.fill, mclapply(1:length(CUST_LISTI), function(i){
#     
#     # i <- 655
#     print(paste0(i, " // ", length(CUST_LISTI)))
#     
#     RE_LOG_PG_ITEMT <- RE_LOG_PG_ITEM[고객순서 %in% CUST_LISTI[i] ]
#     RE_KEYT         <- RE_KEY[CUST_NO %in% RE_LOG_PG_ITEMT$CUST_NO ]
#     
#     key_c <- RE_LOG_PG_ITEMT[,.(CUST_NO, ITEM_NM, ITEM_CD, V1,V2,V3,V4,V5)]
#     key_c$SYSDATE <- SYSDATE
#     
#     RE_KEYT$dts0 <- ymd(substr(RE_KEYT$검색시간,1,10))
#     key_c$dts0 <- ymd(substr(key_c$SYSDATE,1,10))
#     key_c$dts1 <- ymd(substr(key_c$SYSDATE,1,10)) - 1
#     key_c$dts2 <- ymd(substr(key_c$SYSDATE,1,10)) - 2
#     key_c$dts3 <- ymd(substr(key_c$SYSDATE,1,10)) - 3
#     # key_c$dts4 <- ymd(substr(key_c$SYSDATE,1,10)) - 4
#     # key_c$dts5 <- ymd(substr(key_c$SYSDATE,1,10)) - 5
#     # key_c$dts6 <- ymd(substr(key_c$SYSDATE,1,10)) - 6
#     # key_c$dts7 <- ymd(substr(key_c$SYSDATE,1,10)) - 7
#     
#     key_cm <- melt.data.table(key_c, id = c("CUST_NO", "ITEM_NM", "ITEM_CD", "SYSDATE", "V1","V2","V3","V4","V5"))
#     system.time(tot_key <- merge(RE_KEYT, key_cm, by.x = c("CUST_NO", "dts0"), by.y = c("CUST_NO", "value"),all = T))
#     
#     tot_key <- tot_key[!is.na(PARAM_VAL) & !is.na(SYSDATE)]
#     tot_key$diff_min <- round(as.numeric(difftime(tot_key$SYSDATE, tot_key$검색시간,  units = "secs")))
#     tot_key <- tot_key[diff_min >=0][, diff_min := round(diff_min/60)]
#     tot_key$no <- as.character(1:nrow( tot_key))
#     tot_key <- tot_key[ , find_yn := grep(paste(c(V1, V2,V3,V4,V5), collapse = "|"), PARAM_VAL), by ="no"]
#     tot_key <- tot_key[!is.na(find_yn),]
#     
#     
#     if (nrow(tot_key) == 0 ) {
#       
#       tot_key2 <- data.table(CUST_NO = RE_LOG_PG_ITEMT$CUST_NO,
#                              ITEM_CD = RE_LOG_PG_ITEMT$ITEM_CD,
#                              ITEM_NM = RE_LOG_PG_ITEMT$ITEM_NM,
#                              최근첫검색시간I = NA,
#                              최근2검색시간I = NA,
#                              최근3검색시간I = NA,
#                              최근첫검색I = NA,
#                              최근1일검색I = NA,
#                              최근3일검색I = NA,
#                              최근7일검색I = NA)
#     } else {
#       
#       tot_key <- tot_key[order(CUST_NO, ITEM_NM, ITEM_CD, SYSDATE, variable, diff_min)]
#       tot_key <- tot_key[ , seqn := seq(.N), by = c("CUST_NO", "ITEM_NM", "ITEM_CD", "SYSDATE")]
#       tot_key$variable <- as.numeric(gsub("dts","", tot_key$variable))
#       
#       
#       tot_key2 <- tot_key[ , .(최근첫검색시간I = 검색시간[seqn == 1],
#                                       최근2검색시간I = 검색시간[seqn == 2],
#                                       최근3검색시간I = 검색시간[seqn == 3],
#                                       
#                                       최근첫검색I = min(variable),
#                                       최근1일검색I = sum(variable == 1),
#                                       최근3일검색I = sum(variable <= 3),
#                                       최근7일검색I = sum(variable <= 7)), by = .(CUST_NO, ITEM_CD, ITEM_NM)]
#     }
#     
#     tot_key2
#     
#   }, mc.cores = 20))
# )
# 
# }


