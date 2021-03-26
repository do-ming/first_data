# 월예측
# .rs.restartR()

# 패키지 불러오기
library(data.table)
library(plyr)
library(dplyr)
library(ggplot2)
library(stringr)
library(lubridate)
library(MASS)
library(RcppRoll)
library(parallel)
library(doSNOW)
library(doParallel)

# db 접속정보 
source(file = "/home/DTA_CALL/DB_CON.R")


# 에러텀 계산필요시 YES, 미래예측값만 뿌릴시 NO 
TEST_YN <- "NO"
TSR <- 1
TSR_STRT_DT <- "20160801"
#----------------------------------------------------------------
# 1. 데이터 호출 
#----------------------------------------------------------------
gc(T)


yr_df  <- data.frame(YEAR = seq.Date(ymd("2011-01-01"), ymd(paste0(substr(as.character(Sys.time()), 1, 7), "-01")), by = "year"  ))
mon_df <- data.frame(MON = seq.Date(ymd("2011-01-01"), ymd(paste0(substr(as.character(Sys.time()), 1, 7), "-01")), by = "month"  ))

LAST_DT <- last(mon_df$MON) - 1
STRT_DT <- yr_df$YEAR[nrow(yr_df) -  5]

if (STRT_DT > ymd(TSR_STRT_DT)) {TSR_STRT_DT <- gsub( "-", "",STRT_DT)}

# 모델 RUN 날짜 
(SYSDATE <- as.character(Sys.time()))

#----------------------------------------------------------------
CALL_df <- dbGetQuery(conn, paste0("WITH VW_OZ_CALLS_HOURLY AS
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
                                    WHERE STD_YM >= '", substr(gsub("-", "", STRT_DT),1,6),"' AND STD_YM <= '", substr(gsub("-", "", LAST_DT),1,6),"'
                                    ORDER BY 1,2,3"))

X_df <- dbGetQuery(conn, paste0("SELECT YEAR_MON_CD
                                 , CASE WHEN FESTA_YN = 'Y' THEN 1 ELSE 0 END AS SPCL_YN                                 -- 특집여부
                                , SUM(CASE WHEN HOLDY_NM IS NOT NULL THEN 1 ELSE 0 END) AS REAL_HOL_CNT -- CNT          -- 주말제외 휴무일
                                , SUM(CASE WHEN HOLDY_YN = 'Y' THEN 1 ELSE 0 END)       AS REAL_HOL_WEK_CNT -- CNT      -- 주말포함 휴무일  
                                 , SUM(DISTINCT CASE WHEN SPC_HOLDY_YN = 'Y' THEN 1 ELSE 0 END) AS SPC_HOL_YN -- HOL_YN  -- 명절여부
                                , CASE WHEN SUM(CASE WHEN (HOLDY_NM LIKE '구정%' OR HOLDY_NM LIKE '추석%' OR HOLDY_NM LIKE '설날%') OR HOLDY_NM LIKE '대체%' THEN 1 ELSE 0 END) > 2 THEN 
                                 SUM(CASE WHEN (HOLDY_NM LIKE '구정%' OR HOLDY_NM LIKE '추석%' OR HOLDY_NM LIKE '설날%') OR HOLDY_NM LIKE '대체%' THEN 1 ELSE 0 END) ELSE 0 END AS SPC_HOL_CNT -- 대체휴일포함 명절
                                
                                 , SUM(CASE WHEN WEKDAY_NM = 'Sat' THEN 1 ELSE 0 END) AS SAT_CNT  -- 토요일 갯수
                                , SUM(CASE WHEN WEKDAY_NM = 'Sun' THEN 1 ELSE 0 END) AS SUN_CNT  -- 일요일 갯수
                                , COUNT(DISTINCT DT_CD) AS DAY_CNT  -- 날짜 갯수
                                
                                 FROM DTA_OWN.CMM_STD_DT
                                 WHERE YEAR_MON_CD >= '" , substr(gsub( "-", "", STRT_DT),1,6) , "'
                                 
                                 GROUP BY YEAR_MON_CD, CASE WHEN FESTA_YN = 'Y' THEN 1 ELSE 0 END 
                                 ORDER BY YEAR_MON_CD, CASE WHEN FESTA_YN = 'Y' THEN 1 ELSE 0 END 
                                 "))

# TEXT SR 건수 테이블 
TSR_df <- dbGetQuery(conn, paste0("
                                   SELECT SUBSTR(STD_DT,1,6) AS STD_YM, SUM(TSR_CNT) AS CALL_CNT  
                                   FROM DTA_OWN.GSTS_TSR_1M 
                                   WHERE STD_DT >= '", TSR_STRT_DT ,"' AND SUBSTR(STD_DT,1,6) <= '", substr(gsub("-", "", LAST_DT),1,6) ,"'  
                                   GROUP BY SUBSTR(STD_DT,1,6)
                                   ORDER BY SUBSTR(STD_DT,1,6)"))


# 모바일 주문 건수 테이블 
MCPC_df <- dbGetQuery(conn, paste0("
                                    SELECT /*+PARALLEL(A 6) */
                                    SUBSTR(ORD_DAY, 1, 6) AS ORD_YM
                                    , SUM(CASE WHEN A.TOT_ORD_CNT > 0 THEN 1 ELSE 0 END) AS TOT_ORD_CNT
                                    , SUM(CASE WHEN A.NET_ORD_CNT > 0 THEN 1 ELSE 0 END) AS NET_ORD_CNT
                                    , SUM(CASE WHEN A.TOT_ORD_CNT > 0  AND MEDIA IN ('B','P') THEN 1 ELSE 0 END) AS TOT_ORD_MCPC_CNT
                                    , SUM(CASE WHEN A.NET_ORD_CNT > 0  AND MEDIA IN ('B','P') THEN 1 ELSE 0 END) AS NET_ORD_MCPC_CNT
                                    , SUM(CASE WHEN A.TOT_ORD_CNT > 0  AND B.MNG_MEDIA = 'C' THEN 1 ELSE 0 END) AS TOT_ORD_CA_CNT
                                    , SUM(CASE WHEN A.NET_ORD_CNT > 0  AND B.MNG_MEDIA = 'C' THEN 1 ELSE 0 END) AS NET_ORD_CA_CNT  
                                    , SUM(CASE WHEN A.TOT_ORD_CNT > 0  AND MEDIA IN ('B','P') AND B.MNG_MEDIA = 'C' THEN 1 ELSE 0 END) AS TOT_ORD_MCPC_CA_CNT
                                    , SUM(CASE WHEN A.NET_ORD_CNT > 0  AND MEDIA IN ('B','P') AND B.MNG_MEDIA = 'C' THEN 1 ELSE 0 END) AS NET_ORD_MCPC_CA_CNT
                                    
                                    FROM DHUB_SM.TEMP_ORD_NBS_2010 A
                                    LEFT JOIN DHUB_OWN.VW_DH_PRD B ON A.GOODS_CODE = B.PRD_CD
                                    WHERE A.ORD_DAY BETWEEN '", "20120101", "' AND '20131231'
                                    -- AND A.GIFT_TYPE  IN ('00', '04', '05')  --일반상품만(배송비등 제외용)
                                    AND B.PRD_CLS_CD NOT IN ('A39110100', '70602001', 'B31010511')   --상품권제외
                                   AND B.PRD_CLS_CD NOT LIKE 'B310111%'  --상품권제외
                                   AND A.GOODS_CODE !=  900000 --가주문상품 제외  
                                    GROUP BY  SUBSTR(ORD_DAY, 1, 6)
                                    --ORDER BY  SUBSTR(ORD_DAY, 1, 6)
                                    UNION ALL
                                    
                                    SELECT /*+PARALLEL(A 6) */
                                    A.ORD_YM
                                    , SUM(CASE WHEN A.TOT_ORD_NO > 0 THEN 1 ELSE 0 END) AS TOT_ORD_CNT
                                    , SUM(CASE WHEN A.NET_ORD_NO > 0 THEN 1 ELSE 0 END) AS NET_ORD_CNT
                                    , SUM(CASE WHEN A.TOT_ORD_NO > 0  AND CHANL_CD IN ('B','P') THEN 1 ELSE 0 END) AS TOT_ORD_MCPC_CNT
                                    , SUM(CASE WHEN A.NET_ORD_NO > 0  AND CHANL_CD IN ('B','P') THEN 1 ELSE 0 END) AS NET_ORD_MCPC_CNT
                                    , SUM(CASE WHEN A.TOT_ORD_NO > 0  AND A.SRCNG_GBN_CD = 'CA' THEN 1 ELSE 0 END) AS TOT_ORD_CA_CNT
                                    , SUM(CASE WHEN A.NET_ORD_NO > 0  AND A.SRCNG_GBN_CD = 'CA' THEN 1 ELSE 0 END) AS NET_ORD_CA_CNT 
                                    , SUM(CASE WHEN A.TOT_ORD_NO > 0  AND CHANL_CD IN ('B','P') AND A.SRCNG_GBN_CD = 'CA' THEN 1 ELSE 0 END) AS TOT_ORD_MCPC_CA_CNT
                                    , SUM(CASE WHEN A.NET_ORD_NO > 0  AND CHANL_CD IN ('B','P') AND A.SRCNG_GBN_CD = 'CA' THEN 1 ELSE 0 END) AS NET_ORD_MCPC_CA_CNT
                                    -- , SUM(CASE WHEN A.TOT_ORD_NO > 0  AND CHANL_CD IN ('B','P') AND A.SRCNG_GBN_CD = 'CA' AND A.BROAD_ORD_TIME_DTL_CD = 'O1' THEN 1 ELSE 0 END) AS TOT_ORD_MCPC_CA_ONT_CNT
                                    -- , SUM(CASE WHEN A.NET_ORD_NO > 0  AND CHANL_CD IN ('B','P') AND A.SRCNG_GBN_CD = 'CA' AND A.BROAD_ORD_TIME_DTL_CD = 'O1' THEN 1 ELSE 0 END) AS NET_ORD_MCPC_CA_ONT_CNT
                                    FROM  GSBI_OWN.F_ORD_ORD_D A
                                    WHERE A.ORD_DT BETWEEN '20140101'AND '", gsub("-", "", LAST_DT), "'
                                    GROUP BY A.ORD_YM"))



#----------------------------------------------------------------

#----------------------------------------------------------------
dbDisconnect(conn)
rm(conn,drv)


#----------------------------------------------------------------
# 2. 마트구축  
#----------------------------------------------------------------

# CALL_df <- CALL_df %>% filter(기준일자 >= 20160101 & 기준일자 < 20180701)
CALL_df$기준일자 <- ymd(CALL_df$기준일자)

CALL_df$기준월 <- ymd(paste0(CALL_df$기준월, "01"))
CALL_df <- data.table(CALL_df)
#
CALL_mon_df <- CALL_df[,.(
  총_인입콜수 = sum(총_인입콜수),
  ARS_인입콜수 = sum(ARS_인입콜수),
  상담원_인입콜수 = sum(상담원_인입콜수),
  상담원_주문_인입콜수 = sum(상담원_주문_인입콜수),
  상담원_SR_인입콜수 = sum(상담원_SR_인입콜수)
  # 날짜수 = length(unique(기준일자))
), by = "기준월"]

CALL_mon_df$C_R <- CALL_mon_df$상담원_인입콜수/ CALL_mon_df$총_인입콜수
CALL_mon_df$C_R_OD <- CALL_mon_df$상담원_주문_인입콜수/ CALL_mon_df$총_인입콜수
CALL_mon_df$C_R_SR <- CALL_mon_df$상담원_SR_인입콜수/ CALL_mon_df$총_인입콜수
#
CALL_mon_df <- rbind.fill(CALL_mon_df, data.table(기준월 = seq.Date(last(mon_df$MON), ymd(last(mon_df$MON) + 45), by = "month")  ) )


ROLL_TSR_df <- data.frame(TSR_df, 
                          기준월 = ymd(paste0(TSR_df$STD_YM, "01")))
ROLL_TSR_df <- rbind.fill(ROLL_TSR_df, data.frame(기준월 = seq.Date(last(mon_df$MON), ymd(last(mon_df$MON) + 45), by = "month")  ) )

TSR_rm <- c(NA,NA, roll_mean(ROLL_TSR_df$CALL_CNT,3, na.rm=TRUE))

ROLL_TSR_df$ROLL3AVG = round(c(NA,NA, TSR_rm[-((length(TSR_rm)-1):length(TSR_rm))]))



#
C_R_rm <- c(NA,NA, roll_mean(CALL_mon_df$C_R,3, na.rm=TRUE))
C_R_OD_rm <- c(NA,NA, roll_mean(CALL_mon_df$C_R_OD,3, na.rm=TRUE))
C_R_SR_rm <- c(NA,NA, roll_mean(CALL_mon_df$C_R_SR,3, na.rm=TRUE))

#
CALL_mon_df$C_R_rm <- c(NA,NA, C_R_rm[-((length(C_R_rm)-1):length(C_R_rm))])
CALL_mon_df$C_R_OD_rm <- c(NA,NA, C_R_OD_rm[-((length(C_R_OD_rm)-1):length(C_R_OD_rm))])
CALL_mon_df$C_R_SR_rm <- c(NA,NA, C_R_SR_rm[-((length(C_R_SR_rm)-1):length(C_R_SR_rm))])


# 모바일 주문 생성 
MCPC_df <- MCPC_df %>% arrange(ORD_YM )

## 모바일 비중 3개월 이평 ##
MCPC_df$MCPC_RAT <- round(MCPC_df$TOT_ORD_MCPC_CA_CNT/MCPC_df$TOT_ORD_CA_CNT,3)
MCPC_RAT_rm <- c(NA,NA, roll_mean(MCPC_df$MCPC_RAT,3, na.rm=TRUE))
MCPC_df <- rbind.fill(MCPC_df, data.frame(ORD_YM = substr(gsub("-", "", seq.Date(last(mon_df$MON), ymd(last(mon_df$MON) + 45), by = "month")  ),1,6) ))
MCPC_df$MCPC_VAR <- round(c(NA,NA, MCPC_RAT_rm ),3)

## 모바일 콜 3개월 이평 ## 
# MCPC_df$MCPC_RAT <- MCPC_df$TOT_ORD_MCPC_CA_CNT
# MCPC_RAT_rm <- c(NA,NA, roll_mean(MCPC_df$MCPC_RAT,3, na.rm=TRUE))
# MCPC_df <- rbind.fill(MCPC_df, data.frame(ORD_YM = substr(gsub("-", "", seq.Date(last(mon_df$MON), ymd(last(mon_df$MON) + 45), by = "month")  ),1,6) ))
# MCPC_df$MCPC_VAR <- round(c(NA,NA, MCPC_RAT_rm ))

X_df <- merge(X_df, MCPC_df[,c("ORD_YM","MCPC_VAR")], by.x = "YEAR_MON_CD", by.y = "ORD_YM", all.x = T)
rm(MCPC_RAT_rm)


###### 모바일 콜 시계열 예측 후 x인자 활용  ######
MCPC_df$MCPC_CALL <- MCPC_df$TOT_ORD_MCPC_CA_CNT

# data_comp <- decompose(ts((round(MCPC_df$TOT_ORD_MCPC_CA_CNT/MCPC_df$TOT_ORD_CA_CNT,3)), frequency = 12, start = c(2012, 1)))
# plot(data_comp)
# grid()
# data_comp <- decompose(ts((MCPC_df$TOT_ORD_MCPC_CA_CNT), frequency = 12, start = c(2012, 1)))
# plot(data_comp)
# grid()

mX_list <- c("SPCL_YN", "REAL_HOL_CNT",
             "REAL_HOL_WEK_CNT", "SPC_HOL_YN", "SPC_HOL_CNT",
             "SAT_CNT", "SUN_CNT", "DAY_CNT")
ma <- mX_list[c(1,2,4,5)] 

mcpc_rst <- do.call(rbind, lapply(0:0, function(r) {
  print(r)
  # r <- 2
  # load(file = paste0("/home/DTA_CALL/TIME2.RData"))
  ## 최종 선정모델 
  gc(T)
  name <- "TOT_ORD_MCPC_CA_CNT" ; print(name); print("방송상품_모바일콜수")
  system.time( mf_pred_df <- do.call(rbind, lapply(r, function(k){
    # k <- r
    
    day_df <- data.frame(MON = seq.Date(ymd(STRT_DT), ymd("20501201"), by = "months"  ))
    which_pt <- which(day_df$MON == last(mon_df$MON))
    ORD_MON <- c(day_df$MON[which_pt - k ], day_df$MON[which_pt  - k + 1], day_df$MON[which_pt  - k + 2] )
    # 데이터 분할  
    # head(CALL_mon_df)
    yyy <- as.vector(unlist(MCPC_df %>% filter(!is.na(TOT_ORD_MCPC_CA_CNT)) %>% dplyr::select(name)))
    data_df <- yyy
    minus   <- k
    prd_minus <- 3
    data_df <- yyy[1:(length(yyy) - minus )]
    # if(minus == 0) {actual <- NA } else {actual  <- yyy[(length(yyy) - (prd_minus - 1) ) : length(yyy)]}
    if(minus == 0) {actual <- NA } else {actual  <- yyy[(length(yyy) - (minus - 1) ) : (length(yyy)) ][1:prd_minus] }
    
    ## 수정함
    xreg <- as.matrix(X_df[1:length(data_df), ma]  ) 
    
    
    if(minus == 0) {newxreg <- as.matrix(X_df[(length(data_df)+1):(length(yyy) + prd_minus), ma] ) 
    
    } else {
      newxreg <- as.matrix(X_df[(length(data_df)+1):(length(data_df) + prd_minus), ma] ) 
      
      
    }      
    
    xreg[,4] <- sqrt(xreg[,4])
    newxreg[,4] <- sqrt(newxreg[,4])
    
    # 
    expand_df <- expand.grid(p = 0:3, d = 0:3, q = 0:3, P = 0:1, D = 0:2, Q = 0:1 , period = c(0,12))
    
    # 63초 
    system.time(
      result_df <- do.call(rbind.fill , mclapply(1:nrow(expand_df), function(i) {
        # i = 1000
        order_df <- expand_df[i,]
        
        # print(paste(k, "  ", i, " // ",  nrow(expand_df), "  /// ", order_df$p,order_df$d,order_df$q, order_df$P,order_df$D,order_df$Q, order_df$period))
        
        # 시계열 적합
        WB.fit <- tryCatch(arima(log10(data_df), order=c(order_df$p,order_df$d,order_df$q), 
                                 seasonal=list(order=c(order_df$P,order_df$D,order_df$Q), period=order_df$period),
                                 method = c("CSS-ML", "ML", "CSS"), xreg = xreg),
                           error = function(e) print("NOT"),
                           warning = function(w) print("NOT"))
        
        
        
        if (WB.fit == "NOT") {
          RESULT_df <- data.frame(NO = i, order_df,  ORD_MON = "999999", real = 0, pred = 0, se = 0, aic = 0, 
                                  m3 = 0, m6 = 0, m12 = 0, m18 = 0)
          names(RESULT_df) <- c("NO", "p", "d", "q", "P", "D", "Q", "PERIOD", "ORD_MON",  "real", "pred" , "se" ,"aic", 
                                "최3M","최6M","최12M","최18M")
          RESULT_df
        } else {
          
          # 추정값
          fitted_value <- data_df - WB.fit$residuals 
          fitted_value <- as.vector(round(fitted_value))
          # 정확도 %
          fitted_perc  <- round( (data_df - abs(WB.fit$residuals))/data_df , 4)
          fitted_perc <- as.vector((fitted_perc))
          
          avg_fit3  <- mean( rev( fitted_perc )[ 1:3  ] )
          avg_fit6  <- mean( rev( fitted_perc )[ 1:6  ] )
          avg_fit12 <- mean( rev( fitted_perc )[ 1:12  ] )
          avg_fit18 <- mean( rev( fitted_perc )[ 1:18  ] )
          
          # avg_fit3;avg_fit6;avg_fit12;avg_fit18
          
          wb_fcast <- predict(WB.fit, n.ahead = prd_minus , newxreg = newxreg)
          
          RESULT_df <- cbind.data.frame(ORD_MON = ORD_MON,
                                        real  = c(actual, rep(NA, nrow(newxreg) - length(actual) )  ),
                                        pred  = round(10^as.numeric((wb_fcast$pred))) ,
                                        se    = as.numeric((wb_fcast$se)),
                                        aic   = WB.fit$aic,
                                        m3 = avg_fit3,
                                        m6 = avg_fit6,
                                        m12 = avg_fit12,
                                        m18 = avg_fit18)
          RESULT_df <- cbind.data.frame(NO = i, order_df, RESULT_df)
          names(RESULT_df) <- c("NO", "p", "d", "q", "P", "D", "Q", "PERIOD", "ORD_MON",  "real", "pred" , "se" ,"aic",
                                "최3M","최6M","최12M","최18M")
          
        }
        RESULT_df
        
      }, mc.cores = 10 ))
    )
    
    # save(yr_df, file = paste0("/home/DTA_CALL/MON_MDL_BACKUP/TIME_SERIES_select_", name, "__", ORD_MON[2],".RData" ))
    
    # 베스트 모델 찾기 aic 기준 
    result_df$ORD_MON <- as.character(result_df$ORD_MON)
    result_df$gap <- ifelse(is.na(result_df$real), NA, abs(result_df$real - result_df$pred) )
    result_df2 <- result_df %>% filter(ORD_MON == ORD_MON[1]) %>% dplyr::select(NO, ORD_MON, p,d,q,P,D,Q,PERIOD, aic, real, pred, gap, 최3M, 최6M, 최12M, 최18M)
    result_df2 <- unique(result_df2)
    result_df2 <- result_df2 %>% arrange(aic) ; result_df2$aic_no <- 1:nrow(result_df2)
    
    result_df2 <- result_df2 %>% arrange(aic_no)
    
    max_pt <- 20
    # 최종 선택 가중치 
    sample_df  <- result_df %>% filter(NO %in% result_df2$NO[1:max_pt])
    sample_df0 <- result_df %>% filter(NO %in% result_df2$NO[1:5])
    sample_df1 <- result_df %>% filter(NO %in% result_df2$NO[1:10])
    
    resulttop_df <- data.frame(
      topall = data.frame(tapply(sample_df$pred, sample_df$ORD_MON, mean)),
      top5  = data.frame(tapply(sample_df0$pred, sample_df0$ORD_MON, mean)),
      top10 = data.frame(tapply(sample_df1$pred, sample_df1$ORD_MON, mean))
      
    )
    names(resulttop_df) <- c("topall", "top5", "top10")
    names(resulttop_df) <- paste0("aic_", names(resulttop_df))
    
    tmp <- data.frame(ORD_MON = ORD_MON , no = 1:3, 실제콜 = actual, name, resulttop_df)
    
    tmp
    
  })) )
  
  
  mf_pred_df0 <- (mf_pred_df %>% filter(no %in% c(1,2,3) ))
  mf_pred_df00 <-    data.frame(기준월 = substr(gsub("-", "", mf_pred_df0$ORD_MON[1]),1,6),
                                   YEAR_MON_CD = substr(gsub("-", "", mf_pred_df0$ORD_MON),1,6), 
                                   SEQ   = 6,
                                   GUBUN = c("모바일방송상품콜"),
                                   mf_pred_df0)
  #
  mf_pred_df00[,c("aic_topall", "aic_top5", "aic_top10")] <- round(mf_pred_df00[,c("aic_topall", "aic_top5", "aic_top10")])
  mf_pred_df00 <- mf_pred_df00 %>% dplyr::select(기준월, YEAR_MON_CD, SEQ, GUBUN, aic_top5, no)
  names(mf_pred_df00)[which(names(mf_pred_df00) == "aic_top5")] <- "MCPC_VAR"
  mf_pred_df00
}))
rm(mX_list, ma)
# names(mcpc_rst)[which(names(mcpc_rst) == "인입콜수")] <- "MCPC_VAR"

# 
X_df <- merge(X_df, MCPC_df[,c("ORD_YM","TOT_ORD_MCPC_CA_CNT")], by.x = "YEAR_MON_CD", by.y = "ORD_YM", all.x = T)
names(X_df)[which(names(X_df) == "TOT_ORD_MCPC_CA_CNT")] <- "MCPC_CALL"

###### 모바일 콜 시계열 예측 후 x인자 활용  ######

#----------------------------------------------------------------
CALL_mon_df <- data.table(CALL_mon_df )
rm(C_R_rm, C_R_OD_rm, C_R_SR_rm, TSR_rm)
rm(CALL_df)

#----------------------------------------------------------------
# 3. 모델링 예측수행  
#----------------------------------------------------------------
X_list <- c("SPCL_YN", "REAL_HOL_CNT",
            "REAL_HOL_WEK_CNT", "SPC_HOL_YN", "SPC_HOL_CNT",
            "SAT_CNT", "SUN_CNT", "DAY_CNT",
            "MCPC_VAR", "MCPC_CALL")
#### 선택 2개 모델 적재 ####
merge_aft <- 1
# 전체 
a <- X_list[c(1,5,7,8)]
# 특집, 공휴일, 명절수
b <- X_list[c(1,2,5,7,8)]
#### 선택 2개 모델 적재 ####

# save.image(file = paste0("/home/DTA_CALL/TIME1.RData"))
# load(file = paste0("/home/DTA_CALL/TIME1.RData"))



# for(r in c(16:0)) {
# load(file = paste0("/home/DTA_CALL/TIME1.RData"))
r <- 0
print(r)

## 3차모델1순위 선정모델 
gc(T) 
name <- "총_인입콜수" ; print(name); print(as.character(Sys.time())) ; print("3차모델1순위 변수 1_5_7_8")
system.time( final_select_tot_df <- do.call(rbind, lapply(r, function(k){
  # k <- 0
  
  day_df <- data.frame(MON = seq.Date(ymd(STRT_DT), ymd("20501201"), by = "months"  ))
  which_pt <- which(day_df$MON == last(mon_df$MON))
  ORD_MON <- c(day_df$MON[which_pt - k ], day_df$MON[which_pt  - k + 1], day_df$MON[which_pt  - k + 2] )
  # 데이터 분할  
  # head(CALL_mon_df)
  yyy <- as.vector(unlist(CALL_mon_df %>% filter(!is.na(총_인입콜수)) %>% dplyr::select(name)))
  data_df <- yyy
  minus   <- k
  prd_minus <- 3
  data_df <- yyy[1:(length(yyy) - minus )]
  # if(minus == 0) {actual <- NA } else {actual  <- yyy[(length(yyy) - (prd_minus - 1) ) : length(yyy)]}
  if(minus == 0) {actual <- NA } else {actual  <- yyy[(length(yyy) - (minus - 1) ) : (length(yyy)) ][1:prd_minus] }
  
  ## 수정함
  xreg <- as.matrix(X_df[1:length(data_df), a]  ) 
  
  if(minus == 0) {
    newxreg <- as.matrix(X_df[(length(data_df)+1):(length(yyy) + prd_minus), a] ) 
    if(sum(merge_aft == 1 & a %in% "MCPC_CALL") == 1 ){
      mcpc_now <- mcpc_rst[mcpc_rst$기준월 ==  substr(gsub( "-", "", (ORD_MON[1])), 1, 6) ,]
      if(sum(length(a) == 1 &  a == "MCPC_CALL") == 1 ) {newxreg[,1] <-  mcpc_now$MCPC_VAR } else{
        newxreg[,c("MCPC_CALL")] <- mcpc_now$MCPC_VAR }
    }
  } else {
    newxreg <- as.matrix(X_df[(length(data_df)+1):(length(data_df) + prd_minus), a] ) 
    if(sum(merge_aft == 1 & a %in% "MCPC_CALL") == 1 ){
      mcpc_now <- mcpc_rst[mcpc_rst$기준월 ==  substr(gsub( "-", "", (ORD_MON[1])), 1, 6) ,]
      if(sum(length(a) == 1 &  a == "MCPC_CALL") == 1) {newxreg[,1] <-  mcpc_now$MCPC_VAR } else{newxreg[,c("MCPC_CALL")] <- mcpc_now$MCPC_VAR }
    }
  }      
  
  # xreg[,4] <- sqrt(xreg[,4])
  # newxreg[,4] <- sqrt(newxreg[,4])
  
  # 
  expand_df <- expand.grid(p = 0:3, d = 0:3, q = 0:3, P = 0:1, D = 0:2, Q = 0:1 , period = c(0,12))
  
  # 63초 
  system.time(
    result_df <- do.call(rbind.fill , mclapply(1:nrow(expand_df), function(i) {
      # i = 1000
      order_df <- expand_df[i,]
      
      # print(paste(k, "  ", i, " // ",  nrow(expand_df), "  /// ", order_df$p,order_df$d,order_df$q, order_df$P,order_df$D,order_df$Q, order_df$period))
      
      # 시계열 적합
      WB.fit <- tryCatch(arima(log10(data_df), order=c(order_df$p,order_df$d,order_df$q), 
                               seasonal=list(order=c(order_df$P,order_df$D,order_df$Q), period=order_df$period),
                               method = c("CSS-ML", "ML", "CSS"), xreg = xreg),
                         error = function(e) print("NOT"),
                         warning = function(w) print("NOT"))
      
      
      
      if (WB.fit == "NOT") {
        RESULT_df <- data.frame(NO = i, order_df,  ORD_MON = "999999", real = 0, pred = 0, se = 0, aic = 0, 
                                m3 = 0, m6 = 0, m12 = 0, m18 = 0)
        names(RESULT_df) <- c("NO", "p", "d", "q", "P", "D", "Q", "PERIOD", "ORD_MON",  "real", "pred" , "se" ,"aic", 
                              "최3M","최6M","최12M","최18M")
        RESULT_df
      } else {
        
        # 추정값
        fitted_value <- data_df - WB.fit$residuals 
        fitted_value <- as.vector(round(fitted_value))
        # 정확도 %
        fitted_perc  <- round( (data_df - abs(WB.fit$residuals))/data_df , 4)
        fitted_perc <- as.vector((fitted_perc))
        
        avg_fit3  <- mean( rev( fitted_perc )[ 1:3  ] )
        avg_fit6  <- mean( rev( fitted_perc )[ 1:6  ] )
        avg_fit12 <- mean( rev( fitted_perc )[ 1:12  ] )
        avg_fit18 <- mean( rev( fitted_perc )[ 1:18  ] )
        
        # avg_fit3;avg_fit6;avg_fit12;avg_fit18
        
        wb_fcast <- predict(WB.fit, n.ahead = prd_minus , newxreg = newxreg)
        
        RESULT_df <- cbind.data.frame(ORD_MON = ORD_MON,
                                      real  = c(actual, rep(NA, nrow(newxreg) - length(actual) )  ),
                                      pred  = round(10^as.numeric((wb_fcast$pred))) ,
                                      se    = as.numeric((wb_fcast$se)),
                                      aic   = WB.fit$aic,
                                      m3 = avg_fit3,
                                      m6 = avg_fit6,
                                      m12 = avg_fit12,
                                      m18 = avg_fit18)
        RESULT_df <- cbind.data.frame(NO = i, order_df, RESULT_df)
        names(RESULT_df) <- c("NO", "p", "d", "q", "P", "D", "Q", "PERIOD", "ORD_MON",  "real", "pred" , "se" ,"aic",
                              "최3M","최6M","최12M","최18M")
        
      }
      RESULT_df
      
    }, mc.cores = 10 ))
  )
  
  # save(yr_df, file = paste0("/home/DTA_CALL/MON_MDL_BACKUP/TIME_SERIES_select_", name, "__", ORD_MON[2],".RData" ))
  
  # 베스트 모델 찾기 aic 기준 
  result_df$ORD_MON <- as.character(result_df$ORD_MON)
  result_df$gap <- ifelse(is.na(result_df$real), NA, abs(result_df$real - result_df$pred) )
  result_df2 <- result_df %>% filter(ORD_MON == ORD_MON[1]) %>% dplyr::select(NO, ORD_MON, p,d,q,P,D,Q,PERIOD, aic, real, pred, gap, 최3M, 최6M, 최12M, 최18M)
  result_df2 <- unique(result_df2)
  result_df2 <- result_df2 %>% arrange(aic) ; result_df2$aic_no <- 1:nrow(result_df2)
  
  result_df2 <- result_df2 %>% arrange(aic_no)
  
  max_pt <- 20
  # 최종 선택 가중치 
  sample_df  <- result_df %>% filter(NO %in% result_df2$NO[1:max_pt])
  sample_df0 <- result_df %>% filter(NO %in% result_df2$NO[1:5])
  sample_df1 <- result_df %>% filter(NO %in% result_df2$NO[1:10])
  
  resulttop_df <- data.frame(
    topall = data.frame(tapply(sample_df$pred, sample_df$ORD_MON, mean)),
    top5  = data.frame(tapply(sample_df0$pred, sample_df0$ORD_MON, mean)),
    top10 = data.frame(tapply(sample_df1$pred, sample_df1$ORD_MON, mean))
    
  )
  names(resulttop_df) <- c("topall", "top5", "top10")
  names(resulttop_df) <- paste0("aic_", names(resulttop_df))
  
  tmp <- data.frame(ORD_MON = ORD_MON , no = 1:3, 실제콜 = actual, name, resulttop_df)
  
  tmp
  
})) )
gc(T) 

## 3차모델2순위 선정모델 
gc(T) 
name <- "총_인입콜수" ; print(name) ; print(as.character(Sys.time())) ;  print("3차모델2순위 변수 1_2_5_7_8")
system.time( final_tot_df <- do.call(rbind, lapply(r, function(k){
  # k <- 0
  
  day_df <- data.frame(MON = seq.Date(ymd(STRT_DT), ymd("20501201"), by = "months"  ))
  which_pt <- which(day_df$MON == last(mon_df$MON))
  ORD_MON <- c(day_df$MON[which_pt - k ], day_df$MON[which_pt  - k + 1], day_df$MON[which_pt  - k + 2] )
  # 데이터 분할  
  # head(CALL_mon_df)
  yyy <- as.vector(unlist(CALL_mon_df %>% filter(!is.na(총_인입콜수)) %>% dplyr::select(name)))
  data_df <- yyy
  minus   <- k
  prd_minus <- 3
  data_df <- yyy[1:(length(yyy) - minus )]
  # if(minus == 0) {actual <- NA } else {actual  <- yyy[(length(yyy) - (prd_minus - 1) ) : length(yyy)]}
  if(minus == 0) {actual <- NA } else {actual  <- yyy[(length(yyy) - (minus - 1) ) : (length(yyy)) ][1:prd_minus] }
  
  xreg <- as.matrix(X_df[1:length(data_df), b]  ) 
  
  
  if(minus == 0) {
    newxreg <- as.matrix(X_df[(length(data_df)+1):(length(yyy) + prd_minus), b] ) 
    if(sum(merge_aft == 1 & b %in% "MCPC_CALL") == 1 ){
      mcpc_now <- mcpc_rst[mcpc_rst$기준월 ==  substr(gsub( "-", "", (ORD_MON[1])), 1, 6) ,]
      if(sum(length(b) == 1 &  b == "MCPC_CALL") == 1 ) {newxreg[,1] <-  mcpc_now$MCPC_VAR } else{
        newxreg[,c("MCPC_CALL")] <- mcpc_now$MCPC_VAR }
    }
  } else {
    newxreg <- as.matrix(X_df[(length(data_df)+1):(length(data_df) + prd_minus), b] ) 
    if(sum(merge_aft == 1 & b %in% "MCPC_CALL") == 1 ){
      mcpc_now <- mcpc_rst[mcpc_rst$기준월 ==  substr(gsub( "-", "", (ORD_MON[1])), 1, 6) ,]
      if(sum(length(b) == 1 &  b == "MCPC_CALL") == 1) {newxreg[,1] <-  mcpc_now$MCPC_VAR } else{newxreg[,c("MCPC_CALL")] <- mcpc_now$MCPC_VAR }
    }
  }   
  
  # 
  expand_df <- expand.grid(p = 0:3, d = 0:3, q = 0:3, P = 0:1, D = 0:2, Q = 0:1 , period = c(0,12))
  
  # 63초 
  system.time(
    result_df <- do.call(rbind.fill , mclapply(1:nrow(expand_df), function(i) {
      # i = 1000
      order_df <- expand_df[i,]
      
      # print(paste(k, "  ", i, " // ",  nrow(expand_df), "  /// ", order_df$p,order_df$d,order_df$q, order_df$P,order_df$D,order_df$Q, order_df$period))
      
      # 시계열 적합
      WB.fit <- tryCatch(arima(log10(data_df), order=c(order_df$p,order_df$d,order_df$q), 
                               seasonal=list(order=c(order_df$P,order_df$D,order_df$Q), period=order_df$period),
                               method = c("CSS-ML", "ML", "CSS"), xreg = xreg),
                         error = function(e) print("NOT"),
                         warning = function(w) print("NOT"))
      
      
      
      if (WB.fit == "NOT") {
        RESULT_df <- data.frame(NO = i, order_df,  ORD_MON = "999999", real = 0, pred = 0, se = 0, aic = 0, 
                                m3 = 0, m6 = 0, m12 = 0, m18 = 0)
        names(RESULT_df) <- c("NO", "p", "d", "q", "P", "D", "Q", "PERIOD", "ORD_MON",  "real", "pred" , "se" ,"aic", 
                              "최3M","최6M","최12M","최18M")
        RESULT_df
      } else {
        
        # 추정값
        fitted_value <- data_df - WB.fit$residuals 
        fitted_value <- as.vector(round(fitted_value))
        # 정확도 %
        fitted_perc  <- round( (data_df - abs(WB.fit$residuals))/data_df , 4)
        fitted_perc <- as.vector((fitted_perc))
        
        avg_fit3  <- mean( rev( fitted_perc )[ 1:3  ] )
        avg_fit6  <- mean( rev( fitted_perc )[ 1:6  ] )
        avg_fit12 <- mean( rev( fitted_perc )[ 1:12  ] )
        avg_fit18 <- mean( rev( fitted_perc )[ 1:18  ] )
        
        # avg_fit3;avg_fit6;avg_fit12;avg_fit18
        
        wb_fcast <- predict(WB.fit, n.ahead = prd_minus , newxreg = newxreg)
        
        RESULT_df <- cbind.data.frame(ORD_MON = ORD_MON,
                                      real  = c(actual, rep(NA, nrow(newxreg) - length(actual) )  ),
                                      pred  = round(10^as.numeric((wb_fcast$pred))),
                                      se    = as.numeric((wb_fcast$se)),
                                      aic   = WB.fit$aic,
                                      m3 = avg_fit3,
                                      m6 = avg_fit6,
                                      m12 = avg_fit12,
                                      m18 = avg_fit18)
        RESULT_df <- cbind.data.frame(NO = i, order_df, RESULT_df)
        names(RESULT_df) <- c("NO", "p", "d", "q", "P", "D", "Q", "PERIOD", "ORD_MON",  "real", "pred" , "se" ,"aic",
                              "최3M","최6M","최12M","최18M")
        
      }
      RESULT_df
      
    }, mc.cores = 10 ))
  )
  
  # save(yr_df, file = paste0("/home/DTA_CALL/MON_MDL_BACKUP/TIME_SERIES_select_", name, "__", ORD_MON[2],".RData" ))
  
  # 베스트 모델 찾기 aic 기준 
  result_df$ORD_MON <- as.character(result_df$ORD_MON)
  result_df$gap <- ifelse(is.na(result_df$real), NA, abs(result_df$real - result_df$pred) )
  result_df2 <- result_df %>% filter(ORD_MON == ORD_MON[1]) %>% dplyr::select(NO, ORD_MON, p,d,q,P,D,Q,PERIOD, aic, real, pred, gap, 최3M, 최6M, 최12M, 최18M)
  result_df2 <- unique(result_df2)
  result_df2 <- result_df2 %>% arrange(aic) ; result_df2$aic_no <- 1:nrow(result_df2)
  
  result_df2 <- result_df2 %>% arrange(aic_no)
  
  max_pt <- 20
  # 최종 선택 가중치 
  sample_df  <- result_df %>% filter(NO %in% result_df2$NO[1:max_pt])
  sample_df0 <- result_df %>% filter(NO %in% result_df2$NO[1:5])
  sample_df1 <- result_df %>% filter(NO %in% result_df2$NO[1:10])
  
  resulttop_df <- data.frame(
    topall = data.frame(tapply(sample_df$pred, sample_df$ORD_MON, mean)),
    top5  = data.frame(tapply(sample_df0$pred, sample_df0$ORD_MON, mean)),
    top10 = data.frame(tapply(sample_df1$pred, sample_df1$ORD_MON, mean))
    
  )
  names(resulttop_df) <- c("topall", "top5", "top10")
  names(resulttop_df) <- paste0("aic_", names(resulttop_df))
  
  tmp <- data.frame(ORD_MON = ORD_MON , no = 1:3, 실제콜 = actual, name, resulttop_df)
  
  tmp
  
})) )
gc(T)
#----------------------------------------------------------------  

#----------------------------------------------------------------  
# 결과 정리 중  
#----------------------------------------------------------------  
print("결과 정리 중   ")

print("3차모델1순위 시계열") ; print(as.character(Sys.time()))
#####------- 3차모델1순위 시계열 -------#####  
TMP_select_basic <- (final_select_tot_df %>% filter(no == 2))
TMP_select_basic_sum <- TMP_select_basic[,c("aic_topall", "aic_top5", "aic_top10")]
TMP_select_basic_all <- rbind(TMP_select_basic_sum, 
                              data.frame(TMP_select_basic_sum * (1 -  tail(CALL_mon_df$C_R_rm, r + 1 )[(1)] ) ),
                              data.frame(TMP_select_basic_sum *  tail(CALL_mon_df$C_R_rm, r + 1 )[(1)] ),
                              data.frame(TMP_select_basic_sum *  tail(CALL_mon_df$C_R_OD_rm, r + 1 )[(1)] ) ,
                              data.frame(TMP_select_basic_sum *  tail(CALL_mon_df$C_R_SR_rm, r + 1 )[(1)] ) )


TMP_select_basic_all <-    data.frame(기준년 = substr(TMP_select_basic$ORD_MON[1],1,4),
                                         기준월 = substr(gsub("-", "", TMP_select_basic$ORD_MON[1]),1,6), 
                                         SEQ   = 1:5,
                                         GUBUN = c("총콜", "ARS콜", "상담콜", "상담주문콜", "상담SR콜"),
                                         TMP_select_basic_all)
#
TMP_select_basic_all[,c("aic_topall", "aic_top5", "aic_top10")] <- round(TMP_select_basic_all[,c("aic_topall", "aic_top5", "aic_top10")])
TMP_select_basic_all <- TMP_select_basic_all %>% dplyr::select(기준년, 기준월, SEQ, GUBUN, aic_top5)
names(TMP_select_basic_all)[which(names(TMP_select_basic_all) == "aic_top5")] <- "인입콜수"
TMP_select_basic_all <- TMP_select_basic_all %>% mutate(인입비중 = round(인입콜수/인입콜수[SEQ == 1], 3) )
TMP_select_basic_all 
#####------- 3차모델1순위 시계열 -------#####

print("3차모델2순위 시계열") ; print(as.character(Sys.time()))
#####------- 3차모델2순위 시계열 -------#####  
TMP_basic <- (final_tot_df %>% filter(no == 2))
TMP_basic_sum <- TMP_basic[,c("aic_topall", "aic_top5", "aic_top10")]
TMP_basic_all <- rbind(TMP_basic_sum, 
                       data.frame(TMP_basic_sum * (1 -  tail(CALL_mon_df$C_R_rm, r + 1 )[(1)] ) ),
                       data.frame(TMP_basic_sum *  tail(CALL_mon_df$C_R_rm, r + 1 )[(1)] ),
                       data.frame(TMP_basic_sum *  tail(CALL_mon_df$C_R_OD_rm, r + 1 )[(1)] ) ,
                       data.frame(TMP_basic_sum *  tail(CALL_mon_df$C_R_SR_rm, r + 1 )[(1)] ) )


TMP_basic_all <-    data.frame(기준년 = substr(TMP_basic$ORD_MON[1],1,4),
                                  기준월 = substr(gsub("-", "", TMP_basic$ORD_MON[1]),1,6), 
                                  SEQ   = 1:5,
                                  GUBUN = c("총콜", "ARS콜", "상담콜", "상담주문콜", "상담SR콜"),
                                  TMP_basic_all)
#
TMP_basic_all[,c("aic_topall", "aic_top5", "aic_top10")] <- round(TMP_basic_all[,c("aic_topall", "aic_top5", "aic_top10")])
TMP_basic_all <- TMP_basic_all %>% dplyr::select(기준년, 기준월, SEQ, GUBUN, aic_top5)
names(TMP_basic_all)[which(names(TMP_basic_all) == "aic_top5")] <- "인입콜수"
TMP_basic_all <- TMP_basic_all %>% mutate(인입비중 = round(인입콜수/인입콜수[SEQ == 1], 3) )
TMP_basic_all 
#####------- 3차모델2순위 시계열 -------#####
print(as.character(Sys.time()))


#----------------------------------------------------------------  

SYSDATE <- mon_df$MON[(length(mon_df$MON) - r) ]
SYSDATE <- ymd_hms(paste0(SYSDATE, " 08:00:00"))

# 결과 백업 
# save.image(file = paste0("/home/DTA_CALL/MON_MDL_BACKUP/CALL_TIME_BAT__",
#                          gsub( "-", "",substr(SYSDATE, 1,10)), "_",c("일", "월", "화", "수", "목", "금", "토")[wday(SYSDATE)] ,".RData" ))
print("결과 저장")
#####------- 결과 저장 -------#####  
print(as.character(Sys.time()))
source(file = "/home/DTA_CALL/DB_CON_etl.R")

for (i in 1:nrow(TMP_select_basic_all)) { 
  
  # i <- 1
  print(paste(i, "//", nrow(TMP_select_basic_all)))
  
  query.x <- paste0( "INSERT INTO DTA_OWN.CALL_MONTHLY_PRD_TB_1578 
                      (기준년,기준월,SEQ,GUBUN,인입콜수,인입비중,ETL_DATE,ETL_HOUR,ETL_WEK) 
                      VALUES ('",
                     TMP_select_basic_all$기준년[i], "','", 
                     TMP_select_basic_all$기준월[i], "',", 
                     TMP_select_basic_all$SEQ[i], ",'", 
                     TMP_select_basic_all$GUBUN[i], "',", 
                     TMP_select_basic_all$인입콜수[i], ",", 
                     TMP_select_basic_all$인입비중[i], ",'",
                     substr(SYSDATE, 1,10), "','",
                     substr(SYSDATE, 12,13), "','",
                     c("일", "월", "화", "수", "목", "금", "토")[wday(SYSDATE)], "')"  )  
  
  tryCatch(dbSendUpdate(conn, query.x), error = function(e) print("NOT"), warning = function(w) print("NOT"))
  # try(dbSendQuery(conn, query.x))
} 
for (i in 1:nrow(TMP_basic_all)) { 
  
  # i <- 1
  print(paste(i, "//", nrow(TMP_basic_all)))
  
  query.x <- paste0( "INSERT INTO DTA_OWN.CALL_MONTHLY_PRD_TB_12578 
                      (기준년,기준월,SEQ,GUBUN,인입콜수,인입비중,ETL_DATE,ETL_HOUR,ETL_WEK) 
                      VALUES ('",
                     TMP_basic_all$기준년[i], "','", 
                     TMP_basic_all$기준월[i], "',", 
                     TMP_basic_all$SEQ[i], ",'", 
                     TMP_basic_all$GUBUN[i], "',", 
                     TMP_basic_all$인입콜수[i], ",", 
                     TMP_basic_all$인입비중[i], ",'",
                     substr(SYSDATE, 1,10), "','",
                     substr(SYSDATE, 12,13), "','",
                     c("일", "월", "화", "수", "목", "금", "토")[wday(SYSDATE)], "')"  )  
  
  tryCatch(dbSendUpdate(conn, query.x), error = function(e) print("NOT"), warning = function(w) print("NOT"))
  # try(dbSendQuery(conn, query.x))
}

#####------- 결과 저장 -------##### 
dbDisconnect(conn)
rm(conn,drv)

print("결과 적재완료")
print(as.character(Sys.time()))

rm(list = ls())
# }

save.image(file = "/home/DTA_CALL/AWS/DAT/MON/CALL_MON3.RData")
load(file = "/home/DTA_CALL/AWS/DAT/MON/CALL_MON3.RData") 
