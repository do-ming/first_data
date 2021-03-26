install.packages(repos = NULL, type = "source")

install.packages("openxlsx")

library(openxlsx)
rm(list=ls())
data<-read.xlsx("2. 정제 데이터.xlsx",colNames=F)

dyn.load('/Library/Java/JavaVirtualMachines/jdk-13.0.1.jdk/Contents/Home/lib/libjli.dylib')
Sys.setenv("JAVA_HOME"='/Library/Java/JavaVirtualMachines/jdk-13.0.1.jdk/Contents/Home')

Sys.getenv("DYLD_FALLBACK_LIBRARY_PATH")<-c('/Library/Java/JavaVirtualMachines/jdk-13.0.1.jdk/Contents/Home')

options("JAVA_HOME"='/Library/Java/JavaVirtualMachines/jdk-13.0.1.jdk/Contents/Home')
Sys.setenv(LD_LIBRARY_PATH='$JAVA_HOME/server')
dyn.load('/Library/Java/JavaVirtualMachines/jdk-9.0.4.jdk/Contents/Home/lib/server/libjvm.dylib')

library(rJava)

### 패키지 설치 
library(rJava)
if(!require(rJava)) {install.packages("rJava"); library(rJava)} 

if(!require(devtools)) {install.packages("devtools")} 

if(!require(tm)) {install.packages("tm"); library(tm)}

if(!require(KoNLP)) {install.packages("KoNLP"); library(KoNLP)} 

if(!require(stringr)) {install.packages("stringr"); library(stringr)}

if(!require(rvest)) {install.packages("rvest"); library(rvest)}

if(!require(slam)) {install.packages("slam"); library(slam)}

if(!require(topicmodels)) {install.packages("topicmodels"); library(topicmodels)}

if(!require(lda)) {install.packages("lda"); library(lda)}

if(!require(LDAvis)) {install.packages("LDAvis"); library(LDAvis)}

if(!require(servr)) {install.packages("servr"); library(servr)}

if(!require(qgraph)) {install.packages("qgraph"); library(qgraph)}

if(!require(ggplot2)) {install.packages("ggplot2"); library(ggplot2)}

if(!require(wordcloud)) {install.packages("wordcloud"); library(wordcloud)}

if(!require(RColorBrewer)) {install.packages("RColorBrewer"); library(RColorBrewer)}

if(!require(treemap)) {install.packages("treemap"); library(treemap)}

if(!require(reshape2)) {install.packages("reshape2"); library(reshape2)}

if(!require(ggplot2)) {install.packages("ggplot2"); library(ggplot2)} 

if(!require(dplyr)) {install.packages("dplyr"); library(dplyr)} 





#한글로 시작하는 것만 갖고오기

content.clean <- sapply(content, function(content) gsub("[^가-힣]"," ",data$X1))



# 사전 불러오기

useSejongDic()



# 텍스트 전처리 하기

content.cps <- VCorpus(VectorSource(content.clean)) #corpus화 시켜 저장

content.cps.clean <- content.cps #content.cps.clean에 데이터 저장

content.cps.clean <- tm_map(content.cps.clean, stripWhitespace) # corpus의 Whitespace(빈공간) 제거

content.cps.clean <- tm_map(content.cps.clean, removePunctuation) # corpus의 punctuation(.) 제거

content.cps.clean <- tm_map(content.cps.clean, removeNumbers) # corpus 숫자 제거

content.cps.clean <- tm_map(content.cps.clean, content_transformer(tolower)) #전부 소문자로 통일

content.cps.clean <- tm_map(content.cps.clean, removeWords, # 특정 단어 제거 - 영어 불용어(stopwords)
                            
                            stopwords("english"))



# 명사 추출 함수 만들기

NewsNoun <- function(doc) {
  
  d <- as.character(doc)
  
  extractNoun(d) }



# 단어-문서 행렬 생성 

content.TDM <- TermDocumentMatrix(content.cps.clean,
                                  
                                  control=list(tokenize=NewsNoun, #명사 추출함수 적용 
                                               
                                               removePunctuation=T, #점제거
                                               
                                               removeNumbers=T, #숫자제거
                                               
                                               wordLengths=c(2, 5), #단어길이 2~5사이
                                               
                                               weighting=weightTf)) #단어 출현 횟수



content.TDM.matrix <- as.matrix(content.TDM) #메트릭스화 시키기



### 빈도 분석

content.wordcount <- as.array(rollup(content.TDM, 2)) #단어들의 합 계산 

content.wordorder <- order(content.wordcount, decreasing = T) #합 기준 내림차순 정렬 



v1<-data.frame(content.wordcount)

v1$voca <- rownames(v1)

v1 <- arrange(v1,desc(X1))

names(v1) <- c("freq","word")



# 빈도 기반 막대그래프

ggplot(v1[1:15,], aes(x = reorder(word,freq), y= freq, fill = freq)) +
  
  geom_bar(stat = 'identity') +
  
  coord_flip() +
  
  theme_classic() +
  
  ylab("빈도") + xlab("키워드") + ggtitle("텍스트 빈도 그래프") +
  
  scale_fill_gradientn(colours = c("#99CCFF", "#0066CC"), values = c(0,0.2,1))



### 워드클라우드 준비하기

palete <- brewer.pal(8,"Set2") #색 지정하기

windowsFonts(malgun=windowsFont("맑은 고딕")) #글자지정



word.order1 <- v1$freq[1:100] #상위 100개 갖고오기

names(word.order1) <- v1$word[1:100]



#워드클라우드 그리기

wordcloud(names(word.order1), freq=word.order1, rot.per=0.3,scale=c(3,1), 
          
          min.freq=10, random.order=F, random.color=T, colors=palete, family="malgun") #최소 10회 이상 단어 랜덤으로 그리기 





### 동시출현 행렬 (소셜 네트워크)

word.order1 <- order(word.count1, decreasing=T) #빈도수 기반 내림차순 정렬

word.freq1 <- content.TDM.matrix[word.order1[1:20],] #상위 20개 단어 문서 행렬 생성

co.matrix1 <- word.freq1 %*% t(word.freq1) #단어 x 단어 행렬 만들기



library(qgraph)

plot1 <- qgraph(co.matrix1, labels=rownames(co.matrix1), diag=F, #
                
                layout='spring', edge.color='darkblue', vsize=log(diag(co.matrix1))*1.0)





### 토픽분석



# LDA 토픽분석 (수정)



content.TDM <- removeSparseTerms(content.TDM, sparse=0.95) #5% 미만 단어 제거

plan_tdm_1 <- content.TDM[,slam::col_sums(content.TDM)>0] #단어별 합 저장 (결측치 제거)

dtm <- as.DocumentTermMatrix(plan_tdm_1) #단어 행렬 만들기



library(topicmodels)

lda <- LDA(dtm, k = 4, control=list(seed=123456)) # find 10 topic

(term <- terms(lda, 10)) #주제별 10개의 단어





## LDA 시각화 시키기

x <- posterior(lda)$terms #LDA 단어 저장하기

library(ggplot2)



y <- data.frame(t(x[, apply(x, 2, max) > 0.03]))

z <- data.frame(type=paste("Topic", 1), 
                
                keyword=rownames(y), posterior=y[,1])

for(i in 2:4){
  
  z <- rbind(z, data.frame(type=paste("Topic", i),  
                           
                           keyword=rownames(y), posterior=y[,i]))
  
}

ggplot(z, aes(keyword, posterior, fill=as.factor(keyword)))+
  
  geom_bar(position="dodge",stat="identity")+
  
  coord_flip() + 
  
  facet_wrap(~type,nrow=1) +
  
  theme(legend.position="none")


dyn.load(file, DLLpath = DLLpath)
install.packages("rJava")
