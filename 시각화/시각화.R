# 시각화 정리

#1 원도표
####################################
####################################

options(warn=-1)
Sys.setlocale(category = "LC_CTYPE", locale = "ko_KR.UTF-8")
library(dplyr)
library(lubridate)
library(tidyr)
library(ggplot2)
library(RColorBrewer)

blank_theme <- theme_minimal(base_family = "NanumGothic")+
  theme(
    axis.title.x = element_blank(),
    axis.title.y = element_blank(),
    panel.border = element_blank(),
    panel.grid=element_blank(),
    axis.ticks = element_blank(),
    plot.title=element_text(size=20, face="bold",hjust = 0.5)
  )


dat = srch_target %>% dplyr::group_by(age) %>% dplyr::summarize(age_cnt = n()) %>% na.omit()
dat$fraction = dat$age_cnt / sum(dat$age_cnt)
dat$ymax = cumsum(dat$fraction)
dat$ymin = c(0, head(dat$ymax, n=-1))
p1 = ggplot(dat, aes(ymax=ymax, ymin=ymin, xmax=4, xmin=3)) +
  geom_rect(aes(fill=age),alpha=0.6) +
  coord_polar(theta="y") +
  xlim(c(1, 4)) 

p_gender<-p1  + 
  blank_theme +
  theme(axis.text.x=element_blank()) + theme(legend.position=c(.5, .5)) + ggtitle("나이 분포 _ 전체고객") +
  theme(panel.grid=element_blank()) +
  theme(axis.text=element_blank()) +
  theme(axis.ticks=element_blank()) +
  theme(legend.title = element_text(size=15, face="bold" ,family = "DejaVuSerif")) +
  theme(legend.text = element_text(size = 14, family = "DejaVuSerif")) 

p_gender <- p_gender + 
  geom_label(aes(label=paste(round(fraction,3)*100,"%"),x=3.5,y=(ymin+ymax)/2),inherit.aes = TRUE, show.legend = FALSE,size=3) +
  scale_fill_manual(values = brewer.pal(7,"Accent")) +
  labs(fill = paste0('전체 : ',prettyNum(length(unique(srch_target$CUST_NO)),big.mark = ","),'명'))

##################################
##################################

geom_label(stat='count',aes(label=..count..), size=7) #막대차트 개수 추가

# 테마 관련 링크
#http://www.sthda.com/english/wiki/ggplot2-themes-and-background-colors-the-3-elements


# https://www.kaggle.com/headsortails/be-my-guest-recruit-restaurant-eda
# https://www.kaggle.com/kailex/r-eda-for-gstore-glm-keras-xgb
# https://www.kaggle.com/jaseziv83/a-deep-dive-eda-into-all-variables


theme_set(theme_gray(base_size = 20))



library(flexdashboard)
library(ggplot2)
library(readr)
library(highcharter)
library(dplyr)

train <- read_csv('train (3).csv')
train$Pclass <- factor(train$Pclass, labels = c("1st", "2nd", "3rd"))
train$Embarked <- factor(train$Embarked, labels = c("Cherbourg", "Queenstown", "Southhampton"))

tmp_male <- train %>%  filter(Sex=="male", !is.na(Age)) %>% select(Age) %>% .[[1]]
b <- hist(tmp_male, 20, plot=FALSE)
tmp_female <- train %>%  filter(Sex=="female", !is.na(Age)) %>% select(Age) %>% .[[1]]
a <- hist(tmp_female, breaks = b$breaks, plot=FALSE)
df <- data.frame(Age=c(a$mids,b$mids),Density=c(a$density,b$density),Sex=c(rep("female",length(a$mids)),rep("male",length(b$mids))))

highchart() %>% 
  hc_add_series(name="female", select(filter(df,Sex=="female"),Density)[[1]], type="column", color='rgba(255, 192, 203, 0.30)', showInLegend=FALSE) %>% 
  hc_add_series(name="male", select(filter(df,Sex=="male"),Density)[[1]], type="column", color='rgba(68, 170, 255, 0.30)', showInLegend=FALSE) %>% 
  hc_add_series(name="male", select(filter(df,Sex=="male"),Density)[[1]], type="spline", color="#44AAFF") %>% 
  hc_add_series(name="female", select(filter(df,Sex=="female"),Density)[[1]], type="spline", color="#FFC0Cb") %>% 
  hc_tooltip(pointFormat = "<span style=\"color:{series.color}\">{series.name}</span>:
             {point.y:.3f}<br/>",
             shared = FALSE) %>% 
  hc_yAxis(title=list(text='Density')) %>% 
  hc_xAxis(title=list(text='Age'))   


####################################
####################################
####### highcharter #########
# https://www.kaggle.com/tavoosi/suicide-data-full-interactive-dashboard

library(flexdashboard) # Dashboard package
library(highcharter) # Interactive data visualizations
library(plotly) # Interactive data visualizations
library(viridis) # Color gradients
library(tidyverse) # Metapackge
library(countrycode) # Converts country names/codes
library(rjson) # JSON reader
library(crosstalk) # Provides interactivity for HTML widgets
library(DT) # Displaying data tables

data <- read.csv('master.csv') %>%
  filter(year != 2016, # filter out 2016 and countries with 0 data. 
         country != 'Dominica',
         country != 'Saint Kitts and Nevis')

data <- data %>%
  mutate(country = fct_recode(country, "The Bahamas" = "Bahamas"),
         country = fct_recode(country, "Cape Verde" = "Cabo Verde"),
         country = fct_recode(country, "South Korea" = "Republic of Korea"),
         country = fct_recode(country, "Russia" = "Russian Federation"),
         country = fct_recode(country, "Republic of Serbia" = "Serbia"),
         country = fct_recode(country, "United States of America" = "United States"))

data$age <- factor(data$age, levels = c("5-14 years", "15-24 years", "25-34 years", "35-54 years", "55-74 years", "75+ years"))


custom_theme <- hc_theme(
  colors = c('#5CACEE', 'green', 'red'),
  chart = list(
    backgroundColor = '#FAFAFA', 
    plotBorderColor = "black"),
  xAxis = list(
    gridLineColor = "E5E5E5", 
    labels = list(style = list(color = "#333333")), 
    lineColor = "#E5E5E5", 
    minorGridLineColor = "#E5E5E5", 
    tickColor = "#E5E5E5", 
    title = list(style = list(color = "#333333"))), 
  yAxis = list(
    gridLineColor = "#E5E5E5", 
    labels = list(style = list(color = "#333333")), 
    lineColor = "#E5E5E5", 
    minorGridLineColor = "#E5E5E5", 
    tickColor = "#E5E5E5", 
    tickWidth = 1, 
    title = list(style = list(color = "#333333"))),   
  title = list(style = list(color = '#333333', fontFamily = "Lato")),
  subtitle = list(style = list(color = '#666666', fontFamily = "Lato")),
  legend = list(
    itemStyle = list(color = "#333333"), 
    itemHoverStyle = list(color = "#FFF"), 
    itemHiddenStyle = list(color = "#606063")), 
  credits = list(style = list(color = "#666")),
  itemHoverStyle = list(color = 'gray'))


# Create tibble for our line plot.  
overall_tibble <- data %>%
  dplyr::select(year, suicides_no, population) %>%
  dplyr::group_by(year) %>%
  dplyr::summarise(suicide_capita = round((sum(suicides_no)/sum(population))*100000, 2)) 


# Create a line plot.
highchart() %>% 
  hc_add_series(overall_tibble, hcaes(x = year, y = suicide_capita, color = suicide_capita), type = "line") %>%
  hc_tooltip(crosshairs = TRUE, borderWidth = 1.5, headerFormat = "", pointFormat = paste("Year: <b>{point.x}</b> <br> Suicides: <b>{point.y}</b>")) %>%
  hc_title(text = "Worldwide suicides by year") %>%  ## 제목
  hc_subtitle(text = "1985-2015") %>% ## 서브제목
  hc_xAxis(title = list(text = "Year")) %>%  ## x축
  hc_yAxis(title = list(text = "Suicides per 100K people"), # y축
           allowDecimals = F,
           plotLines = list(list(
             color = "black", width = 1, dashStyle = "Dash", 
             value = mean(overall_tibble$suicide_capita),
             label = list(text = "Mean = 13.12",   ### 참조선
                          style = list(color = "black", fontSize = 11))))) %>%
  hc_legend(enabled = FALSE) %>% #x축 제목 삭제(부제목)
  hc_add_theme(custom_theme)

##############################
##############################
####### 구분한 line그래프

# Create tibble for sex so we can use it when creating our line plot.  
sex_tibble <- data %>%
  dplyr::select(year, sex, suicides_no, population) %>%
  group_by(year, sex) %>%
  dplyr::summarise(suicide_capita = round((sum(suicides_no)/sum(population))*100000, 2))

# Pick color for gender.
sex_color <- c("#EE6AA7", "#87CEEB") # baby blue & pink

# Create line plot.
highchart() %>% 
  hc_add_series(sex_tibble, hcaes(x = year, y = suicide_capita, group = sex), type = "line", color = sex_color) %>%
  hc_tooltip(crosshairs = TRUE, borderWidth = 1.5, headerFormat = "", pointFormat = paste("Year: <b>{point.x}</b> <br>","Gender: <b>{point.sex}</b><br>", "Suicides: <b>{point.y}</b>")) %>%
  hc_title(text = "Worldwide suicides by Gender") %>% 
  hc_subtitle(text = "1985-2015") %>%
  hc_xAxis(title = list(text = "Year")) %>%
  hc_yAxis(title = list(text = "Suicides per 100K people"),
           allowDecimals = FALSE,
           plotLines = list(list(
             color = "black", width = 1, dashStyle = "Dash",
             value = mean(overall_tibble$suicide_capita),
             label = list(text = "Mean = 13.12", 
                          style = list(color = 'black', fontSize = 11))))) %>% 
  hc_add_theme(custom_theme)



########################################################
########################################################
################# 원그래프 ##############
# Grab worldwide number of suicides per 100K people from the data
total_suicides <- round(mean(overall_tibble$suicide_capita), 2)

# First, make a tibble of suicide by sex. We will use this for our pie chart.
pie_sex <- data %>%
  dplyr::select(sex, suicides_no, population) %>%
  group_by(sex) %>%
  dplyr::summarise(suicide_capita = round((sum(suicides_no)/sum(population))*100000, 2))


# Create pie chart for sex. 
highchart() %>% 
  hc_add_series(pie_sex, hcaes(x = sex, y = suicide_capita, 
                               color = sex_color), type = "pie") %>%
  hc_tooltip(borderWidth = 1.5, headerFormat = "", pointFormat = paste("Gender: <b>{point.sex} ({point.percentage:.1f}%)</b> <br> Suicides per 100K: <b>{point.y}</b>")) %>%
  hc_title(text = "<b>Worldwide suicides by Gender</b>", style = (list(fontSize = '14px'))) %>%  #제목
  hc_subtitle(text = "1985-2015", style = (list(fontSize = '10px'))) %>% #부제목
  hc_plotOptions(pie = list(dataLabels = list(distance = 5, 
                                              style = list(fontSize = 20)), 
                            size = 350)) %>% 
  hc_add_theme(custom_theme)

#############################################
#############################################
############# 막대 그래프 ##################

# Create tibble for overall suicides by country
country_bar <- data %>%
  dplyr::select(country, suicides_no, population) %>%
  dplyr::group_by(country) %>%
  dplyr::summarise(suicide_capita = round((sum(suicides_no)/sum(population))*100000, 2)) %>%
  dplyr::arrange(desc(suicide_capita))

# Create interactive bar plot
highchart() %>%
  hc_add_series(country_bar, hcaes(x = country, y = suicide_capita, color = suicide_capita), type = "bar")  %>% 
  hc_tooltip(borderWidth = 1.5, 
             pointFormat = paste("Suicides: <b>{point.y}</b>")) %>%
  hc_legend(enabled = FALSE) %>%
  hc_title(text = "Suicides by country") %>%  ## 주제목  
  hc_subtitle(text = "1985-2015") %>% # 부제목
  hc_xAxis(categories = country_bar$country, 
           labels = list(step = 1),
           min = 0, max = 25,  #보이는 갯수 확인하기
           scrollbar = list(enabled = TRUE)) %>% # 스크롤 만들기
  hc_yAxis(title = list(text = "Suicides per 100K people")) %>%
  hc_plotOptions(bar = list(stacking = "normal", 
                            pointPadding = 0, groupPadding = 0, borderWidth = 0.5)) %>% 
  hc_add_theme(custom_theme)
