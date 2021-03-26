library(highcharter)
library(dplyr)
library(stringr)
library(purrr)
#######################################################################
#######################################################################
# 차트 타입 설명

n <- 5
set.seed(123)
colors <- c("#d35400", "#2980b9", "#2ecc71", "#f1c40f", "#2c3e50", "#7f8c8d")
colors2 <- c("#000004", "#3B0F70", "#8C2981", "#DE4968", "#FE9F6D", "#FCFDBF")

df <- data.frame(x = seq_len(n) - 1) %>% 
  mutate(
    y = 10 + x + 10 * sin(x),
    y = round(y, 1),
    z = (x*y) - median(x*y),
    e = 10 * abs(rnorm(length(x))) + 2,
    e = round(e, 1),
    low = y - e,
    high = y + e,
    value = y,
    name = sample(fruit[str_length(fruit) <= 5], size = n),
    color = rep(colors, length.out = n),
    segmentColor = rep(colors2, length.out = n)
  )

create_hc <- function(t) {
    dont_rm_high_and_low <- c("arearange", "areasplinerange",
                            "columnrange", "errorbar")
  is_polar <- str_detect(t, "polar")
  t <- str_replace(t, "polar", "")
  
  if(!t %in% dont_rm_high_and_low) df <- df %>% select(-e, -low, -high)
  
  highchart() %>%
    hc_title(text = paste(ifelse(is_polar, "polar ", ""), t),
             style = list(fontSize = "15px")) %>% 
    hc_chart(type = t,
             polar = is_polar) %>% 
    hc_xAxis(categories = df$name) %>% 
    hc_add_series(df, name = "Fruit Consumption", showInLegend = FALSE) 
  }

hcs <- c("line", "spline",  "area", "areaspline",
         "column", "bar", "waterfall" , "funnel", "pyramid",
         "pie" , "treemap", "scatter", "bubble",
         "arearange", "areasplinerange", "columnrange", "errorbar",
         "polygon", "polarline", "polarcolumn", "polarcolumnrange",
         "coloredarea", "coloredline")  %>% 
map(create_hc) 
# http://jkunst.com/highcharter/highcharts.html

#############################################################
##############################################################


data(iris)
data(citytemp)

hc_yAxis(min=0,max=10000) # y축 범위 지정

####################
# 테마정리
####################

hc_add_theme(hc_theme_null()) #빈테마
hc_add_theme(hc_theme_darkunica()) #검은색 테마
hc_add_theme(hc_theme_sandsignika()) #깔끔한 회색 테마
hc_add_theme(hc_theme_ffx()) #파란 그라데이션 테마
hc_add_theme(hc_theme_smpl()) #심플 테마


####################
# line chart 
####################

hc <- highchart() %>% 
  hc_xAxis(categories = citytemp$month) %>%  ## y축 설정하기
  hc_add_series(name = "Tokyo", data = citytemp$tokyo) %>% #line 그래프 추가하기 
  hc_add_series(name = "London", data = citytemp$london) %>% 
  hc_add_series(name = "Other city",
                data = (citytemp$tokyo + citytemp$london)/2)



#3d로 그리기
hc %>% 
  hc_chart(type = "column",
           options3d = list(enabled = T, beta = 30, alpha = 50))
#alpha :  3d  뒤쪽으로 꺽는 각도
#beta : 오른쪽으로 꺽는 각도


#축설정
hc %>% 
#여기부터 x축
  hc_xAxis(title = list(text = "Month in x Axis"), #x축 이름 설정
           opposite = TRUE, # 반대로 그래기
           plotLines = list( #추가적인 참조선 그리기
             list(label = list(text = "This is a plotLine"), #참조선 이름 추가
                  color = "#FF0000", #선색
                  width = 3, #선의 넓이
                  value = 0))) %>% #0부터 11까지 그려지는 것으로 보임 
#여기부터 y축
  hc_yAxis(title = list(text = "Temperature in y Axis"), 
           opposite = TRUE,
           minorTickInterval = "auto", #구분참조선 넣기
           minorGridLineDashStyle = "LongDashDotDot", #점선으로 변경
           showFirstLabel = FALSE, #y축 첫값 표시
           showLastLabel = FALSE) #y축 끝값 표시


#범위형 추가
hc %>% 
  hc_add_series(name = "London", data = citytemp$london, type = "area")


# 부수적 기능 추가
# 제목, legend
hc %>% 
  # 축 제목넣기
  #<i> 기울이기 , <b>진하게
  hc_title(text = "This is a title with <i>margin</i> and <b>Strong or bold text</b>",
           margin = 20, align = "left", #margin : 그래프와의 거리, align : 위치
           style = list(color = "#90ed7d", useHTML = TRUE)) %>%
  # 부제목
  hc_subtitle(text = "And this is a subtitle with more information",
              align = "left",
              style = list(color = "#2b908f", fontWeight = "bold")) %>% 
  hc_credits(enabled = TRUE, # add credits 참고글 넣기
             text = "www.lonk.tomy.site",
             href = "http://jkunst.com") %>%
  # legend 위치 넣기
  hc_legend(align = "left", verticalAlign = "top",
            layout = "vertical", x = 0, y = 100)


####################
# 시계열 차트
####################
library(quantmod)
x <- getSymbols("GOOG", auto.assign = FALSE) #
hchart(x)


highchart(type = "stock") %>% 
  hc_add_series(usdjpy, id = "usdjpy")

# 일짜에 대해서 까지 명확히 필요!
highchart(type = "stock") %>%
  hc_xAxis(categories = citytemp$month) %>%  ## y축 설정하기
  hc_add_series(name = "Tokyo", data = citytemp$tokyo) %>% #line 그래프 추가하기 
  hc_add_series(name = "London", data = citytemp$london) %>% 
  hc_add_series(name = "Other city",
                data = (citytemp$tokyo + citytemp$london)/2)

####################
# 박스차트
####################
data(diamonds, package = "ggplot2")


hcboxplot(x = diamonds$x, var = diamonds$color, 
          name = "Length", color = "#2980b9") 
#var은 그룹에 따른 박스플랏

hcboxplot(x = diamonds$x, var = diamonds$color, var2 = diamonds$cut,
          outliers = FALSE) %>% 
  hc_chart(type = "column") # to put box vertical
#var2 는 그룹을 더 나누고 싶을때
#type = cloumn 은 축 전환

####################
# 막대차트
####################

data("favorite_pies")

highchart() %>% 
  # Data
  hc_add_series(favorite_pies, "column", hcaes(x = pie, y = percent ,color=pie), name = "Pie") %>%
#  hc_add_series(favorite_bars, "pie", hcaes(name = bar, y = percent), name = "Bars") %>%
  # Optiosn for each type of series
  # x축 lengend 제거
  hc_plotOptions(
    series = list(
      showInLegend = FALSE,
      pointFormat = "{point.y}%"
    ),
    # 컬럼별 색 넣기 (hcaes에 color넣는 거랑 같다.)
    column = list(
      colorByPoint = TRUE
    )) %>%
  # Axis
  hc_yAxis(
    title = list(text = "percentage of tastiness"),
    labels = list(format = "{value}%"), max = 100 #% 넣고 max= 100까지만 표시
  ) %>% 
  hc_xAxis(categories = favorite_pies$pie) #x축 값 넣


# 막대와 라인 같이
highchart() %>% 
  hc_add_series(name="ckick", favorite_pies$percent, type="column", color='rgba(255, 192, 203, 0.30)', showInLegend=FALSE) %>% 
  hc_add_series(name="ckick", favorite_pies$percent, type="spline", color="#FFC0Cb") %>% 

  hc_tooltip(pointFormat = "<span style=\"color:{series.color}\">{series.name}</span>:
             {point.y:.1f}<br/>",
             shared = FALSE) %>% 
  hc_yAxis(title=list(text='고객수')) %>% 
  hc_xAxis(title=list(text='상품수'))



####################
# 원도표
####################
data("favorite_bars")
highchart() %>% 
  hc_add_series(favorite_bars, "pie", hcaes(name = bar, y = percent), name = "Bars")
  


####################
# 히트맵
####################
library(treemap)
library(viridisLite)

data(GNI2014)
tm <- treemap(GNI2014, index = c("continent", "iso3"),
              vSize = "population", vColor = "GNI",
              type = "value", palette = viridis(6))

hctreemap(tm)

####################
# 산점도
####################

data(stars)
colors <- c("#FB1108","#FD150B","#FA7806","#FBE426","#FCFB8F",
            "#F3F5E7", "#C7E4EA","#ABD6E6","#9AD2E1")

# 컬러화
stars$color <- colorize(log(stars$temp), colors)

### 클릭했을경우 나타내고 싶은 포맷
x <- c("Luminosity", "Temperature", "Distance") 
y <- sprintf("{point.%s:.2f}", c("lum", "temp", "distance"))
tltip <- tooltip_table(x, y)

# scatter(산점도), hcaes()
hchart(stars, "scatter", hcaes(temp, lum, size = radiussun, color = color)) %>% 
  hc_chart(backgroundColor = "black") %>% 
  hc_xAxis(type = "logarithmic", reversed = TRUE) %>% 
  hc_yAxis(type = "logarithmic", gridLineWidth = 0) %>% 
  hc_title(text = "Our nearest Stars") %>% 
  hc_subtitle(text = "In a Hertzsprung-Russell diagram") %>% 
  hc_tooltip(useHTML = TRUE, headerFormat = "", pointFormat = tltip) %>% 
  hc_size(height = 450, width = 600)

####################
# Weathers Radials
####################

data("weather")

### 커서지정
x <- c("Min", "Mean", "Max")
y <- sprintf("{point.%s}", c("min_temperaturec", "mean_temperaturec", "max_temperaturec"))
tltip <- tooltip_table(x, y)

# 차트그리기
hchart(weather, type = "columnrange",
       hcaes(x = date, low = min_temperaturec, high = max_temperaturec,
             color = mean_temperaturec)) %>% 
  hc_chart(polar = TRUE) %>%
  hc_yAxis( max = 30, min = -10, labels = list(format = "{value} C"),
            showFirstLabel = FALSE) %>% 
  hc_xAxis(
    title = list(text = ""), gridLineWidth = 0.5,
    labels = list(format = "{value: %b}")) %>% 
  hc_tooltip(useHTML = TRUE, pointFormat = tltip,
             headerFormat = as.character(tags$small("{point.x:%d %B, %Y}")))


####################
# 움직임차트
####################

highchart() %>% 
  hc_chart(type = "column") %>% 
  hc_yAxis(max = 6, min = 0) %>% 
  hc_add_series(name = "A", data = c(2,3,4), zIndex = -10) %>% 
  hc_add_series(name = "B",
                data = list(
                  list(sequence = c(1,2,3,4)),
                  list(sequence = c(3,2,1,3)),
                  list(sequence = c(2,5,4,3))
                )) %>% 
  hc_add_series(name = "C",
                data = list(
                  list(sequence = c(3,2,1,3)),
                  list(sequence = c(2,5,4,3)),
                  list(sequence = c(1,2,3,4))
                )) %>% 
  hc_motion(enabled = TRUE,
            labels = 2000:2003,
            series = c(1,2))



library("idbr")
library("purrr")
library("dplyr")
idb_api_key("35f116582d5a89d11a47c7ffbfc2ba309133f09d")
yrs <-  seq(1980, 2030, by = 5)

df <- map_df(c("male", "female"), function(sex){
  mutate(idb1("US", yrs, sex = sex), sex_label = sex)
})

names(df) <- tolower(names(df))

df <- df %>%
  mutate(population = pop*ifelse(sex_label == "male", -1, 1))

series <- df %>% 
  group_by(sex_label, age) %>% 
  do(data = list(sequence = .$population)) %>% 
  ungroup() %>% 
  group_by(sex_label) %>% 
  do(data = .$data) %>%
  mutate(name = sex_label) %>% 
  list_parse()

maxpop <- max(abs(df$population))

xaxis <- list(categories = sort(unique(df$age)),
              reversed = FALSE, tickInterval = 5,
              labels = list(step = 5))

highchart() %>%
  hc_chart(type = "bar") %>%
  hc_motion(enabled = TRUE, labels = yrs, series = c(0,1), autoplay = TRUE, updateInterval = 1) %>% 
  hc_add_series_list(series)
