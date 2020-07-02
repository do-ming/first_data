# https://github.com/tidyverse/dtplyr
# https://www.business-science.io/code-tools/2019/08/15/big-data-dtplyr.html

library(data.table)
library(dtplyr)
library(dplyr, warn.conflicts = FALSE)

data(iris)
iris1 <- dtplyr::lazy_dt(iris)
head(iris1)

mtcars2 %>% 
  filter(wt < 5) %>% 
  dplyr::mutate(l100k = 235.21 / mpg) %>% # liters / 100 km
  dplyr::group_by(cyl) %>% 
  dplyr::summarise(l100k = mean(l100k))


# Why is dtplyr slower than data.table?
#   There are three primary reasons that dtplyr will always be somewhat slower than data.table:
#   
#   Each dplyr verb must do some work to convert dplyr syntax to data.table syntax. This takes time proportional to the complexity of the input code, not the input data, so should be a negligible overhead for large datasets. Initial benchmarks suggest that the overhead should be under 1ms per dplyr call.
# 
# Some data.table expressions have no direct dplyr equivalent. For example, there’s no way to express cross- or rolling-joins with dplyr.
# 
# To match dplyr semantics, mutate() does not modify in place by default. This means that most expressions involving mutate() must make a copy that would not be necessary if you were using data.table directly. (You can opt out of this behaviour in lazy_dt() with immutable = FALSE).
# 
# Code of Conduct