#source("/home/shared/.cred/rs.R")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)

start <- as.Date("01-01-16",format="%m-%d-%y")
end   <- as.Date("12-31-16",format="%m-%d-%y")

dates <- sample(seq(start, end, by="day"), 40)

rs <- connect_rs()

l_dfs <- lapply(dates, function(x) {sample_a_day(rs,x,users=100) %>% as_tibble()})
