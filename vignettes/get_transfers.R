#source("/home/shared/.cred/rs.R")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)
library(readr)
library(dplyr)
library(lubridate)

start <- as.Date("01-01-16",format="%m-%d-%y")
end   <- as.Date("03-31-16",format="%m-%d-%y")

dates <- seq(start, end, by="day")

rs <- connect_rs()

l_dfs <- lapply(dates, function(x) {sample_a_day(rs,x,users=1000) %>% as_tibble()})

sample_df <- bind_rows(l_dfs)

bart_od <- transactions_to_bart_transfers(sample_df)

write_csv(bart_od,"~/Documents/Projects/BAM_github_repos/clpr/bart_od.csv")

