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

l_dfs <- lapply(dates[1:3], function(x) {
    sample_day_of_transactions(rs,x,n_users=100) %>%
    as_tibble() %>%
    mutate(date=x)
  })

sample_df <- bind_rows(l_dfs)

df1 <- parse_time(sample_df)

bart_od <- bart_transactions_as_transfers(df1)

write_csv(bart_od,"~/Documents/Projects/BAM_github_repos/clpr/bart_od.csv")
