#source("/home/shared/.cred/rs.R")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)
library(readr)
library(dplyr)
library(lubridate)

start <- as.Date("01-01-16",format="%m-%d-%y")
end   <- as.Date("12-31-16",format="%m-%d-%y")

dates <- sample(seq(start, end, by="day"),10)

source("~/.keys/rs.R")
rs <- connect_rs()

l_dfs <- lapply(dates, function(x) {
    sample_day_of_transactions(rs,x,n_users=10000) %>%
    as_tibble() %>%
    mutate(date=x)
  })

sample_df <- bind_rows(l_dfs)

df1 <- parse_clipper_time(sample_df)

bart_od <- bart_transactions_as_transfers(df1)

#anonymize again
bart_od$cardid_anony <- anonymizer::anonymize(bart_od$cardid_anony,
                                  .algo = "crc32",
                                  .seed = 1)

write_csv(bart_od,here::here("data-raw-local/bart_od.csv"))
