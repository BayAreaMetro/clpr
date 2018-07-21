#source("/home/shared/.cred/rs.R")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)
library(readr)
library(dplyr)
library(lubridate)
library(clpr)

start <- as.Date("10-02-16",format="%m-%d-%y")
end   <- as.Date("10-08-16",format="%m-%d-%y")

dates <- seq(start, end, by="day")

source("~/.keys/rs.R")
rs <- connect_rs()

l_dfs <- lapply(dates, function(x) {
  day_of_transactions(rs,x) %>%
    as_tibble() %>%
    mutate(date=x)
})

l_dfs <- lapply(l_dfs,function(x){select(x,-securitymoduleid)})

l_dfs2 <- lapply(l_dfs,function(x){try(bart_transactions_as_transfers(x))})


nicetime <- function(df1){
  out_time_df <- spread_time_column(df1$transaction_time, prefix="tag_on_")
  in_time_df <- spread_time_column(df1$time_of_previous, prefix="tag_out_")

  bart_od3 <- cbind(df1,in_time_df,out_time_df)
}

l_dfs3 <- lapply(l_dfs2,function(x){try(nicetime(x))})

l_dfs4 <- lapply(l_dfs3,function(x){
  try(
    x$cardid_anony <- anonymizer::anonymize(x$cardid_anony,
                                            .algo = "crc32",
                                            .seed = 1)
  )
})
