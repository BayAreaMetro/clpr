context("db sampling is working")
library(clpr)

source("~/.keys/rs.R")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)
library(readr)
library(dplyr)
library(lubridate)

start <- as.Date("01-01-16",format="%m-%d-%y")
end   <- as.Date("12-31-16",format="%m-%d-%y")

randomdate <- sample(seq(start, end, by="day"),1)

test_that("we can connect to the db", {
  rs <- connect_rs()
  expect_is(rs,'PqConnection')
})

test_that("we can sample a day of transactions from the db", {
  date1 <- randomdate
  rs <- connect_rs()
  tbl1 <- sample_day_of_transactions(rs,date1,n_users=10)
  df1 <- tbl1
  expect_equal(length(unique(df1$cardid_anony)),10)
})

test_that("transaction time is within a reasonable hour (5am to 10pm)", {
  date1 <- randomdate
  rs <- connect_rs()
  tbl1 <- sample_day_of_transactions(rs,date1,n_users=10)
  df1 <- tbl1
  df_time <- spread_time_column(tbl1$transaction_time)
  expect_true(median(df_time$t_hour) > 5 &
                median(df_time$t_hour) < 20)
})


test_that("we parse BART transfers OK", {
  date1 <- as.Date("01-01-16",format="%m-%d-%y")
  rs <- connect_rs()
  tbl1 <- sample_day_of_transactions(rs,date1,n_users=1000)
  df1 <- tbl1

  bart_od <- bart_transactions_as_transfers(df1)

  bart_od_check <- bart_od %>%
    filter((!is.na(transfer_to_operator) | !is.na(transfer_from_operator))) %>%
    head(n=1)

  checkid <- bart_od_check %>%
    pull(cardid_anony)

  operator_ids <- df1 %>%
    filter(cardid_anony==checkid) %>%
    pull(operatorid)

  expect_true(4 %in% operator_ids)
  expect_true(length(unique(operator_ids))>1)
})





