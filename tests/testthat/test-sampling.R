context("db sampling is working")
library(clpr)

#source("YOUR_CREDENTIALS_FILE")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)
library(readr)
library(dplyr)
library(lubridate)

test_that("we can connect to the db", {
  rs <- connect_rs()
  expect_is(rs,'PqConnection')
})

test_that("we can sample a day of transactions from the db", {
  date1 <- as.Date("01-01-16",format="%m-%d-%y")
  rs <- connect_rs()
  tbl1 <- sample_day_of_transactions(rs,date1,n_users=10)
  df1 <- tbl1 %>% as_tibble()
  expect_equal(length(unique(df1$cardid_anony)),10)
})

test_that("we parse time on transactions", {
  date1 <- as.Date("01-01-16",format="%m-%d-%y")
  rs <- connect_rs()
  tbl1 <- sample_day_of_transactions(rs,date1,n_users=10)
  df1 <- tbl1 %>% as_tibble() %>% mutate(date=date1)
  df1 <- parse_clipper_time(df1)
  expect_true(median(df1$hour) > 5 &
                median(df1$hour) < 20)
})


test_that("we parse BART transfers OK", {
  date1 <- as.Date("01-01-16",format="%m-%d-%y")
  rs <- connect_rs()
  tbl1 <- sample_day_of_transactions(rs,date1,n_users=1000)
  df1 <- tbl1 %>% as_tibble() %>% mutate(date=date1)
  df1 <- parse_clipper_time(df1)

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





