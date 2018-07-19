context("db sampling is working")
library(clpr)

#source("/home/shared/.cred/rs.R")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)
library(readr)
library(dplyr)
library(lubridate)

end   <- as.Date("03-31-16",format="%m-%d-%y")

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
  df1 <- parse_time(df1)
  expect_true("hour" %in% names(df1))
})



