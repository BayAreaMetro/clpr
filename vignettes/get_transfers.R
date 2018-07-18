source("/home/shared/.cred/rs.R")
library(DBI)
library(RPostgres)
library(dbplyr)
library(dplyr)
rs <- dbConnect(RPostgres::Postgres(),
                "user" = Sys.getenv("RSUSER"),
                "password" = Sys.getenv("RSPASSWORD"),
                "dbname" = Sys.getenv("RSDB"),
                "port"=Sys.getenv("RSPORT"),
                "host" = Sys.getenv("RSHOST"))

dt_l <- descriptive_tables(rs)

start <- as.Date("01-01-16",format="%m-%d-%y")
end   <- as.Date("12-31-16",format="%m-%d-%y")

dates <- sample(seq(start, end, by="day"), 10)

rs <- dbConnect(RPostgres::Postgres(),
                "user" = Sys.getenv("RSUSER"),
                "password" = Sys.getenv("RSPASSWORD"),
                "dbname" = Sys.getenv("RSDB"),
                "port"=Sys.getenv("RSPORT"),
                "host" = Sys.getenv("RSHOST"))

faretable_name <- partition_fares(rs,start_date=dates[[2]])

df2 <- sample_users(rs, faretable_name=faretable_name)

#fares <- partition_fares(rs)
#devices <- partition_devices(rs)

