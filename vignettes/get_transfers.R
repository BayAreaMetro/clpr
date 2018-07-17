source("/home/shared/.cred/rs.R")
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

faretable_name <- partition_fares(con,start_date=dates[[1]])
df1 <- sample_users(rs, faretable_name=faretable_name)


#fares <- partition_fares(rs)
#devices <- partition_devices(rs)

