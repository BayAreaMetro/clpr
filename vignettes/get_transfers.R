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

dates <- sample(seq(start, end, by="day"),30)

rs <- connect_rs()

l_dfs <- lapply(dates, function(x) {sample_a_day(rs,x,users=5000) %>% as_tibble()})

sample_df <- bind_rows(l_dfs)


od <- sample_df %>%
  group_by(cardid_anony) %>%
  mutate(count = n())

hist(od$count)

t <- strftime(od$psttime, format="%H:%M:%S")
xx <- as.POSIXct(t, format="%H:%M:%S")

od$hour <- hour(xx)
od$minute <- minute(xx)

od <- od %>%
  group_by(cardid_anony) %>%
  mutate(timediff = abs(difftime(psttime, lag(psttime),units="mins")))

od <- od %>%
  group_by(cardid_anony) %>%
  mutate(check1=any(timediff<200),
         median_timediff=median(timediff, na.rm=TRUE),
         check2=any(median_timediff<200))

od <- od %>%
  select(cardid_anony, hour, minute, timediff, locationname.origin,locationname.destination, median_timediff, check1, check2, everything()) %>%
  arrange(cardid_anony, hour, minute, psttime) %>%
  mutate()

write_csv(od1,"~/Documents/Projects/BAM_github_repos/clpr/clipper_2016_origin_destination_route_device_sample.csv")

