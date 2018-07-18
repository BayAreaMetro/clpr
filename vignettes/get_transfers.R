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

dates <- sample(seq(start, end, by="day"),1)

rs <- connect_rs()

l_dfs <- lapply(dates, function(x) {sample_a_day(rs,x,users=1000) %>% as_tibble()})

sample_df <- bind_rows(l_dfs)

od <- sample_df %>%
  group_by(cardid_anony,yday) %>%
  mutate(transaction_count = n())

od$is_bart <- od$operatorid_tr==4

od <- od %>%
  group_by(cardid_anony,yday,is_bart) %>%
  mutate(bart_tr_count = n(),
         tr_count_diff = transaction_count-bart_tr_count)


od <- od %>%
  group_by(cardid_anony) %>%
  arrange(hour, minute) %>%
  mutate(timediff = abs(difftime(psttime, lag(psttime),units="mins")),
         from_bart = lag(operatorid_tr)==4,
         to_bart = lead(operatorid_tr)==4,
         from_not_bart = lag(operatorid_tr)!=4,
         to_not_bart = lead(operatorid_tr)!=4,
         transfer_to_not_bart = (from_bart & to_not_bart & lead(timediff<80)),
         transfer_from_not_bart = (to_bart & from_not_bart & timediff<80))

od <- od %>%
  group_by(cardid_anony,yday) %>%
  mutate(tr_200_min_any=any(timediff<200),
         median_timediff=median(timediff, na.rm=TRUE),
         tr_200_min_med=any(median_timediff<200))


meaningful_transfer_vars <- c("cardid_anony","hour","minute",
                              "yday","wday","month",
                              "transfer_to_not_bart",
                              "transfer_from_not_bart",
                              "from_bart","to_bart",
                              "from_not_bart","to_not_bart",
                              "locationname.origin","locationname.destination",
                              "transferdiscountflag","transaction_count",
                              "bart_tr_count","tr_count_diff",
                              "timediff","median_timediff",
                              "operatorid_tr","participantname",
                              "tr_200_min_any","tr_200_min_med",
                              "routename","vehicleid_dvcl",
                              "installdate","sublocation",
                              "tripsequencenumber","sequencenumber")

od <- od %>%
  select(meaningful_transfer_vars) %>%
  arrange(cardid_anony, hour, minute) %>%
  mutate()

bart_rider_ids <- od %>%
  filter(operatorid_tr==4) %>%
  pull(cardid_anony)

od_bart <- od %>%
  filter(cardid_anony %in% bart_rider_ids)

od_bart_potential_transferer_ids <- od_bart %>%
  filter(transaction_count > 4) %>%
  pull(cardid_anony)

od_bart_xfer <- od %>%
  filter(cardid_anony %in% od_bart_potential_transferer_ids) %>%
  arrange(cardid_anony,yday,hour,minute,transfer_to_not_bart,transfer_from_not_bart)

View(od_bart_xfer)

write_csv(od_bart_xfer,"~/Documents/Projects/BAM_github_repos/clpr/clipper_2016_origin_destination_route_device_training.csv")

