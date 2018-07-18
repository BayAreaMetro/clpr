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
         transfer_to_not_bart = (is_bart & from_bart & to_not_bart & lead(timediff<40)),
         transfer_from_not_bart = (is_bart & to_bart & from_not_bart & timediff<120),
         transfer_from_operator = lag(participantname),
         transfer_to_operator = lead(participantname),
         transfer_from_operator_time = timediff,
         transfer_to_operator_time = lead(timediff),
         transfer_from_route = lag(routename),
         transfer_to_route = lead(routename))

od <- od %>%
  group_by(cardid_anony) %>%
  arrange(hour, minute) %>%
  mutate(transfer_from_operator=case_when(from_bart ~ lag(transfer_from_operator)),
         transfer_from_route=case_when(from_bart ~ lag(transfer_from_route)))

meaningful_transfer_vars <- c("cardid_anony","hour","minute",
                              "yday","wday","month",
                              "is_bart","transfer_from_route",
                              "transfer_to_route",
                              "transfer_to_not_bart",
                              "transfer_from_not_bart",
                              "locationname.origin","locationname.destination",
                              "transfer_to_operator",
                              "transfer_from_operator",
                              "transfer_from_operator_time",
                              "transfer_to_operator_time",
                              "transferdiscountflag","transaction_count",
                              "bart_tr_count","tr_count_diff",
                              "timediff",
                              "operatorid_tr","participantname",
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

#View(od_bart_xfer)
bart_xfer_only <- od_bart_xfer[od_bart_xfer$is_bart & !is.na(od_bart_xfer$locationname.destination),]

meaningful_transfer_vars2 <- c("cardid_anony","hour","minute",
                              "yday","wday","month",
                              "locationname.origin","locationname.destination",
                              "transfer_to_operator","transfer_from_route",
                              "transfer_to_route",
                              "transfer_from_operator",
                              "transfer_from_operator_time",
                              "transfer_to_operator_time",
                              "transferdiscountflag",
                              "transaction_count",
                              "bart_tr_count","tr_count_diff")

bart_xfer_only <- bart_xfer_only %>% select(meaningful_transfer_vars2)

write_csv(od_bart_xfer,"~/Documents/Projects/BAM_github_repos/clpr/clipper_2016_origin_destination_route_device_training.csv")

