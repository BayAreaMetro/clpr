#'
#'@param con a connection to the redshift db
#'@export
#'@importFrom dbplyr in_schema
#'@importFrom dplyr tbl
descriptive_tables <- function(){
  con <- connect_rs()
  dl <- list()
  dl$operators <- dplyr::tbl(con, dbplyr::in_schema("ctp",
                                                    "operators"))
  dl$routes <- dplyr::tbl(con, dbplyr::in_schema("ctp",
                                                 "routes"))
  dl$locations <- dplyr::tbl(con, dbplyr::in_schema("ctp",
                                                    "locations"))
  return(dl)
}

#'
#'@param
#'@returns a list of transactions (devices and)
#'@export
#'@importFrom dbplyr
#'@importFrom dplyr tbl
#'@importFrom readr read_file
#'@importFrom here here
fares_for_day <- function(partition_time="10:00:00",
                          start_date="2016-01-01") {
  con <- connect_rs()

  end_date = as.Date(start_date) + 1
  date_title <- gsub("-", "_",start_date)

  #drop_tbl <- glue::glue('DROP TABLE IF EXISTS "clipper_days"."fares_{date_title}";',date_title = date_title)
  #dbExecute(con, drop_tbl)

  day_tables_sql <- readr::read_file(here::here('inst/sql/day_fares.sql'))
  day_tables_sql <- glue::glue(day_tables_sql,
                               date_title = date_title,
                               start_date = start_date,
                               end_date = end_date,
                               partition_time = partition_time)
  result = tryCatch({
    dbExecute(con, day_tables_sql)
  }, error = function(e) {
    print(e)
  })

  tblname <- paste0("fares_",date_title)
  return(tblname)
}

devices_for_day <- function(partition_time="10:00:00",
                              start_date="2016-01-01") {
  con <- connect_rs()
  end_date = as.Date(start_date) + 1
  date_title <- gsub("-", "_",start_date)
  tblname <- paste0("devices_",date_title)

  #drop_tbl <- glue::glue('DROP TABLE IF EXISTS "clipper_days"."devices_{date_title}";',date_title = date_title)
  #dbExecute(con, drop_tbl)

  day_tables_sql <- readr::read_file(here::here('inst/sql/day_devices.sql'))
  day_tables_sql <- glue::glue(day_tables_sql,
                               date_title = date_title,
                               start_date = start_date,
                               end_date = end_date)
  result = tryCatch({
    dbExecute(con, day_tables_sql)
  }, error = function(e) {
  })
  return(tblname)
}

sample_a_day <- function(rs,date1, users) {
  faretable_name <- fares_for_day(start_date=date1)
  device_table_name <- devices_for_day(start_date=date1)
  all_result_tbl <- all_for_day_sample(rs,faretable_name,device_table_name, users=users)
  human_readable_result_tbl <- make_user_sample_human_readable(rs,all_result_tbl)
}


#'@importFrom dplyr pull
sample_user_ids <- function(transactions_day, n) {
  card_ids <- transactions_day %>%
    dplyr::pull(cardid_anony)
  unique_card_ids <- unique(card_ids)
  unique_card_ids_sample <- sample(unique_card_ids, n)
  return(unique_card_ids_sample)
}


all_for_day_sample <- function(rs,faretable_name,device_table_name,users=users) {
  transactions_day <- dplyr::tbl(rs, dbplyr::in_schema("clipper_days",faretable_name))

  sample_ids <- sample_user_ids(transactions_day, n=users)

  transactions_day_user_sample <- dplyr::tbl(rs, dbplyr::in_schema("clipper_days",faretable_name)) %>%
    filter(cardid_anony %in% sample_ids)

  devices_day_sample <- tbl(rs,in_schema("clipper_days",device_table_name))

  transactions_simple_devices <- left_join(transactions_day_user_sample,
                                           devices_day_sample,
                                           by=c("sequencenumber"=
                                                  "sequencenumber",
                                                "generationtime"=
                                                  "generationtime",
                                                "deviceserialnumber"=
                                                  "deviceserialnumber"),
                                           suffix=c("_tr","_dvc"))

  device_locations <- tbl(rs,in_schema("clipper","devicelocations")) %>%
    select(installdate,modelid,vehicleid,placeid,locationname,sublocation,deviceid) %>%
    rename(vehicleid_dvcl = vehicleid)

  recent_device_locations <- device_locations %>%
    group_by(deviceid) %>% filter(installdate < "2017-01-01") %>%
    top_n(1,installdate)

  transactions_simple_devices_locations <- left_join(transactions_simple_devices,
                                                     recent_device_locations,
                                                     by=c("deviceserialnumber"=
                                                            "deviceid"),
                                                     suffix=c("","_dvcl"))

  return(transactions_simple_devices_locations)
}

make_user_sample_human_readable <- function(rs,user_sample_tbl) {
  tr2 <- user_sample_tbl %>% select(select_vars1)
  tr2 <- tr2 %>% rename("locationname.device"="locationname")

  participants <- tbl(rs, in_schema("clipper","participants"))
  routes <- tbl(rs, in_schema("clipper","routes"))
  locations <- tbl(rs, in_schema("clipper","locations"))

  transactions_simple <- tr2

  transactions_simple_tbl <- as_tibble(transactions_simple)

  transactions_simple_tbl$destinationlocation <- as.integer(transactions_simple_tbl$destinationlocation)
  transactions_simple_tbl$originlocation <- as.integer(transactions_simple_tbl$originlocation)

  participants_simple <- participants %>%
    select(participantid,participantname) %>%
    as_tibble()

  tr0 <- left_join(transactions_simple_tbl,
                   participants_simple,
                   by=c("operatorid_tr"=
                          "participantid"),
                   copy=TRUE)


  tr1 <- left_join(tr0,
                   participants_simple,
                   by=c("transferoperator"=
                          "participantid"),
                   suffix=c('','.transfer'))

  routes_simple <- routes %>%
    select(routeid,participantid,routename) %>%
    as_tibble()

  tr2 <- left_join(tr1,
                   routes_simple,
                   by=c("operatorid_tr"=
                          "participantid",
                        "routeid_tr"=
                          "routeid"))

  locations_simple <- locations %>%
    select(locationcode,participantid,locationname) %>%
    as_tibble()

  tr3 <- left_join(tr2,
                   locations_simple,
                   by=c("originlocation"=
                          "locationcode",
                        "operatorid_tr"=
                          "participantid")) %>%
    rename("locationname.origin"="locationname")

  tr4 <- left_join(tr3,
                   locations_simple,
                   by=c("destinationlocation"=
                          "locationcode",
                        "operatorid_tr"=
                          "participantid")) %>%
    rename("locationname.destination"="locationname") %>%
    select(select_vars2)
  return(tr4)
}



#' @importFrom odbc dbClearResult dbDisconnect dbSendQuery

extract_sequence_by_date <- function(con){
  extract_sequence_query <- read_file("inst/sql/extract_sequence.sql")
  df_raw <- odbc::dbSendQuery(con,extract_sequence_query)

  # Process data: add date and time variables
  working <- mutate(df_raw,
                    TagOnDate = as.Date(TagOnTime),
                    CircadianDayOfWeek = weekdays(CircadianDate),
                    TagOnHour = as.numeric(stringr::str_sub(TagOnTime, 12,13)),
                    TagOnMin  = as.numeric(stringr::str_sub(TagOnTime, 15,16)))

  odbc::dbClearResult(df_raw)
  odbc::dbDisconnect(con)

  # Process data: use lags to build a running sequence of movements (monitor and then manually update the max lag)
  working <- mutate(working, RecordSequence = paste(TagOnHour, TagOnLocationName, TagOffLocationName))
  working <- select(working, ClipperCardID, CircadianDate, CircadianDayOfWeek, TagOnDate, TagOnHour, TagOnMin, RecordSequence)

  lags_needed <- max((tally(group_by(working, ClipperCardID, CircadianDate)))$n, na.rm = TRUE)

  # TODO: Can we make this more elegant?
  working.group <- working %.%
    arrange(CircadianDate, ClipperCardID, TagOnDate, TagOnHour, TagOnMin) %.%
    mutate(RunSequence = RecordSequence) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 1)) & ClipperCardID == lag(ClipperCardID, n = 1), paste(lag(RecordSequence, n = 1),RecordSequence),RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 2)) & ClipperCardID == lag(ClipperCardID, n = 2), paste(lag(RecordSequence, n = 2), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 3)) & ClipperCardID == lag(ClipperCardID, n = 3), paste(lag(RecordSequence, n = 3), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 4)) & ClipperCardID == lag(ClipperCardID, n = 4), paste(lag(RecordSequence, n = 4), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 5)) & ClipperCardID == lag(ClipperCardID, n = 5), paste(lag(RecordSequence, n = 5), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 6)) & ClipperCardID == lag(ClipperCardID, n = 6), paste(lag(RecordSequence, n = 6), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 7)) & ClipperCardID == lag(ClipperCardID, n = 7), paste(lag(RecordSequence, n = 7), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 8)) & ClipperCardID == lag(ClipperCardID, n = 8), paste(lag(RecordSequence, n = 8), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 9)) & ClipperCardID == lag(ClipperCardID, n = 9), paste(lag(RecordSequence, n = 9), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =10)) & ClipperCardID == lag(ClipperCardID, n =10), paste(lag(RecordSequence, n =10), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =11)) & ClipperCardID == lag(ClipperCardID, n =11), paste(lag(RecordSequence, n =11), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =12)) & ClipperCardID == lag(ClipperCardID, n =12), paste(lag(RecordSequence, n =12), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =13)) & ClipperCardID == lag(ClipperCardID, n =13), paste(lag(RecordSequence, n =13), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =14)) & ClipperCardID == lag(ClipperCardID, n =14), paste(lag(RecordSequence, n =14), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =15)) & ClipperCardID == lag(ClipperCardID, n =15), paste(lag(RecordSequence, n =15), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =16)) & ClipperCardID == lag(ClipperCardID, n =16), paste(lag(RecordSequence, n =16), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =17)) & ClipperCardID == lag(ClipperCardID, n =17), paste(lag(RecordSequence, n =17), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =18)) & ClipperCardID == lag(ClipperCardID, n =18), paste(lag(RecordSequence, n =18), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =19)) & ClipperCardID == lag(ClipperCardID, n =19), paste(lag(RecordSequence, n =19), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =20)) & ClipperCardID == lag(ClipperCardID, n =20), paste(lag(RecordSequence, n =20), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =21)) & ClipperCardID == lag(ClipperCardID, n =21), paste(lag(RecordSequence, n =21), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =22)) & ClipperCardID == lag(ClipperCardID, n =22), paste(lag(RecordSequence, n =22), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =23)) & ClipperCardID == lag(ClipperCardID, n =23), paste(lag(RecordSequence, n =23), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =24)) & ClipperCardID == lag(ClipperCardID, n =24), paste(lag(RecordSequence, n =24), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =25)) & ClipperCardID == lag(ClipperCardID, n =25), paste(lag(RecordSequence, n =25), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =26)) & ClipperCardID == lag(ClipperCardID, n =26), paste(lag(RecordSequence, n =26), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =27)) & ClipperCardID == lag(ClipperCardID, n =27), paste(lag(RecordSequence, n =27), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =28)) & ClipperCardID == lag(ClipperCardID, n =28), paste(lag(RecordSequence, n =28), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =29)) & ClipperCardID == lag(ClipperCardID, n =29), paste(lag(RecordSequence, n =29), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =30)) & ClipperCardID == lag(ClipperCardID, n =30), paste(lag(RecordSequence, n =30), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =31)) & ClipperCardID == lag(ClipperCardID, n =31), paste(lag(RecordSequence, n =31), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =32)) & ClipperCardID == lag(ClipperCardID, n =32), paste(lag(RecordSequence, n =32), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =33)) & ClipperCardID == lag(ClipperCardID, n =33), paste(lag(RecordSequence, n =33), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =34)) & ClipperCardID == lag(ClipperCardID, n =34), paste(lag(RecordSequence, n =34), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =35)) & ClipperCardID == lag(ClipperCardID, n =35), paste(lag(RecordSequence, n =35), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =36)) & ClipperCardID == lag(ClipperCardID, n =36), paste(lag(RecordSequence, n =36), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =37)) & ClipperCardID == lag(ClipperCardID, n =37), paste(lag(RecordSequence, n =37), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =38)) & ClipperCardID == lag(ClipperCardID, n =38), paste(lag(RecordSequence, n =38), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =39)) & ClipperCardID == lag(ClipperCardID, n =39), paste(lag(RecordSequence, n =39), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =40)) & ClipperCardID == lag(ClipperCardID, n =40), paste(lag(RecordSequence, n =40), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =41)) & ClipperCardID == lag(ClipperCardID, n =41), paste(lag(RecordSequence, n =40), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =42)) & ClipperCardID == lag(ClipperCardID, n =42), paste(lag(RecordSequence, n =42), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =43)) & ClipperCardID == lag(ClipperCardID, n =43), paste(lag(RecordSequence, n =43), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =44)) & ClipperCardID == lag(ClipperCardID, n =44), paste(lag(RecordSequence, n =44), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =45)) & ClipperCardID == lag(ClipperCardID, n =45), paste(lag(RecordSequence, n =45), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =46)) & ClipperCardID == lag(ClipperCardID, n =46), paste(lag(RecordSequence, n =46), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =47)) & ClipperCardID == lag(ClipperCardID, n =47), paste(lag(RecordSequence, n =47), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =48)) & ClipperCardID == lag(ClipperCardID, n =48), paste(lag(RecordSequence, n =48), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =49)) & ClipperCardID == lag(ClipperCardID, n =49), paste(lag(RecordSequence, n =49), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =50)) & ClipperCardID == lag(ClipperCardID, n =50), paste(lag(RecordSequence, n =50), RunSequence), RunSequence))

  # Process data: extract a data set that has movements for each CardID for each CircadianDate
  working.indiv <- select(working.group, ClipperCardID, CircadianDate, CircadianDayOfWeek, RunSequence)
  working.indiv <- working.indiv %.%
    group_by(ClipperCardID, CircadianDate) %.%
    filter(stringr::str_length(RunSequence) == max(stringr::str_length(RunSequence)))

  sequence.freq <- tally(group_by(working.indiv, CircadianDate, RunSequence))
  return(sequence.freq)
}


extract_sequence_for_random_week <- function(con){
  extract_sequence_random_week_sql_query <- read_file("inst/sql/extract_sequence_random_week_sql_query.sql")
  df_raw <- sqlQuery(connection,extract_sequence_random_week_sql_query)

  # Process data: create unique circadian date ID
  working <- df_raw %.%
    select(-AgencyID, -AgencyName, -PaymentProductID, -PaymentProductName, -TagOnLocationID, -RouteID, -RouteName, -TagOffLocationID) %.%
    mutate(TagOnHour = as.numeric(stringr::str_sub(TagOnTime_Time, 1,2))) %.%
    mutate(DayID = 1000000 * Year + 10000 * Month + 100 * CircadianDayOfWeek + RandomWeekID)

  odbc::dbClearResult(df_raw)
  odbc::dbDisconnect(con)

  # Process data: keep the data ideas with the year, month, etc
  date_info <- as.data.frame(unique(working$DayID))
  names(date_info)[names(date_info)=="unique(working$DayID)"] <- "DayID"

  day_of_week.name <- c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

  date_info <- date_info %.%
    mutate(Year               = as.numeric(stringr::str_sub(as.character(DayID), 1, 4))) %.%
    mutate(Month              = as.numeric(stringr::str_sub(as.character(DayID), 5, 6))) %.%
    mutate(CircadianDayOfWeek = as.numeric(stringr::str_sub(as.character(DayID), 7, 8))) %.%
    mutate(RandomWeekID       = as.numeric(stringr::str_sub(as.character(DayID), 9, 10))) %.%
    mutate(Month = month.name[Month]) %.%
    mutate(CircadianDayOfWeek = day_of_week.name[CircadianDayOfWeek])

  table(date_info$CircadianDayOfWeek)

  # Process data: use lags to build a running sequence of movements (monitor and then manually update the max lag)
  working <- mutate(working, RecordSequence = paste(TagOnHour, TagOnLocationName, TagOffLocationName))
  working <- select(working, ClipperCardID, DayID, TagOnHour, TripSequenceID, RecordSequence)

  lags_needed <- max((tally(group_by(working, ClipperCardID, DayID)))$n, na.rm = TRUE)

  # TODO: Can we make this more elegant?
  working.group <- working %.%
    arrange(DayID, ClipperCardID, TripSequenceID) %.%
    mutate(RunSequence = RecordSequence) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 1)) & ClipperCardID == lag(ClipperCardID, n = 1), paste(lag(RecordSequence, n = 1),RecordSequence),RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 2)) & ClipperCardID == lag(ClipperCardID, n = 2), paste(lag(RecordSequence, n = 2), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 3)) & ClipperCardID == lag(ClipperCardID, n = 3), paste(lag(RecordSequence, n = 3), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 4)) & ClipperCardID == lag(ClipperCardID, n = 4), paste(lag(RecordSequence, n = 4), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 5)) & ClipperCardID == lag(ClipperCardID, n = 5), paste(lag(RecordSequence, n = 5), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 6)) & ClipperCardID == lag(ClipperCardID, n = 6), paste(lag(RecordSequence, n = 6), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 7)) & ClipperCardID == lag(ClipperCardID, n = 7), paste(lag(RecordSequence, n = 7), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 8)) & ClipperCardID == lag(ClipperCardID, n = 8), paste(lag(RecordSequence, n = 8), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n = 9)) & ClipperCardID == lag(ClipperCardID, n = 9), paste(lag(RecordSequence, n = 9), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =10)) & ClipperCardID == lag(ClipperCardID, n =10), paste(lag(RecordSequence, n =10), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =11)) & ClipperCardID == lag(ClipperCardID, n =11), paste(lag(RecordSequence, n =11), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =12)) & ClipperCardID == lag(ClipperCardID, n =12), paste(lag(RecordSequence, n =12), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =13)) & ClipperCardID == lag(ClipperCardID, n =13), paste(lag(RecordSequence, n =13), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =14)) & ClipperCardID == lag(ClipperCardID, n =14), paste(lag(RecordSequence, n =14), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =15)) & ClipperCardID == lag(ClipperCardID, n =15), paste(lag(RecordSequence, n =15), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =16)) & ClipperCardID == lag(ClipperCardID, n =16), paste(lag(RecordSequence, n =16), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =17)) & ClipperCardID == lag(ClipperCardID, n =17), paste(lag(RecordSequence, n =17), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =18)) & ClipperCardID == lag(ClipperCardID, n =18), paste(lag(RecordSequence, n =18), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =19)) & ClipperCardID == lag(ClipperCardID, n =19), paste(lag(RecordSequence, n =19), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =20)) & ClipperCardID == lag(ClipperCardID, n =20), paste(lag(RecordSequence, n =20), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =21)) & ClipperCardID == lag(ClipperCardID, n =21), paste(lag(RecordSequence, n =21), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =22)) & ClipperCardID == lag(ClipperCardID, n =22), paste(lag(RecordSequence, n =22), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =23)) & ClipperCardID == lag(ClipperCardID, n =23), paste(lag(RecordSequence, n =23), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =24)) & ClipperCardID == lag(ClipperCardID, n =24), paste(lag(RecordSequence, n =24), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =25)) & ClipperCardID == lag(ClipperCardID, n =25), paste(lag(RecordSequence, n =25), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =26)) & ClipperCardID == lag(ClipperCardID, n =26), paste(lag(RecordSequence, n =26), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =27)) & ClipperCardID == lag(ClipperCardID, n =27), paste(lag(RecordSequence, n =27), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =28)) & ClipperCardID == lag(ClipperCardID, n =28), paste(lag(RecordSequence, n =28), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =29)) & ClipperCardID == lag(ClipperCardID, n =29), paste(lag(RecordSequence, n =29), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =30)) & ClipperCardID == lag(ClipperCardID, n =30), paste(lag(RecordSequence, n =30), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =31)) & ClipperCardID == lag(ClipperCardID, n =31), paste(lag(RecordSequence, n =31), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =32)) & ClipperCardID == lag(ClipperCardID, n =32), paste(lag(RecordSequence, n =32), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =33)) & ClipperCardID == lag(ClipperCardID, n =33), paste(lag(RecordSequence, n =33), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =34)) & ClipperCardID == lag(ClipperCardID, n =34), paste(lag(RecordSequence, n =34), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =35)) & ClipperCardID == lag(ClipperCardID, n =35), paste(lag(RecordSequence, n =35), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =36)) & ClipperCardID == lag(ClipperCardID, n =36), paste(lag(RecordSequence, n =36), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =37)) & ClipperCardID == lag(ClipperCardID, n =37), paste(lag(RecordSequence, n =37), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =38)) & ClipperCardID == lag(ClipperCardID, n =38), paste(lag(RecordSequence, n =38), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =39)) & ClipperCardID == lag(ClipperCardID, n =39), paste(lag(RecordSequence, n =39), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =40)) & ClipperCardID == lag(ClipperCardID, n =40), paste(lag(RecordSequence, n =40), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =41)) & ClipperCardID == lag(ClipperCardID, n =41), paste(lag(RecordSequence, n =40), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =42)) & ClipperCardID == lag(ClipperCardID, n =42), paste(lag(RecordSequence, n =42), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =43)) & ClipperCardID == lag(ClipperCardID, n =43), paste(lag(RecordSequence, n =43), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =44)) & ClipperCardID == lag(ClipperCardID, n =44), paste(lag(RecordSequence, n =44), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =45)) & ClipperCardID == lag(ClipperCardID, n =45), paste(lag(RecordSequence, n =45), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =46)) & ClipperCardID == lag(ClipperCardID, n =46), paste(lag(RecordSequence, n =46), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =47)) & ClipperCardID == lag(ClipperCardID, n =47), paste(lag(RecordSequence, n =47), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =48)) & ClipperCardID == lag(ClipperCardID, n =48), paste(lag(RecordSequence, n =48), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =49)) & ClipperCardID == lag(ClipperCardID, n =49), paste(lag(RecordSequence, n =49), RunSequence), RunSequence)) %.%
    mutate(RunSequence = ifelse(!is.na(lag(ClipperCardID, n =50)) & ClipperCardID == lag(ClipperCardID, n =50), paste(lag(RecordSequence, n =50), RunSequence), RunSequence))

  # Process data: extract a data set that has movements for each CardID for each CircadianDate
  working.indiv <- select(working.group, ClipperCardID, DayID, RunSequence)
  working.indiv <- working.indiv %.%
    group_by(ClipperCardID, DayID) %.%
    filter(stringr::str_length(RunSequence) == max(stringr::str_length(RunSequence)))

  sequence.freq <- tally(group_by(working.indiv, DayID, RunSequence))

  sequence.freq.join <- left_join(sequence.freq, date_info, by = "DayID")

  return(sequence.freq.join)
}
