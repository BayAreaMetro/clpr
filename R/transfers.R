# Extract Sequence for Random Weekday to CSV.R
#
# Take a month of data from the anonymized Clipper SQL server table, build transaction sequences for
# each card holder, and push the data to a CSV for analysis in other Alpha test scripts.  Note that
# this version of the anonmyzed data suppresses the actual date, instead providing a year, month,
# day of week, and week indicator.
#
# 2014 08 08 dto
#

# Overhead
suppressMessages(library(dplyr))
suppressMessages(library(plyr))
library(RODBC)
library(stringr)
setwd("M:/Data/Clipper/Alpha Tests/_working")

# Connect to the SQL Server via ODBC
# 1.  Download ODBC driver (http://www.microsoft.com/en-us/download/details.aspx?id=36434)
# 2.  Search Windows ODBC --> 'Set up data sources (ODBC)
# 3.  Add ODBC Driver 11 for SQL Server connection

connection <- odbcConnect("Clipper Alpha", uid = "XXXX", pwd = "XXXX")
df_raw <- sqlQuery(connection,"SELECT
                    DATEPART(yyyy,[CircadianDate]) AS Year
                   ,DATEPART(month,[CircadianDate]) AS Month
                   ,DATEPART(dw,[CircadianDate]) AS CircadianDayOfWeek
                   ,[RandomWeekID]
                   ,CONVERT(varchar(max),[ClipperCardID],2) AS ClipperCardID
                   ,[TripSequenceID]
                   ,[AgencyID]
                   ,[AgencyName]
                   ,[PaymentProductID]
                   ,[PaymentProductName]
                   ,[FareAmount]
                   ,[TagOnTime_Time]
                   ,[TagOnLocationID]
                   ,LTRIM(RTRIM([TagOnLocationName])) AS TagOnLocationName
                   ,[RouteID]
                   ,[RouteName]
                   ,[TagOffTime_Time]
                   ,[TagOffLocationID]
                   ,LTRIM(RTRIM([TagOffLocationName])) AS TagOffLocationName
                   FROM [gis].[Clipper].[ThreeRandomWeeks-2013-03]")

odbcCloseAll()

# Process data: create unique circadian date ID
working <- df_raw %.%
  select(-AgencyID, -AgencyName, -PaymentProductID, -PaymentProductName, -TagOnLocationID, -RouteID, -RouteName, -TagOffLocationID) %.%
  mutate(TagOnHour = as.numeric(str_sub(TagOnTime_Time, 1,2))) %.%
  mutate(DayID = 1000000 * Year + 10000 * Month + 100 * CircadianDayOfWeek + RandomWeekID)

# Process data: keep the data ideas with the year, month, etc
date_info <- as.data.frame(unique(working$DayID))
names(date_info)[names(date_info)=="unique(working$DayID)"] <- "DayID"

day_of_week.name <- c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

date_info <- date_info %.%
  mutate(Year               = as.numeric(str_sub(as.character(DayID), 1, 4))) %.%
  mutate(Month              = as.numeric(str_sub(as.character(DayID), 5, 6))) %.%
  mutate(CircadianDayOfWeek = as.numeric(str_sub(as.character(DayID), 7, 8))) %.%
  mutate(RandomWeekID       = as.numeric(str_sub(as.character(DayID), 9, 10))) %.%
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
  filter(str_length(RunSequence) == max(str_length(RunSequence)))

sequence.freq <- tally(group_by(working.indiv, DayID, RunSequence))

sequence.freq.join <- left_join(sequence.freq, date_info, by = "DayID")

# Write sequence frequency to disk
write.csv(sequence.freq.join, file = "Sequence Frequencies for Random Weekday.csv", row.names = FALSE, quote = T)


Make_to_from_plot <- function(transfer_df, a_string, b_string){

  # A to B
  a_to_b <- transfer_df %>%
    mutate(from_AgencyName = str_trim(from_AgencyName)) %>%
    mutate(to_AgencyName   = str_trim(to_AgencyName)) %>%
    filter(from_AgencyName == a_string) %>%
    filter(to_AgencyName   == b_string)

  a_to_b <- a_to_b %>%
    mutate(key_time = ifelse(!is.na(Diff_Min_TagOff_to_TagOn), Diff_Min_TagOff_to_TagOn, Diff_Min_TagOn_to_TagOn)) %>%
    group_by(Year, Month, CircadianDayOfWeek, RandomWeekID, key_time) %>%
    summarise(transfers = n()) %>%
    group_by(Year, Month, key_time) %>%
    summarise(typical_weekdays = n(), median_transfers = median(transfers)) %>%
    mutate(cumulative_median_transfers = cumsum(median_transfers)) %>%
    select(Year, Month, key_time, cumulative_median_transfers) %>%
    mutate(movement = paste(a_string," to ", b_string)) %>%
    filter(key_time > 0) %>%
    filter(key_time < 130)

  # B to A
  b_to_a <- transfer_df %>%
    mutate(from_AgencyName = str_trim(from_AgencyName)) %>%
    mutate(to_AgencyName   = str_trim(to_AgencyName)) %>%
    filter(from_AgencyName == b_string) %>%
    filter(to_AgencyName   == a_string)

  b_to_a <- b_to_a %>%
    mutate(key_time = ifelse(!is.na(Diff_Min_TagOff_to_TagOn), Diff_Min_TagOff_to_TagOn, Diff_Min_TagOn_to_TagOn)) %>%
    group_by(Year, Month, CircadianDayOfWeek, RandomWeekID, key_time) %>%
    summarise(transfers = n()) %>%
    group_by(Year, Month, key_time) %>%
    summarise(typical_weekdays = n(), median_transfers = median(transfers)) %>%
    mutate(cumulative_median_transfers = cumsum(median_transfers)) %>%
    select(Year, Month, key_time, cumulative_median_transfers) %>%
    mutate(movement = paste(b_string," to ", a_string)) %>%
    filter(key_time > 0) %>%
    filter(key_time < 130)

  # Merge and plot
  data.to_plot <- rbind(a_to_b, b_to_a)

  # Plot
  plot <- ggplot(data.to_plot, aes(x = key_time, y = cumulative_median_transfers, colour = movement, group = movement)) +
    xlab("Time between Tags") +
    ylab("Cumulative Sampled Transfers") +
    theme(text = element_text(size = 16), axis.title.x = element_text(vjust = -0.5), axis.title.y = element_text(vjust = 2.0), panel.grid.major.x = element_line(colour = "black")) +
    geom_line(size = 2) +
    ggtitle(paste(a_string,"to/from", b_string))

  return(plot)

}


Build_Database <- function(transfers_df, transactions_df, transfer_rules_df, sampling_rate_float, output_file_string, append_boolean){

  # Summarise the transfers for typical weekdays
  typical.weekday <- transfers_df %>%
    mutate(from_AgencyName = str_trim(from_AgencyName)) %>%
    mutate(to_AgencyName   = str_trim(to_AgencyName)) %>%
    filter(CircadianDayOfWeek > 2) %>%
    filter(CircadianDayOfWeek < 6) %>%
    mutate(key_time = ifelse(!is.na(Diff_Min_TagOff_to_TagOn), Diff_Min_TagOff_to_TagOn, Diff_Min_TagOn_to_TagOn)) %>%
    select(Year, Month, CircadianDayOfWeek, RandomWeekID, from_AgencyName, to_AgencyName, key_time)

  transfer.data <- inner_join(typical.weekday, transfer_rules_df, by = c("from_AgencyName", "to_AgencyName"))

  transfer.sum <- transfer.data %>%
    filter(key_time <= max_time) %>%
    select(-key_time, -max_time) %>%
    group_by(Year, Month, CircadianDayOfWeek, RandomWeekID, from_AgencyName, to_AgencyName) %>%
    summarise(sampled_transfers = n())

  # Join the transactions
  working.transactions <- transactions_df %>%
    mutate(AgencyName = str_trim(AgencyName))

  from_transactions <- working.transactions %>%
    select(from_AgencyName = AgencyName, from_Agency_Sampled_Transactions = Sampled_Transactions, Year, Month, CircadianDayOfWeek, RandomWeekID)

  to_transactions <- working.transactions %>%
    select(  to_AgencyName = AgencyName,   to_Agency_Sampled_Transactions = Sampled_Transactions, Year, Month, CircadianDayOfWeek, RandomWeekID)

  transfer.write <- left_join(transfer.sum,   from_transactions, by = c("Year", "Month", "CircadianDayOfWeek", "RandomWeekID", "from_AgencyName"))
  transfer.write <- left_join(transfer.write, to_transactions,   by = c("Year", "Month", "CircadianDayOfWeek", "RandomWeekID", "to_AgencyName"))

  # Estimate transfers & tranactions using the sample rate
  transfer.write <- transfer.write %>%
    mutate(estimated_transfers = sampled_transfers / SAMPLING_RATE) %>%
    mutate(estimated_from_agency_transactions = from_Agency_Sampled_Transactions / SAMPLING_RATE) %>%
    mutate(estimated_to_agency_transactions   = to_Agency_Sampled_Transactions   / SAMPLING_RATE)

  # Write to disk
  if (append_boolean) {
    existing <- read.table(file = output_file_string, header = TRUE, sep = ",", stringsAsFactors = FALSE)
    transfer.write <- rbind(existing, transfer.write)

  }

  write.csv(transfer.write, output_file_string, row.names = FALSE, quote = T)

}
