# Extract Sequence by Date to CSV.R
#
# Take a month of data from the anonymized Clipper SQL server table, build transaction sequences for 
# each card holder, and push the data to a CSV for analysis in other Alpha test scripts.
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

connection <- odbcConnect("Clipper Alpha", uid = "XXX", pwd = "XXX")
df_raw <- sqlQuery(connection,"SELECT [CircadianDate]
      ,CONVERT(varchar(max),[ClipperCardID],2) AS ClipperCardID
      ,[AgencyID]
      ,[AgencyName]
      ,[PaymentProductID]
      ,[PaymentProductName]
      ,[FareAmount]
      ,[TagOnTime]
      ,[TagOnLocationID]
      ,LTRIM(RTRIM([TagOnLocationName])) AS TagOnLocationName
      ,[RouteID]
      ,[RouteName]
      ,[TagOffTime]
      ,[TagOffLocationID]
      ,LTRIM(RTRIM([TagOffLocationName])) AS TagOffLocationName
  FROM [gis].[Clipper].[AlphaTest-2013-04]")

odbcCloseAll()

# Process data: add date and time variables
working <- mutate(df_raw, 
                  TagOnDate = as.Date(TagOnTime),
                  CircadianDayOfWeek = weekdays(CircadianDate),
                  TagOnHour = as.numeric(str_sub(TagOnTime, 12,13)),
                  TagOnMin  = as.numeric(str_sub(TagOnTime, 15,16))) 

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
  filter(str_length(RunSequence) == max(str_length(RunSequence)))

sequence.freq <- tally(group_by(working.indiv, CircadianDate, RunSequence))

# Write sequence frequency to disk
write.csv(sequence.freq, file = "Sequence Frequencies by Date.csv", row.names = FALSE, quote = T)


