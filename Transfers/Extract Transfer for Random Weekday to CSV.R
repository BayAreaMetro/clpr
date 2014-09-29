# Extract Transfer for Random Weekday to CSV.R
#
# Take a month of data from the anonymized Clipper SQL server table, build set of pontential transfers using a 
# MAX_TIME minute window (max for ANY plausible transfers), write data to .RData and CSV for subsequent analysis.  
#
# dto
#

# Overhead
suppressMessages(library(dplyr))
library(stringr)
library(RODBC)
setwd("M:/Data/Clipper/Transfers/From Anonymous")

# Parameters 
MAX_TIME = 120

# TODO: Update when SGB pushes new dataset

# Connect to the SQL Server via ODBC
# 1.  Download ODBC driver (http://www.microsoft.com/en-us/download/details.aspx?id=36434)
# 2.  Search Windows ODBC --> 'Set up data sources (ODBC)
# 3.  Add ODBC Driver 11 for SQL Server connection

connection <- odbcConnect("Clipper Alpha", uid = "XXX", pwd = "XXX")
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

# Process data: create unique circadian date ID index, use it to build a small date database 
working_date <- df_raw %.%
  select(Year, Month, CircadianDayOfWeek, RandomWeekID) %.%
  group_by(Year, Month, CircadianDayOfWeek, RandomWeekID) %.%
  summarise(Count = n())

day_of_week.name <- c("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")

date_info <- working_date %.%
  select(-Count) %.%
  mutate(DayID = 1000000 * Year + 10000 * Month + 100 * CircadianDayOfWeek + RandomWeekID) %.%
  mutate(Month_Name = month.name[Month]) %.%
  mutate(CircadianDayOfWeek_Name = day_of_week.name[CircadianDayOfWeek])

head(date_info, n = 31)

# Process data: Create data set to extract A and B from
working.for_A_and_B <- df_raw %.%
  mutate(DayID = 1000000 * Year + 10000 * Month + 100 * CircadianDayOfWeek + RandomWeekID) %.%
  mutate(TagOnHour  = as.numeric(str_sub(TagOnTime_Time,  1,2))) %.%
  mutate(TagOnMin   = as.numeric(str_sub(TagOnTime_Time,  4,5))) %.%
  mutate(TagOffHour = as.numeric(str_sub(TagOffTime_Time, 1,2))) %.%
  mutate(TagOffMin  = as.numeric(str_sub(TagOffTime_Time, 4,5)))

# Process data: Extract datasets A and B
working.A <- working.for_A_and_B %.%
  select(ClipperCardID, DayID, TripSequenceID, 
         from_TagOnHour         = TagOnHour,         from_TagOnMin           =  TagOnMin, 
         from_TagOffHour        = TagOffHour,        from_TagOffMin          = TagOffMin,
         from_TagOnLocationName = TagOnLocationName, from_TagOffLocationName = TagOffLocationName,
         from_RouteName         = RouteName,         from_AgencyName         = AgencyName)

working.B <- working.for_A_and_B %.%
  mutate(TripSequenceID = TripSequenceID - 1) %.%
  select(ClipperCardID, DayID, TripSequenceID, 
         to_TagOnHour         = TagOnHour,         to_TagOnMin   =  TagOnMin, 
         to_TagOnLocationName = TagOnLocationName,
         to_RouteName         = RouteName,         to_AgencyName = AgencyName) %.%
  filter(TripSequenceID > 0) %.%
  mutate(to_TagOnHour = ifelse(to_TagOnHour < 3, to_TagOnHour + 24, to_TagOnHour))

# Process data: Join A and B, compute time gap
working.join <- inner_join(working.A, working.B, by = c("ClipperCardID", "DayID", "TripSequenceID"))

working.join <- working.join %.%
  mutate(Diff_Min_TagOn_to_TagOn  = 60 * (to_TagOnHour - from_TagOnHour)  + (to_TagOnMin - from_TagOnMin)) %.%
  mutate(Diff_Min_TagOff_to_TagOn = 60 * (to_TagOnHour - from_TagOffHour) + (to_TagOnMin - from_TagOffMin))

table(working.join$Diff_Min_TagOn_to_TagOn)

# Process data: filter based on m
working.output <- working.join %.%
  filter(Diff_Min_TagOn_to_TagOn <= MAX_TIME)

# Write data to disk
working.output <- left_join(working.output, date_info, by = "DayID")
write.csv(working.output, "Transfer Database for Random Weekdays.csv", row.names = FALSE, quote = T)
save(working.output, file = "Transfer Database for Random Weekdays.RData")










