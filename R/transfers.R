library(stringr)

#'  Flattens transactional samples into a per-bart transactions 
#' 
#' @param od_df a sample of transactions, effectively from the raw sfofaretransactions table, joined to other tables
#' @returns bart_od_transfer_df table in which transfers in and out of bart are captured with metadata
#' @importFrom dplyr group_by mutate case_when lag arrange filter select
transactions_to_bart_transfers(tr_df){
  od <- tr_df %>%
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
           transfer_to_not_bart = (is_bart & from_bart & to_not_bart & lead(timediff<60)),
           transfer_from_not_bart = (is_bart & to_bart & from_not_bart & timediff<120),
           transfer_from_operator = case_when(transfer_from_not_bart ~ lag(participantname)),
           transfer_to_operator = case_when(transfer_to_not_bart ~ lead(participantname)),
           transfer_from_operator_time = case_when(transfer_from_not_bart ~ timediff),
           transfer_to_operator_time = case_when(transfer_to_not_bart ~ lead(timediff)),
           transfer_from_route = case_when(transfer_from_not_bart ~ lag(routename)),
           transfer_to_route = case_when(transfer_to_not_bart ~ lead(routename))

  #this is confusing, but you have to pull routes and transfers from
  #operators from two-transactions back
  #so here we effectively reach the lag back 2 transactions
  od <- od %>%
    group_by(cardid_anony) %>%
    arrange(hour, minute) %>%
    mutate(transfer_from_operator=case_when(from_bart ~ lag(transfer_from_operator)),
           transfer_from_route=case_when(from_bart ~ lag(transfer_from_route)))

  od <- od %>%
    select(transaction_transfer_vars) %>% #see variables.R
    arrange(cardid_anony, hour, minute) %>%
    mutate()

  bart_rider_ids <- od %>%
    filter(operatorid_tr==4) %>%
    pull(cardid_anony)

  od_bart <- od %>%
    filter(cardid_anony %in% bart_rider_ids)

  bart_od_transfer_df <- od_bart[od_bart$is_bart & 
                            !is.na(od_bart$locationname.destination),]

  bart_od_transfer_df <- bart_od_transfer_df %>% 
    select(bart_flattened_transfers_variables) #see variables.R

  return(bart_od_transfer_df)
}


#'Build a database of transfers
#'
#'The anonymous Clipper data is potentially an excellent source for quantifying operator-to-operator transit movements.
#'Here, we seek to understand transfers in the travel model sense,
#'i.e. a movement between an origin and destination that requires
#'a tranfer between or within transit operators. This definition differs from transfers
#'as defined by certain transit agencies, which generally allow multiple boardings at no or reduced cost
#'during a narrow time window.
#'Here, we implement a set of rules to build a database of transfers.
#'This script uses data derived from the anonymous Clipper data
#'see Extract Transfer for Random Weekday to CSV.R.
#'@param df_row a row from transfer_filenames_df, which is included in the package
#'@param data_dir the directory in which the transfer and transaction data resides
#'@param transfer_rules_df a set of travel model rules on what constituted a transfer
#'@param sampling_rate_float
#'@param output_file_string
#'@param append_boolean
#'@returns the path to which the output was written
Build_Database <- function(df_row,data_dir,
                           transfer_rules_df, sampling_rate_float,
                           output_file_string, append_boolean){
  load(paste0(data_dir,"/",df_row[['Transfers']]))
  transfers_df <- working.output

  load(paste0(data_dir,"/",df_row[['Transactions']]))
  transactions_df <- operator_counts

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

  output_filename <- paste0(data_dir,"/",output_file_string)
  write.csv(transfer.write, output_filename, row.names = FALSE, quote = T)
  return(output_filename)
}


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

