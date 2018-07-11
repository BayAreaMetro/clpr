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
