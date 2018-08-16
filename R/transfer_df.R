#' This is an R script to create a table with transfer information for all combinations of operators for a given time window
#'
#' Adds column to indicate a transfer under given time windows
#' @param tr_df a dataframe of transactions
#' @param mins time period to qualify as a transfer (in minutes)
#' @returns tr_df a dataframe of transactions
identify_transfer_for_time <- function(tr_df, mins) {
  tr_df <- tr_df %>%
    clpr::drop_tagons() %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(time_from_prev_tagon =
                    case_when(is.na(tagon_time) ~ difftime(transaction_time, lag(transaction_time, order_by=transaction_time), units="mins"))) %>%
    dplyr::mutate(time_from_prev =
                    case_when(!is.na(tagon_time) ~ difftime(transaction_time, lag(tagon_time, order_by=transaction_time), units="mins"))) %>%
    dplyr::mutate(is_transfer = ifelse(time_from_prev <= mins | time_from_prev_tagon <= mins, 1, 0))
  tr_df$is_transfer[is.na(tr_df$is_transfer)] = 0
  return(tr_df)
}

#' Creates a summary table of all combinations of transfers
#' @param tr_df a dataframe of all transactions
#' @param mins time period to qualify as a transfer (in minutes)
#' @returns transfer_df a dataframe of transfer information
create_transfer_df <- function(tr_df, mins) {
  transfer_df <- tr_df %>%
  identify_transfer_for_time(mins) %>%
  dplyr::group_by(participantname, participantname.transfer) %>%
  summarise(from_operator_id = mean(transferoperator),
              to_operator_id = mean(operatorid),
              num_trips = n(),
              num_transfers = sum(is_transfer),
              ratio_transfers = mean(is_transfer),
              num_discounted = sum(transferdiscountflag),
              ratio_discounted = sum(transferdiscountflag)/sum(is_transfer))
  return(transfer_df)
}

