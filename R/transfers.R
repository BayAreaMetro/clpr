library(stringr)

#' Adds columns identifying bart transactions, including lags and leads
#' @param tr_df a dataframe of transactions
#' @return tr_df a dataframe of transactions with columns: from_bart, to_bart, from_not_bart, to_not_bart
bart_identify <- function(tr_df) {
    tr_df <- tr_df %>%
      dplyr::group_by(cardid_anony) %>%
      dplyr::arrange(transaction_time) %>%
      dplyr::mutate(is_bart = operatorid==4,
             from_bart = lag(operatorid)==4,
             to_bart = lead(operatorid)==4,
             exit_to_not_bart = (is_bart & from_bart & !to_bart),
             entrance_from_not_bart = (is_bart & to_bart & !from_bart))
    return(tr_df)
}

#' Adds columns describing the route, time, and operator name of previous and later transactions
#' @param tr_df a dataframe of transactions
#' @return tr_df a dataframe of transactions
bart_lag_and_lead_metadata <- function(tr_df) {
    tr_df <- tr_df %>%
      dplyr::group_by(cardid_anony) %>%
      dplyr::arrange(transaction_time) %>%
      dplyr::mutate(timediff = abs(difftime(transaction_time, lag(transaction_time),units="mins")),
             time_of_previous = lag(transaction_time),
             transfer_from_time = round(timediff,2),
             transfer_to_operator_time = round(lead(timediff),2),
             transfer_to_not_bart = (exit_to_not_bart & transfer_to_operator_time<180),
             transfer_from_not_bart = (entrance_from_not_bart & transfer_from_time<180),
             transfer_from_operator = case_when(transfer_from_not_bart ~ lag(participantname)),
             transfer_to_operator = case_when(transfer_to_not_bart ~ lead(participantname)),
             transfer_from_route = case_when(transfer_from_not_bart ~ lag(routename)),
             transfer_to_route = case_when(transfer_to_not_bart ~ lead(routename)))

    #this is confusing, but you have to pull routes and transfers from
    #operators from two-transactions back
    #so here we effectively reach the lag back 2 transactions
    tr_df <- tr_df %>%
      dplyr::group_by(cardid_anony) %>%
      dplyr::arrange(transaction_time) %>%
      dplyr::mutate(transfer_from_operator=case_when(from_bart ~ lag(transfer_from_operator)),
             transfer_from_route=case_when(from_bart ~ lag(transfer_from_route)),
             transfer_from_operator_time = case_when(from_bart ~ lag(transfer_from_time)))
    return(tr_df)
}

#' Adds a column with a counts of the number of transactions per user
#' @param tr_df a dataframe of transactions
#' @return tr_df a dataframe of transactions with a transaction_count column
transactions_per_user <- function(tr_df) {
    tr_df <- tr_df %>%
      dplyr::group_by(cardid_anony,yday) %>%
      dplyr::mutate(transaction_count = n())
    return(tr_df)
}


#' Adds a column with a counts of the number of bart transactions per user and diff with all transactions
#' @param tr_df a dataframe of transactions
#' @return tr_df a dataframe of transactions with a bart_tr_count and tr_count_diff column
bart_transactions_per_user <- function(tr_df) {
    tr_df <- tr_df %>%
      dplyr::group_by(cardid_anony,yday,is_bart) %>%
      dplyr::mutate(bart_tr_count = n(),
             tr_count_diff = transaction_count-bart_tr_count)
    return(tr_df)
}


nicetime <- function(df1){
  out_time_df <- spread_time_column(df1$transaction_time, prefix="tag_on_")
  in_time_df <- spread_time_column(df1$time_of_previous, prefix="tag_out_")

  bart_od3 <- cbind(df1,in_time_df,out_time_df)
}


#' Spreads multiple transactions across columns into one-row-per-bart-trip (with a focus on transfers in and out)
#'
#' @param tr_df a sample of transactions, effectively from the raw sfofaretransactions table, joined to other tables
#' @return bart_xfer_df table in which transfers in and out of bart are captured with metadata
#' @importFrom dplyr group_by mutate case_when lag arrange filter select
as_bart_journeys <- function(tr_df){
  bart_rider_ids <- tr_df %>%
    dplyr::filter(operatorid==4) %>%
    dplyr::pull(cardid_anony)

  tr_df <- tr_df %>%
    dplyr::filter(cardid_anony %in% bart_rider_ids)

  tr_df <- bart_identify(tr_df)
  tr_df <- bart_lag_and_lead_metadata(tr_df)

  #here we drop the first in the series of bart transactions
  #the relevant metadata for the first has been pulled
  #onto the final transaction in bart_lag_and_lead_metadata()
  bart_xfer_df <- tr_df[tr_df$is_bart &
                            !is.na(tr_df$locationname.destination),]
  bart_xfer_df <- bart_xfer_df %>% ungroup() %>% as_tibble()
  return(bart_xfer_df)
}
