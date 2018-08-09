library(stringr)

#' Adds columns identifying bart transactions, including lags and leads
#' @param tr_df a dataframe of transactions
#' @returns tr_df a dataframe of transactions with columns: from_bart, to_bart, from_not_bart, to_not_bart
ggt_identify <- function(tr_df) {
  tr_df <- tr_df %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(is_ggt = operatorid==11,
                  from_ggt = lag(operatorid)==11,
                  to_ggt = lead(operatorid)==11,
                  exit_to_not_ggt = (is_ggt & from_ggt & !to_ggt),
                  entrance_from_not_ggt = (is_ggt & to_ggt & !from_ggt))
  return(tr_df)
}

#' Filters transactions to ggt intra-transfers only
#' @param tr_df a dataframe of transactions
#' @returns tr_df a dataframe of transactions
ggt_to_ggt <- function(tr_df) {
  tr_df <- ggt_identify(tr_df)
  tr_df <- tr_df %>%
    dplyr::filter(is_ggt & from_ggt & subtype == 3) %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(first_trip = !is.na(lead(transaction_time)),
                  second_trip = !is.na(lag(transaction_time)),
                  timediff = abs(difftime(transaction_time, lag(transaction_time),units="mins")),
                  time_of_previous = lag(transaction_time)) %>%
    dplyr::filter(first_trip | second_trip) %>%
    dplyr::filter(lead(timediff) < 180 | timediff < 180)
}

#' Creates combined dataframe of multiple transactions per row
#' @param tr_df a dataframe of transactions
#' @returns tr_df a dataframe of transactions
combined_ggt_transactions <- function (tr_df) {
  tr_df <- tr_df %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(second_originlocation = lead(originlocation),
                  second_destinationlocation = lead(destinationlocation),
                  second_locationname.origin = lead(locationname.origin),
                  second_locationname.destination = lead(locationname.destination),
                  second_timediff = lead(timediff),
                  second_time_of_previous = lead(time_of_previous)) %>%
    dplyr::filter(!is.na(second_originlocation))
}
