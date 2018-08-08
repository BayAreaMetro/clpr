#' Filters transactions to BART intra-transfers only
#' @param tr_df a dataframe of transactions
#' @returns tr_df a dataframe of transactions
bart_to_bart <- function(tr_df) {
  tr_df <- bart_identify(tr_df)
  tr_df <- tr_df %>%
    dplyr::filter(is_bart & from_bart & subtype == 5) %>%
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
combined_bart_transactions <- function (tr_df) {
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
