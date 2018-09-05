#' Adds column classifying transfers as given by the transfer rules dataframe
#' @param tr_df a dataframe of rides
#' @return tr_df a dataframe of rides
add_transfer_time <- function(tr_df) {
  tr_df <- tr_df %>%
    as_rides() %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(previous_time = lag(transaction_time, order_by = transaction_time)) %>%
    dplyr::mutate(transfer_time = difftime(transaction_time, previous_time, units="mins")) %>%
    dplyr::mutate(previous_participant = lag(participantname, order_by = transaction_time)) %>%
    dplyr::left_join(transfer_rules_df,
                     by=c("previous_participant" = "from_AgencyName",
                          "participantname" = "to_AgencyName"))
  return(tr_df)
}

#' Adds column grouping applicable rides as numbered journeys
#' @param tr_df a dataframe of rides
#' @return tr_df a dataframe of rides
add_journey_id <- function(tr_df) {
  tr_df <- tr_df %>%
    dplyr::arrange(cardid_anony, transaction_time) %>%
    dplyr::mutate(is_transfer = transfer_time < max_time)

  tr_df$is_transfer <- ifelse(is.na(tr_df$is_transfer),
                               FALSE, tr_df$is_transfer)

  ids <- 1 - tr_df$is_transfer
  journey_ids <- Reduce("+", ids, accumulate=TRUE)

  tr_df <- tr_df %>%
    ungroup() %>%
    dplyr::mutate(journey_id = journey_ids)

  return(tr_df)
}
