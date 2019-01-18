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
    dplyr::mutate(previous_participant = lag(participantname, order_by = transaction_time))
  return(tr_df)
}

#' Adds columns grouping applicable rides as numbered journeys along with number of legs per journey
#' @param tr_df a dataframe of rides
#' @return tr_df a dataframe of rides
add_journey_id <- function(tr_df, time=60) {
  tr_df <- tr_df %>%
    add_transfer_time() %>%

    dplyr::arrange(cardid_anony, transaction_time) %>%
    dplyr::mutate(is_transfer = transfer_time < time)

  tr_df$is_transfer <- ifelse(is.na(tr_df$is_transfer),
                               FALSE, tr_df$is_transfer)

  ids <- 1 - tr_df$is_transfer
  journey_ids <- Reduce("+", ids, accumulate=TRUE)

  tr_df <- tr_df %>%
    ungroup() %>%
    dplyr::mutate(journey_id = journey_ids)

  num_legs <- as.data.frame(table(tr_df$journey_id))
  num_legs$Var1 <- num_legs$Freq
  num_legs <- rep(num_legs$Var1, num_legs$Freq)

  tr_df <- tr_df %>%
    mutate(num_legs = num_legs)

  return(tr_df)
}
