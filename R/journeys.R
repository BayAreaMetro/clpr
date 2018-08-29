#' Adds column classifying transfers as given by the transfer rules dataframe
#' @param tr_df a dataframe of rides
#' @returns tr_df a dataframe of rides
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

#' Adds column identifying applicable groups of rides as journeys
#' @param tr_df a dataframe of rides
#' @returns tr_df a dataframe of rides
add_journey_id <- function(tr_df) {
  tr_df <- tr_df %>%
    dplyr::arrange(cardid_anony, transaction_time) %>%
    dplyr::mutate(is_transfer = transfer_time < max_time)

  journey_ids <- c()
  id <- 0
  for (row in 1:nrow(tr_df)) {
    if (is.na(tr_df[row, "is_transfer"]) | tr_df[row, "is_transfer"] == FALSE) {
      id <- id + 1
      }
    journey_ids <- c(journey_ids, id)
  }

   tr_df <- tr_df %>%
    ungroup() %>%
    dplyr::mutate(journey_id = journey_ids)
  return(tr_df)
}
