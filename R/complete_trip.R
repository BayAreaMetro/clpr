#' Add columns for tag on time, trip duration to tag off transactions
#' @param tr_df dataframe of transactions
#' @returns tr_df dataframe of transactions
add_tagon_time <- function(tr_df) {
  tr_df <- tr_df %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(tagging_off = (subtype == 3 | subtype == 5)) %>%
    dplyr::mutate(tagon_time = case_when(tagging_off ~ lag(transaction_time, order_by=transaction_time))) %>%
    dplyr::mutate(trip_duration = difftime(transaction_time, tagon_time, units="mins"))
  return(tr_df)
}

#' Add columns for tag on time, trip duration to tag off transactions using DB native functions
#' @param tr_df tbl of transactions
#' @returns tr_df tbl of transactions
add_tagon_time_db <- function(tr_df) {
  tr_df <- tr_df %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(tagging_off = (subtype == 3 | subtype == 5)) %>%
    dplyr::mutate(tagon_time = case_when(tagging_off ~ lag(transaction_time, order_by=transaction_time))) %>%
    dplyr::mutate(trip_duration = DATEDIFF(minute, transaction_time, tagon_time))
  return(tr_df)
}

#' Add column for previous purse amount and actual trip cost for trips with subtypes 2/3
#' @param tr_df dataframe of transactions
#' @returns tr_df dataframe of transactions
add_trip_cost <- function(tr_df) {
  tr_df <- tr_df %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(previous_purseamount = case_when(subtype == 3 ~ lag(purseamount, order_by=transaction_time))) %>%
    dplyr::mutate(trip_cost = case_when(subtype == 3 ~ previous_purseamount - purseamount,
                                        subtype != 3 ~ purseamount))
  return(tr_df)
}

#' Add column for previous purse amount and actual trip cost for trips with subtypes 2/3
#' @param tr_df dataframe of transactions
#' @returns tr_df dataframe of transactions
add_trip_cost_db <- function(tr_df) {
  tr_df <- tr_df %>%
    dplyr::group_by(cardid_anony) %>%
    dplyr::arrange(transaction_time) %>%
    dplyr::mutate(previous_purseamount = case_when(subtype == 3 ~ lag(purseamount, order_by=transaction_time))) %>%
    dplyr::mutate(trip_cost = case_when(subtype == 3 ~ previous_purseamount - purseamount,
                                        subtype != 3 ~ purseamount))
  return(tr_df)
}


#' Drop rows with tag on subtypes after recording the relevant tag on information in the tag off transactions
#' @param tr_df dataframe of transactions
#' @returns tr_df dataframe of transactions
drop_tagons <- function(tr_df) {
  tr_df <- tr_df %>%
    add_tagon_time() %>%
    add_trip_cost() %>%
    dplyr::filter(subtype != 2 & subtype != 4)
  return(tr_df)
}

#' Drop rows with tag on subtypes after recording the relevant tag on information in the tag off transactions
#' Do it in the database (faster)
#' @param tbl dataframe of transactions
#' @returns tbl dataframe of transactions
drop_tagons_db <- function(tr_df) {
  tr_df <- tr_df %>%
    add_tagon_time_db() %>%
    add_trip_cost() %>%
    dplyr::filter(subtype != 2 & subtype != 4)
  return(tr_df)
}

