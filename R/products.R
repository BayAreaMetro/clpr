#' Add human-readable product names to transaction entries
#'
#' @param tr_tbl a transactions tibble connection as returned from, for example day_of_transactions(rs,date)
#' @return tr_tbl a transactions tibble with a text value based product description column
#'
#' @importFrom dplyr left_join
get_product_description <- function(tr_tbl) {
  rs <- connect_rs()
  p_map <- tbl(rs, dbplyr::in_schema("clipper","contractprodtypemap")) %>% as_tibble()
  p_text <- tbl(rs, dbplyr::in_schema("clipper","products")) %>% as_tibble
  product_meta <- inner_join(p_map,p_text, by = c("issuerid", "producttype"))

  tr_out <- left_join(tr_tbl,product_meta,
                             by=c("contractid"="contracttype",
                                  "operatorid"="issuerid")) %>%
    rename(product_description=description) %>%
    select(-c(productcategory.y,productcategory.x,producttype))
  return(tr_out)
}


