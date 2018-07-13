library(boxr)
library(dplyr)
library(tidyr)
box_auth()
transfer_filenames_df <- as.data.frame(box_ls(51200568211))
transfer_filenames_df <- tibble(name=transfer_filenames_df[,c('name')],
                                type="")
transfer_filenames_df$year <- vapply(transfer_filenames_df$name,function(x) {as.integer(stringr::str_sub(x, 1, 4))}, FUN.VALUE=0)
transfer_filenames_df$year <- vapply(transfer_filenames_df$name,function(x) {as.integer(stringr::str_sub(x, 1, 4))}, FUN.VALUE=0)
transfer_filenames_df$month <- vapply(transfer_filenames_df$name,function(x) {as.integer(stringr::str_sub(x, 8, 9))}, FUN.VALUE=0)
transfer_filenames_df$month <- vapply(transfer_filenames_df$name,function(x) {as.integer(stringr::str_sub(x, 8, 9))}, FUN.VALUE=0)
transfer_filenames_df[grepl("Transaction",transfer_filenames_df$name),]$type <- "Transactions"
transfer_filenames_df[grepl("Transfer",transfer_filenames_df$name),]$type <- "Transfers"
transfer_filenames_df <- transfer_filenames_df %>%
  group_by(year,month,type) %>%
  mutate(grouped_id = row_number()) %>%
  spread(type,name) %>%
  select(-grouped_id)
saveRDS(transfer_filenames_df, here::here('data-raw/transfer_filenames_df'))
devtools::use_data(transfer_filenames_df, overwrite = TRUE)
