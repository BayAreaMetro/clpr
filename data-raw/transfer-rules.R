transfer_rules_df <- read.table(file = here::here('data-raw/transfer_rules_database.csv'), header = TRUE, sep = ",", stringsAsFactors = FALSE)
saveRDS(transfer_rules_df, here::here('data-raw/transfer_rules_df'))
devtools::use_data(transfer_rules_df, overwrite = TRUE)
