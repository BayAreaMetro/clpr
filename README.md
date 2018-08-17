clpr
================

-   [Goal](#goal)
-   [Installation](#installation)
-   [Setup](#setup)
-   [Testing](#testing)
-   [Example Usage](#example-usage)
-   [Contributing](#contributing)
-   [2014 By-Operator Transfer Summary](#by-operator-transfer-summary)
-   [Vignettes/Examples](#vignettesexamples)
-   [Resulting Data](#resulting-data)

This is an [R package](http://kbroman.org/pkg_primer/) with analysis utilities and approaches for a data set of obscured and anonymized Clipper smart card transactions.

Goal
====

This package can be used to support documentation and collaboration around the analysis of anonymized Clipper trip data with extensible, open-source, industry-standard data analysis tools like [RStudio](https://en.wikipedia.org/wiki/RStudio).

It can be used to help answer questions like the following:

1.  What are Station-to-Station Tabulations for Fixed-Guideway systems?
2.  What are Major Transfer Movements?

For example: - BART to/from MUNI - Ferry Service to BART - The above movements are (ideally) station and route specific

Installation
============

``` r
if (!require(devtools)) {
    install.packages('devtools')
}
devtools::install_github('bayareametro/clpr')
```

This package has a number of dependencies, the major ones being the `tidyverse` and `RPostgres`

We've tested it on an MTC Windows 10 machine and Mac OS Sierra and it seems to work on both, though we need to do more testing.

Setup
=====

If you [define environmental variables](https://stat.ethz.ch/R-manual/R-devel/library/base/html/Sys.setenv.html) for the database, you can use the `connect_rs()` function to connect to the database. See expected variable names in [R/connect\_db.R](R/connect_db.R)

Otherwise, you'll have to connect to the db as you prefer.

Testing
=======

If you set environmental variables as above, you can run some of the (admittedly not complete) tests with Ctrl/Cmd + Shift + T or `devtools::test().`

Example Usage
=============

Sample a day of transactions by user

Note that date must be formatted as below for now. YYYY-MM-DD

Note that we source a local R script that defines the database connection details.

``` r
library(DBI)
library(dbplyr)
library(dplyr)
library(clpr)
source("~/.keys/rs.R")
rs <- connect_rs()
date <- "2016-04-25"
transactions_tbl <- sample_day_of_transactions(rs,date,n_users=100)
transactions_df <- as_tibble(transactions_tbl)
```

First, lets use the `drop_tagons` function to change the unit of observation from transactions to rides, where a ride is a ride on an operator.

``` r
rides_df <- drop_tagons(transactions_df)
```

We can also create a dataframe summarizing transfers within a given time window (in minutes), using the `create_transfer_df` function.

``` r
transfer_df <- create_transfer_df(rides_df, 120)
knitr::kable(transfer_df)
```

| participantname.transfer | participantname     |  from\_operator\_id|  to\_operator\_id|  num\_transfers|  num\_discounted|  transfer\_revenue|
|:-------------------------|:--------------------|-------------------:|-----------------:|---------------:|----------------:|------------------:|
| AC Transit               | BART                |                   1|                 4|               0|                0|                0.0|
| BART                     | AC Transit          |                   4|                 1|               0|                0|                0.0|
| BART                     | BART                |                   4|                 4|               1|                0|                3.2|
| BART                     | SF Muni             |                   4|                18|               0|                0|                0.0|
| Golden Gate Transit      | Golden Gate Transit |                  11|                11|               1|                1|                6.2|
| SF Muni                  | BART                |                  18|                 4|               0|                0|                0.0|
| SF Muni                  | SF Muni             |                  18|                18|               0|                0|                0.0|

Alternatively, we can use the `bart_transactions_as_transfers` function to change the unit of observation from transactions to rides on BART only, with additional information about the rides that individuals may have taken before or after boarding BART. For example, taking a ferry and then BART.

``` r
bart_od <- bart_transactions_as_transfers(transactions_df)
```

The outcome includes the time of the previous transaction to BART tag-on. For example, a user tagged off of the ferry at 7:05 and then onto bart at 7:20. Or, a user tagged onto an SF Muni bus at 7:00 and then onto bart at 7:30. It also includes the time they tagged onto the following ride.

You can use use the convenience function `bart_od_nicetime` to spread the timestamp column into day of year, month, hour, and minute integers.

``` r
out_time_df <- spread_time_column(bart_od$transaction_time, prefix="tag_out_")
in_time_df <- spread_time_column(bart_od$time_of_previous, prefix="tag_on_")
bart_od_nicetime <- cbind(bart_od,in_time_df,out_time_df)
```

This can working with the time data easier. For example, plotting a histogram of the tag on hour.

``` r
hist(bart_od_nicetime$tag_on_hour, breaks=24)
```

![](readme_files/figure-markdown_github/unnamed-chunk-6-1.png)

We can also pull a full day of transactions using `day_of_transactions`.

``` r
rs <- connect_rs()
date <- "2016-04-25"
transactions_tbl <- day_of_transactions(rs,date)
transactions_df <- as_tibble(transactions_tbl)
time_df <- spread_time_column(transactions_df$transaction_time, prefix="trnsct_")
transactions_df <- cbind(transactions_df,time_df)
```

``` r
hist(transactions_df$trnsct_hour, breaks=24)
```

![](readme_files/figure-markdown_github/unnamed-chunk-8-1.png)

Contributing
============

You can contribute code, data, or questions. Please feel free to [open an issue](https://github.com/BayAreaMetro/clpr/issues) with any questions about how to use the package.

2014 By-Operator Transfer Summary
=================================

To help validate the MTC travel model, MTC (David Ory) summarized successive movements made by a single Clipper card within [pre-defined time windows](data-raw/transfer_rules_database.csv). We refer to these as transfers.

The main scripts [extract-data](vignettes/extract-data.Rmd), [create interactive pair-wise by tag time difference plots](vignettes/To-and-From-Interactive.Rmd), and [build a database of transfer summaries](vignettes/Build-Transfer-Database.Rmd).

Vignettes/Examples
==================

Further background and vignettes (these are in draft form, some may be more useful than others).

-   [Alpha-Test-01---Find-People-for-Random-Weekday.Rmd](vignettes/AlphaTest01---Find-People-for-Random-Weekday.Rmd)
-   [Alpha-Test-01---Find-People.Rmd](vignettes/Alpha-Test-01---Find-People.Rmd)
-   [Alpha-Test-02---If-A-and-B-and-C-for-Random-Weekday.Rmd](vignettes/Alpha-Test-02---If-A-and-B-and-C-for-Random-Weekday.Rmd)
-   [Alpha-Test-02---If-A-and-B-and-C.Rmd](vignettes/Alpha-Test-02---If-A-and-B-and-C.Rmd)
-   [Build-Transfer-Database.Rmd](vignettes/Build-Transfer-Database.Rmd)
-   [To-and-From-Interactive.Rmd](vignettes/To-and-From-Interactive.Rmd)
-   [extract-data.Rmd](vignettes/extract-data.Rmd)
-   [get-transfers.R](vignettes/extract-data.Rmd) - get transfers on and off bart

Resulting Data
==============

The [resulting data](data-raw/Transfers%20by%20day%20by%20agency%20pair.csv) is summarized in [this Tableau workbook](data-raw/Clipper%20Transfers.twb).
