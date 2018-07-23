
-   [clpr](#clpr)
-   [Installation](#installation)
-   [Background](#background)
-   [Goal](#goal)
-   [Vignettes/Examples](#vignettesexamples)
-   [Resulting Data](#resulting-data)

clpr
====

This is an [R package](http://kbroman.org/pkg_primer/) with analysis utilities and approaches for a data set of obscured and anonymized Clipper smart card transactions.

Goal
====

This package can be used to support documentation and collaboration around the analysis of anonymized Clipper trip data with extensible, open-source, industry-standard data analysis tools like [RStudio](https://en.wikipedia.org/wiki/RStudio).

It can be used to help answer questions like the following:

1. What are Station-to-Station Tabulations for Fixed-Guideway systems? 
2. What are Major Transfer Movements? 

For example:
- BART to/from MUNI
- CALTRAIN to/from MUNI
- CALTRAIN to/from SCVTA
- Ferry Service to/from MUNI or BART
- The above movements are (ideally) station and route specific

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
==========
If you [define environmental variables](https://stat.ethz.ch/R-manual/R-devel/library/base/html/Sys.setenv.html) for the database, you can use the `connect_rs()` function to connect to the database. See expected variable names in [R/connect_db.R](R/connect_db.R) 

Otherwise, you'll have to connect to the db as you prefer. 

Testing
===========
If you set environmental variables as above, you can run some of the (admittedly not complete) tests with Ctrl/Cmd + Shift + T or `devtools::test().`

Example Usage
===========

Sample a day of transactions by user

Note that date must be formatted as below for now. YYYY-MM-DD

```{r}
rs <- connect_rs()
date <- "2016-04-25"
transactions_tbl <- sample_day_of_transactions(rs,date,n_users=100)
transactions_df <- as_tibble(transactions_tbl)
```

Flatten those transactions into per-row BART transfers. 

```{r}
bart_od <- bart_transactions_as_transfers(sample_df)
```

Use lubridate to spread the timestamp column into day of year, month, hour, and minute integers. 

```{r}
out_time_df <- spread_time_column(bart_od$transaction_time, prefix="tag_out_")
in_time_df <- spread_time_column(bart_od$time_of_previous, prefix="tag_on_")
bart_od_nicetime <- cbind(bart_od,in_time_df,out_time_df)
```

Pull a full day of transactions

```{r}
rs <- connect_rs()
date <- "2016-04-25"
transactions_tbl <- day_of_transactions(rs,date,n_users=100, drop_existing_table=FALSE)
transactions_df <- as_tibble(transactions_tbl)
```

Contributing
============

You can contribute code, data, or questions. Please feel free to [open an issue](https://github.com/BayAreaMetro/clpr/issues) with any questions about how to use the package.  

2014 By-Operator Transfer Summary 
====

To help validate the MTC travel model, MTC (David Ory) summarized successive movements made by a single Clipper card within [pre-defined time windows](data-raw/transfer_rules_database.csv). We refer to these as transfers.

The main scripts are eponymous. They [extract-data](vignettes/extract-data.Rmd), [create interactive pair-wise by tag time difference plots](vignettes/To-and-From-Interactive.Rmd), and [build a database of transfer summaries](vignettes/Build-Transfer-Database.Rmd).

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
