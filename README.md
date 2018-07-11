
-   [clpr](#clpr)
-   [Installation](#installation)
-   [Background](#background)
-   [Goal](#goal)
-   [Vignettes/Examples](#vignettesexamples)
-   [Resulting Data](#resulting-data)

clpr
====

This is an R package with analysis utilities and approaches for a data set of obscured and anonymized Clipper smart card transactions.

Installation
============

``` r
if (!require(devtools)) {
    install.packages('devtools')
}
devtools::install_github('bayareametro/clpr')
```

Background
==========

To help validate the MTC travel model, MTC (David Ory) summarized successive movements made by a single Clipper card within [pre-defined time windows](data-raw/transfer_rules_database.csv). We refer to these as transfers.

In this repository, we've moved the scripts and markdown documents around to fit within R Packaging specifications in order to manage maintenance and collaboration around these scripts going forward.

Goal
====

The main scripts are eponymous. They [extract-data.Rmd](vignettes/extract-data.Rmd), [create interactive pair-wise by tag time difference plots](vignettes/To%20and%20From%20Interactive.Rmd), and [vignettes/build a database of transfer summaries](Build%20Transfer%20Database.Rmd).

Vignettes/Examples
==================

Further background and vignettes (these are in draft form, some may be more useful than others).

-   [Alpha Test 01 - Find People for Random Weekday.Rmd](vignettes/Alpha%20Test%2001%20-%20Find%20People%20for%20Random%20Weekday.Rmd)
-   [Alpha Test 01 - Find People.Rmd](vignettes/Alpha%20Test%2001%20-%20Find%20People.Rmd)
-   [Alpha Test 02 - If A and B and C for Random Weekday.Rmd](vignettes/Alpha%20Test%2002%20-%20If%20A%20and%20B%20and%20C%20for%20Random%20Weekday.Rmd)
-   [Alpha Test 02 - If A and B and C.Rmd](vignettes/Alpha%20Test%2002%20-%20If%20A%20and%20B%20and%20C.Rmd)
-   [Build Transfer Database.Rmd](vignettes/Build%20Transfer%20Database.Rmd)
-   [To and From Interactive.Rmd](vignettes/To%20and%20From%20Interactive.Rmd)
-   [extract-data.Rmd](vignettes/extract-data.Rmd)

Resulting Data
==============

The [resulting data](data-raw/Transfers%20by%20day%20by%20agency%20pair.csv) is summarized in [this Tableau workbook](data-raw/Clipper%20Transfers.twb) [here](http://analytics.mtc.ca.gov/foswiki/Main/ClipperTransfers).
