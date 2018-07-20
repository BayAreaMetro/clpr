
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
