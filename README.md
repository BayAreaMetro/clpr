clpr
================

This is an [R package](http://kbroman.org/pkg_primer/) with analysis utilities and approaches for a data set of obscured and anonymized Clipper smart card transactions.

Goal
====

This package can be used to support documentation and collaboration around the analysis of anonymized Clipper trip data with extensible, open-source, industry-standard data analysis tools like [RStudio](https://en.wikipedia.org/wiki/RStudio).

It can be used to help answer questions like the following:

1. What are Station-to-Station Tabulations for Fixed-Guideway systems? 
2. What are Major Transfer Movements? 
3. What are the uses of various Clipper products like?

For example:
* BART to/from MUNI  
* Ferry Service to BART  
* The above movements are (ideally) station and route specific  

Dependencies
============

The DV Data Lake schemas `clipper` and `ctp` are the major dependency of this package. Some limited documentation for those schemas can be found [here](https://github.com/BayAreaMetro/DataServices/tree/master/Project-Documentation/clipper). 

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

Please see the [tutorial](https://bayareametro.github.io/clpr/articles/clpr-tutorial.html) and the [reference](https://bayareametro.github.io/clpr/reference/index.html). 

Background
===============

This package was started as a set of R Markdown scripts in 2014. Those scripts were based around a database that is not available to us presently. So for now the scripts were removed from the repository but are part of git history. They may be useful for understanding how to work with these data in the future and can be found [here](https://github.com/BayAreaMetro/clpr/commit/808808adcdb73519a7bf6006e0dd84ba716cbe8d) and [here](https://github.com/BayAreaMetro/clpr/commit/7645ceb541b6353909034b709407cb5aaacecdf6).   


Building the Docs
===============

Docs can be re-built with [pkgdown](https://github.com/r-lib/pkgdown) like so:

```
library(pkgdown)
pkgdown::build_site()
```

The output to the 'docs' folder. 

