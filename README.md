clpr
=================

This is an R package with analysis utilities and approaches for a data set of obscured and anonymized Clipper smart card transactions.

# Installation



# Status

MTC is working to release a data set of Clipper smart card transactions.  The transactions are anonymized, obscured, and sampled to protect customer identities.  To date, the data set is being Alpha tested within MTC.  The scripts in this repository support the Alpha testing work.

anonymous-clipper/Transfers
=================

To help validate the MTC travel model, here we summarize successive movements made by a single Clipper card within [pre-defined time windows](transfer_rules_database.csv).  We refer to these as transfers.  The scripts here [extract the data from the anonymous Clipper database](Extract Transfer for Random Weekday to CSV.R), [create interactive pair-wise by tag time difference plots](To and From Interactive.Rmd), and [build a database of transfer summaries](Build Transfer Database.Rmd).  The [resulting data](Summaries/Transfers by day by agency pair.csv) is summarized in [this Tableau workbook](Summaries/Clipper Transfers.twb) [here](http://analytics.mtc.ca.gov/foswiki/Main/ClipperTransfers).  


