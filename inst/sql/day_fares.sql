--=======================================================================================================
--This query subset transactions to 1 travel model day. 
--It takes parameters for start_day and end_day and the time at which the day should be partitioned
--It also casts the UTC timestamp on the data to PST, using a function which accounts 
--for daylight savings time
--an example use of this query r is here:
--https://github.com/BayAreaMetro/clpr/blob/d84eb0a39124ccdacd98badfb692894b4c4b98ae/R/extract.R#L84
--=======================================================================================================
create table ctp.fares_{date_title}
SORTKEY(cardid_anony,
        generationtime)
AS SELECT convert_timezone('US/Pacific', generationtime) as transaction_time,
          generationtime,
          FUNC_SHA1(applicationserialnumber) as cardid_anony,
          operatorid,
          vehicleid,
          routeid,
          recordtype,
          subtype,
          contractid,
          contractserialnumber,
          productcategory,
          farecategory,
          transferoperator,
          transferdiscountflag,
          rideannulled,
          originlocation,
          tripsequencenumber,
          institutionid,
          envelopeheaderversion,
          transactionqualifiernegativefare,
          transactionqualifierspecialfailure,
          securitymoduleid,
          deviceserialnumber,
          sequencenumber,
          applicationserialnumber,
          destinationlocation,
          purseamount,
          pursebalance
FROM clipper.sfofaretransaction
    WHERE generationtime > '{start_date} {partition_time}'
    AND generationtime < '{end_date} {partition_time}';
