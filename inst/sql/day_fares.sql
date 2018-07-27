create table ctp.fares_{date_title}
SORTKEY(cardid_anony,
        generationtime)
AS SELECT ((generationtime) AT TIME ZONE 'UTC') AT TIME ZONE 'PST' as transaction_time,
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
          destinationlocation
FROM clipper.sfofaretransaction
    WHERE generationtime > '{start_date} {partition_time}'
    AND generationtime < '{end_date} {partition_time}';
