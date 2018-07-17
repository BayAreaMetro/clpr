create table clipper_days.fares_{date_title} 
SORTKEY(cardid_anony,
        generationtime)
AS SELECT ((generationtime) AT TIME ZONE 'UTC') AT TIME ZONE 'PST' as psttime,
          generationtime,
          md5(applicationtransactionsequencenumber) as cardid_anony,
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
FROM ctp.y2016 
    WHERE generationtime > '{start_date} {partition_time}'
    AND generationtime < '{end_date} {partition_time}';