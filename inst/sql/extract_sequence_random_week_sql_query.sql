    SELECT
        DATEPART(yyyy,
        [CircadianDate]) AS Year,
        DATEPART(month,
        [CircadianDate]) AS Month,
        DATEPART(dw,
        [CircadianDate]) AS CircadianDayOfWeek,
        [RandomWeekID],
        CONVERT(varchar(max),
        [ClipperCardID],
        2) AS ClipperCardID,
        [TripSequenceID],
        [AgencyID],
        [AgencyName],
        [PaymentProductID],
        [PaymentProductName],
        [FareAmount],
        [TagOnTime_Time],
        [TagOnLocationID],
        LTRIM(RTRIM([TagOnLocationName])) AS TagOnLocationName,
        [RouteID],
        [RouteName],
        [TagOffTime_Time],
        [TagOffLocationID],
        LTRIM(RTRIM([TagOffLocationName])) AS TagOffLocationName,

    FROM
        [gis].[Clipper].[ThreeRandomWeeks-2013-03]