    SELECT
        [Year],
        [Month],
        [DayofWeekID] AS CircadianDayOfWeek,
        [DayOfWeek] AS CircadianDayOfWeek_Name,
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
        [gis].[Clipper].[ThreeRandomWeeksAllMonths]
    WHERE
        (
            Month =
