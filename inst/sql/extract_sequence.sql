    SELECT
        [CircadianDate],
        CONVERT(varchar(max),
        [ClipperCardID],
        2) AS ClipperCardID,
        [AgencyID],
        [AgencyName],
        [PaymentProductID],
        [PaymentProductName],
        [FareAmount],
        [TagOnTime],
        [TagOnLocationID],
        LTRIM(RTRIM([TagOnLocationName])) AS TagOnLocationName,
        [RouteID],
        [RouteName],
        [TagOffTime],
        [TagOffLocationID],
        LTRIM(RTRIM([TagOffLocationName])) AS TagOffLocationName,

    FROM
        [gis].[Clipper].[AlphaTest-2013-04]
