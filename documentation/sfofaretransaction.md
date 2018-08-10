
## Data dictionary

| variable name                            | description                                                  | note1                                                                                                                          | note2                                                   | note3                                                              | 
|------------------------------------------|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|--------------------------------------------------------------------| 
| ApplicationSerialNumber (K)              | Clipper card number                                          | used for chaining information                                                                                                  |                                                         |                                                                    | 
| ApplicationTransactionSequenceNumber (L) | this number is incremented every time you use your card      | putting money on your card increments this number -- this is the reason you may see a number skipped in the transactions table | 1 is initialization                                     |                                                                    | 
| GenerationTime (H)                       | Date/time field for when Clipper interacted with the system  | UTC                                                                                                                            | [subtract 8 hours (or 7 in the summer) to get local time](https://github.com/BayAreaMetro/clpr/blob/master/inst/sql/day_fares.sql#L4) | subtract 11 hours (or 10 in the summer) to get the date of service | 
| OperatorID (B)                           | decoded in Participant parameter table (almost alphabetical) |                                                                                                                                |                                                         |                                                                    | 

## Notes

* Berkeley institution ID's don't go into effect on this table until august 1, 2017
