# ReadMe:
## Here, The required task is to build a simplified query processor that accesses data from the partitioned ratings table. (From Database-Partitioning-1)
**Input Data:** 
Same as in Database-Partitioning-1 i.e. ratings.dat file.

### Required Task: 
**Below description of the functions implemented in Python script:**

**• RangeQuery() –**

- Implemented a Python function RangeQuery that takes as input: (1) Ratings table
  stored in PostgreSQL, (2) RatingMinValue (3) RatingMaxValue (4)
  open connection
- Please note that the RangeQuery would not use ratings table but it would use the
  range and round robin partitions of the ratings table.
- RangeQuery() then returns all tuples for which the rating value is larger than or
  equal to RatingMinValue and less than or equal to RatingMaxValue.
- The returned tuples are stored in a text file, named RangeQueryOut.txt (in
the same directory where DbQuery.py is present) such that each
line represents a tuple that has the following format such that PartitionName
represents the full name of the partition i.e. RangeRatingsPart1 or
RoundRobinRatingsPart4 etc. in which this tuple resides.
PartitionName, UserID, MovieID, Rating


**• PointQuery() –**

- Implemented a Python function PointQuery that takes as input: (1) Ratings table
 stored in PostgreSQL, (2) RatingValue. (3) open connection
- Please note that the PointQuery would not use ratings table but it would use the
range and round robin partitions of the ratings table.
- PointQuery() then returns all tuples for which the rating value is equal to
RatingValue.
- The returned tuples are stored in a text file, named PointQueryOut.txt such that each
line represents a tuple that has the following format such that PartitionName
represents the full name of the partition i.e. RangeRatingsPart1 or
RoundRobinRatingsPart4 etc. in which this tuple resides.
PartitionName, UserID, MovieID, Rating
