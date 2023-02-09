# Promantle
Experimental ways of storing data for high speed queries

* [x] Custom range triangular data
* [ ] Multi-page data with baked-in sorting

## Requirements

Uses Cockroach DB: https://www.cockroachlabs.com/docs/releases/index.html
or Postgresql DB: https://www.postgresql.org/

## Triangular data

A pre-aggregated data set for keeping a log of data, and being able to query arbitrary
ranges of data at different levels of detail.

You need to set-up a triangular list, with a list of the data-points to aggregate
and the different levels of detail to keep. This needs to be specified up-front.

```csharp
     // Create a Triangular list with a given 'key' and source data type
     // Here we are keying on date (so we can aggregate across hours, days, months, etc)
     // And we are going to feed the list with 'MySourceType' objects
var subject = TriangularList<DateTime, MySourceType>
     // Supply database connection
    .Create.UsingStorage(storage)
     // Set the 'key' used for ranges and levels-of-detail. This is: Database-type, Function to read from source data items, Function used to read ranges
    .KeyOn("TIMESTAMP", DateFromSourceType, DateMinMax)
     // Add an aggregation. This names a value extracted from source data
     // Arguments are: Name for storing and querying, Function to read from source data, Function that aggregates multiple items, Database-type
    .Aggregate<decimal>("Spent", item => item.SpentAmount, (a,b) => a+b, "DECIMAL")        // <-- functions can be lambdas
     // We can add as many aggregations as we want
    .Aggregate<decimal>("Earned", EarnedFromSourceType, DecimalSumAggregate, "DECIMAL")    // <-- or methods from a class
     // And have complex aggregations that calculate a value from the source data (rather than just extracting a stored value)
    .Aggregate<decimal>("MaxTransaction", item => Math.Max(item.EarnedAmount, item.SpentAmount), (a,b) => Math.Max(a,b), "DECIMAL")
    
     // We then add the ranges over which the data will be aggregated.
     // The ranks should have the smallest key range as the lowest rank,
     // and continue growing in range to the highest rank.
     // It's up to the programmer to ensure the ranges are strictly increasing.
     
     // Arguments are: Rank order, Rank name used for querying, Function that returns the key split into these ranges
     // The smallest rank is the smallest range you can query for.
    .Rank(1, "PerMinute", DateTimeMinutes) // multiple source data aggregated into per-minute buckets
    .Rank(2, "PerHour",   DateTimeHours)   // multiple per-minute buckets aggregated into per-hour buckets
    .Rank(3, "PerDay",    DateTimeDays)    // hours into days
    .Rank(4, "PerWeek",   DateTimeWeeks)   // days into weeks
    
    // Finally, build to get the TriangularList instance.
    // This will throw exceptions if it finds basic problems.
    // This will also create the required tables if not already present.
    .Build();
```

Note: Only the stated `Aggregate<>`s are stored in the database. If the source data items have extra data, it is ignored.

Then you can feed the list with new data. If you have already populated the database, you can query the existing data
and add to it.

```csharp
subject.WriteItem(new MySourceType{
    EarnedAmount = 2.5m,
    SpentAmount = 5.1m,
    RecordedDate = new DateTime(2020,5,5, 10,11,12, DateTimeKind.Utc)
});
```

When querying for aggregate data, it will be returned with a count of source items that have been combined at that point,
and the upper and lower bounds of the 'key' value in the data items combined at that point (which is different from the range that
the aggregation covers).

Be careful with aggregations; make sure they make sense. Do not aggregate averages, as you will get invalid value. Use a sum (adding)
aggregation and divide the outcome by value count instead:

```csharp
var spentOnDay = subject.ReadDataAtPoint<decimal>("Spent", "PerDay", new DateTime(2020,5,5,  0,0,1, DateTimeKind.Utc));
var averageSpend = spentOnDay.Value / spentOnDay.Count;
```

When making rank-range functions, it usually makes sense to transform the key value to a scalar, and then divide it by some amount.
It's best that the boundaries for each rank line-up, but it is not required.

```csharp
    private static readonly DateTime _baseDate = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static long DateTimeMinutes(DateTime item) => (long)Math.Floor((item - _baseDate).TotalMinutes);
    private static long DateTimeHours(DateTime item) => DateTimeMinutes(item) / 60;
    private static long DateTimeDays(DateTime item)  => DateTimeHours(item)   / 24;
    private static long DateTimeWeeks(DateTime item) => DateTimeDays(item)    / 7;
```

Item keys and rank-range functions are allowed to be entirely arbitrary: (see `PromantleTests.TriangularListTests.can_use_arbitrary_values_for_keys_and_ranks` for a worked example)

```csharp
public enum Geolocation: long
{
    China, India, USA, ...
}

. . .

var subject = TriangularList<Geolocation, SaleWithLocation>
    .Create.UsingStorage(storage)
    .KeyOn("INT", s=>s.SalesLocation, Regions.MinMax)
    .Aggregate<decimal>("Cost", s=>s.Cost, DecimalSumAggregate, "DECIMAL")
    .Aggregate<decimal>("Price", s=>s.SoldPrice, DecimalSumAggregate, "DECIMAL")
    .Rank(1, "Country",   r=>(long)r)
    .Rank(2, "Landmass",   r=>(long)Regions.LocationToLandmass(r))
    .Rank(3, "Zone",    r=>(long)Regions.LocationToGeoZone(r))
    .Rank(4, "Worldwide",    Regions.LocationToWorldwide)
    .Build();

subject.WriteItem(new SaleWithLocation(DateTime.Now, 10.00m, 35.00m, Geolocation.Angola));
// and so on...

// Get sales across Europe (that is, all in same 'Landmass' as Germany)
var sales = subject.ReadDataAtPoint<decimal>("Price", "Landmass", Geolocation.Germany);
var costs = subject.ReadDataAtPoint<decimal>("Cost", "Landmass", Geolocation.Germany);
var profit = sales.Value - costs.Value;
if (profit < 0) RunNewAdCampaign();
```

### TO DO:

* [x] Add ranks and aggregates
* [x] Query points and ranges
* [x] Get the upper/lower range of a single data point
* [x] Demonstrate averages
* [x] Demonstrate different data and key types
* [x] Demonstrate data persistence
* [x] Demonstrate different aggregates (max/min/..?)
* [ ] Tests around very sparse data
* [ ] Query for source data items under a given rank-1 value.

```

Rank Zero
(original
data pts)    Rank 1      Rank 2    . . .   Rank n
            :  ___      :
A     \     :     \     :
B      |    :      |    :
C      +---  AE    |    :
D      |    :      |    :
E     /     :      |___  AJ               ↑
F  \        :      |    :                 :
G   |       :      |    :                 :
H   +------  FJ    |    :                 : AN
I   |       :      |    :                 :
J  /        :  ___/     :                 :
K    \      :       \   :                 ↓
L     |____  KN      |__ KN
M     |     :        |  :
N    /      :       /   :

```