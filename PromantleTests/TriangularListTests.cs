//#define UseCockroach

using System.Diagnostics;
using Npgsql;
using NUnit.Framework;
using Promantle;
using PromantleTests.Helpers;
// ReSharper disable AssignNullToNotNullAttribute
// ReSharper disable RedundantTypeArgumentsOfMethod
#pragma warning disable CS8602

namespace PromantleTests;

[TestFixture, SingleThreaded]
public class TriangularListTests : DbTestBase
{
    [Test]
    public void the_database_hooks_work()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");

        // Create table
        var written = storage.EnsureTableForRank(1, 1, "INT", new BasicColumn("test", "int"));
        Console.WriteLine($"Wrote DB: {written}");

        // Write single value
        storage.WriteAtRank(1, 1, "test", 1, 2, 3, 4, 15, 20);

        // Read single value back
        var value = storage.ReadAtRank(1, 1, "test", 2);
        Assert.That(value?.ParentPosition, Is.EqualTo(1));
        Assert.That(value?.Position, Is.EqualTo(2));
        Assert.That(value?.Count, Is.EqualTo(3));
        Assert.That(value?.Value, Is.EqualTo(4));
        Assert.That(value?.LowerBound, Is.EqualTo(15));
        Assert.That(value?.UpperBound, Is.EqualTo(20));

        // More values
        storage.WriteAtRank(rank: 1, rankCount: 1, aggregateName: "test", parentPosition: 2, position: 3, count: 4, value: 5, lowerBound: 25, upperBound: 30);
        storage.WriteAtRank(1, 1, "test", 2, 4, 5, 6, 35, 40);
        storage.WriteAtRank(1, 1, "test", 2, 5, 6, 7, 45, 50);

        // Read multiple values back
        var values = storage.ReadWithRank(1, 1, "test", 2, 4).ToList();
        Assert.That(values.Count, Is.EqualTo(3));

        Assert.That(values[0].Value, Is.EqualTo(4));
        Assert.That(values[0].Count, Is.EqualTo(3));
        Assert.That(values[0].Position, Is.EqualTo(2));
        Assert.That(values[0].LowerBound, Is.EqualTo(15));
        Assert.That(values[0].UpperBound, Is.EqualTo(20));
        Assert.That(values[0].ParentPosition, Is.EqualTo(1));

        Assert.That(values[1].Value, Is.EqualTo(5));
        Assert.That(values[1].Count, Is.EqualTo(4));
        Assert.That(values[1].Position, Is.EqualTo(3));
        Assert.That(values[1].LowerBound, Is.EqualTo(25));
        Assert.That(values[1].UpperBound, Is.EqualTo(30));
        Assert.That(values[1].ParentPosition, Is.EqualTo(2));

        Assert.That(values[2].Value, Is.EqualTo(6));
        Assert.That(values[2].Count, Is.EqualTo(5));
        Assert.That(values[2].Position, Is.EqualTo(4));
        Assert.That(values[2].LowerBound, Is.EqualTo(35));
        Assert.That(values[2].UpperBound, Is.EqualTo(40));
        Assert.That(values[2].ParentPosition, Is.EqualTo(2));

        // Read back by parent
        var parentValues = storage.ReadWithParentRank(1, 1, "test", 2).ToList();
        Assert.That(parentValues.Count, Is.EqualTo(3));
        Assert.That(parentValues[0].Value, Is.EqualTo(5));
        Assert.That(parentValues[1].Value, Is.EqualTo(6));
        Assert.That(parentValues[2].Value, Is.EqualTo(7));
    }

    [Test]
    public void can_create_a_new_list_with_functions()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(0, "PerHour", DateTimeHours)
            .Build();

        Assert.That(subject, Is.Not.Null);
    }

    [Test]
    public void can_write_a_single_scaled_value()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(0, "PerHour", DateTimeHours)
            .Build();

        var cc = subject.WriteItem(new TestComplexType());

        Console.WriteLine($"{cc} calculations");
        Assert.That(cc, Is.GreaterThan(0));
    }

    [Test]
    public void can_read_a_single_scaled_value()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(0, "PerHour", DateTimeHours)
            .Build();

        subject.WriteItem(new TestComplexType
        {
            EarnedAmount = 2.5m,
            SpentAmount = 5.1m,
            RecordedDate = new DateTime(2020, 5, 5, 10, 11, 12, DateTimeKind.Utc)
        });

        var value = subject.ReadAggregateDataAtPoint<decimal>("Spent", "PerHour", new DateTime(2020, 5, 5, 10, 10, 32)); // should find it if in same hour

        Assert.That(value, Is.Not.Null);
        Assert.That(value, Is.EqualTo(5.1m));
    }

    [Test]
    public void can_read_a_range_over_a_single_scaled_value()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(0, "PerHour", DateTimeHours)
            .Build();

        subject.WriteItem(new TestComplexType
        {
            EarnedAmount = 2.5m,
            SpentAmount = 5.1m,
            RecordedDate = new DateTime(2020, 5, 5, 10, 11, 12, DateTimeKind.Utc)
        });

        var values = subject.ReadAggregateDataOverRange<decimal>("Spent", "PerHour", new DateTime(2020, 1, 1, 8, 30, 29), new DateTime(2021, 1, 1, 16, 45, 31)).ToList();

        Assert.That(values.Count, Is.GreaterThan(0));
        Assert.That(values[0], Is.EqualTo(5.1m));
    }

    [Test]
    public void can_read_the_children_of_a_single_scaled_value()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(0, "PerHour", DateTimeHours)
            .Build();

        subject.WriteItem(new TestComplexType("2020-05-05T09:11:12", 5.0m, 2.5m));
        subject.WriteItem(new TestComplexType("2020-05-05T10:11:12", 5.1m, 2.5m));
        subject.WriteItem(new TestComplexType("2020-05-05T10:20:30", 5.2m, 2.5m));
        subject.WriteItem(new TestComplexType("2020-05-05T10:30:40", 5.3m, 2.5m));
        subject.WriteItem(new TestComplexType("2020-05-05T11:05:01", 5.4m, 2.5m));
        subject.WriteItem(new TestComplexType("2020-05-05T12:05:01", 5.5m, 2.5m));

        // Read all items in the lower rank (in this case, base items)
        var values = subject.ReadDataUnderPoint<decimal>("Spent", "PerHour", new DateTime(2020, 5, 5, 10, 0, 0, DateTimeKind.Utc)).ToList();

        Assert.That(values.Count, Is.EqualTo(3));
        Assert.That(values[0].Value, Is.EqualTo(5.1m));
        Assert.That(values[0].Count, Is.EqualTo(1));
        Assert.That(values[1].Value, Is.EqualTo(5.2m));
        Assert.That(values[1].Count, Is.EqualTo(1));
        Assert.That(values[2].Value, Is.EqualTo(5.3m));
        Assert.That(values[2].Count, Is.EqualTo(1));
    }

    [Test]
    public void can_read_a_range_over_multiple_scaled_values()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "rangeTest");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(1, "PerMinute", DateTimeMinutes)
            .Rank(2, "PerHour", DateTimeHours)
            .Rank(3, "PerDay", DateTimeDays)
            .Rank(4, "PerWeek", DateTimeWeeks)
            .Build();

        var hr = 60.0;
        var opCount = 0;
        var baseDate = new DateTime(2020, 5, 5, 0, 0, 0, DateTimeKind.Utc); //                     H
        opCount += subject.WriteItem(new TestComplexType(baseDate, .5, 1.01m, 2.50m)); // 2020-05-05 00:00:30
        opCount += subject.WriteItem(new TestComplexType(baseDate, 10, 2.01m, 4.50m)); // 2020-05-05 00:10:00
        opCount += subject.WriteItem(new TestComplexType(baseDate, 20.5, 3.01m, 6.50m)); // 2020-05-05 00:20:30
        opCount += subject.WriteItem(new TestComplexType(baseDate, 40, 4.01m, 4.50m)); // 2020-05-05 00:40:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, hr, 5.01m, 2.50m)); // 2020-05-05 01:00:00
        opCount += subject.WriteItem(new TestComplexType(baseDate, 1 * hr + 30, 4.01m, 4.50m)); // 2020-05-05 01:30:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 2 * hr + 15, 3.01m, 6.50m)); // 2020-05-05 02:15:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 3 * hr + 30, 2.01m, 4.50m)); // 2020-05-05 03:30:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 4 * hr + 45, 1.01m, 2.50m)); // 2020-05-05 04:45:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 5 * hr + .05, 2.01m, 4.50m)); // 2020-05-05 05:00:03 
        opCount += subject.WriteItem(new TestComplexType(baseDate, 5 * hr + .10, 2.01m, 4.50m)); // 2020-05-05 05:00:06 
        opCount += subject.WriteItem(new TestComplexType(baseDate, 5 * hr + .15, 2.01m, 4.50m)); // 2020-05-05 05:00:09 _

        Console.WriteLine($"Total operations: {opCount}"); // This really only get optimal as data size grows significantly
        Console.WriteLine(subject.DumpTables());


        var values = subject.ReadAggregateDataOverRange<decimal>("Spent", "PerHour", new DateTime(2020, 1, 1, 0, 0, 0), new DateTime(2021, 1, 1, 0, 0, 0)).ToList();


        Assert.That(values.Count, Is.EqualTo(6));
        Assert.That(values[0], Is.EqualTo(10.04m));
    }

    [Test]
    public void aggregated_data_can_be_read_with_count_and_source_key_range()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "rangeTest");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(1, "PerHour", DateTimeHours)
            .Rank(2, "PerDay", DateTimeDays)
            .Build();

        var baseDate = new DateTime(2020, 5, 5, 0, 0, 0, DateTimeKind.Utc);

        for (int i = 0; i < 48; i++) // one day in 30 min increments
        {
            subject.WriteItem(new TestComplexType(baseDate, i * 30, 1.01m, 2.50m));
        }

        var value = subject.ReadDataAtPoint<decimal>("Spent", "PerHour", new DateTime(2020, 5, 5, 5, 0, 0, DateTimeKind.Utc));

        Console.WriteLine(value?.ToString() ?? "<null>");

        Assert.That(value, Is.Not.Null);
        Assert.That(value.Value, Is.EqualTo(2.02));
        Assert.That(value.Count, Is.EqualTo(2));
#if UseCockroach
        Assert.That(value.LowerBound.ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 05:00")); // Bad when using Postgres?
        Assert.That(value.UpperBound.ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 05:30"));
#else
        Assert.That(value.LowerBound.ToUniversalTime().ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 05:00")); // Bad when using Postgres?
        Assert.That(value.UpperBound.ToUniversalTime().ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 05:30"));
#endif
    }

    [Test, Explicit("Large data set. Takes a while on Postgres, and ages in CRDB")]
    // Using Cockroach, this takes about 6 minutes on Window. For some reason, it takes *hours* on my Linux machine (which has a slower CPU).
    // On Windows, Postgres is about 7x faster than Crdb (Psql=400 op/s; Crdb=55 op/s)
    // On Linux, Postgres takes about a minute (Psql=300 op/s; Crdb=???)
    public void can_handle_a_large_input_data_set()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "rangeTest");
        var sw = new Stopwatch();
        sw.Restart();

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(1, "PerMinute", DateTimeMinutes)
            .Rank(2, "PerHour", DateTimeHours)
            .Rank(3, "PerDay", DateTimeDays)
            .Rank(4, "PerWeek", DateTimeWeeks)
            .Build();

        sw.Stop();
        Console.WriteLine($"Initial setup took {sw.Elapsed};");

        var hr = 60.0;
        var day = 1440.0;
        var opCount = 0;
        var baseDate = new DateTime(2020, 5, 5, 0, 0, 0, DateTimeKind.Utc);

        sw.Restart();
        for (int i = 0; i < 10_000; i++) // 10_000 entries covering 30 years of data
        {
            opCount += subject.WriteItem(new TestComplexType(baseDate, i * hr + i, 1.01m, 2.50m));
            opCount += subject.WriteItem(new TestComplexType(baseDate, i * day + i * hr + i, 2.01m, 4.50m));
        }

        sw.Stop();

        var rate = 20_000.0 / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"Writing data took {sw.Elapsed}; Total operations: {opCount}; {rate:0.0} op/second"); // This really only get efficient as data size grows significantly
        //     571'028 operations in triangular data (about 700x faster than naive)
        // 400'000'000 operations with naive re-calculation (20'000 recalculations, each with 2*n data-points -- assumes very efficient recalculation)

        // One year of data by week. Should be 52 items.
        sw.Restart();
        var values = subject.ReadAggregateDataOverRange<decimal>("Spent", "PerWeek", new DateTime(2023, 1, 1, 0, 0, 0), new DateTime(2024, 1, 1, 0, 0, 0)).ToList();
        sw.Stop();
        Console.WriteLine($"Reading one year of aggregated data by week took {sw.Elapsed}");


        sw.Restart();
        var data = subject.ReadDataOverRange<decimal>("Spent", "PerWeek", new DateTime(2023, 1, 1, 0, 0, 0), new DateTime(2024, 1, 1, 0, 0, 0)).ToList();
        sw.Stop();
        Console.WriteLine($"Reading one year of data-points aggregated by week took {sw.Elapsed}");

        Console.WriteLine("Values: " + string.Join(", ", values));
        Console.WriteLine("Data points:\r\n    " + string.Join("\r\n    ", data));
        Assert.That(values.Count, Is.EqualTo(53));
    }

    [Test]
    public void can_aggregate_the_same_data_multiple_different_ways()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "rangeTest");

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("MaxTransaction", MaxFromTestComplexType, DecimalMaxAggregate, "DECIMAL") // complex aggregate: both source data are combined
            .Rank(1, "PerHour", DateTimeHours)
            .Rank(2, "PerDay", DateTimeDays)
            .Build();

        var baseDate = new DateTime(2020, 5, 5, 0, 0, 0, DateTimeKind.Utc);

        int dataPointCount = 48;
        var expected = 0.0m;
        var totalSpends = 0.0m;
        var totalEarns = 0.0m;
        for (int i = 0; i < dataPointCount; i++) // one day in 30 min increments
        {
            var spend = 1.01m + ((i % 5 * (i % 24)) * 1.0m);
            var earn = 80.0m - spend;

            totalSpends += spend;
            totalEarns += earn;

            expected = Math.Max(expected, spend);
            expected = Math.Max(expected, earn);

            subject.WriteItem(new TestComplexType(baseDate, i * 30, spend, earn));
        }

        // Now pick out the maximum individual value across multiple properties and data-points
        var maximumIndividualValue = subject.ReadDataAtPoint<decimal>("MaxTransaction", "PerDay", new DateTime(2020, 5, 5, 5, 0, 0, DateTimeKind.Utc));
        var spent = subject.ReadDataAtPoint<decimal>("Spent", "PerDay", new DateTime(2020, 5, 5, 5, 0, 0, DateTimeKind.Utc));
        var earned = subject.ReadDataAtPoint<decimal>("Earned", "PerDay", new DateTime(2020, 5, 5, 5, 0, 0, DateTimeKind.Utc));

        Console.WriteLine("Max value:  " + (maximumIndividualValue?.ToString() ?? "<null>"));
        Console.WriteLine("Avg spend:  " + (spent.Value / spent.Count));
        Console.WriteLine("Avg earned: " + (earned.Value / earned.Count));

        Assert.That(maximumIndividualValue, Is.Not.Null);
        Assert.That(maximumIndividualValue.Value, Is.EqualTo(expected));
        Assert.That(spent.Value / spent.Count, Is.EqualTo(totalSpends / dataPointCount).Within(0.0001));
        Assert.That(earned.Value / earned.Count, Is.EqualTo(totalEarns / dataPointCount).Within(0.0001));
        Assert.That(maximumIndividualValue.Count, Is.EqualTo(dataPointCount));
#if UseCockroach
        Assert.That(maximumIndividualValue.LowerBound.ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 00:00"));
        Assert.That(maximumIndividualValue.UpperBound.ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 23:30"));
#else
        Assert.That(maximumIndividualValue.LowerBound.ToUniversalTime().ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 00:00"));
        Assert.That(maximumIndividualValue.UpperBound.ToUniversalTime().ToString("yyyy-MM-dd HH:mm"), Is.EqualTo("2020-05-05 23:30"));
#endif
    }

    [Test]
    public void data_is_persistent()
    {
        var baseDate = new DateTime(2020, 5, 5, 0, 0, 0, DateTimeKind.Utc);
        var hr = 60.0;

        // Connection 1
        {
            ResetDatabase();
            var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "persistentTest");

            var subject = TriangularList<DateTime, TestComplexType>
                .Create.UsingStorage(storage)
                .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
                .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
                .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
                .Rank(1, "PerMinute", DateTimeMinutes)
                .Rank(2, "PerHour", DateTimeHours)
                .Rank(3, "PerDay", DateTimeDays)
                .Rank(4, "PerWeek", DateTimeWeeks)
                .Build();

            subject.WriteItem(new TestComplexType(baseDate, .5, 1.01m, 2.50m));
            subject.WriteItem(new TestComplexType(baseDate, 10, 2.01m, 4.50m));
            subject.WriteItem(new TestComplexType(baseDate, 20.5, 3.01m, 6.50m));
            subject.WriteItem(new TestComplexType(baseDate, 40, 4.01m, 4.50m));
            subject.WriteItem(new TestComplexType(baseDate, hr, 5.01m, 2.50m));
            subject.WriteItem(new TestComplexType(baseDate, 1 * hr + 30, 4.01m, 4.50m));
            subject.WriteItem(new TestComplexType(baseDate, 2 * hr + 15, 3.01m, 6.50m));
            subject.WriteItem(new TestComplexType(baseDate, 3 * hr + 30, 2.01m, 4.50m));
            subject.WriteItem(new TestComplexType(baseDate, 4 * hr + 45, 1.01m, 2.50m));
            subject.WriteItem(new TestComplexType(baseDate, 5 * hr + .05, 2.01m, 4.50m));
        }

        NpgsqlConnection.ClearAllPools(); // Fully disconnect from DB

        // Connection 2
        {
            var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "persistentTest");

            // create a new list with same parameters as original
            var subject2 = TriangularList<DateTime, TestComplexType>
                .Create.UsingStorage(storage)
                .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
                .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
                .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
                .Rank(1, "PerMinute", DateTimeMinutes)
                .Rank(2, "PerHour", DateTimeHours)
                .Rank(3, "PerDay", DateTimeDays)
                .Rank(4, "PerWeek", DateTimeWeeks)
                .Build();

            // write more data
            subject2.WriteItem(new TestComplexType(baseDate, 5 * hr + .10, 2.01m, 4.50m));
            subject2.WriteItem(new TestComplexType(baseDate, 5 * hr + .15, 2.01m, 4.50m));

            // Query from both sets of data
            var values = subject2.ReadAggregateDataOverRange<decimal>("Spent", "PerHour", new DateTime(2020, 1, 1, 0, 0, 0), new DateTime(2021, 1, 1, 0, 0, 0)).ToList();

            Assert.That(values.Count, Is.EqualTo(6));
            Assert.That(values[0], Is.EqualTo(10.04m));
        }
    }

    [Test] // On Linux machine, sparse = 150 op/s (dense = 280 op/s)
    public void can_support_sparse_data()
    {
        // This stores data that will not be in every Rank-1 bucket.
        // The storage should handle non-dense data nicely, and without storing
        // loads of zero-values for unoccupied buckets.
        
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "rangeTestSparse");
        var sw = new Stopwatch();
        sw.Restart();

        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn("TIMESTAMP", DateFromTestComplexType, DateMinMax)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(1, "PerMinute", DateTimeMinutes)
            .Rank(2, "PerHour", DateTimeHours)
            .Rank(3, "PerDay", DateTimeDays)
            .Rank(4, "PerWeek", DateTimeWeeks)
            .Rank(5, "PerMonth", DateTimeMonths)
            .Rank(6, "PerYear", DateTimeYears)
            .Rank(7, "AllTime", DateTimeAll)
            .Build();

        sw.Stop();
        Console.WriteLine($"Initial setup took {sw.Elapsed};");

        var hr = 60.0;
        var opCount = 0;

        sw.Restart();
        for (int i = 0; i < 1000; i++)
        {
            var year = (i % 99) + 2000;
            var month = ((i+6) % 12) + 1;
            var day = (i % 25) + 1;
            opCount += subject.WriteItem(new TestComplexType(new DateTime(year,month,day,  0,0,0, DateTimeKind.Utc), i, 1.0m, 2.0m));
        }

        sw.Stop();

        var rate = 1000.0 / sw.Elapsed.TotalSeconds;
        Console.WriteLine($"Writing data took {sw.Elapsed}; Total operations: {opCount}; {rate:0.0} op/second");
        
        
        // Reading at the year level should have dense data
        sw.Restart();
        var values = subject.ReadAggregateDataOverRange<decimal>("Spent", "PerYear", new DateTime(2020,1,1,0,0,0), new DateTime(2023,1,1,0,0,0)).ToList();
        sw.Stop();
        Console.WriteLine($"Reading three years of aggregated data by year took {sw.Elapsed}");

        // Reading at the minute level should be very sparse
        sw.Restart();
        // 2 weeks in minutes
        var data = subject.ReadDataOverRange<decimal>("Spent", "PerMinute", new DateTime(2023, 1, 1, 0, 0, 0), new DateTime(2023, 1, 14, 0, 0, 0)).ToList();
        sw.Stop();
        Console.WriteLine($"Reading two weeks of data-points aggregated by minute took {sw.Elapsed}");

        Console.WriteLine("Values: " + string.Join(", ", values));
        Console.WriteLine("Data points:\r\n    " + string.Join("\r\n    ", data));
        Assert.That(values.Count, Is.EqualTo(4));
    }

    [Test]
    public void can_use_arbitrary_values_for_keys_and_ranks()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "GeoLocalRanks");
        
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
        subject.WriteItem(new SaleWithLocation(DateTime.Now, 11.00m, 34.00m, Geolocation.Spain));
        subject.WriteItem(new SaleWithLocation(DateTime.Now, 12.00m, 33.00m, Geolocation.UK));
        subject.WriteItem(new SaleWithLocation(DateTime.Now, 13.00m, 32.00m, Geolocation.Taiwan));
        subject.WriteItem(new SaleWithLocation(DateTime.Now, 14.00m, 31.00m, Geolocation.USA));
        subject.WriteItem(new SaleWithLocation(DateTime.Now, 15.00m, 30.00m, Geolocation.Mexico));
        subject.WriteItem(new SaleWithLocation(DateTime.Now, 16.00m, 29.00m, Geolocation.Ghana));
        // and so on...
        
        // Get sales across Europe (that is, all in same 'Landmass' as Germany. Could use any European country here)
        var sales = subject.ReadDataAtPoint<decimal>("Price", "Landmass", Geolocation.Germany);
        var costs = subject.ReadDataAtPoint<decimal>("Cost", "Landmass", Geolocation.Germany);
        var profit = sales.Value - costs.Value;
        //if (profit < 0) RunNewAdCampaign();
        
        Assert.That(costs.Value, Is.EqualTo(23.0m));
        Assert.That(costs.Count, Is.EqualTo(2));
        Assert.That(sales.Value, Is.EqualTo(67.0m));
        Assert.That(sales.Count, Is.EqualTo(2));
        Assert.That(profit, Is.EqualTo(44.0m));
    }



    // --- Rank Classification Functions --- //
    private static readonly DateTime _baseDate = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static long DateTimeMinutes(DateTime item) => (long)Math.Floor((item - _baseDate).TotalMinutes);
    private static long DateTimeHours(DateTime item) => (long)Math.Floor((item - _baseDate).TotalHours);
    private static long DateTimeDays(DateTime item) => (long)Math.Floor((item - _baseDate).TotalDays);
    private static long DateTimeWeeks(DateTime item) => (long)Math.Floor((item - _baseDate).TotalDays / 7.0);
    private static long DateTimeMonths(DateTime item) => (item.Year * 13) + item.Month;
    private static long DateTimeYears(DateTime item) => item.Year;
    private static long DateTimeAll(DateTime item) => 1; // this is an easy way to aggregate everything

    // --- Aggregate Classification Functions --- //
    private static DateTime DateFromTestComplexType(TestComplexType item) => item.RecordedDate;
    private static void DateMinMax(DateTime a, DateTime b, out DateTime min, out DateTime max)
    {
        if (a > b) { min = b; max = a; }
        else { min = a; max = b; }
    }
    
    // --- Aggregate Data Functions --- //
    private static decimal SpentFromTestComplexType(TestComplexType item) => item.SpentAmount;
    private static decimal EarnedFromTestComplexType(TestComplexType item) => item.EarnedAmount;
    private static decimal MaxFromTestComplexType(TestComplexType item) => Math.Max(item.EarnedAmount, item.SpentAmount);

    private static decimal DecimalMaxAggregate(decimal a, decimal b) => Math.Max(a,b);
    private static decimal DecimalSumAggregate(decimal a, decimal b) => a+b;
}


public class TestComplexType
{
    public DateTime RecordedDate { get; init; }
    public decimal SpentAmount { get; init; }
    public decimal EarnedAmount { get; init; }

    public TestComplexType() { }
    
    public TestComplexType(string dateStr, decimal spend, decimal earned)
    {
        RecordedDate = DateTime.ParseExact(dateStr, "yyyy-MM-ddTHH:mm:ss", null);
        SpentAmount=spend;
        EarnedAmount=earned;
    }

    public TestComplexType(DateTime baseDate, double minutesAdvance, decimal spend, decimal earned)
    {
        RecordedDate = baseDate.AddMinutes(minutesAdvance);
        SpentAmount=spend;
        EarnedAmount=earned;
    }
}

public static class Regions
{
    public static GeoLandmass LocationToLandmass(Geolocation location)
    {
        return location switch
        {
            Geolocation.China => GeoLandmass.EastAsia,
            Geolocation.India => GeoLandmass.Subcontinent,
            Geolocation.USA => GeoLandmass.NorthAmerica,
            Geolocation.Indonesia => GeoLandmass.Oceania,
            Geolocation.Pakistan => GeoLandmass.Subcontinent,
            Geolocation.Nigeria => GeoLandmass.SubSahara,
            Geolocation.Brazil => GeoLandmass.SouthAmerica,
            Geolocation.Bangladesh => GeoLandmass.Subcontinent,
            Geolocation.Russia => GeoLandmass.WestAsia,
            Geolocation.Mexico => GeoLandmass.NorthAmerica,
            Geolocation.Japan => GeoLandmass.EastAsia,
            Geolocation.Philippines => GeoLandmass.Oceania,
            Geolocation.Ethiopia => GeoLandmass.NorthAfrica,
            Geolocation.Egypt => GeoLandmass.NorthAfrica,
            Geolocation.Vietnam => GeoLandmass.EastAsia,
            Geolocation.DrCongo => GeoLandmass.SubSahara,
            Geolocation.Iran => GeoLandmass.WestAsia,
            Geolocation.Turkey => GeoLandmass.WestAsia,
            Geolocation.Germany => GeoLandmass.Europe,
            Geolocation.France => GeoLandmass.Europe,
            Geolocation.UK => GeoLandmass.Europe,
            Geolocation.Thailand => GeoLandmass.Oceania,
            Geolocation.Tanzania => GeoLandmass.SubSahara,
            Geolocation.SouthAfrica => GeoLandmass.SubSahara,
            Geolocation.Italy => GeoLandmass.Europe,
            Geolocation.Myanmar => GeoLandmass.EastAsia,
            Geolocation.SouthKorea => GeoLandmass.EastAsia,
            Geolocation.Colombia => GeoLandmass.CentralAmerica,
            Geolocation.Spain => GeoLandmass.Europe,
            Geolocation.Kenya => GeoLandmass.SubSahara,
            Geolocation.Argentina => GeoLandmass.SouthAmerica,
            Geolocation.Algeria => GeoLandmass.NorthAfrica,
            Geolocation.Sudan => GeoLandmass.NorthAfrica,
            Geolocation.Uganda => GeoLandmass.SubSahara,
            Geolocation.Iraq => GeoLandmass.WestAsia,
            Geolocation.Ukraine => GeoLandmass.Europe,
            Geolocation.Canada => GeoLandmass.NorthAfrica,
            Geolocation.Poland => GeoLandmass.Europe,
            Geolocation.Morocco => GeoLandmass.NorthAfrica,
            Geolocation.Uzbekistan => GeoLandmass.WestAsia,
            Geolocation.SaudiArabia => GeoLandmass.WestAsia,
            Geolocation.Yemen => GeoLandmass.WestAsia,
            Geolocation.Peru => GeoLandmass.SouthAmerica,
            Geolocation.Angola => GeoLandmass.SubSahara,
            Geolocation.Afghanistan => GeoLandmass.WestAsia,
            Geolocation.Malaysia => GeoLandmass.EastAsia,
            Geolocation.Mozambique => GeoLandmass.SubSahara,
            Geolocation.Ghana => GeoLandmass.SubSahara,
            Geolocation.IvoryCoast => GeoLandmass.SubSahara,
            Geolocation.Nepal => GeoLandmass.Subcontinent,
            Geolocation.Venezuela => GeoLandmass.SouthAmerica,
            Geolocation.Madagascar => GeoLandmass.SubSahara,
            Geolocation.Australia => GeoLandmass.Oceania,
            Geolocation.NorthKorea => GeoLandmass.EastAsia,
            Geolocation.Cameroon => GeoLandmass.SubSahara,
            Geolocation.Niger => GeoLandmass.SubSahara,
            Geolocation.Taiwan => GeoLandmass.EastAsia,
            Geolocation.Mali => GeoLandmass.SubSahara,
            Geolocation.SriLanka => GeoLandmass.Subcontinent,
            Geolocation.Syria => GeoLandmass.WestAsia,
            Geolocation.BurkinaFaso => GeoLandmass.SubSahara,
            Geolocation.Malawi => GeoLandmass.SubSahara,
            Geolocation.Chile => GeoLandmass.SouthAmerica,
            Geolocation.Kazakhstan => GeoLandmass.WestAsia,
            Geolocation.Zambia => GeoLandmass.SubSahara,
            Geolocation.Romania => GeoLandmass.Europe,
            Geolocation.Ecuador => GeoLandmass.SouthAmerica,
            Geolocation.Netherlands => GeoLandmass.Europe,
            Geolocation.Somalia => GeoLandmass.NorthAfrica,
            Geolocation.Senegal => GeoLandmass.SubSahara,
            Geolocation.Guatemala => GeoLandmass.CentralAmerica,
            Geolocation.Chad => GeoLandmass.NorthAfrica,
            _ => GeoLandmass.Other
        };
    }

    public static GeoZone LocationToGeoZone(Geolocation location)
    {
        var landmass = LocationToLandmass(location);
        return landmass switch
        {
            GeoLandmass.Europe => GeoZone.EMEA,
            GeoLandmass.WestAsia => GeoZone.EMEA,
            GeoLandmass.EastAsia => GeoZone.APAC,
            GeoLandmass.Subcontinent => GeoZone.APAC,
            GeoLandmass.NorthAfrica => GeoZone.EMEA,
            GeoLandmass.SubSahara => GeoZone.EMEA,
            GeoLandmass.Oceania => GeoZone.APAC,
            GeoLandmass.NorthAmerica => GeoZone.AMER,
            GeoLandmass.CentralAmerica => GeoZone.AMER,
            GeoLandmass.SouthAmerica => GeoZone.AMER,
            GeoLandmass.Other => GeoZone.APAC,
            _ => GeoZone.APAC
        };
    }

    public static long LocationToWorldwide(Geolocation location)
    {
        return 1; // only one world, so far.
    }

    /// <summary>
    /// Min/max doesn't mean much with this, so we give arbitrary values.
    /// We could order the countries by GDP or population to give a range of values that way.
    /// </summary>
    public static void MinMax(Geolocation a, Geolocation b, out Geolocation min, out Geolocation max)
    {
        min = (Geolocation)Math.Min((long)a,(long)b);
        max = (Geolocation)Math.Max((long)a,(long)b);
    }
}


public class SaleWithLocation
{
    public DateTime RecordedDate { get; init; }
    public decimal Cost { get; init; }
    public decimal SoldPrice { get; init; }
    public Geolocation SalesLocation { get; init; }

    public SaleWithLocation() { }

    public SaleWithLocation(DateTime date, decimal cost, decimal soldFor, Geolocation location)
    {
        RecordedDate = date;
        Cost = cost;
        SoldPrice = soldFor;
        SalesLocation = location;
    }
}

public enum GeoZone : long
{
    // ReSharper disable InconsistentNaming
    APAC, EMEA, AMER
    // ReSharper restore InconsistentNaming
}

public enum GeoLandmass: long
{
    Europe, WestAsia, EastAsia, Subcontinent, NorthAfrica, SubSahara,
    Oceania, NorthAmerica, CentralAmerica, SouthAmerica, Other
}

public enum Geolocation: long
{
    China, India, USA, Indonesia, Pakistan, Nigeria,
    Brazil, Bangladesh, Russia, Mexico, Japan,
    Philippines, Ethiopia, Egypt, Vietnam, DrCongo,
    Iran, Turkey, Germany, France, UK, Thailand,
    Tanzania, SouthAfrica, Italy, Myanmar, SouthKorea,
    Colombia, Spain, Kenya, Argentina, Algeria,
    Sudan, Uganda, Iraq, Ukraine, Canada, Poland,
    Morocco, Uzbekistan, SaudiArabia, Yemen, Peru,
    Angola, Afghanistan, Malaysia, Mozambique, Ghana,
    IvoryCoast, Nepal, Venezuela, Madagascar,
    Australia, NorthKorea, Cameroon, Niger, Taiwan,
    Mali, SriLanka, Syria, BurkinaFaso, Malawi,
    Chile, Kazakhstan, Zambia, Romania, Ecuador,
    Netherlands, Somalia, Senegal, Guatemala, Chad
}