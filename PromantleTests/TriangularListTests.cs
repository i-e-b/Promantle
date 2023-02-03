using NUnit.Framework;
using Promantle;
using PromantleTests.Helpers;

namespace PromantleTests;

[TestFixture, SingleThreaded]
public class TriangularListTests:DbTestBase
{
    [Test]
    public void the_database_hooks_work()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");
        
        // Create table
        var written = storage.EnsureTableForRank(1,1, new BasicColumn("test","int"));
        Console.WriteLine($"Wrote DB: {written}");
        
        // Write single value
        storage.WriteAtRank(1, 1, "test", 1, 2, 3, 4);
        
        // Read single value back
        var value = storage.ReadAtRank(1,1, "test", 2);
        Assert.That(value?.ParentPosition, Is.EqualTo(1));
        Assert.That(value?.Position, Is.EqualTo(2));
        Assert.That(value?.Count, Is.EqualTo(3));
        Assert.That(value?.Value, Is.EqualTo(4));
        
        // More values
        storage.WriteAtRank(1, 1, "test", 2, 3, 4, 5);
        storage.WriteAtRank(1, 1, "test", 2, 4, 5, 6);
        storage.WriteAtRank(1, 1, "test", 2 ,5, 6, 7);
        
        // Read multiple values back
        var values = storage.ReadWithRank(1,1, "test", 2, 4).ToList();
        Assert.That(values.Count, Is.EqualTo(3));
        Assert.That(values[0].Value, Is.EqualTo(4));
        Assert.That(values[0].Count, Is.EqualTo(3));
        Assert.That(values[0].Position, Is.EqualTo(2));
        Assert.That(values[0].ParentPosition, Is.EqualTo(1));
        Assert.That(values[1].Value, Is.EqualTo(5));
        Assert.That(values[1].Count, Is.EqualTo(4));
        Assert.That(values[1].Position, Is.EqualTo(3));
        Assert.That(values[1].ParentPosition, Is.EqualTo(2));
        Assert.That(values[2].Value, Is.EqualTo(6));
        Assert.That(values[2].Count, Is.EqualTo(5));
        Assert.That(values[2].Position, Is.EqualTo(4));
        Assert.That(values[2].ParentPosition, Is.EqualTo(2));
        
        // Read back by parent
        var parentValues = storage.ReadWithParentRank(1,1, "test", 2).ToList();
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
            .KeyOn(DateFromTestComplexType)
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
            .KeyOn(DateFromTestComplexType)
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
            .KeyOn(DateFromTestComplexType)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(0, "PerHour", DateTimeHours)
            .Build();
        
        subject.WriteItem(new TestComplexType{
            EarnedAmount = 2.5m,
            SpentAmount = 5.1m,
            RecordedDate = new DateTime(2020,5,5, 10,11,12, DateTimeKind.Utc)
        });
        
        var value = subject.ReadAggregate<decimal>("Spent", "PerHour", new DateTime(2020,5,5, 10,10,32)); // should find it if in same hour
        
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
            .KeyOn(DateFromTestComplexType)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(0, "PerHour", DateTimeHours)
            .Build();
        
        subject.WriteItem(new TestComplexType{
            EarnedAmount = 2.5m,
            SpentAmount = 5.1m,
            RecordedDate = new DateTime(2020,5,5, 10,11,12, DateTimeKind.Utc)
        });
        
        var values = subject.ReadAggregateRange<decimal>("Spent", "PerHour", new DateTime(2020,1,1, 8,30,29), new DateTime(2021,1,1, 16,45,31)).ToList();
        
        Assert.That(values.Count, Is.GreaterThan(0));
        Assert.That(values[0], Is.EqualTo(5.1m));
    }
    
    [Test]
    public void can_read_a_range_over_multiple_scaled_values()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "rangeTest");
        
        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn(DateFromTestComplexType)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(1, "PerMinute", DateTimeMinutes)
            .Rank(2, "PerHour",   DateTimeHours)
            .Rank(3, "PerDay",    DateTimeDays)
            .Rank(4, "PerWeek",   DateTimeWeeks)
            .Build();
        
        var hr = 60.0;
        var opCount = 0;
        var baseDate = new DateTime(2020,5,5, 0,0,0, DateTimeKind.Utc);            //                     H
        opCount += subject.WriteItem(new TestComplexType(baseDate, .5,        1.01m, 2.50m)); // 2020-05-05 00:00:30
        opCount += subject.WriteItem(new TestComplexType(baseDate, 10,        2.01m, 4.50m)); // 2020-05-05 00:10:00
        opCount += subject.WriteItem(new TestComplexType(baseDate, 20.5,      3.01m, 6.50m)); // 2020-05-05 00:20:30
        opCount += subject.WriteItem(new TestComplexType(baseDate, 40,        4.01m, 4.50m)); // 2020-05-05 00:40:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, hr,        5.01m, 2.50m)); // 2020-05-05 01:00:00
        opCount += subject.WriteItem(new TestComplexType(baseDate, 1*hr + 30, 4.01m, 4.50m)); // 2020-05-05 01:30:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 2*hr + 15, 3.01m, 6.50m)); // 2020-05-05 02:15:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 3*hr + 30, 2.01m, 4.50m)); // 2020-05-05 03:30:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 4*hr + 45, 1.01m, 2.50m)); // 2020-05-05 04:45:00 _
        opCount += subject.WriteItem(new TestComplexType(baseDate, 5*hr + .05,2.01m, 4.50m)); // 2020-05-05 05:00:03 
        opCount += subject.WriteItem(new TestComplexType(baseDate, 5*hr + .10,2.01m, 4.50m)); // 2020-05-05 05:00:06 
        opCount += subject.WriteItem(new TestComplexType(baseDate, 5*hr + .15,2.01m, 4.50m)); // 2020-05-05 05:00:09 _
        
        Console.WriteLine($"Total operations: {opCount}"); // This really only get optimal as data size grows significantly
        Console.WriteLine(subject.DumpTables());
        
        
        var values = subject.ReadAggregateRange<decimal>("Spent", "PerHour", new DateTime(2020,1,1,  0,0,0), new DateTime(2021,1,1, 0,0,0)).ToList();
        
        
        Assert.That(values.Count, Is.EqualTo(6));
        Assert.That(values[0], Is.EqualTo(10.04m));
    }

    [Test, Explicit("Takes around 6 minutes on my laptop.")]
    public void can_handle_a_large_input_data_set()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "rangeTest");
        
        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn(DateFromTestComplexType)
            .Aggregate<decimal>("Spent", SpentFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Aggregate<decimal>("Earned", EarnedFromTestComplexType, DecimalSumAggregate, "DECIMAL")
            .Rank(1, "PerMinute", DateTimeMinutes)
            .Rank(2, "PerHour",   DateTimeHours)
            .Rank(3, "PerDay",    DateTimeDays)
            .Rank(4, "PerWeek",   DateTimeWeeks)
            .Build();
        
        var hr = 60.0;
        var day = 1440.0;
        var opCount = 0;
        var baseDate = new DateTime(2020,5,5, 0,0,0, DateTimeKind.Utc);

        for (int i = 0; i < 10_000; i++) // 10_000 -> 30 years of data
        {
            opCount += subject.WriteItem(new TestComplexType(baseDate, i*hr + i,           1.01m, 2.50m));
            opCount += subject.WriteItem(new TestComplexType(baseDate, i*day + i*hr + i,   2.01m, 4.50m));
        }
        
        Console.WriteLine($"Total operations: {opCount}"); // This really only get optimal as data size grows significantly
        //     571'028 operations in triangular data (about 700x faster than naive)
        // 400'000'000 operations with naive re-calculation (20'000 recalculations, each with 2*n data-points -- assumes very efficient recalculation)
        
        //Console.WriteLine(subject.DumpTables());

        // One year of data by week. Should be 52 items.
        var values = subject.ReadAggregateRange<decimal>("Spent", "PerWeek", new DateTime(2023,1,1,  0,0,0), new DateTime(2024,1,1, 0,0,0)).ToList();
        
        Console.WriteLine(string.Join(", ", values));
        Assert.That(values.Count, Is.EqualTo(53));
    }

    // --- Rank Classification Functions --- //
    private static long DateTimeMinutes(DateTime item)
    {
        var baseDate = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return (long)Math.Floor((item - baseDate).TotalMinutes);
    }
    private static long DateTimeHours(DateTime item)
    {
        var baseDate = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return (long)Math.Floor((item - baseDate).TotalHours);
    }
    private static long DateTimeDays(DateTime item)
    {
        var baseDate = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return (long)Math.Floor((item - baseDate).TotalDays);
    }
    private static long DateTimeWeeks(DateTime item)
    {
        var baseDate = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return (long)Math.Floor((item - baseDate).TotalDays / 7.0);
    }
    
    // --- Aggregate Classification Functions --- //
    private static DateTime DateFromTestComplexType(TestComplexType item) => item.RecordedDate;
    private static decimal SpentFromTestComplexType(TestComplexType item) => item.SpentAmount;
    private static decimal EarnedFromTestComplexType(TestComplexType item) => item.EarnedAmount;
    private static decimal DecimalSumAggregate(decimal a, decimal b) => a+b;
}

public class TestComplexType
{
    public DateTime RecordedDate { get; init; }
    public decimal SpentAmount { get; init; }
    public decimal EarnedAmount { get; init; }

    public TestComplexType() { }

    public TestComplexType(DateTime baseDate, double minutesAdvance, decimal spend, decimal earned)
    {
        RecordedDate = baseDate.AddMinutes(minutesAdvance);
        SpentAmount=spend;
        EarnedAmount=earned;
    }
}