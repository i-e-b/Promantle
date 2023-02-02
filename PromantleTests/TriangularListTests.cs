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
    public void can_read_a_range_of_scaled_values()
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

    private static long DateTimeHours(DateTime item)
    {
        var baseDate = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return (long)Math.Floor((item.Date - baseDate).TotalDays);
    }

    
    private static DateTime DateFromTestComplexType(TestComplexType item) => item.RecordedDate;
    private static decimal SpentFromTestComplexType(TestComplexType item) => item.SpentAmount;
    private static decimal EarnedFromTestComplexType(TestComplexType item) => item.EarnedAmount;
    private static decimal DecimalSumAggregate(decimal a, decimal b) => a+b;
}

public class TestComplexType
{
    public DateTime RecordedDate { get; set; }
    public decimal SpentAmount { get; set; }
    public decimal EarnedAmount { get; set; }
}