using NUnit.Framework;
using Promantle;
using PromantleTests.Helpers;

namespace PromantleTests;

[TestFixture]
public class TriangularListTests:DbTestBase
{

    [Test]
    public void can_create_a_new_list_with_functions()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");
        
        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn(DateFromTestComplexType)
            .Aggregate<double>("Spent", SpentFromTestComplexType, DoubleSumAggregate)
            .Aggregate<double>("Earned", EarnedFromTestComplexType, DoubleSumAggregate)
            .Rank(0, "PerHour", DateTimeHours)
            .Build();
        
        subject.WriteItem(new TestComplexType());
        
        Assert.Inconclusive("not yet implemented");
    }

    [Test]
    public void can_read_a_single_scaled_value()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");
        
        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn(DateFromTestComplexType)
            .Aggregate<double>("Spent", SpentFromTestComplexType, DoubleSumAggregate)
            .Aggregate<double>("Earned", EarnedFromTestComplexType, DoubleSumAggregate)
            .Rank(0, "PerHour", DateTimeHours)
            .Build();
        
        subject.WriteItem(new TestComplexType());
        
        var value = subject.ReadAggregate<double>("Spent", "PerHour", new DateTime(2023,1,1,  8,30,29));
        
        
        Assert.Inconclusive("not yet implemented");
    }
    
    [Test]
    public void can_read_a_range_of_scaled_values()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort, "test1");
        
        var subject = TriangularList<DateTime, TestComplexType>
            .Create.UsingStorage(storage)
            .KeyOn(DateFromTestComplexType)
            .Aggregate<double>("Spent", SpentFromTestComplexType, DoubleSumAggregate)
            .Aggregate<double>("Earned", EarnedFromTestComplexType, DoubleSumAggregate)
            .Rank(0, "PerHour", DateTimeHours)
            .Build();
        
        subject.WriteItem(new TestComplexType());
        
        var values = subject.ReadAggregateRange<double>("Spent", "PerHour", new DateTime(2023,1,1, 8,30,29), new DateTime(2023,1,1, 16,45,31));
        
        
        Assert.Inconclusive("not yet implemented");
    }

    private static long DateTimeHours(DateTime item)
    {
        var baseDate = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return (long)Math.Floor((item.Date - baseDate).TotalDays);
    }

    
    private static DateTime DateFromTestComplexType(TestComplexType item) => item.RecordedDate;
    private static double SpentFromTestComplexType(TestComplexType item) => item.SpentAmount;
    private static double EarnedFromTestComplexType(TestComplexType item) => item.EarnedAmount;
    private static double DoubleSumAggregate(double a, double b) => a+b;
}

public class TestComplexType
{
    public DateTime RecordedDate { get; set; }
    public double SpentAmount { get; set; }
    public double EarnedAmount { get; set; }
}