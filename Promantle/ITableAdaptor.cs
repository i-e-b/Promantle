namespace Promantle;

public class AggregateValue
{
    public object? Value { get; set; }
    public long Count { get; set; }
}

public class BasicColumn
{
    public readonly string Name;
    public readonly string Type;

    public BasicColumn(string name, string type)
    {
        Name = name;
        Type = type;
    }
}

/// <summary>
/// Basic adaptor for storing and querying 
/// </summary>
public interface ITableAdaptor
{
    /// <summary>
    /// Read all values of a single rank and aggregate between two inclusive bounds
    /// </summary>
    IEnumerable<AggregateValue> ReadWithRank(int rank, int rankCount, string aggregateName, long start, long end);

    /// <summary>
    /// Read a single value of a single rank and aggregate at a single position
    /// </summary>
    AggregateValue? ReadAtRank(int rank, int rankCount, string aggregateName, long position);

    /// <summary>
    /// Write a single value of a single rank and aggregate at a single position
    /// </summary>
    void WriteAtRank(int rank, int rankCount, string aggregateName, long position, long count, object value);
    
    /// <summary>
    /// Make sure rank table exists. Returns <c>true</c> if it needed to be created
    /// </summary>
    bool EnsureTableForRank(int rank, int rankCount, params BasicColumn[] aggregateNames);
}