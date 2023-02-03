using System.Text;

namespace Promantle;

public class AggregateValue
{
    /// <summary>
    /// Aggregated value
    /// </summary>
    public object? Value { get; set; }

    /// <summary>
    /// Bottom range of real data in this aggregation
    /// </summary>
    public object? LowerBound { get; set; }

    /// <summary>
    /// Top range of real data in this aggregation
    /// </summary>
    public object? UpperBound { get; set; }

    /// <summary>
    /// Count of zero-rank values aggregated at this point
    /// </summary>
    public long Count { get; set; }

    /// <summary>
    /// Position in this rank
    /// </summary>
    public long Position { get; set; }
    
    /// <summary>
    /// Position in next rank up (less detailed, more aggregated)
    /// </summary>
    public long ParentPosition { get; set; }
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
    /// Read all values of a single aggregate that all share a single parent in the next rank up.
    /// </summary>
    IEnumerable<AggregateValue> ReadWithParentRank(int rank, int rankCount, string aggregateName, long parentPosition);
    
    /// <summary>
    /// Read a single value of a single rank and aggregate at a single position
    /// </summary>
    AggregateValue? ReadAtRank(int rank, int rankCount, string aggregateName, long position);

    /// <summary>
    /// Write a single value of a single rank and aggregate at a single position
    /// </summary>
    /// <param name="rank">The rank we are writing to</param>
    /// <param name="rankCount">Total number of ranks in the triangular list</param>
    /// <param name="aggregateName">Name of the aggregate we are writing a value for</param>
    /// <param name="parentPosition">Position value of the next rank up (less detailed, more aggregated)</param>
    /// <param name="position">Position value of this value</param>
    /// <param name="count">Total of zero-rank values that are aggregated here</param>
    /// <param name="value">Aggregated value to write</param>
    /// <param name="lowerBound">Lowest key that is aggregated at this point</param>
    /// <param name="upperBound">Highest key that is aggregated at this point</param>
    void WriteAtRank(int rank, int rankCount, string aggregateName, long parentPosition, long position, long count, object? value, object? lowerBound, object? upperBound);
    
    /// <summary>
    /// Make sure rank table exists. Returns <c>true</c> if it needed to be created
    /// </summary>
    bool EnsureTableForRank(int rank, int rankCount, string keyType, params BasicColumn[] aggregateNames);

    /// <summary>
    /// Read the current maximum position number in a given rank table
    /// </summary>
    /// <param name="rank">The rank we are querying</param>
    /// <param name="rankCount">Total number of ranks in the triangular list</param>
    /// <returns>Max position, or zero</returns>
    long MaxPosition(int rank, int rankCount);

    /// <summary>
    /// Diagnostics. Write table info to the supplied StringBuilder
    /// </summary>
    void DumpTableForRank(StringBuilder sb, int rank, int rankCount);
}