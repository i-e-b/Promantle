using System.Text;

namespace Promantle;

/// <summary>
/// Basic adaptor for storing and querying aggregated data
/// </summary>
public interface IAggregateTableAdaptor
{
    /// <summary>
    /// Read all values of a single rank and aggregate between two inclusive bounds
    /// </summary>
    IEnumerable<AggregateValue> ReadWithRank(string groupName, int rank, int rankCount, string aggregateName, long start, long end);

    /// <summary>
    /// Read all values of a single aggregate that all share a single parent in the next rank up.
    /// </summary>
    IEnumerable<AggregateValue> ReadWithParentRank(string groupName, int rank, int rankCount, string aggregateName, long parentPosition);
    
    /// <summary>
    /// Read a single value of a single rank and aggregate at a single position
    /// </summary>
    AggregateValue? ReadAtRank(string groupName, int rank, int rankCount, string aggregateName, long position);

    /// <summary>
    /// Write a single value of a single rank and aggregate at a single position
    /// </summary>
    /// <param name="groupName">Name of the table group</param>
    /// <param name="rank">The rank we are writing to</param>
    /// <param name="rankCount">Total number of ranks in the triangular list</param>
    /// <param name="aggregateName">Name of the aggregate we are writing a value for</param>
    /// <param name="parentPosition">Position value of the next rank up (less detailed, more aggregated)</param>
    /// <param name="position">Position value of this value</param>
    /// <param name="count">Total of zero-rank values that are aggregated here</param>
    /// <param name="value">Aggregated value to write</param>
    /// <param name="lowerBound">Lowest key that is aggregated at this point</param>
    /// <param name="upperBound">Highest key that is aggregated at this point</param>
    void WriteAtRank(string groupName, int rank, int rankCount, string aggregateName, long parentPosition, long position, long count, object? value, object? lowerBound, object? upperBound);
    
    /// <summary>
    /// Make sure rank table exists. Returns <c>true</c> if it needed to be created
    /// </summary>
    bool EnsureTableForRank(string groupName, int rank, int rankCount, string keyType, params BasicColumn[] aggregateNames);

    /// <summary>
    /// Read the current maximum position number in a given rank table
    /// </summary>
    /// <param name="groupName">Name of the table group</param>
    /// <param name="rank">The rank we are querying</param>
    /// <param name="rankCount">Total number of ranks in the triangular list</param>
    /// <returns>Max position, or zero</returns>
    long MaxPosition(string groupName, int rank, int rankCount);

    /// <summary>
    /// Diagnostics. Write table info to the supplied StringBuilder
    /// </summary>
    void DumpTableForRank(StringBuilder sb, string groupName, int rank, int rankCount);

    /// <summary>
    /// Delete the table for a given name and rank.
    /// </summary>
    void DeleteTableForRank(string name, int rank, int rankCount);
}