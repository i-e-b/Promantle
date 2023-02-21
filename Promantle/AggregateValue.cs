namespace Promantle;

/// <summary>
/// Container for values returned when querying an aggregation
/// </summary>
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