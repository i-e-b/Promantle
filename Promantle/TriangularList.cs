namespace Promantle;


/// <summary>
/// A storage and query tool for large sets of data
/// that are mostly accessed in aggregate form.
/// <p></p>
/// Data is not stored in this class, but is written and read from specially formed database tables.
/// <p></p>
/// This is very experimental, so may be janky in unexpected ways.
/// <p></p>
/// For experimentation, this connects directly to a database with a named table
/// </summary>
/// <typeparam name="TK">Key for data stored in this list. Used for ranking and querying</typeparam>
/// <typeparam name="TV">Value of data stored in this list. Returned in queries (often in aggregate)</typeparam>
/// <remarks>
/// We can <b>supply</b> whatever data we like, but we must have an aggregation for all data that
/// is to be <b>stored</b>
/// <p></p>
/// We have a 'rank zero', which is individual data items. (not yet enforced).
/// Each rank above that must aggregate the RZ data to fewer items (not enforced).
/// At each rank, there is at most one item with a given 'position'.
/// Where data is aggregated, we store BOTH the aggregated data,
/// AND the total number of RZ items that have been aggregated to this point.
/// <p></p>
/// Be careful with aggregations. Avoid averages -- sum instead and divide by the total count.
/// </remarks>
public class TriangularList<TK, TV>
{
    private readonly ITableAdaptor _storage;
    private readonly KeyFunction _keyFunction;
    private readonly Dictionary<string, Aggregator> _aggregateByName;
    private readonly Dictionary<string, Rank> _ranksByName;
    private readonly Dictionary<int, Rank> _ranksByNumber;

    /// <summary>
    /// For a given item, give a 'position' value.
    /// <p></p>
    /// This value must be constant and stable for all
    /// items with the same value.
    /// <p></p>
    /// The value returned is used for sorting and searching, so should
    /// give an ordered result over ordered sets of items.
    /// </summary>
    public delegate long RankFunction(TK item);

    /// <summary>
    /// Read the key from a list item
    /// </summary>
    public delegate TK KeyFunction(TV item);

    /// <summary>
    /// Represents a line of data that is aggregated from the stored items
    /// </summary>
    public abstract class Aggregator
    {
        /// <summary>
        /// Name of the aggregation, used for querying
        /// </summary>
        public string Name = "";
    }

    /// <summary>
    /// Type specific aggregator
    /// </summary>
    public class Aggregator<TA> : Aggregator
    {
        public readonly Func<TV, TA> Select;
        public readonly Func<TA, TA, TA> Combine;

        public Aggregator(Func<TV, TA> select, Func<TA, TA, TA> combine)
        {
            Select = select;
            Combine = combine;
        }
    }

    /// <summary>
    /// Represents a rank of scale
    /// </summary>
    public class Rank
    {
        /// <summary>
        /// Function of data -> position
        /// </summary>
        public readonly RankFunction RankFunction;

        /// <summary>
        /// Name (for reading data)
        /// </summary>
        public readonly string Name;

        /// <summary>
        /// Rank number (used internally for storage)
        /// </summary>
        public readonly int RankNumber;

        public Rank(int rankNumber, string name, RankFunction rankFunction)
        {
            RankNumber = rankNumber;
            RankFunction = rankFunction;
            Name = name;
        }
    }

    /// <summary>
    /// Helper to build triangular lists
    /// </summary>
    public static TriangularListBuilder<TK, TV> Create => new();

    /// <summary>
    /// Create a new list, with: a keying function, a set of ranks, and a set of data aggregations.
    /// <p></p>
    /// For a more intuitive way to create, see <see cref="Create"/>
    /// </summary>
    public TriangularList(ITableAdaptor storage, KeyFunction keyFunction, List<Rank> orderedRanks, Dictionary<string, Aggregator> aggregateByName)
    {
        _storage = storage;
        _keyFunction = keyFunction;

        // Build dictionaries from the list (with our own preferred ranking numbers)
        _ranksByName = new Dictionary<string, Rank>();
        _ranksByNumber = new Dictionary<int, Rank>();
        throw new NotImplementedException();
    }

    /// <summary>
    /// Write a new item to the list's storage,
    /// and update aggregations
    /// </summary>
    public void WriteItem(TV item)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Read a single aggregate value at a target key
    /// </summary>
    /// <param name="rank">Name of the rank/range the aggregate value should come from</param>
    /// <param name="aggregation">Name of the aggregation to read</param>
    /// <param name="target">The key for the point or range to read</param>
    /// <typeparam name="TA">Type of the data to be returned</typeparam>
    public TA? ReadAggregate<TA>(string aggregation, string rank, TK target)
    {
        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // TODO: read the single aggregation value at the given rank

        throw new NotImplementedException();
    }

    /// <summary>
    /// Read a range of aggregate values over a range of key values
    /// </summary>
    /// <param name="rank">Name of the rank/range the aggregate value should come from</param>
    /// <param name="aggregation">Name of the aggregation to read</param>
    /// <param name="start">inclusive start of the range to return</param>
    /// <param name="end">inclusive end of the range to return</param>
    /// <typeparam name="TA">Type of the data to be returned</typeparam>
    public IEnumerable<TA> ReadAggregateRange<TA>(string aggregation, string rank, TK start, TK end)
    {
        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // TODO: read all the integral rank values, and read the aggregations

        throw new NotImplementedException();
    }
}

/// <summary>
/// Helper to build triangular lists
/// </summary>
/// <typeparam name="TK">Key type</typeparam>
/// <typeparam name="TV">Data type</typeparam>
public class TriangularListBuilder<TK, TV>
{
    private TriangularList<TK, TV>.KeyFunction? _keyFunction;
    private ITableAdaptor? _storage;
    private readonly Dictionary<int, TriangularList<TK, TV>.Rank> _ranksByNumber;
    private readonly Dictionary<string, TriangularList<TK, TV>.Aggregator> _aggregateByName;

    public TriangularListBuilder()
    {
        _aggregateByName = new Dictionary<string, TriangularList<TK, TV>.Aggregator>();
        _ranksByNumber = new Dictionary<int, TriangularList<TK, TV>.Rank>();
    }

    public TriangularListBuilder<TK, TV> UsingStorage(ITableAdaptor storage)
    {
        _storage = storage;
        return this;
    }

    public TriangularListBuilder<TK, TV> KeyOn(TriangularList<TK, TV>.KeyFunction keyFunction)
    {
        if (_keyFunction is null) _keyFunction = keyFunction;
        else throw new Exception("Key function already supplied");
        return this;
    }

    /// <summary>
    /// Add a new rank with name, position, and ranking function
    /// </summary>
    public TriangularListBuilder<TK, TV> Rank(int rank, string name, TriangularList<TK, TV>.RankFunction rankFunction)
    {
        if (rank < 0) throw new Exception("Rank must be 0 or greater.");
        if (_ranksByNumber.ContainsKey(rank)) throw new Exception($"Duplicated rank at {rank}. Tried to add '{name}', already have '{_ranksByNumber[rank].Name}'");

        _ranksByNumber.Add(rank, new TriangularList<TK, TV>.Rank(rank, name, rankFunction));
        return this;
    }

    /// <summary>
    /// Add a new aggregation that will be calculated and stored by the list
    /// </summary>
    /// <param name="name">Name of the aggregation, for querying</param>
    /// <param name="selector">Function to read an initial value from stored items</param>
    /// <param name="combiner">Function to aggregate two values (which may be aggregates themselves), returning a third value of the same type</param>
    public TriangularListBuilder<TK, TV> Aggregate<TA>(string name, Func<TV, TA> selector, Func<TA, TA, TA> combiner)
    {
        if (_aggregateByName.ContainsKey(name)) throw new Exception($"Duplicated aggregation '{name}'");

        _aggregateByName.Add(name, new TriangularList<TK, TV>.Aggregator<TA>(selector, combiner));
        return this;
    }

    /// <summary>
    /// Build and return the list
    /// </summary>
    public TriangularList<TK, TV> Build()
    {
        if (_keyFunction is null) throw new Exception("Key function was not set");
        if (_storage is null) throw new Exception("Storage was not set");
        var orderedRanks = GetOrderedRanks();

        var result = new TriangularList<TK, TV>(_storage, _keyFunction, orderedRanks, _aggregateByName);

        return result;
    }

    /// <summary>
    /// Sort ranks into order, and make sure there are no gaps
    /// </summary>
    private List<TriangularList<TK, TV>.Rank> GetOrderedRanks()
    {
        var ranks = _ranksByNumber.Keys.ToList();
        ranks.Sort();
        var lowest = ranks[0];
        var highest = ranks[^1];
        var orderedRanks = new List<TriangularList<TK, TV>.Rank>();

        for (var i = lowest; i <= highest; i++)
        {
            if (!_ranksByNumber.ContainsKey(i)) throw new Exception($"Gap in ranks: expected {i}, but it was not found.");
            orderedRanks.Add(_ranksByNumber[i]);
        }

        return orderedRanks;
    }
}