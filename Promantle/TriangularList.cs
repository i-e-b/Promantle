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
    private readonly int _rankCount;
    
    /// <summary> Unique ID for rank-zero values </summary>
    private long _nextZero;

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
        
        /// <summary>
        /// Type to be used in the database
        /// </summary>
        public string StorageType = "";
        
        /// <summary>
        /// Data type
        /// </summary>
        public Type? DataType;
        
        public abstract object? SelectValue(object? dataObject);
        public abstract object? CombineValues(object? a, object? b);
    }

    /// <summary>
    /// Type specific aggregator
    /// </summary>
    public class Aggregator<TA> : Aggregator
    {
        public readonly Func<TV, TA> Select;
        public readonly Func<TA, TA, TA> Combine;

        public Aggregator(Func<TV, TA> select, Func<TA, TA, TA> combine, string storageType)
        {
            Select = select;
            Combine = combine;
            StorageType = storageType;
            DataType = typeof(TA);
        }

        public override object? SelectValue(object? dataObject)
        {
            if (dataObject is not TV value) throw new Exception($"Incorrect type: dataObject should be '{typeof(TV).Name}', but was {dataObject?.GetType().Name ?? "<null>"}");
            
            return Select(value);
        }

        public override object? CombineValues(object? a, object? b)
        {
            if (a is not TA valueA) throw new Exception($"Incorrect type: 'a' should be '{typeof(TA).Name}', but was {a?.GetType().Name ?? "<null>"}");
            if (b is not TA valueB) throw new Exception($"Incorrect type: 'b' should be '{typeof(TA).Name}', but was {b?.GetType().Name ?? "<null>"}");
            
            return Combine(valueA, valueB);
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

        public Rank Renumber(int rankNum)
        {
            return new Rank(rankNum, Name, RankFunction);
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
        if (orderedRanks.Count < 1) throw new Exception("Triangular list must have at least one rank");
        
        _storage = storage;
        _keyFunction = keyFunction;

        // Build dictionaries from the list
        // We use our own rank numbers, user can query only by name.
        _ranksByName = new Dictionary<string, Rank>();
        _ranksByNumber = new Dictionary<int, Rank>();
        _aggregateByName = new Dictionary<string, Aggregator>();

        var rankNum = 1;
        foreach (var rank in orderedRanks)
        {
            var selfRank = rank.Renumber(rankNum);
            _ranksByName.Add(rank.Name, selfRank);
            _ranksByNumber.Add(rankNum, selfRank);
            rankNum++;
        }
        
        var aggregateNames = new BasicColumn[aggregateByName.Count];
        var aggregateIdx = 0;
        foreach (var kvp in aggregateByName)
        {
            aggregateNames[aggregateIdx++] = new BasicColumn(kvp.Key, kvp.Value.StorageType);
            _aggregateByName.Add(kvp.Key, kvp.Value);
        }
        
        // ensure the various tables
        _rankCount = _ranksByNumber.Count;
        for (int rank = 0; rank <= _rankCount; rank++)
        {
            _storage.EnsureTableForRank(rank, _rankCount, aggregateNames);
        }
        
        _nextZero = _storage.MaxPosition(0, _rankCount) + 1;
    }

    /// <summary>
    /// Write a new item to the list's storage, and update aggregations.
    /// This will immediately update all aggregations.
    /// </summary>
    /// <returns>Number of calculations made</returns>
    public int WriteItem(TV item)
    {
        var calcCount = 0;
        
        // Get the key out of the value
        var key = _keyFunction(item);
        var zeroRankIndex = _nextZero++;
        
        // for each aggregation, go through the ranks and re-sum data
        foreach (var kvp in _aggregateByName)
        {
            var aggName = kvp.Key;
            var aggCombine = kvp.Value.CombineValues;
            
            // Write rank zero data
            var rzObject = kvp.Value.SelectValue(item);
            var parentPos = _ranksByNumber[1].RankFunction(key);
            _storage.WriteAtRank(0, _rankCount, aggName, parentPos, zeroRankIndex, 1, rzObject);
            
            // Work up through all ranks, re-aggregating the data
            for (int childRank = 0; childRank < _rankCount; childRank++)
            {
                var parentRank = childRank+1;
                var grandparentRank = parentRank+1;
                
                // aggregate data under a parent
                parentPos = _ranksByNumber[parentRank].RankFunction(key);
                var child = _storage.ReadWithParentRank(childRank, _rankCount, aggName, parentPos).ToList();
                calcCount += child.Count;
                var newCount = child.Sum(av => av.Count);
                var newAggValue = child.Select(av=>av.Value).Aggregate((a,b) => aggCombine(a,b));
                
                // write back
                var grandparentPos = grandparentRank <= _rankCount ? _ranksByNumber[grandparentRank].RankFunction(key) : 0;
                _storage.WriteAtRank(parentRank, _rankCount, aggName, grandparentPos, parentPos, newCount, newAggValue);
            }
        }
        return calcCount;
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

        // Read the value
        var rankInfo = _ranksByName[rank];
        var position = rankInfo.RankFunction(target);
        var value = _storage.ReadAtRank(rankInfo.RankNumber, _rankCount, aggregation, position);
        
        if (value is null) return default;

        if (value.Value is not TA final)
        {
            throw new Exception($"Expected type '{typeof(TA).Name}', but aggregate '{aggregation}' at rank '{rank}' has type '{value.Value?.GetType().Name ?? "<null>"}'");
        }
        
        return final;
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

        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // Determine search values
        var rankInfo = _ranksByName[rank];
        var startPos = rankInfo.RankFunction(start);
        var endPos = rankInfo.RankFunction(end);
        if (endPos < startPos) throw new Exception("Start position is after end position");
        
        // Read the value
        var value = _storage.ReadWithRank(rankInfo.RankNumber, _rankCount, aggregation, startPos, endPos).ToList();
        
        if (value.Count < 1) return Array.Empty<TA>();
        
        // Check types are as expected
        if (value[0].Value is not TA)
        {
            throw new Exception($"Expected type '{typeof(TA).Name}', but aggregate '{aggregation}' at rank '{rank}' has type '{value[0].Value?.GetType().Name ?? "<null>"}'");
        }
        
        // Convert to the correct type
        return value.Where(av => av.Value is not null).Select(av => (TA)av.Value!);
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

    /// <summary>
    /// Set the storage adaptor
    /// </summary>
    public TriangularListBuilder<TK, TV> UsingStorage(ITableAdaptor storage)
    {
        _storage = storage;
        return this;
    }

    /// <summary>
    /// Set the function that pulls the key value out of data items
    /// </summary>
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
    /// <param name="storageType">Database column type to store the values</param>
    public TriangularListBuilder<TK, TV> Aggregate<TA>(string name, Func<TV, TA> selector, Func<TA, TA, TA> combiner, string storageType)
    {
        if (_aggregateByName.ContainsKey(name)) throw new Exception($"Duplicated aggregation '{name}'");

        _aggregateByName.Add(name, new TriangularList<TK, TV>.Aggregator<TA>(selector, combiner, storageType));
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