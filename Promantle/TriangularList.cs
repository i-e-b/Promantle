using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Promantle;


/// <summary>
/// Value and range information for aggregated data
/// </summary>
/// <typeparam name="TA">Type of the aggregated value</typeparam>
/// <typeparam name="TK">Type of the key</typeparam>
[SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global")]
public class AggregateValue<TA, TK>
{
    /// <summary>
    /// Aggregated value
    /// </summary>
    public TA? Value { get; set; }

    /// <summary>
    /// Count of original (zero-rank) values aggregated into <see cref="Value"/>
    /// </summary>
    public long Count { get; set; }
    
    /// <summary>
    /// Bottom range of real data in this aggregation
    /// </summary>
    public TK? LowerBound { get; set; }

    /// <summary>
    /// Top range of real data in this aggregation
    /// </summary>
    public TK? UpperBound { get; set; }

    /// <summary>
    /// Output a diagnostic string for this aggregate value
    /// </summary>
    public override string ToString()
    {
        return $"Value='{Value?.ToString() ?? "<null>"}'; Count='{Count}'; Data range '{LowerBound?.ToString() ?? "<null>"}'..'{UpperBound?.ToString() ?? "<null>"}';";
    }
}

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
    private readonly string _name;
    private readonly IAggregateTableAdaptor _storage;
    private readonly KeyFunction _keyFunction;
    private readonly KeyMinMaxFunction _minMaxFunction;
    private readonly Dictionary<string, Aggregator> _aggregateByName;
    private readonly Dictionary<string, Rank> _ranksByName;
    private readonly Dictionary<int, Rank> _ranksByNumber;
    private readonly int _rankCount;
    
    /// <summary>Set to true when <see cref="DeleteAllTablesAndData"/> is called. This causes method calls to fail.</summary>
    private bool _deleted;
    private const string DeletedMessage = "This instance has been deleted. Create a new instance before calling.";
    
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
    /// Function that orders a pair of key values.
    /// This is used to burn ranges into the various aggregations
    /// </summary>
    public delegate void KeyMinMaxFunction(TK a, TK b, out TK min, out TK max);

    /// <summary>
    /// Represents a line of data that is aggregated from the stored items
    /// </summary>
    [SuppressMessage("ReSharper", "MemberCanBeProtected.Global")]
    [SuppressMessage("ReSharper", "NotAccessedField.Global")]
    [SuppressMessage("ReSharper", "UnusedMember.Global")]
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
    public static TriangularListBuilder<TK, TV> Create(string name) => new(name);

    /// <summary>
    /// Create a new list, with: a keying function, a set of ranks, and a set of data aggregations.
    /// <p></p>
    /// For a more intuitive way to create, see <see cref="Create"/>
    /// </summary>
    public TriangularList(string name, IAggregateTableAdaptor storage,
        KeyFunction keyFunction, KeyMinMaxFunction minMaxFunction, string keyStorageType,
        List<Rank> orderedRanks,
        Dictionary<string, Aggregator> aggregateByName)
    {
        _deleted = true;
        if (orderedRanks.Count < 1) throw new Exception("Triangular list must have at least one rank");

        _name = name;
        _storage = storage;
        _keyFunction = keyFunction;
        _minMaxFunction = minMaxFunction;

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
            _storage.EnsureTableForRank(_name, rank, _rankCount, keyStorageType, aggregateNames);
        }
        
        _nextZero = _storage.MaxPosition(_name, 0, _rankCount) + 1;
        _deleted = false;
    }

    /// <summary>
    /// Write a new item to the list's storage, and update aggregations.
    /// This will immediately update all aggregations.
    /// </summary>
    /// <returns>Number of calculations made</returns>
    public int WriteItem(TV item)
    {
        if (_deleted) throw new Exception(DeletedMessage);
        var calcCount = 0;
        
        // Get the key out of the value
        var key = _keyFunction(item);
        var zeroRankIndex = _nextZero++;
        
        // TODO: do each aggregation across a single rank loop.
        // for each aggregation, go through the ranks and re-sum data
        foreach (var kvp in _aggregateByName)
        {
            var aggName = kvp.Key;
            var aggCombine = kvp.Value.CombineValues;
            
            // Write rank zero data
            var rzObject = kvp.Value.SelectValue(item);
            var parentPos = _ranksByNumber[1].RankFunction(key);
            _storage.WriteAtRank(_name, 0, _rankCount, aggName, parentPos, zeroRankIndex, 1, rzObject, key, key); // for rank zero, upper and lower bounds are the same
            
            // Work up through all ranks, re-aggregating the data
            for (int childRank = 0; childRank < _rankCount; childRank++)
            {
                var parentRank = childRank+1;
                var grandparentRank = parentRank+1;
                
                // aggregate data under a parent
                parentPos = _ranksByNumber[parentRank].RankFunction(key);
                var child = _storage.ReadWithParentRank(_name, childRank, _rankCount, aggName, parentPos).ToList();
                if (child.Count < 1) continue;
                
                calcCount += child.Count;
                var newCount = child.Sum(av => av.Count);
                var newAggValue = child.Select(av=>av.Value).Aggregate((a,b) => aggCombine(a,b));
                GetKeyRange(child, out var lower, out var upper);
                
                // write back
                var grandparentPos = grandparentRank <= _rankCount ? _ranksByNumber[grandparentRank].RankFunction(key) : 0;
                _storage.WriteAtRank(_name, parentRank, _rankCount, aggName, grandparentPos, parentPos, newCount, newAggValue, lower, upper);
            }
        }
        return calcCount;
    }

    /// <summary>
    /// Use the Min/Max function to find an overall upper and lower bound on a list of data
    /// </summary>
    private void GetKeyRange(List<AggregateValue> children, out object? lower, out object? upper)
    {
        if (_deleted) throw new Exception(DeletedMessage);
        // this guess *should* be correct
        TK? lowest = NullCast<TK>(children[0].LowerBound);
        TK? highest = NullCast<TK>(children[^1].UpperBound);
        
        // double check
        foreach (var child in children)
        {
            if (lowest is null) lowest = (TK?)child.LowerBound;
            else if (child.LowerBound is TK low) _minMaxFunction(lowest, low, out lowest, out _);
            
            if (highest is null) highest = (TK?)child.UpperBound;
            else if (child.UpperBound is TK high) _minMaxFunction(highest, high, out _, out highest);
        }
        
        lower = lowest;
        upper = highest;
    }

    private TX? NullCast<TX>(object? v)
    {
        if (typeof(TX).IsEnum && v is not null)
        {
            // Do enum fiddling
            try
            {
                return (TX)Enum.ToObject(typeof(TX), v);
            }
            catch
            {
                // ignore
            }
        }

        return v switch
        {
            null => default!,
            TX match => match,
            _ => default
        };
    }

    /// <summary>
    /// Read a single aggregate value at a target key.
    /// For full datapoint details, see <see cref="ReadDataAtPoint{TA}"/>
    /// </summary>
    /// <param name="rank">Name of the rank/range the aggregate value should come from</param>
    /// <param name="aggregation">Name of the aggregation to read</param>
    /// <param name="target">The key for the point or range to read</param>
    /// <typeparam name="TA">Type of the data to be returned</typeparam>
    public TA? ReadAggregateDataAtPoint<TA>(string aggregation, string rank, TK target)
    {
        if (_deleted) throw new Exception(DeletedMessage);
        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // Read the value
        var rankInfo = _ranksByName[rank];
        var position = rankInfo.RankFunction(target);
        var value = _storage.ReadAtRank(_name, rankInfo.RankNumber, _rankCount, aggregation, position);
        
        if (value is null) return default;

        if (value.Value is not TA final)
        {
            throw new Exception($"Expected type '{typeof(TA).Name}', but aggregate '{aggregation}' at rank '{rank}' has type '{value.Value?.GetType().Name ?? "<null>"}'");
        }
        
        return final;
    }

    /// <summary>
    /// Read a single datapoint at a target key.
    /// To read just the aggregated data, see <see cref="ReadAggregateDataAtPoint{TA}"/>
    /// </summary>
    /// <param name="rank">Name of the rank/range the aggregate value should come from</param>
    /// <param name="aggregation">Name of the aggregation to read</param>
    /// <param name="target">The key for the point or range to read</param>
    /// <typeparam name="TA">Type of the data to be returned</typeparam>
    /// <returns>A single aggregate value that includes the source value range and count</returns>
    public AggregateValue<TA, TK>? ReadDataAtPoint<TA>(string aggregation, string rank, TK target)
    {
        if (_deleted) throw new Exception(DeletedMessage);
        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // Read the value
        var rankInfo = _ranksByName[rank];
        var position = rankInfo.RankFunction(target);
        var value = _storage.ReadAtRank(_name, rankInfo.RankNumber, _rankCount, aggregation, position);
        
        if (value is null) return default;

        // Sanity check types
        if (value.Value is not TA) throw new Exception($"Expected value type '{typeof(TA).Name}', but aggregate '{aggregation}' at rank '{rank}' has type '{value.Value?.GetType().Name ?? "<null>"}'");
        
        // Build return structure
        return TypedAggregateValue<TA>(value);
    }

    /// <summary>
    /// Read all child items that are aggregated by the given rank and target point.
    /// If the given rank is the lowest, the stored original data-points will be read.
    /// </summary>
    /// <param name="rank">Name of the rank/range the aggregate value should come from</param>
    /// <param name="aggregation">Name of the aggregation to read</param>
    /// <param name="target">The key for the point or range to read</param>
    /// <typeparam name="TA">Type of the data to be returned</typeparam>
    /// <returns>Multiple aggregate values that include the source value range and count at each point</returns>
    public IEnumerable<AggregateValue<TA, TK>> ReadDataUnderPoint<TA>(string aggregation, string rank, TK target)
    {
        if (_deleted) throw new Exception(DeletedMessage);
        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // Determine search values
        var rankInfo = _ranksByName[rank];
        var position = rankInfo.RankFunction(target);
        
        // Read the value
        var value = _storage.ReadWithParentRank(_name, rankInfo.RankNumber - 1, _rankCount, aggregation, position).ToList();
        
        if (value.Count < 1) return Array.Empty<AggregateValue<TA, TK>>();
        
        // Check types are as expected
        if (value[0].Value is not TA)
        {
            throw new Exception($"Expected type '{typeof(TA).Name}', but aggregate '{aggregation}' at rank '{rank}' has type '{value[0].Value?.GetType().Name ?? "<null>"}'");
        }
        
        // Convert to the correct type
        return value.Where(av => av.Value is not null).Select(TypedAggregateValue<TA>);
    }
    
    /// <summary>
    /// Read a range of aggregate values over a range of key values.
    /// For full datapoint details, see <see cref="ReadDataOverRange{TA}"/>
    /// </summary>
    /// <param name="rank">Name of the rank/range the aggregate value should come from</param>
    /// <param name="aggregation">Name of the aggregation to read</param>
    /// <param name="start">inclusive start of the range to return</param>
    /// <param name="end">inclusive end of the range to return</param>
    /// <typeparam name="TA">Type of the data to be returned</typeparam>
    public IEnumerable<TA> ReadAggregateDataOverRange<TA>(string aggregation, string rank, TK start, TK end)
    {
        if (_deleted) throw new Exception(DeletedMessage);
        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // Determine search values
        var rankInfo = _ranksByName[rank];
        var startPos = rankInfo.RankFunction(start);
        var endPos = rankInfo.RankFunction(end);
        if (endPos < startPos) throw new Exception("Start position is after end position");
        
        // Read the value
        var value = _storage.ReadWithRank(_name, rankInfo.RankNumber, _rankCount, aggregation, startPos, endPos).ToList();
        
        if (value.Count < 1) return Array.Empty<TA>();
        
        // Check types are as expected
        if (value[0].Value is not TA)
        {
            throw new Exception($"Expected type '{typeof(TA).Name}', but aggregate '{aggregation}' at rank '{rank}' has type '{value[0].Value?.GetType().Name ?? "<null>"}'");
        }
        
        // Convert to the correct type
        return value.Where(av => av.Value is not null).Select(av => (TA)av.Value!);
    }

    /// <summary>
    /// Read a range of aggregate values over a range of key values.
    /// To read just the aggregated data, see <see cref="ReadAggregateDataOverRange{TA}"/>
    /// </summary>
    /// <param name="rank">Name of the rank/range the aggregate value should come from</param>
    /// <param name="aggregation">Name of the aggregation to read</param>
    /// <param name="start">inclusive start of the range to return</param>
    /// <param name="end">inclusive end of the range to return</param>
    /// <typeparam name="TA">Type of the data to be returned</typeparam>
    /// <returns>Multiple aggregate values that include the source value range and count at each point</returns>
    public IEnumerable<AggregateValue<TA, TK>> ReadDataOverRange<TA>(string aggregation, string rank, TK start, TK end)
    {
        if (_deleted) throw new Exception(DeletedMessage);
        if (!_aggregateByName.ContainsKey(aggregation)) throw new Exception($"No aggregation '{aggregation}' is registered");
        if (!_ranksByName.ContainsKey(rank)) throw new Exception($"No scale rank '{rank}' is registered");

        // Determine search values
        var rankInfo = _ranksByName[rank];
        var startPos = rankInfo.RankFunction(start);
        var endPos = rankInfo.RankFunction(end);
        if (endPos < startPos) throw new Exception("Start position is after end position");
        
        // Read the value
        var value = _storage.ReadWithRank(_name, rankInfo.RankNumber, _rankCount, aggregation, startPos, endPos).ToList();
        
        if (value.Count < 1) return Array.Empty<AggregateValue<TA, TK>>();
        
        // Check types are as expected
        if (value[0].Value is not TA)
        {
            throw new Exception($"Expected type '{typeof(TA).Name}', but aggregate '{aggregation}' at rank '{rank}' has type '{value[0].Value?.GetType().Name ?? "<null>"}'");
        }
        
        // Convert to the correct type
        return value.Where(av => av.Value is not null).Select(TypedAggregateValue<TA>);
    }

    private AggregateValue<TA, TK> TypedAggregateValue<TA>(AggregateValue av)
    {
        return new AggregateValue<TA, TK>
        {
            Count = av.Count,
            Value = NullCast<TA>(av.Value),
            LowerBound = NullCast<TK>(av.LowerBound),
            UpperBound = NullCast<TK>(av.UpperBound)
        };
    }

    /// <summary>
    /// Output a diagnostic string of data stored
    /// </summary>
    public string DumpTables()
    {
        if (_deleted) throw new Exception(DeletedMessage);
        var sb = new StringBuilder(10_000);
        
        // Read the various tables
        for (int rank = 0; rank <= _rankCount; rank++)
        {
            _storage.DumpTableForRank(sb, _name, rank, _rankCount);
        }
        
        return sb.ToString();
    }

    /// <summary>
    /// Drop all tables related to this triangular list.
    /// This will delete all data, and will render this instance useless.
    /// <p></p>
    /// The same triangular list can be created again after deleting, but it will be empty
    /// </summary>
    public void DeleteAllTablesAndData()
    {
        if (_deleted) throw new Exception(DeletedMessage);
        
        _deleted = true;
        
        for (int rank = 0; rank <= _rankCount; rank++)
        {
            _storage.DeleteTableForRank(_name, rank, _rankCount);
        }
    }
}

/// <summary>
/// Helper to build triangular lists
/// </summary>
/// <typeparam name="TK">Key type</typeparam>
/// <typeparam name="TV">Data type</typeparam>
public class TriangularListBuilder<TK, TV>
{
    private readonly string _name;

    // Key stuff
    private string? _keyStorageType;
    private TriangularList<TK, TV>.KeyFunction? _keyFunction;
    private TriangularList<TK,TV>.KeyMinMaxFunction? _minMaxFunction;
    
    // Database
    private IAggregateTableAdaptor? _storage;
    
    // Ranks and aggregates
    private readonly Dictionary<int, TriangularList<TK, TV>.Rank> _ranksByNumber;
    private readonly Dictionary<string, TriangularList<TK, TV>.Aggregator> _aggregateByName;

    public TriangularListBuilder(string name)
    {
        _name = name;
        _aggregateByName = new Dictionary<string, TriangularList<TK, TV>.Aggregator>();
        _ranksByNumber = new Dictionary<int, TriangularList<TK, TV>.Rank>();
    }

    /// <summary>
    /// Set the storage adaptor
    /// </summary>
    public TriangularListBuilder<TK, TV> UsingStorage(IAggregateTableAdaptor storage)
    {
        _storage = storage;
        return this;
    }

    /// <summary>
    /// Set the functions that read the key values out of data items
    /// </summary>
    /// <param name="keyStorageType">Database type representing the key type</param>
    /// <param name="keyFunction">Function that take a single data item, and returns a single key value. The key values are fed to
    /// the RankFunctions for each rank to make aggregate ranges</param>
    /// <param name="minMax">Function that takes two key values, and orders them into minimum and maximum. This is used to
    /// return the ranges of keys that are present in each aggregated value in a rank.</param>
    public TriangularListBuilder<TK, TV> KeyOn(string keyStorageType, TriangularList<TK, TV>.KeyFunction keyFunction, TriangularList<TK, TV>.KeyMinMaxFunction minMax)
    {
        if (_keyStorageType is null) _keyStorageType = keyStorageType;
        else throw new Exception("Key storage type already supplied");
        
        if (_keyFunction is null) _keyFunction = keyFunction;
        else throw new Exception("Key function already supplied");
        
        if (_minMaxFunction is null) _minMaxFunction = minMax;
        else throw new Exception("Min/Max function already supplied");
        
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
        if (_minMaxFunction is null) throw new Exception("Key min/max function was not set");
        if (_keyStorageType is null) throw new Exception("Key storage type was not set");
        if (_storage is null) throw new Exception("Storage was not set");
        
        if (_aggregateByName.Count < 1) throw new Exception("No aggregations were supplied");
        
        var orderedRanks = GetOrderedRanks();
        
        if (orderedRanks.Count < 1) throw new Exception("No ranks were supplied");

        var result = new TriangularList<TK, TV>(_name, _storage, _keyFunction, _minMaxFunction, _keyStorageType, orderedRanks, _aggregateByName);

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