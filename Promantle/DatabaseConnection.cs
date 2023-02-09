//#define UseCockroach

using System.ComponentModel;
using System.Data;
using System.Text;
using Npgsql;

namespace Promantle;

/// <summary>
/// Really dumb database adaptor for testing
/// </summary>
public class DatabaseConnection : ITableAdaptor
{
#if UseCockroach
    public const string SchemaName = "triangles"; // separate schema for triangular data
#else
    public const string SchemaName = "public"; // postgres schemas tricky with test setup
#endif
    public const string CountPostfix = "_count";
    public const string ValuePostfix = "_value";
    public string ConnectionString { get; set; }
    public string BaseTableName { get; set; }

    public DatabaseConnection(int port, string tableName)
    {
#if UseCockroach
        // CRDB:
        ConnectionString = $"Server=127.0.0.1;Port={port};Database=defaultdb;User Id=unit;Password=test;Include Error Detail=true;CommandTimeout=10;SSL Mode=Require;Trust Server Certificate=true;";
#else
        // Postgres:
        ConnectionString = "Server=127.0.0.1;Port=54448;Database=testdb;User Id=postgres;Password=password;Include Error Detail=true;CommandTimeout=360;Enlist=false;No Reset On Close=true;";
#endif
        
        BaseTableName = tableName;
        Console.WriteLine($"Connection: table={BaseTableName}; str='{ConnectionString}'");
    }

    #region PoorMansDapper
    /// <summary>
    /// Check to see if a given "schema.table" is known
    /// </summary>
    private bool TableExists(string schema, string name)
    {
        const string query = @"
SELECT EXISTS (
	SELECT 1
	FROM information_schema.tables 
	WHERE table_schema = :schema
	AND table_name = :name
);";

        return SimpleSelect(query, new { schema = schema.ToLowerInvariant(), name = name.ToLowerInvariant() }) as bool? == true;
    }

    
    /// <summary>Read a single value from a parametric query</summary>
    /// <remarks> Poor man's Dapper, here so we don't bring library dependencies with us. </remarks>
    private void SimpleExecute(string queryText, object? parameters)
    {
        using var conn = new NpgsqlConnection(ConnectionString);
        conn.Open();

        using var cmd = BuildCmd(queryText, parameters, conn);

        cmd.ExecuteNonQuery();
    }
    
    /// <summary>Read a single value from a parametric query</summary>
    /// <remarks> Poor man's Dapper, here so we don't bring library dependencies with us. </remarks>
    private object? SimpleSelect(string queryText, object? parameters)
    {
        using var conn = new NpgsqlConnection(ConnectionString);
        conn.Open();

        using var cmd = BuildCmd(queryText, parameters, conn);

        return cmd.ExecuteScalar();
    }

    /// <summary>Read multiple simple values from a parametric query</summary>
    /// <remarks> Poor man's Dapper, here so we don't bring library dependencies with us. </remarks>
    private IEnumerable<T> SimpleSelectMany<T>(string queryText, object? parameters, Func<IDataRecord, T> select)
    {
        using var conn = new NpgsqlConnection(ConnectionString);
        conn.Open();

        using var cmd = BuildCmd(queryText, parameters, conn);

        using var rdr = cmd.ExecuteReader();
        var result = new List<T>();

        while (rdr.Read())
        {
            result.Add(select(rdr));
        }

        rdr.Close();
        conn.Close();
        return result;
    }

    private static NpgsqlCommand BuildCmd(string queryText, object? parameters, NpgsqlConnection conn)
    {
        var cmd = new NpgsqlCommand(queryText, conn);

        if (parameters == null) return cmd;
        
        var props = TypeDescriptor.GetProperties(parameters);
        foreach (PropertyDescriptor prop in props)
        {
            var val = prop.GetValue(parameters);
            if (val is null) continue;

            if (val.GetType().IsEnum) // cast enums to base type
            {
                var type = Enum.GetUnderlyingType(val.GetType());
                var underVal = Convert.ChangeType(val, type);
                cmd.Parameters.AddWithValue(prop.Name, underVal);
            }
            else // pass normal types directly
            {
                cmd.Parameters.AddWithValue(prop.Name, val);
            }
        }

        return cmd;
    }
    #endregion
    
    public IEnumerable<AggregateValue> ReadWithRank(int rank, int rankCount, string aggregateName, long start, long end)
    {
        // See `EnsureTableForRank` for table definition

        // TODO: protect aggregateName from injection
        var synthName = SynthName(rank, rankCount);
        return SimpleSelectMany<AggregateValue>(
            $"SELECT position, parentPosition, COALESCE({aggregateName}{CountPostfix},0), {aggregateName}{ValuePostfix}, lowerBound,  upperBound" +
            $" FROM {SchemaName}.{synthName}" +
            "  WHERE position BETWEEN :start AND :end;",
            new {start, end},
            ReadAggregateValue);
    }

    public IEnumerable<AggregateValue> ReadWithParentRank(int rank, int rankCount, string aggregateName, long parentPosition)
    {
        // See `EnsureTableForRank` for table definition
        
        // TODO: protect aggregateName from injection
        var synthName = SynthName(rank, rankCount);
        return SimpleSelectMany<AggregateValue>(
            $"SELECT position, parentPosition, COALESCE({aggregateName}{CountPostfix},0), {aggregateName}{ValuePostfix}, lowerBound,  upperBound" +
            $" FROM {SchemaName}.{synthName}" +
            "  WHERE parentPosition = :parentPosition;",
            new {parentPosition},
            ReadAggregateValue);
    }

    public AggregateValue? ReadAtRank(int rank, int rankCount, string aggregateName, long position)
    {
        // See `EnsureTableForRank` for table definition
        
        // TODO: protect aggregateName from injection
        var synthName = SynthName(rank, rankCount);
        return SimpleSelectMany<AggregateValue>(
                $"SELECT position, parentPosition, {aggregateName}{CountPostfix}, {aggregateName}{ValuePostfix}, lowerBound,  upperBound" +
                $" FROM {SchemaName}.{synthName}" +
                "  WHERE position = :position" +
                "  LIMIT 1;",
                new {position}, 
                ReadAggregateValue)
            .FirstOrDefault();
    }

    private static AggregateValue ReadAggregateValue(IDataRecord rdr)
    {
        return new AggregateValue{
            Position = rdr.GetInt64(0), ParentPosition = rdr.GetInt64(1),
            Count = rdr.GetInt64(2), Value = rdr.GetValue(3),
            LowerBound = rdr.GetValue(4), UpperBound = rdr.GetValue(5)
        };
    }

    public void WriteAtRank(int rank, int rankCount,
        string aggregateName, long parentPosition, long position, long count,
        object? value, object? lowerBound, object? upperBound)
    {
        // See `EnsureTableForRank` for table definition
        var synthName = SynthName(rank, rankCount);
        var countCol = $"{aggregateName}{CountPostfix}";
        var valueCol = $"{aggregateName}{ValuePostfix}";

        SimpleExecute($"INSERT INTO {SchemaName}.{synthName}" +
                      $"       ( position, parentPosition,  lowerBound,  upperBound,  {countCol}, {valueCol})" +
                      " VALUES (:position, :parentPosition, :lowerBound, :upperBound, :count,     :value)" +
                      " ON CONFLICT (position) DO UPDATE SET " +
                      " position=:position, parentPosition=:parentPosition, lowerBound=:lowerBound, " +
                      $"upperBound=:upperBound, {countCol}=:count, {valueCol}=:value;",
            new { position, parentPosition, lowerBound, upperBound, count, value });
    }

    public long MaxPosition(int rank, int rankCount)
    {
        try
        {
            var synthName = SynthName(rank, rankCount);
            return SimpleSelect($"SELECT MAX(position) FROM {SchemaName}.{synthName};", new { }) as long? ?? 0;
        }
        catch
        {
            return 0;
        }
    }

    public void DumpTableForRank(StringBuilder sb, int rank, int rankCount)
    {
        var synthName = SynthName(rank, rankCount);
        
        sb.AppendLine();
        sb.AppendLine(synthName);
        SimpleSelectMany($"SELECT * FROM {SchemaName}.{synthName};", new{}, rdr => ReaderRowToString(sb, rdr));
        
        sb.AppendLine();
    }

    private int ReaderRowToString(StringBuilder sb, IDataRecord rdr)
    {
        var end = rdr.FieldCount;
        for (int i = 0; i < end; i++)
        {
            sb.Append(rdr.GetName(i)+"="+rdr.GetValue(i)+"; ");
        }
        sb.AppendLine();
        return end;
    }

    public bool EnsureTableForRank(int rank, int rankCount, string keyType, params BasicColumn[] aggregates)
    {
        var synthName = SynthName(rank, rankCount);
        
        if (TableExists(SchemaName, synthName)) return false; // already exists

        var sb = new StringBuilder();
        
        sb.Append($"CREATE TABLE {SchemaName}.{synthName}");
        sb.AppendLine("(");
        
        // Unique position of this value, plus position in next rank that includes this value in aggregate
        // (this is key value fed through one of the rank position functions)
        sb.AppendLine("    position INT8 not null primary key"); // basic position (INT8 = Int64 = long)
        sb.AppendLine(",   parentPosition INT8"); // position in the next (more aggregated) rank
        
        // Upper and lower bounds of the key values that are aggregated at this point
        sb.AppendLine($",   lowerBound {keyType}"); // lowest of key values in values aggregated here
        sb.AppendLine($",   upperBound {keyType}"); // highest of key values aggregated here
        

        foreach (var agg in aggregates)
        {
            if (agg is null) throw new Exception("Invalid aggregation (null value in aggregates)");
            if (string.IsNullOrWhiteSpace(agg.Name) || string.IsNullOrWhiteSpace(agg.Type)) throw new Exception($"Invalid aggregate: '{agg.Name ?? "<null>"}' has type '{agg.Type ?? "<null>"}'");
            sb.AppendLine($",   {agg.Name}{CountPostfix} INT8");       // count of values that are summed here
            sb.AppendLine($",   {agg.Name}{ValuePostfix} {agg.Type}"); // the summed value
        }
        
#if UseCockroach
        // CRDB only:
        sb.AppendLine($",   INDEX index_my_pos_{synthName} (position)");
        sb.AppendLine($",   INDEX index_parent_{synthName} (parentPosition)");
#endif
        sb.Append(");"); // close definition
        
#if !UseCockroach
        // Postgres syntax:
        sb.AppendLine($"CREATE INDEX index_my_pos_{synthName} ON  {SchemaName}.{synthName} (position);");
        sb.AppendLine($"CREATE INDEX index_parent_{synthName} ON  {SchemaName}.{synthName} (parentPosition);");
#endif
        
        var query = sb.ToString();
        //Console.WriteLine(query);
        SimpleExecute(query, null);
        return true;
    }

    private string SynthName(int rank, int rankCount)
    {
        var synthName = $"{BaseTableName}_{rank}_of_{rankCount}";
        return synthName;
    }
}