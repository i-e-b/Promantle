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
    public const string SchemaName = "triangles"; // separate schema for triangular data
    public const string CountPostfix = "_count";
    public const string ValuePostfix = "_value";
    public string ConnectionString { get; set; }
    public string BaseTableName { get; set; }

    public DatabaseConnection(int port, string tableName)
    {
        ConnectionString = $"Server=127.0.0.1;Port={port};Database=defaultdb;User Id=unit;Password=test;Include Error Detail=true;CommandTimeout=10;SSL Mode=Require;Trust Server Certificate=true;";
        BaseTableName = tableName;
        Console.WriteLine($"Connection: table={BaseTableName}; str='{ConnectionString}'");
    }

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
    private object? SimpleSelect(string queryText, object? parameters)
    {
        using var conn = new NpgsqlConnection(ConnectionString);
        conn.Open();

        using var cmd = BuildCmd(queryText, parameters, conn);

        return cmd.ExecuteScalar();
    }

    /// <summary>Read multiple simple values from a parametric query</summary>
    /// <remarks> Poor man's Dapper, here so we don't bring library dependencies with us. </remarks>
    private IEnumerable<T> SimpleSelectMany<T>(string queryText, object? parameters, Func<IDataReader, T> select)
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
            cmd.Parameters.AddWithValue(prop.Name, val);
        }

        return cmd;
    }

    public IEnumerable<AggregateValue> ReadWithRank(int rank, int rankCount, string aggregateName, long start, long end)
    {
        // TODO: protect aggregateName from injection
        var synthName = SynthName(rank, rankCount);
        return SimpleSelectMany<AggregateValue>(
            $"SELECT {aggregateName}{CountPostfix}, {aggregateName}{ValuePostfix}" +
            $" FROM {SchemaName}.{synthName}" +
            "  WHERE position BETWEEN :start AND :end;",
            new {start, end}, rdr => new AggregateValue{Count = rdr.GetInt64(0), Value = rdr.GetValue(1)});
    }

    public AggregateValue? ReadAtRank(int rank, int rankCount, string aggregateName, long position)
    {
        // TODO: protect aggregateName from injection
        var synthName = SynthName(rank, rankCount);
        return SimpleSelectMany<AggregateValue>(
            $"SELECT {aggregateName}{CountPostfix}, {aggregateName}{ValuePostfix}" +
            $" FROM {SchemaName}.{synthName}" +
            "  WHERE position = :position" +
            "  LIMIT 1;",
            new {position}, rdr => new AggregateValue{Count = rdr.GetInt64(0), Value = rdr.GetValue(1)}).FirstOrDefault();
    }

    public void WriteAtRank(int rank, int rankCount, string aggregateName, long position, long count, object value)
    {
        var synthName = SynthName(rank, rankCount);
        var countCol = $"{aggregateName}{CountPostfix}";
        var valueCol = $"{aggregateName}{ValuePostfix}";
        SimpleSelect($"UPSERT INTO {SchemaName}.{synthName}" +
                     $"       ( position, {countCol}, {valueCol})" +
                     " VALUES (:position, :count,     :value)",
            new {position, count, value});
    }

    public bool EnsureTableForRank(int rank, int rankCount, params BasicColumn[] aggregates)
    {
        var synthName = SynthName(rank, rankCount);
        
        if (TableExists(SchemaName, synthName)) return false; // already exists

        var sb = new StringBuilder();
        
        sb.Append("CREATE TABLE ");
        sb.Append($"{SchemaName}.{synthName}");
        sb.AppendLine("(");
        sb.AppendLine("    position INT8 not null primary key"); // basic position (INT8 = Int64 = long)

        foreach (var agg in aggregates)
        {
            sb.AppendLine($",   {agg.Name}{CountPostfix} INT8");       // count of values that are summed here
            sb.AppendLine($",   {agg.Name}{ValuePostfix} {agg.Type}"); // the summed value
        }
        
        sb.Append(");"); // close definition
        
        var query = sb.ToString();
        Console.WriteLine(query);
        SimpleSelect(query, null);
        return true;
    }

    private string SynthName(int rank, int rankCount)
    {
        var synthName = $"{BaseTableName}_{rank}_of_{rankCount}";
        return synthName;
    }
}