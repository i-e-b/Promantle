using System.Reflection;

namespace Promantle;

/// <summary>
/// A class to help store and query pre-cached lists,
/// which can be paged or sorted at generation time.
/// This trades update latency for query speed.
/// </summary>
public class MultiPager<T>
{
    private readonly string _name;
    private readonly int _pageSize;
    private readonly Dictionary<string,PropertyInfo> _properties; // name => property info
    private readonly List<BasicColumn> _dbColumns;

    /// <summary>
    /// Create a new multi-pager for the type <see cref="T"/>
    /// </summary>
    public MultiPager(string name, IMultiPageTableAdaptor storage, int pageSize)
    {
        _name = name;
        _pageSize = pageSize;
        _properties = new Dictionary<string, PropertyInfo>();
        FindProperties(typeof(T), _properties);
        
        _dbColumns = new List<BasicColumn>();
        GuessColumns(_properties, _dbColumns);
        
        storage.EnsurePagedTable(_name, _dbColumns);
    }

    private void GuessColumns(Dictionary<string,PropertyInfo> properties, List<BasicColumn> dbColumns)
    {
        foreach (var kvp in properties)
        {
            dbColumns.Add(new BasicColumn(kvp.Key+"_data", GuessSqlType(kvp.Value.PropertyType))); // data storage
            dbColumns.Add(new BasicColumn(kvp.Key+"_page", "INT")); // 
        }
    }

    private static string GuessSqlType(Type type, bool recurse = false)
    {
        if (type.IsEnum) // cast enums to base type
        {
            if (recurse) throw new Exception($"Failed to find underlying type of enum {type.Name}");
            return GuessSqlType(Enum.GetUnderlyingType(type), true);
        }
        
        // See Postgres docs: https://www.postgresql.org/docs/current/datatype.html

        #region Numeric
        if (type == typeof(int)) return "INT";
        if (type == typeof(int?)) return "INT";
        if (type == typeof(long)) return "INT8";
        if (type == typeof(long?)) return "INT8";
        if (type == typeof(double)) return "FLOAT8";
        if (type == typeof(double?)) return "FLOAT8";
        if (type == typeof(decimal)) return "DECIMAL";
        if (type == typeof(decimal?)) return "DECIMAL";
        #endregion
        
        if (type == typeof(string)) return "TEXT";
        if (type == typeof(bool?)) return "BOOLEAN";
        if (type == typeof(bool)) return "BOOLEAN";
        
        if (type == typeof(DateTime)) return "TIMESTAMP";
        if (type == typeof(DateTime?)) return "TIMESTAMP";
        if (type == typeof(TimeSpan)) return "INTERVAL";
        if (type == typeof(TimeSpan?)) return "INTERVAL";
        
        // TODO: Could fall back to JSON type (and serialise this side) for arbitrary types
        
        throw new Exception($"No SQL type known for C# type {type.Name}");
    }

    private static void FindProperties(Type type, IDictionary<string, PropertyInfo> properties)
    {
        var props = type.GetProperties();

        foreach (var propertyInfo in props)
        {
            if ( ! propertyInfo.CanRead) continue; // ignore write-only. We will include read-only.
            
            properties.Add(propertyInfo.Name, propertyInfo);
        }
    }
}