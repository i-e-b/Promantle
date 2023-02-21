namespace Promantle;

/// <summary>
/// SQL column name and matching SQL type
/// </summary>
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