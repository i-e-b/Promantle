namespace Promantle;

/// <summary>
/// Basic adaptor for storing and querying pre-paged and sorted data
/// </summary>
public interface IMultiPageTableAdaptor
{
    /// <summary>
    /// Make sure multi-paged table exists. Returns <c>true</c> if it needed to be created
    /// </summary>
    bool EnsurePagedTable(string groupName, List<BasicColumn> dbColumns);
}