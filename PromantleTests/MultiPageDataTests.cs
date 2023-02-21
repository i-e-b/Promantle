using System.Diagnostics.CodeAnalysis;
using NUnit.Framework;
using Promantle;
using PromantleTests.Helpers;

// ReSharper disable AssignNullToNotNullAttribute
// ReSharper disable RedundantTypeArgumentsOfMethod
#pragma warning disable CS8602

namespace PromantleTests;

#pragma warning disable CS8602
[TestFixture, SingleThreaded]
public class MultiPageDataTests : DbTestBase
{
    
    [Test]
    public void can_create_a_multi_pager_with_a_type_and_page_size()
    {
        ResetDatabase();
        var storage = new DatabaseConnection(InMemCockroachDb.LastValidSqlPort);

        var subject = new MultiPager<TestPagedType>("pagerTest1", storage, pageSize: 10);

        Assert.That(subject, Is.Not.Null);
    }

}

/// <summary>
/// The multi-pager pages over all public properties on the type with a getter.
/// </summary>
[SuppressMessage("ReSharper", "UnusedMember.Global")]
[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
public class TestPagedType
{
    public int AssetId { get; set; }
    public string Name { get; set; }="";
    public string Location { get; set; }="";
    public string ItemType { get; set; }="";
    public DateTime AcquiredDate { get; set; }
}