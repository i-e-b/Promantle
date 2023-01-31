using NUnit.Framework;

namespace PromantleTests.Helpers;

[SingleThreaded]
public class DbTestBase
{
    private InMemCockroachDb? _cockroachInstance;

    protected void ResetDatabase()
    {
        _cockroachInstance?.ResetDatabase();
    }

    private void StartCockroach()
    {
        _cockroachInstance ??= new InMemCockroachDb();
    }
    
    private void StopCockroach()
    {
        _cockroachInstance?.Dispose();
        _cockroachInstance = null;
    }

    [OneTimeSetUp]
    public void Setup()
    {
        StartCockroach();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        StopCockroach();
    }
}