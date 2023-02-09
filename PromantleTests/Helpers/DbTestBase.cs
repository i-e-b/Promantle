using NUnit.Framework;

namespace PromantleTests.Helpers;

[SingleThreaded]
public class DbTestBase
{
    private InMemCockroachDb? _cockroachInstance;
    private InMemPostgresDb? _postgresInstance;

    protected void ResetDatabase()
    {
        _cockroachInstance?.ResetDatabase();
        _postgresInstance?.ResetDatabase();
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
    
    
    private void StartPostgres()
    {
        _postgresInstance ??= new InMemPostgresDb();
    }
    
    private void StopPostgres()
    {
        _postgresInstance?.Dispose();
        _postgresInstance = null;
    }

    [OneTimeSetUp]
    public void Setup()
    {
        StartPostgres();
        StartCockroach();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
        StopCockroach();
        StopPostgres();
    }
}