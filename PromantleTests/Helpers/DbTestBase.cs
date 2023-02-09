#define UseCockroach

using NUnit.Framework;

namespace PromantleTests.Helpers;

[SingleThreaded]
public class DbTestBase
{
#if UseCockroach
    private InMemCockroachDb? _cockroachInstance;
#else
    private InMemPostgresDb? _postgresInstance;
#endif

    protected void ResetDatabase()
    {
#if UseCockroach
        _cockroachInstance?.ResetDatabase();
#else
        _postgresInstance?.ResetDatabase();
#endif
    }

#if UseCockroach
    private void StartCockroach()
    {
        _cockroachInstance ??= new InMemCockroachDb();
    }
    
    private void StopCockroach()
    {
        _cockroachInstance?.Dispose();
        _cockroachInstance = null;
    }
    
#else
    
    private void StartPostgres()
    {
        _postgresInstance ??= new InMemPostgresDb();
    }
    
    private void StopPostgres()
    {
        _postgresInstance?.Dispose();
        _postgresInstance = null;
    }
#endif

    [OneTimeSetUp]
    public void Setup()
    {
#if UseCockroach
        StartCockroach();
#else
        StartPostgres();
#endif
    }

    [OneTimeTearDown]
    public void OneTimeTearDown()
    {
#if UseCockroach
        StopCockroach();
#else
        StopPostgres();
#endif
    }
}