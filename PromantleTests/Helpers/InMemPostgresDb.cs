using System.Data;
using System.Diagnostics;
using Npgsql;

namespace PromantleTests.Helpers;

/// <summary>
/// Runs a temporary non persistent database instance.
/// NOTE: this is significantly slower to start-up than <see cref="InMemCockroachDb"/>
/// </summary>
public class InMemPostgresDb: PersistentProcessBase, IDisposable
{
    private readonly Process? _instance;
        
    /// <summary> Exe path => expected temp path </summary>
    /// <remarks>These need to be configured ahead of time. See comments in <see cref="FindPostgres"/></remarks>
    private readonly Dictionary<string,string> _possiblePaths = new()
    {
        {"/usr/lib/postgresql/12/bin/postgres",@"/tmp/pgtemp"},
        {"/usr/lib/postgresql/15/bin/postgres",@"/tmp/pgtemp"},
        {@"C:\pgsql\bin\postgres.exe", @"C:\temp\pgtemp"} // C:\pgsql\bin\postgres.exe -D "C:\temp\pgtemp"
    };

    // If you need to manage this on the command line, use this command:
    //     psql -p 54448 -h 127.0.0.1 -d postgres
    //
    private const string DirectConnectionString = "Server=127.0.0.1;Port=54448;Database=postgres;Include Error Detail=true;Enlist=false;No Reset On Close=true;Timeout=20;CommandTimeout=60;";

    public InMemPostgresDb():base("postgres")
    {
        NpgsqlConnection.ClearAllPools(); // we're going to get a new server, so all pools are invalid
        var sw = new Stopwatch();
        sw.Start();

        SignalPostgresToStop(); // in case other things
        KillAllProcesses();
            
        FindPostgres(out var exePath, out var dataPath);

        WriteToTerminal($"Bringing up postgres for test user={Environment.UserName}, command= {exePath} -D \"{dataPath}\"");
            
        _instance = new Process{
            StartInfo = new ProcessStartInfo(exePath, $"-D \"{dataPath}\""){
                RedirectStandardInput = true,
                RedirectStandardError = true,
                RedirectStandardOutput = true
            }
        };
            
        _instance.Start();
            
        Thread.Sleep(250); // give a little time for the process to get going
        _instance.StandardError.Peek(); // delay until postgres is nearly up
        WaitForPostgresToRespond(exePath, dataPath);

        try
        {
            ResetDatabase();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to reset Postgres: {ex}");
            _instance.Kill();
            Console.WriteLine(_instance?.StandardError.ReadToEnd()??"");
            Console.WriteLine(_instance?.StandardOutput.ReadToEnd()??"");
                
            WriteToTerminal("Failed to bring up postgres:");
            WriteToTerminal(_instance?.StandardError.ReadToEnd()??"");
            WriteToTerminal(_instance?.StandardOutput.ReadToEnd()??"");
            WriteToTerminal(";;");
            _instance = null;
            throw;
        }
            
        StartSession(_instance.Id);

        sw.Stop();
        Console.WriteLine($"Bringing up Postgres DB took {sw.Elapsed}");
    }

    /// <summary>
    /// Postgres can enter a state where it give "FATAL" errors that aren't really errors.
    /// Wait for those to stop.
    /// </summary>
    private void WaitForPostgresToRespond(string exePath, string dataPath)
    {
        while (_instance?.HasExited == false)
        {
            try
            {
                NpgsqlConnection.ClearAllPools(); // we're going to get a new server, so all pools are invalid
                using var conn = new NpgsqlConnection(DirectConnectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                    
                // CREATE ROLE IF NOT EXISTS...
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = "SELECT 1;";
                _ = cmd.ExecuteScalar()?.ToString();
                return;
            }
            catch (Exception ex)
            {
                if (ex.InnerException is System.Net.Sockets.SocketException)
                {
                    WriteToTerminal($"Postgres did not start up successfully, due to socket error. Try as user '{Environment.UserName}': [{exePath} -D \"{dataPath}\"]");
                    WriteToTerminal("Socket exception? " + ex);
                    return;
                }

                WriteToTerminal($"Failed, going to try again: {ex.Message}");
                Thread.Sleep(100);
            }
        }
        WriteToTerminal($"Postgres did not start up successfully? Try as user '{Environment.UserName}': [{exePath} -D \"{dataPath}\"]");
    }

        
    /// <summary>
    /// The complex, but platform independent way to shutdown a postgres server
    /// </summary>
    private void SignalPostgresToStop()
    {
        FindPostgres(out var exePath, out var dataPath);
            
        var ctlPath = exePath.Replace("bin\\postgres","bin\\pg_ctl").Replace("bin/postgres","bin/pg_ctl");
        //pg_ctl stop:  https://www.postgresql.org/docs/9.1/app-pg-ctl.html
        using var sigProc = new Process{ StartInfo = new ProcessStartInfo(ctlPath, $"stop -D \"{dataPath}\"") };
            
        sigProc.Start();
        var ok = sigProc.WaitForExit(1000);
        if (!ok) Console.WriteLine($"pg_ctl failed? [ {ctlPath} stop -D \"{dataPath}\" ]");
    }

    public void Dispose()
    {
        Console.WriteLine("Shutting down Postgres");
        if (_instance != null)
        {
            try
            {
                SignalPostgresToStop();

                var died = _instance.WaitForExit(2000);
                if (!died)
                {
                    Console.WriteLine("Postgres did not end gracefully. Killing.");
                    _instance.Kill();
                }

                Console.WriteLine("[start of postgres server messages]");
                Console.WriteLine(_instance.StandardError.ReadToEnd() ?? "");
                Console.WriteLine(_instance.StandardOutput.ReadToEnd() ?? "");
                Console.WriteLine("[end of postgres server messages]");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Shutdown error: {ex}");
            }
        }
        EndSession();

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Drop and recreate standard databases
    /// </summary>
    public void ResetDatabase()
    {
        NpgsqlConnection.ClearAllPools();
        // Standard pg_hba.conf allows all localhost connections, so we don't need a user or password.
        // We *do* need an initial DB, and there should be one called 'postgres' pre-created.
        // This gives us just enough to connect and set up the databases and users we use in the tests
        using var conn = new NpgsqlConnection(DirectConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
                    
        // CREATE ROLE IF NOT EXISTS...
        cmd.CommandType = CommandType.Text;
        cmd.CommandText = "SELECT rolname FROM pg_roles WHERE rolname='postgres';";
        var needRole = cmd.ExecuteScalar()?.ToString();
        if (needRole is null)
        {
            cmd.CommandText = "CREATE USER postgres WITH PASSWORD 'password';";
            cmd.ExecuteNonQuery();
        }

        // wipe and recreate basic DBs (note: pg_sleep() is a floating-point number of seconds)
        cmd.CommandText = @"
DROP SCHEMA public CASCADE;
DROP SCHEMA IF EXISTS triangles CASCADE;
DROP DATABASE IF EXISTS testdb;
";
        cmd.ExecuteNonQuery();

        cmd.CommandText = @"
CREATE SCHEMA public;
CREATE SCHEMA triangles;
";
        cmd.ExecuteNonQuery();
        
        
        cmd.CommandText = @"
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;

GRANT ALL ON SCHEMA triangles TO postgres;
GRANT ALL ON SCHEMA triangles TO public;

CREATE DATABASE testdb;";
        cmd.ExecuteNonQuery();
    }
        
    private void FindPostgres(out string exePath, out string dataPath)
    {
        foreach (var set in _possiblePaths)
        {
            if (!File.Exists(set.Key)) continue;
            exePath = set.Key;
            dataPath = set.Value;
            return;
        }
            
        throw new Exception("Did not find local postgres install." +
                            "To run these tests, read the instructions at https://pulse.ewater.services/playbook/Setuplocaldevunittestdbs ");
        /*
             
To install on Windows:
======================

Binary packages available from https://www.enterprisedb.com/download-postgresql-binaries

Choose version 12.11 to be compatible with installed services; otherwise choose latest.

Expand that archive, and move content so `C:\pgsql\bin\initdb.exe` and `C:\pgsql\bin\postgres.exe` exist.
Create a blank folder at `C:\temp\pgtemp`

Start a powershell session
```
cd C:\pgsql\bin
.\initdb.exe -D "C:\temp\pgtemp"
```

Edit the file at `C:\temp\pgtemp\postgresql.conf`
specifically, these settings:

```
port = 54448
wal_level = minimal        # DO NOT use on production servers
max_wal_senders = 0        # DO NOT use on production servers
fsync = off                # DO NOT use on production servers
synchronous_commit = off   # DO NOT use on production servers
full_page_writes = off     # DO NOT use on production servers
checkpoint_timeout = 25min
max_wal_size = 2GB
```

Then the postgres server can be run with
```
C:\pgsql\bin\postgres.exe -D "C:\temp\pgtemp"
```
and can be ended with `ctrl-C`


To install on Ubuntu
====================

```
apt install postgresql postgresql-contrib
```

             */
    }

}