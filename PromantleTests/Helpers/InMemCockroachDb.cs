using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Npgsql;

namespace PromantleTests.Helpers;

/// <summary>
/// Runs a temporary non persistent database instance.
/// This class is a complex mess to cope with both Windows and Linux and running in a test harness.
/// </summary>
public class InMemCockroachDb: PersistentProcessBase, IDisposable
{
    private int _thisPid;
    private Process? _instance;
    private string? _tempFile;

    public static int LastValidSqlPort { get; private set; }
        
    private readonly Dictionary<string,PlatformSetting> _possiblePaths = new()
    {
        {"/usr/local/bin/cockroach", new PlatformSetting{Timeout = 500, TempFile = "/home/gitlab-runner/crdb_temp"}},
        {@"C:\cockroach-windows\cockroach.exe", new PlatformSetting{Timeout=25, TempFile = GetRandomPath()}}
    };

    public InMemCockroachDb():base("cockroach")
    {
        NpgsqlConnection.ClearAllPools(); // we're going to get a new server, so all pools are invalid
        var sw = new Stopwatch();
        sw.Start();

        KillAllProcesses(); // kill off any stray processes
            
        var ok = StartCrdbExe();
        if (!ok) throw new Exception("Could not bring up crdb");
            
        StartSession(_thisPid); // lock the session

        sw.Stop();
        WriteToTerminal($"Cockroach up pid={_thisPid}; port={LastValidSqlPort};");
        Console.WriteLine($"Bringing up Cockroach DB took {sw.Elapsed}");
    }

    public void Dispose()
    {
        Console.WriteLine("Shutting down CRDB");
            
        if (StopCrdb()) return;

        EndSession(); // unlock session, allowing next to proceed
            
        if (_tempFile != null && File.Exists(_tempFile)) File.Delete(_tempFile);

        WriteToTerminal($"Cockroach down  pid={_thisPid}; port={LastValidSqlPort};");
    }

    private bool StopCrdb()
    {
        if (_instance is null)
        {
            Console.WriteLine("No crdb instance found during shutdown");
            return true;
        }

        try
        {
            _instance.StandardInput.WriteLine("\\q\n");
            var ended = _instance?.WaitForExit(1500) ?? false;
            if (!ended)
            {
                Console.WriteLine("Crdb did not end promptly!");
                WriteToTerminal($"Crdb did not end correctly pid={_thisPid}; port={LastValidSqlPort};");
                _instance?.Kill(true);
            }
            else
            {
                Console.WriteLine("Crdb ended gracefully");
            }


            Console.WriteLine("[start of CRDB server messages]");
            Console.WriteLine(_instance?.StandardError.ReadToEnd() ?? "");
            Console.WriteLine(_instance?.StandardOutput.ReadToEnd() ?? "");
            Console.WriteLine("[end of CRDB server messages]");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Shutdown error: {ex}");
        }
        finally
        {
            Console.WriteLine("CRDB shutdown complete");
        }

        return false;
    }

    private bool StartCrdbExe()
    {
        try
        {
            var crdbPath = FindCockroach(out var settings);
            FreeTcpPorts(out var sqlPort, out var httpPort);
            var arguments = $"demo --disable-demo-license --no-example-database --sql-port {sqlPort} --http-port {httpPort} --echo-sql --listening-url-file \"{settings.TempFile}\"";

            if (File.Exists(settings.TempFile)) File.Delete(settings.TempFile);

            Console.WriteLine($"Bringing up crdb for test: [ {arguments} ]");
            _instance = new Process
            {
                StartInfo = new ProcessStartInfo(crdbPath, arguments)
                {
                    RedirectStandardInput = true,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true
                }
            };

            _instance.Start();

            _thisPid = _instance.Id;
                
            WaitForCockroachUp(settings.TempFile);
            _tempFile = settings.TempFile;

            // ReSharper disable once CommentTypo
            // "\unset errexit" should stop the process ending when a SQL error occurs (a feature of demo mode) 
            _instance.StandardInput.WriteLine(@"
\unset errexit
CREATE ROLE IF NOT EXISTS unit WITH LOGIN PASSWORD 'test';
GRANT root TO unit;

CREATE DATABASE IF NOT EXISTS testDb;");

            _instance.StandardError.Peek(); // wait until something comes out of the app
            var died = _instance.WaitForExit(settings.Timeout); // wake up time -- should not be needed, but Linux build is being tricky
            if (died)
            {
                throw new Exception($"crdb instance died AFTER sending commands, with exit code {_instance.ExitCode}");
            }
                
            LastValidSqlPort = sqlPort;
            return true;
        }
        catch (Exception ex)
        {
            if (_instance?.HasExited == false) _instance?.Kill();
            Console.WriteLine(_instance?.StandardError.ReadToEnd() ?? "");
            Console.WriteLine(_instance?.StandardOutput.ReadToEnd() ?? "");
            Console.WriteLine($"Failed to start crdb: {ex}");
            _instance = null;
            return false;
        }
    }

    private void WaitForCockroachUp(string markerFile)
    {
        var sw = new Stopwatch();
        sw.Start();
        while ( ! File.Exists(markerFile))
        {
            if (sw.Elapsed.TotalSeconds > 15) throw new Exception("Cockroach never came up?");
            Thread.Sleep(100);
        }
    }

    /// <summary>
    /// Get a system-assigned port we can use for connections.
    /// If we don't supply a http port to crdb, it will try to use 8080, which is very likely occupied.
    /// Crdb will *refuse* to start without a valid HTTP port.
    /// </summary>
    private static void FreeTcpPorts(out int sql, out int http)
    {
        // Ubuntu can get stuck on 127.0.0.1:54448. Not sure why.
        // So we iterate random high ports to find a free pair.

        Exception? cause;
        try
        {
            var tcp1 = new TcpListener(IPAddress.Loopback, 0); // zero should cause the OS to give us a random free port
            var tcp2 = new TcpListener(IPAddress.Loopback, 0);
            tcp1.Start();
            tcp2.Start();
                
            http = ((IPEndPoint)tcp2.LocalEndpoint).Port;
            sql = ((IPEndPoint)tcp1.LocalEndpoint).Port;
                
            tcp2.Stop();
            tcp1.Stop();
            return;
        }
        catch (Exception ex)
        {
            cause = ex;
            Console.WriteLine("level=dbg " + ex);
        }
        throw new Exception("Could not find a free port pair for CockroachDB to run on", cause);
    }

    private static string GetRandomPath()
    {
        return Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    }
        

    private string FindCockroach(out PlatformSetting settings)
    {
        settings = new PlatformSetting();
        foreach (var path in _possiblePaths)
        {
            if (File.Exists(path.Key))
            {
                settings = path.Value;
                return path.Key;
            }
        }
        throw new Exception("Did not find local cockroach install." +
                            "To run these tests, get a package from" +
                            " ( https://www.cockroachlabs.com/docs/releases/index.html )" +
                            $" and install to one of these paths: {string.Join(", ", _possiblePaths.Keys)}");
    }

    /// <summary>
    /// Destroy and recreate standard databases
    /// </summary>
    public void ResetDatabase()
    {
        using var conn = new NpgsqlConnection(@$"Server=127.0.0.1;Database=defaultdb;User Id=unit;Password=test;Port={LastValidSqlPort};Include Error Detail=true;Timeout=3;");
        conn.Open();
        using var cmd = conn.CreateCommand();

        // wipe and recreate basic DBs
        cmd.CommandText = @"DROP SCHEMA IF EXISTS triangles CASCADE;";
        cmd.ExecuteNonQuery();
        
        cmd.CommandText = @"CREATE SCHEMA IF NOT EXISTS triangles;";
        cmd.ExecuteNonQuery();
    }
}