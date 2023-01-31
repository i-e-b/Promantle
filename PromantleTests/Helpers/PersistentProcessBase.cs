using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using DiskQueue;

namespace PromantleTests.Helpers;

[SuppressMessage("ReSharper", "PropertyCanBeMadeInitOnly.Global")]
    internal class PlatformSetting
    {
        public int Timeout { get; set; }
        public string TempFile { get; set; } = "";
    }

public abstract class PersistentProcessBase
{
    private readonly string _queueName;
    private IPersistentQueue? _queue;

    protected PersistentProcessBase(string processName)
    {
        _queueName = Path.Combine(Path.GetTempPath(), $"pidQ_{processName}");
    }

    /// <summary>
    /// Register a process that you started, and lock access to running this process.
    /// Throws an exception if an instance is already running. This lock applies across all processes on the machine.
    /// </summary>
    protected void StartSession(int pid)
    {
        var data = new byte[4];
        data[0] = (byte)(pid >> 24 & 0xFF);
        data[1] = (byte)(pid >> 16 & 0xFF);
        data[2] = (byte)(pid >>  8 & 0xFF);
        data[3] = (byte)(pid       & 0xFF);
            
        _queue = PersistentQueue.WaitFor(_queueName, TimeSpan.FromSeconds(60));
        using (var session = _queue.OpenSession()) {
            session.Enqueue(data);
            session.Flush();
        }
    }

    protected void EndSession()
    {
        KillAllProcesses();
    }

    /// <summary>
    /// Try to end any processes we might have left around
    /// </summary>
    private void TryKill(int pid)
    {
        try
        {
            var proc = Process.GetProcessById(pid);
            if (proc.HasExited) return;
                    
            proc.Kill();
            var dead = proc.WaitForExit(20_000);
            if (!dead)
            {
                Console.WriteLine($"Process {pid} refused to die");
                return;
            }

            Console.WriteLine($"crdb {pid} was left over, but should now be killed.");
        }
        catch
        {
            Console.WriteLine($"crdb {pid} already gone?");
            TryUnixKill(pid);
        }
    }
        
    /// <summary>
    /// Last ditch method to kill process on build agent
    /// </summary>
    private static void TryUnixKill(int pid)
    {
        try
        {
            var proc = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "kill",
                    //Arguments = $"--timeout 2000 KILL --signal TERM {pid}" // send a request, then be forceful if not done in 2 sec. Ubuntu doesn't support this.
                    Arguments = $"-9 {pid}" // be very forceful
                }
            };
            proc.Start();
            var ok = proc.WaitForExit(2000);
            if (!ok)
            {
                proc.Kill();
                Console.WriteLine($"    Failed to kill process PID={pid}");
            }

            if (proc.ExitCode != 0) Console.WriteLine($"    Unexpected exit code {proc.ExitCode} for PID={pid}");
            else Console.WriteLine("    ...terminated");
        }
        catch
        {
            Console.WriteLine("unix process kill failed");
        }
    }

    /// <summary>
    /// Try to end any processes that are waiting on the queue
    /// </summary>
    protected void KillAllProcesses()
    {
        _queue ??= PersistentQueue.WaitFor(_queueName, TimeSpan.FromSeconds(60));
        using (var session = _queue.OpenSession()) {
            while (true)
            {
                var data = session.Dequeue();
                if (data is null) break;

                int pid = (data[0] << 24) + (data[1] << 16) + (data[2] << 8) + (data[3]);
                TryKill(pid);

                session.Flush();
            }
        }
        _queue?.Dispose();
        _queue = null;
    }
        

    /// <summary>
    /// Write directly to the console, skipping NUnit redirections
    /// </summary>
    protected static void WriteToTerminal(string msg)
    {
        // this is a trick to directly output to the console even when running in a test context
        using var stdout = Console.OpenStandardOutput();
        var bytes = Encoding.ASCII.GetBytes(msg);
        stdout.Write(bytes,0,bytes.Length);
        stdout.WriteByte(0x0D); //\r
        stdout.WriteByte(0x0A); //\n
        stdout.Flush();
    }
}