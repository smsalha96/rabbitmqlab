using System;

namespace DotnetConsumer;

/// <summary>
/// Standardized logging helper to match Python logging format
/// </summary>
public static class LogHelper
{
    private static string GetPodId() => 
        Environment.GetEnvironmentVariable("HOSTNAME") ?? "unknown-pod";

    public static void LogInfo(string reqId, string message)
    {
        var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        var podId = GetPodId();
        Console.WriteLine($"[{ts}] [INFO ] [POD:{podId}] [REQ:{reqId}] {message}");
    }

    public static void LogWarn(string reqId, string message)
    {
        var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        var podId = GetPodId();
        Console.WriteLine($"[{ts}] [WARN ] [POD:{podId}] [REQ:{reqId}] {message}");
    }

    public static void LogError(string reqId, string message)
    {
        var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        var podId = GetPodId();
        Console.WriteLine($"[{ts}] [ERROR] [POD:{podId}] [REQ:{reqId}] {message}");
    }

    public static void LogSystem(string message)
    {
        var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        var podId = GetPodId();
        Console.WriteLine($"[{ts}] [SYS  ] [POD:{podId}] {message}");
    }
}
