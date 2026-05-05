using Microsoft.Data.SqlClient;
using System.Text.Json;

namespace DotnetConsumer;

public class SqlServerRepository
{
    private readonly string _connectionString;

    public SqlServerRepository()
    {
        var server = Environment.GetEnvironmentVariable("SQL_SERVER") ?? "sqlserver-service";
        var database = Environment.GetEnvironmentVariable("SQL_DATABASE") ?? "IncidentDB";
        var user = Environment.GetEnvironmentVariable("SQL_USER") ?? "sa";
        var password = Environment.GetEnvironmentVariable("SQL_PASSWORD") ?? "YourStrong@Passw0rd";

        _connectionString = $"Server={server};Database={database};User Id={user};Password={password};TrustServerCertificate=True;";
    }

    public async Task SaveTicket(JsonDocument data)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            var ticketId = data.RootElement.GetProperty("ticket_id").GetString();
            var title = data.RootElement.GetProperty("title").GetString();
            var description = data.RootElement.GetProperty("description").GetString();
            var severity = data.RootElement.GetProperty("severity").GetString();

            var sql = @"
                INSERT INTO support_tickets (ticket_id, title, description, severity, status, created_at)
                VALUES (@ticket_id, @title, @description, @severity, 'open', GETUTCDATE())";

            using var cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@ticket_id", ticketId);
            cmd.Parameters.AddWithValue("@title", title ?? "");
            cmd.Parameters.AddWithValue("@description", description ?? "");
            cmd.Parameters.AddWithValue("@severity", severity ?? "medium");

            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"[SQL] Saved ticket: {ticketId}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SQL ERROR] {ex.Message}");
        }
    }

    public async Task LogPatternEvent(string pattern, string details)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO pattern_events (pattern_name, event_details, created_at)
                VALUES (@pattern, @details, GETUTCDATE())";

            using var cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@pattern", pattern);
            cmd.Parameters.AddWithValue("@details", details);

            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine($"[SQL] Logged pattern: {pattern}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SQL ERROR] {ex.Message}");
        }
    }

    public async Task InitializeDatabase()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // Create tables if they don't exist
            var sql = @"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'support_tickets')
                CREATE TABLE support_tickets (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    ticket_id NVARCHAR(50) UNIQUE,
                    title NVARCHAR(200),
                    description NVARCHAR(MAX),
                    severity NVARCHAR(20),
                    status NVARCHAR(20),
                    created_at DATETIME2,
                    updated_at DATETIME2
                );

                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pattern_events')
                CREATE TABLE pattern_events (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    pattern_name NVARCHAR(50),
                    event_details NVARCHAR(MAX),
                    created_at DATETIME2
                );
            ";

            using var cmd = new SqlCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync();

            Console.WriteLine("[SQL] Database initialized");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SQL INIT ERROR] {ex.Message}");
        }
    }
}
