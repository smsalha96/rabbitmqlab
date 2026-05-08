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
        
        // Get password from environment - NO hardcoded fallback for security
        var password = Environment.GetEnvironmentVariable("SQL_PASSWORD");
        if (string.IsNullOrEmpty(password))
        {
            throw new InvalidOperationException("SQL_PASSWORD environment variable is required");
        }

        _connectionString = $"Server={server};Database={database};User Id={user};Password={password};TrustServerCertificate=True;";
    }

    /// <summary>
    /// Save ticket to unified incidents table (matches Python schema)
    /// Uses ticket_id as request_id (primary identifier matching logs)
    /// </summary>
    public async Task SaveTicket(JsonDocument data, string podId, double durationSeconds)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // ticket_id is the request_id from logs - this is our primary identifier
            var ticketId = data.RootElement.GetProperty("ticket_id").GetString();
            if (string.IsNullOrEmpty(ticketId))
            {
                ticketId = Guid.NewGuid().ToString();
                LogHelper.LogWarn("SAVE", "ticket_id missing from message, generated new UUID");
            }

            var title = data.RootElement.TryGetProperty("title", out var titleProp) 
                ? titleProp.GetString() 
                : "";
            var description = data.RootElement.TryGetProperty("description", out var descProp)
                ? descProp.GetString()
                : "";
            var severity = data.RootElement.TryGetProperty("severity", out var sevProp)
                ? sevProp.GetString()
                : "medium";
            
            // Get email if present
            var email = data.RootElement.TryGetProperty("email", out var emailProp) 
                ? emailProp.GetString() 
                : "unknown@dotnet.internal";

            // Random category (matching Python behavior)
            var categories = new[] { "Hardware Issue", "Network Outage", "Software Bug", "Access Request", "Billing Inquiry" };
            var category = categories[Random.Shared.Next(categories.Length)];

            // Insert with request_id as primary identifier (NOT auto-increment id)
            var sql = @"
                IF NOT EXISTS (SELECT 1 FROM incidents WHERE request_id = @request_id)
                BEGIN
                    INSERT INTO incidents (
                        request_id,
                        email, 
                        description, 
                        category, 
                        severity,
                        pod_id, 
                        duration_seconds, 
                        source, 
                        created_at
                    )
                    VALUES (
                        @request_id,
                        @email, 
                        @description, 
                        @category,
                        @severity,
                        @pod_id, 
                        @duration, 
                        'dotnet', 
                        GETUTCDATE()
                    )
                END
                ELSE
                BEGIN
                    UPDATE incidents 
                    SET updated_at = GETUTCDATE()
                    WHERE request_id = @request_id
                END";

            using var cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@request_id", ticketId);
            cmd.Parameters.AddWithValue("@email", email ?? "");
            cmd.Parameters.AddWithValue("@description", description ?? "");
            cmd.Parameters.AddWithValue("@category", category);
            cmd.Parameters.AddWithValue("@severity", severity);
            cmd.Parameters.AddWithValue("@pod_id", podId);
            cmd.Parameters.AddWithValue("@duration", durationSeconds);

            await cmd.ExecuteNonQueryAsync();
            
            LogHelper.LogInfo(ticketId, 
                $"✔ Persisted → request_id={ticketId} category={category} duration={durationSeconds:F3}s");
        }
        catch (Exception ex)
        {
            LogHelper.LogError("DB-ERR", $"✘ SQL Server error | {ex.Message}");
            throw;
        }
    }

    public async Task InitializeDatabase()
    {
        var maxRetries = 5;
        var retryDelay = 2000; // 2 seconds

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                LogHelper.LogSystem($"Initializing database schema (attempt {attempt}/{maxRetries})...");

                // Create incidents table with request_id as primary identifier (matches logs)
                var createTableSql = @"
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'incidents' AND schema_id = SCHEMA_ID('dbo'))
                    BEGIN
                        CREATE TABLE incidents (
                            id INT IDENTITY(1,1),
                            request_id NVARCHAR(100) PRIMARY KEY,
                            email NVARCHAR(200),
                            description NVARCHAR(MAX),
                            category NVARCHAR(100),
                            severity NVARCHAR(20),
                            pod_id NVARCHAR(50),
                            duration_seconds FLOAT,
                            source NVARCHAR(20),
                            created_at DATETIME2 DEFAULT GETUTCDATE(),
                            updated_at DATETIME2
                        );
                        CREATE INDEX idx_incidents_created_at ON incidents(created_at DESC);
                        CREATE INDEX idx_incidents_source ON incidents(source);
                        PRINT 'Table incidents created successfully';
                    END
                    ELSE
                    BEGIN
                        PRINT 'Table incidents already exists';
                    END";

                using var cmd = new SqlCommand(createTableSql, connection);
                cmd.CommandTimeout = 30;
                await cmd.ExecuteNonQueryAsync();

                // Verify table exists
                var verifyTableSql = "SELECT COUNT(*) FROM sys.tables WHERE name = 'incidents'";
                using var verifyCmd = new SqlCommand(verifyTableSql, connection);
                var tableExists = (int)await verifyCmd.ExecuteScalarAsync() > 0;

                if (tableExists)
                {
                    LogHelper.LogSystem("✓ Database schema initialized (unified incidents table)");
                    return; // Success!
                }
                else
                {
                    throw new Exception("Table creation appeared to succeed but table does not exist");
                }
            }
            catch (SqlException ex) when (ex.Number == 4060 || ex.Number == 18456)
            {
                // Database does not exist or login failed
                LogHelper.LogError("DB-INIT", $"Database connection failed (attempt {attempt}/{maxRetries}): {ex.Message}");
                
                if (attempt < maxRetries)
                {
                    LogHelper.LogSystem($"Retrying in {retryDelay/1000} seconds...");
                    await Task.Delay(retryDelay);
                    retryDelay *= 2; // Exponential backoff
                }
                else
                {
                    LogHelper.LogError("DB-INIT", "❌ Failed to initialize database after all retries");
                    throw;
                }
            }
            catch (Exception ex)
            {
                LogHelper.LogError("DB-INIT", $"Database initialization failed (attempt {attempt}/{maxRetries}): {ex.Message}");
                
                if (attempt < maxRetries)
                {
                    LogHelper.LogSystem($"Retrying in {retryDelay/1000} seconds...");
                    await Task.Delay(retryDelay);
                }
                else
                {
                    LogHelper.LogError("DB-INIT", "❌ Failed to initialize database after all retries");
                    throw;
                }
            }
        }
    }

    public async Task LogPatternEvent(string pattern, string details)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // Create pattern_events table if it doesn't exist
            var createTableSql = @"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pattern_events')
                BEGIN
                    CREATE TABLE pattern_events (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        pattern_name NVARCHAR(50),
                        event_details NVARCHAR(MAX),
                        created_at DATETIME2 DEFAULT GETUTCDATE()
                    );
                END";

            using var createCmd = new SqlCommand(createTableSql, connection);
            await createCmd.ExecuteNonQueryAsync();

            // Insert pattern event
            var sql = @"
                INSERT INTO pattern_events (pattern_name, event_details, created_at)
                VALUES (@pattern, @details, GETUTCDATE())";

            using var cmd = new SqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@pattern", pattern);
            cmd.Parameters.AddWithValue("@details", details);

            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            LogHelper.LogError("PATTERN-LOG", $"Pattern logging failed: {ex.Message}");
        }
    }
}