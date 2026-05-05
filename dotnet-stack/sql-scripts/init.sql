-- Create database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'IncidentDB')
BEGIN
    CREATE DATABASE IncidentDB;
END
GO

USE IncidentDB;
GO

-- Support Tickets Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'support_tickets')
BEGIN
    CREATE TABLE support_tickets (
        id INT IDENTITY(1,1) PRIMARY KEY,
        ticket_id NVARCHAR(50) UNIQUE NOT NULL,
        title NVARCHAR(200),
        description NVARCHAR(MAX),
        severity NVARCHAR(20),
        status NVARCHAR(20),
        created_at DATETIME2 DEFAULT GETUTCDATE(),
        updated_at DATETIME2 DEFAULT GETUTCDATE()
    );

    CREATE INDEX idx_ticket_id ON support_tickets(ticket_id);
    CREATE INDEX idx_severity ON support_tickets(severity);
    CREATE INDEX idx_created_at ON support_tickets(created_at);
END
GO

-- Pattern Events Table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'pattern_events')
BEGIN
    CREATE TABLE pattern_events (
        id INT IDENTITY(1,1) PRIMARY KEY,
        pattern_name NVARCHAR(50) NOT NULL,
        event_details NVARCHAR(MAX),
        created_at DATETIME2 DEFAULT GETUTCDATE()
    );

    CREATE INDEX idx_pattern_name ON pattern_events(pattern_name);
    CREATE INDEX idx_created_at ON pattern_events(created_at);
END
GO

-- Stored Procedure: Get tickets by severity
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_GetTicketsBySeverity')
    DROP PROCEDURE sp_GetTicketsBySeverity;
GO

CREATE PROCEDURE sp_GetTicketsBySeverity
    @Severity NVARCHAR(20)
AS
BEGIN
    SELECT 
        ticket_id,
        title,
        description,
        severity,
        status,
        created_at
    FROM support_tickets
    WHERE severity = @Severity
    ORDER BY created_at DESC;
END
GO

-- Stored Procedure: Get pattern statistics
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_GetPatternStats')
    DROP PROCEDURE sp_GetPatternStats;
GO

CREATE PROCEDURE sp_GetPatternStats
AS
BEGIN
    SELECT 
        pattern_name,
        COUNT(*) as event_count,
        MAX(created_at) as last_event
    FROM pattern_events
    GROUP BY pattern_name
    ORDER BY event_count DESC;
END
GO

PRINT 'Database initialized successfully';
