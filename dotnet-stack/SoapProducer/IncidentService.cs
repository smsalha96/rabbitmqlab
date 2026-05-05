using RabbitMQ.Client;
using SoapProducer.Models;
using System.Text;
using System.Text.Json;

namespace SoapProducer.Services;

public class IncidentService : IIncidentService
{
    private readonly IConnection _connection;
    private readonly ILogger<IncidentService> _logger;

    public IncidentService(ILogger<IncidentService> logger)
    {
        _logger = logger;
        
        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "ha-rabbitmq",
            Port = 5672,
            UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "admin",
            Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "labpassword123"
        };

        _connection = factory.CreateConnection();
    }

    // Pattern 1: Point-to-Point
    public async Task<IncidentResponse> CreateTicket(CreateTicketRequest request)
    {
        using var channel = _connection.CreateModel();
        
        channel.QueueDeclare(
            queue: "dotnet_incident_queue",
            durable: true,
            exclusive: false,
            autoDelete: false
        );

        var ticketId = Guid.NewGuid().ToString();
        var message = new
        {
            ticket_id = ticketId,
            title = request.Title,
            description = request.Description,
            severity = request.Severity,
            created_at = DateTime.UtcNow,
            pattern = "point_to_point"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        var properties = channel.CreateBasicProperties();
        properties.Persistent = true;

        channel.BasicPublish(
            exchange: "",
            routingKey: "dotnet_incident_queue",
            basicProperties: properties,
            body: body
        );

        _logger.LogInformation($"[.NET] Created ticket: {ticketId}");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = "Ticket created successfully",
            TicketId = ticketId,
            Timestamp = DateTime.UtcNow
        });
    }

    // Pattern 2: Pub/Sub
    public async Task<IncidentResponse> TestPubSub(TestMessageRequest request)
    {
        using var channel = _connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: "dotnet_incident_broadcast",
            type: ExchangeType.Fanout,
            durable: true
        );

        var message = new
        {
            message = request.Message,
            timestamp = DateTime.UtcNow,
            pattern = "pub_sub"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        channel.BasicPublish(
            exchange: "dotnet_incident_broadcast",
            routingKey: "",
            basicProperties: null,
            body: body
        );

        _logger.LogInformation($"[.NET] Broadcast message: {request.Message}");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = "Message broadcast to all consumers",
            Timestamp = DateTime.UtcNow
        });
    }

    // Pattern 3: Routing
    public async Task<IncidentResponse> TestRouting(string severity, TestMessageRequest request)
    {
        using var channel = _connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: "dotnet_incident_routing",
            type: ExchangeType.Direct,
            durable: true
        );

        var message = new
        {
            message = request.Message,
            severity = severity,
            timestamp = DateTime.UtcNow,
            pattern = "routing"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        channel.BasicPublish(
            exchange: "dotnet_incident_routing",
            routingKey: severity,
            basicProperties: null,
            body: body
        );

        _logger.LogInformation($"[.NET] Routed message with severity: {severity}");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = $"Message routed with severity: {severity}",
            Timestamp = DateTime.UtcNow
        });
    }

    // Pattern 4: Topic Exchange
    public async Task<IncidentResponse> TestTopic(string routingKey, TestMessageRequest request)
    {
        using var channel = _connection.CreateModel();

        channel.ExchangeDeclare(
            exchange: "dotnet_incident_topic",
            type: ExchangeType.Topic,
            durable: true
        );

        var message = new
        {
            message = request.Message,
            routing_key = routingKey,
            timestamp = DateTime.UtcNow,
            pattern = "topic"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        channel.BasicPublish(
            exchange: "dotnet_incident_topic",
            routingKey: routingKey,
            basicProperties: null,
            body: body
        );

        _logger.LogInformation($"[.NET] Topic message with key: {routingKey}");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = $"Message sent with routing key: {routingKey}",
            Timestamp = DateTime.UtcNow
        });
    }

    // Pattern 5: Dead Letter Queue
    public async Task<IncidentResponse> TestDLQ(bool forceFail, TestMessageRequest request)
    {
        using var channel = _connection.CreateModel();

        // Declare DLX
        channel.ExchangeDeclare("dotnet_dlq_dead_exchange", ExchangeType.Direct, true);
        channel.QueueDeclare("dotnet_dlq_dead_queue", true, false, false);
        channel.QueueBind("dotnet_dlq_dead_queue", "dotnet_dlq_dead_exchange", "");

        // Main queue with DLX
        var args = new Dictionary<string, object>
        {
            { "x-dead-letter-exchange", "dotnet_dlq_dead_exchange" }
        };

        channel.QueueDeclare("dotnet_dlq_main_queue", true, false, false, args);

        var message = new
        {
            message = request.Message,
            force_fail = forceFail,
            timestamp = DateTime.UtcNow,
            pattern = "dlq"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        channel.BasicPublish("", "dotnet_dlq_main_queue", null, body);

        _logger.LogInformation($"[.NET] DLQ test message (force_fail={forceFail})");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = "DLQ test message sent",
            Timestamp = DateTime.UtcNow
        });
    }

    // Pattern 6: TTL
    public async Task<IncidentResponse> TestTTL(int ttlSeconds, TestMessageRequest request)
    {
        using var channel = _connection.CreateModel();

        var args = new Dictionary<string, object>
        {
            { "x-dead-letter-exchange", "dotnet_dlq_dead_exchange" },
            { "x-message-ttl", ttlSeconds * 1000 }
        };

        channel.QueueDeclare("dotnet_ttl_queue", true, false, false, args);

        var message = new
        {
            message = request.Message,
            ttl_seconds = ttlSeconds,
            timestamp = DateTime.UtcNow,
            pattern = "ttl"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        channel.BasicPublish("", "dotnet_ttl_queue", null, body);

        _logger.LogInformation($"[.NET] TTL message ({ttlSeconds}s)");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = $"Message will expire in {ttlSeconds} seconds",
            Timestamp = DateTime.UtcNow
        });
    }

    // Pattern 7: Priority Queue
    public async Task<IncidentResponse> TestPriority(int priority, TestMessageRequest request)
    {
        using var channel = _connection.CreateModel();

        var args = new Dictionary<string, object>
        {
            { "x-max-priority", 10 }
        };

        channel.QueueDeclare("dotnet_priority_queue", true, false, false, args);

        var message = new
        {
            message = request.Message,
            priority = priority,
            timestamp = DateTime.UtcNow,
            pattern = "priority"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));
        var properties = channel.CreateBasicProperties();
        properties.Priority = (byte)priority;

        channel.BasicPublish("", "dotnet_priority_queue", properties, body);

        _logger.LogInformation($"[.NET] Priority message (priority={priority})");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = $"Message sent with priority: {priority}",
            Timestamp = DateTime.UtcNow
        });
    }

    // Pattern 8: ACK Modes
    public async Task<IncidentResponse> TestAckModes(string mode, TestMessageRequest request)
    {
        using var channel = _connection.CreateModel();

        channel.QueueDeclare("dotnet_ack_test_queue", true, false, false);

        var message = new
        {
            message = request.Message,
            ack_mode = mode,
            timestamp = DateTime.UtcNow,
            pattern = "ack_modes"
        };

        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        channel.BasicPublish("", "dotnet_ack_test_queue", null, body);

        _logger.LogInformation($"[.NET] ACK mode test: {mode}");

        return await Task.FromResult(new IncidentResponse
        {
            Success = true,
            Message = $"ACK mode test: {mode}",
            Timestamp = DateTime.UtcNow
        });
    }
}
