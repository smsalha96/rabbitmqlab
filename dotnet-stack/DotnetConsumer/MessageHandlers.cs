using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DotnetConsumer;

public class MessageHandlers
{
    private readonly SqlServerRepository _repository;
    private readonly IModel _channel;

    public MessageHandlers(SqlServerRepository repository, IModel channel)
    {
        _repository = repository;
        _channel = channel;
    }

    // Pattern 1: Point-to-Point (Default Queue)
    public void ProcessTicket(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        
        Console.WriteLine($"[.NET Consumer] Processing ticket: {message}");

        try
        {
            var data = JsonDocument.Parse(message);
            _repository.SaveTicket(data).Wait();
            _repository.LogPatternEvent("point_to_point", message).Wait();

            _channel.BasicAck(e.DeliveryTag, false);
            Console.WriteLine($"[.NET] ✓ Ticket processed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 2: Pub/Sub
    public void ProcessPubSub(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        
        Console.WriteLine($"[.NET Consumer] Pub/Sub message: {message}");

        try
        {
            _repository.LogPatternEvent("pub_sub", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);
            Console.WriteLine($"[.NET] ✓ Pub/Sub processed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 3: Routing
    public void ProcessRouting(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var routingKey = e.RoutingKey;
        
        Console.WriteLine($"[.NET Consumer] Routing message [{routingKey}]: {message}");

        try
        {
            _repository.LogPatternEvent($"routing_{routingKey}", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);
            Console.WriteLine($"[.NET] ✓ Routing processed: {routingKey}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 4: Topic
    public void ProcessTopic(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var routingKey = e.RoutingKey;
        
        Console.WriteLine($"[.NET Consumer] Topic message [{routingKey}]: {message}");

        try
        {
            _repository.LogPatternEvent($"topic_{routingKey}", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);
            Console.WriteLine($"[.NET] ✓ Topic processed: {routingKey}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 5: DLQ Main Queue
    public void ProcessDLQMain(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        
        Console.WriteLine($"[.NET Consumer] DLQ main: {message}");

        try
        {
            var data = JsonDocument.Parse(message);
            var forceFail = data.RootElement.GetProperty("force_fail").GetBoolean();

            if (forceFail)
            {
                Console.WriteLine($"[.NET] ✗ Forcing NACK - message will go to DLQ");
                _channel.BasicNack(e.DeliveryTag, false, false);
            }
            else
            {
                _repository.LogPatternEvent("dlq_success", message).Wait();
                _channel.BasicAck(e.DeliveryTag, false);
                Console.WriteLine($"[.NET] ✓ DLQ message processed successfully");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 5: DLQ Dead Queue
    public void ProcessDLQDead(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        
        Console.WriteLine($"[.NET Consumer] DLQ dead queue: {message}");

        try
        {
            _repository.LogPatternEvent("dlq_dead_letter", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);
            Console.WriteLine($"[.NET] ✓ Dead letter processed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
        }
    }

    // Pattern 7: Priority Queue
    public void ProcessPriority(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var priority = e.BasicProperties.Priority;
        
        Console.WriteLine($"[.NET Consumer] Priority message [p={priority}]: {message}");

        try
        {
            _repository.LogPatternEvent($"priority_{priority}", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);
            Console.WriteLine($"[.NET] ✓ Priority processed: {priority}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 8: ACK Modes Test
    public void ProcessAckTest(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        
        Console.WriteLine($"[.NET Consumer] ACK test: {message}");

        try
        {
            var data = JsonDocument.Parse(message);
            var mode = data.RootElement.GetProperty("ack_mode").GetString();

            _repository.LogPatternEvent($"ack_mode_{mode}", message).Wait();

            switch (mode)
            {
                case "manual_ack":
                    Console.WriteLine("[.NET] Using manual ACK");
                    _channel.BasicAck(e.DeliveryTag, false);
                    break;

                case "manual_nack_requeue":
                    Console.WriteLine("[.NET] Using NACK with requeue");
                    _channel.BasicNack(e.DeliveryTag, false, true);
                    break;

                case "manual_nack_drop":
                    Console.WriteLine("[.NET] Using NACK without requeue");
                    _channel.BasicNack(e.DeliveryTag, false, false);
                    break;

                case "auto_ack_risk":
                    Console.WriteLine("[.NET] Auto-ACK (risky - already removed from queue)");
                    // No ACK needed - autoAck=true in consumer
                    break;

                default:
                    _channel.BasicAck(e.DeliveryTag, false);
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[.NET ERROR] {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }
}
