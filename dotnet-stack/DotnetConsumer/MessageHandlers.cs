using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DotnetConsumer;

public class MessageHandlers
{
    private readonly SqlServerRepository _repository;
    private readonly IModel _channel;
    private readonly string _podId;

    public MessageHandlers(SqlServerRepository repository, IModel channel)
    {
        _repository = repository;
        _channel = channel;
        _podId = Environment.GetEnvironmentVariable("HOSTNAME") ?? "unknown-pod";
    }

    // Pattern 1: Point-to-Point (Default Queue)
    public void ProcessTicket(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var startTime = DateTime.Now;

        try
        {
            var data = JsonDocument.Parse(message);
            var ticketId = data.RootElement.GetProperty("ticket_id").GetString() ?? "UNKNOWN";
            var email = data.RootElement.TryGetProperty("email", out var emailProp)
                ? emailProp.GetString()
                : "unknown@test.com";
            var severity = data.RootElement.TryGetProperty("severity", out var sevProp)
                ? sevProp.GetString()
                : "n/a";

            // Log receipt with full metadata
            LogHelper.LogInfo(ticketId,
                $"▶ RECEIVED | path: DEFAULT exchange → routing_key='{e.RoutingKey}' | " +
                $"delivery_tag={e.DeliveryTag} | redelivered={e.Redelivered} | " +
                $"email='{email}' | severity='{severity}' | body={body.Length}B");

            // Simulate processing
            Thread.Sleep(Random.Shared.Next(1500, 3500));

            var duration = (DateTime.Now - startTime).TotalSeconds;

            // Save to database
            _repository.SaveTicket(data, _podId, duration).Wait();
            _repository.LogPatternEvent("point_to_point", message).Wait();

            // ACK
            _channel.BasicAck(e.DeliveryTag, false);

            LogHelper.LogInfo(ticketId,
                $"✔ ACK sent | ack_mode='manual_ack' | total_time={duration:F3}s | " +
                $"path: DEFAULT exchange → routing_key='{e.RoutingKey}'");
        }
        catch (Exception ex)
        {
            var ticketId = "PARSE-ERR";
            try
            {
                var data = JsonDocument.Parse(message);
                ticketId = data.RootElement.GetProperty("ticket_id").GetString() ?? "UNKNOWN";
            }
            catch { }

            LogHelper.LogError(ticketId, $"✘ Processing failed | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 2: Pub/Sub
    public void ProcessPubSub(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        try
        {
            var data = JsonDocument.Parse(message);
            var reqId = data.RootElement.TryGetProperty("request_id", out var reqProp)
                ? reqProp.GetString()
                : "UNKNOWN";

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"▶ BROADCAST RECEIVED | exchange='dotnet_incident_broadcast' | " +
                $"delivery_tag={e.DeliveryTag} | ALL .NET consumers got this!");

            _repository.LogPatternEvent("pub_sub", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);

            LogHelper.LogInfo(reqId ?? "UNKNOWN", "✔ ACK | Pub/Sub processed");
        }
        catch (Exception ex)
        {
            LogHelper.LogError("PUBSUB-ERR", $"✘ Error | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 3: Routing
    public void ProcessRouting(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var routingKey = e.RoutingKey;

        try
        {
            var data = JsonDocument.Parse(message);
            var reqId = data.RootElement.TryGetProperty("request_id", out var reqProp)
                ? reqProp.GetString()
                : "UNKNOWN";

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"▶ ROUTING RECEIVED | exchange='dotnet_incident_routing' | " +
                $"routing_key='{routingKey}' | delivery_tag={e.DeliveryTag}");

            _repository.LogPatternEvent($"routing_{routingKey}", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"✔ ACK | Routed message: severity='{routingKey}'");
        }
        catch (Exception ex)
        {
            LogHelper.LogError("ROUTING-ERR", $"✘ Error | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 4: Topic
    public void ProcessTopic(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var routingKey = e.RoutingKey;

        try
        {
            var data = JsonDocument.Parse(message);
            var reqId = data.RootElement.TryGetProperty("request_id", out var reqProp)
                ? reqProp.GetString()
                : "UNKNOWN";

            // Determine which patterns matched
            var parts = routingKey.Split('.');
            var matched = new List<string>();
            
            if (routingKey.StartsWith("incident.") || routingKey == "incident")
                matched.Add("incident.#");
            if (parts.Length == 3 && parts[1] == "critical")
                matched.Add("*.critical.*");
            if (parts.Length == 3 && parts[0] == "incident" && parts[2] == "network")
                matched.Add("incident.*.network");

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"▶ TOPIC RECEIVED | exchange='dotnet_incident_topic' | " +
                $"routing_key='{routingKey}' | matched_bindings=[{string.Join(", ", matched)}] | " +
                $"delivery_tag={e.DeliveryTag}");

            _repository.LogPatternEvent($"topic_{routingKey}", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"✔ ACK | topic key='{routingKey}' matched {matched.Count} binding(s): [{string.Join(", ", matched)}]");
        }
        catch (Exception ex)
        {
            LogHelper.LogError("TOPIC-ERR", $"✘ Error | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 5: DLQ Main Queue
    public void ProcessDLQMain(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        try
        {
            var data = JsonDocument.Parse(message);
            var reqId = data.RootElement.TryGetProperty("request_id", out var reqProp)
                ? reqProp.GetString()
                : "UNKNOWN";
            var forceFail = data.RootElement.TryGetProperty("force_fail", out var failProp) 
                && failProp.GetBoolean();

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"▶ DLQ-MAIN RECEIVED | queue='dotnet_dlq_main_queue' | " +
                $"force_fail={forceFail} | delivery_tag={e.DeliveryTag}");

            if (forceFail)
            {
                LogHelper.LogWarn(reqId ?? "UNKNOWN",
                    "⚠ DLQ force_fail=True → NACK + requeue=False | " +
                    "RabbitMQ will route this to dotnet_dlq_dead_exchange → dotnet_dlq_dead_queue");
                _channel.BasicNack(e.DeliveryTag, false, false);
            }
            else
            {
                _repository.LogPatternEvent("dlq_success", message).Wait();
                _channel.BasicAck(e.DeliveryTag, false);
                LogHelper.LogInfo(reqId ?? "UNKNOWN", "✔ ACK | DLQ message processed successfully");
            }
        }
        catch (Exception ex)
        {
            LogHelper.LogError("DLQ-MAIN-ERR", $"✘ Error | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 5: DLQ Dead Queue
    public void ProcessDLQDead(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        try
        {
            var data = JsonDocument.Parse(message);
            var reqId = data.RootElement.TryGetProperty("request_id", out var reqProp)
                ? reqProp.GetString()
                : "UNKNOWN";

            LogHelper.LogWarn(reqId ?? "UNKNOWN",
                $"▶ DLQ-DEAD RECEIVED | queue='dotnet_dlq_dead_queue' | " +
                $"Message in dead letter queue - manual review needed | " +
                $"delivery_tag={e.DeliveryTag}");

            _repository.LogPatternEvent("dlq_dead_letter", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);

            LogHelper.LogInfo(reqId ?? "UNKNOWN", "✔ ACK | Dead letter logged");
        }
        catch (Exception ex)
        {
            LogHelper.LogError("DLQ-DEAD-ERR", $"✘ Error | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 7: Priority Queue
    public void ProcessPriority(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var priority = e.BasicProperties.Priority;

        try
        {
            var data = JsonDocument.Parse(message);
            var reqId = data.RootElement.TryGetProperty("request_id", out var reqProp)
                ? reqProp.GetString()
                : "UNKNOWN";

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"▶ PRIORITY RECEIVED | queue='dotnet_priority_queue' | " +
                $"priority={priority} | delivery_tag={e.DeliveryTag}");

            _repository.LogPatternEvent($"priority_{priority}", message).Wait();
            _channel.BasicAck(e.DeliveryTag, false);

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"✔ ACK | Priority {priority} message processed (higher priority processed first)");
        }
        catch (Exception ex)
        {
            LogHelper.LogError("PRIORITY-ERR", $"✘ Error | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }

    // Pattern 8: ACK Modes Test
    public void ProcessAckTest(object? sender, BasicDeliverEventArgs e)
    {
        var body = e.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        try
        {
            var data = JsonDocument.Parse(message);
            var reqId = data.RootElement.TryGetProperty("request_id", out var reqProp)
                ? reqProp.GetString()
                : "UNKNOWN";
            var ackMode = data.RootElement.TryGetProperty("ack_mode", out var modeProp)
                ? modeProp.GetString()
                : "manual_ack";

            LogHelper.LogInfo(reqId ?? "UNKNOWN",
                $"▶ ACK-TEST RECEIVED | ack_mode='{ackMode}' | " +
                $"delivery_tag={e.DeliveryTag}");

            switch (ackMode)
            {
                case "manual_ack":
                    _channel.BasicAck(e.DeliveryTag, false);
                    LogHelper.LogInfo(reqId ?? "UNKNOWN", "✔ ACK | Success → remove from queue");
                    break;

                case "manual_nack_requeue":
                    LogHelper.LogWarn(reqId ?? "UNKNOWN",
                        "⚠ ACK_MODE=manual_nack_requeue → NACK + requeue=True | " +
                        "message goes BACK to queue and will be retried");
                    _channel.BasicNack(e.DeliveryTag, false, true);
                    break;

                case "manual_nack_drop":
                    LogHelper.LogWarn(reqId ?? "UNKNOWN",
                        "⚠ ACK_MODE=manual_nack_drop → NACK + requeue=False | " +
                        "message discarded permanently");
                    _channel.BasicNack(e.DeliveryTag, false, false);
                    break;

                case "auto_ack_risk":
                    LogHelper.LogWarn(reqId ?? "UNKNOWN",
                        "⚠ ACK_MODE=auto_ack_risk → RabbitMQ already removed this message | " +
                        "if we crash NOW the message is LOST forever");
                    // No ACK needed - already auto-acked
                    break;

                default:
                    _channel.BasicAck(e.DeliveryTag, false);
                    LogHelper.LogInfo(reqId ?? "UNKNOWN", $"✔ ACK | Unknown mode '{ackMode}' - using manual_ack");
                    break;
            }

            _repository.LogPatternEvent($"ack_test_{ackMode}", message).Wait();
        }
        catch (Exception ex)
        {
            LogHelper.LogError("ACK-TEST-ERR", $"✘ Error | {ex.Message}");
            _channel.BasicNack(e.DeliveryTag, false, false);
        }
    }
}
