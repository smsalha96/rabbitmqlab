using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using DotnetConsumer;

Console.WriteLine("[.NET Consumer] Starting...");

var repository = new SqlServerRepository();
await repository.InitializeDatabase();

var factory = new ConnectionFactory
{
    HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "ha-rabbitmq",
    Port = 5672,
    UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "admin",
    Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "labpassword123"
};

var connection = factory.CreateConnection();
var channel = connection.CreateModel();

var handlers = new MessageHandlers(repository, channel);

// Pattern 1: Point-to-Point
channel.QueueDeclare("dotnet_incident_queue", durable: true, exclusive: false, autoDelete: false);
var consumer1 = new EventingBasicConsumer(channel);
consumer1.Received += handlers.ProcessTicket;
channel.BasicConsume("dotnet_incident_queue", autoAck: false, consumer: consumer1);
Console.WriteLine("[.NET] Listening on: dotnet_incident_queue");

// Pattern 2: Pub/Sub
channel.ExchangeDeclare("dotnet_incident_broadcast", ExchangeType.Fanout, durable: true);
var pubsubQueue = channel.QueueDeclare().QueueName;
channel.QueueBind(pubsubQueue, "dotnet_incident_broadcast", "");
var consumer2 = new EventingBasicConsumer(channel);
consumer2.Received += handlers.ProcessPubSub;
channel.BasicConsume(pubsubQueue, autoAck: false, consumer: consumer2);
Console.WriteLine("[.NET] Listening on: dotnet_incident_broadcast (pub/sub)");

// Pattern 3: Routing
channel.ExchangeDeclare("dotnet_incident_routing", ExchangeType.Direct, durable: true);
channel.QueueDeclare("dotnet_incident_queue", durable: true, exclusive: false, autoDelete: false);
channel.QueueBind("dotnet_incident_queue", "dotnet_incident_routing", "critical");
channel.QueueBind("dotnet_incident_queue", "dotnet_incident_routing", "high");
var consumer3 = new EventingBasicConsumer(channel);
consumer3.Received += handlers.ProcessRouting;
channel.BasicConsume("dotnet_incident_queue", autoAck: false, consumer: consumer3);
Console.WriteLine("[.NET] Bound routing keys: critical, high");

// Pattern 4: Topic
channel.ExchangeDeclare("dotnet_incident_topic", ExchangeType.Topic, durable: true);
var topicQueue = channel.QueueDeclare().QueueName;
channel.QueueBind(topicQueue, "dotnet_incident_topic", "incident.#");
channel.QueueBind(topicQueue, "dotnet_incident_topic", "*.critical.*");
channel.QueueBind(topicQueue, "dotnet_incident_topic", "incident.*.network");
var consumer4 = new EventingBasicConsumer(channel);
consumer4.Received += handlers.ProcessTopic;
channel.BasicConsume(topicQueue, autoAck: false, consumer: consumer4);
Console.WriteLine("[.NET] Topic patterns: incident.#, *.critical.*, incident.*.network");

// Pattern 5: DLQ
channel.ExchangeDeclare("dotnet_dlq_dead_exchange", ExchangeType.Direct, durable: true);
channel.QueueDeclare("dotnet_dlq_dead_queue", durable: true, exclusive: false, autoDelete: false);
channel.QueueBind("dotnet_dlq_dead_queue", "dotnet_dlq_dead_exchange", "");

var dlqArgs = new Dictionary<string, object>
{
    { "x-dead-letter-exchange", "dotnet_dlq_dead_exchange" }
};
channel.QueueDeclare("dotnet_dlq_main_queue", durable: true, exclusive: false, autoDelete: false, arguments: dlqArgs);

var consumer5Main = new EventingBasicConsumer(channel);
consumer5Main.Received += handlers.ProcessDLQMain;
channel.BasicConsume("dotnet_dlq_main_queue", autoAck: false, consumer: consumer5Main);
Console.WriteLine("[.NET] Listening on: dotnet_dlq_main_queue");

var consumer5Dead = new EventingBasicConsumer(channel);
consumer5Dead.Received += handlers.ProcessDLQDead;
channel.BasicConsume("dotnet_dlq_dead_queue", autoAck: false, consumer: consumer5Dead);
Console.WriteLine("[.NET] Listening on: dotnet_dlq_dead_queue");

// Pattern 6: TTL (uses DLQ for expired messages)
var ttlArgs = new Dictionary<string, object>
{
    { "x-dead-letter-exchange", "dotnet_dlq_dead_exchange" }
};
channel.QueueDeclare("dotnet_ttl_queue", durable: true, exclusive: false, autoDelete: false, arguments: ttlArgs);
Console.WriteLine("[.NET] TTL queue declared (expired → DLQ)");

// Pattern 7: Priority Queue
var priorityArgs = new Dictionary<string, object>
{
    { "x-max-priority", 10 }
};
channel.QueueDeclare("dotnet_priority_queue", durable: true, exclusive: false, autoDelete: false, arguments: priorityArgs);
var consumer7 = new EventingBasicConsumer(channel);
consumer7.Received += handlers.ProcessPriority;
channel.BasicConsume("dotnet_priority_queue", autoAck: false, consumer: consumer7);
Console.WriteLine("[.NET] Listening on: dotnet_priority_queue (max-priority=10)");

// Pattern 8: ACK Modes
channel.QueueDeclare("dotnet_ack_test_queue", durable: true, exclusive: false, autoDelete: false);
var consumer8 = new EventingBasicConsumer(channel);
consumer8.Received += handlers.ProcessAckTest;
channel.BasicConsume("dotnet_ack_test_queue", autoAck: false, consumer: consumer8);
Console.WriteLine("[.NET] Listening on: dotnet_ack_test_queue");

Console.WriteLine("[.NET Consumer] Ready! Waiting for messages...");

// Keep running
var cancellationTokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
    e.Cancel = true;
    cancellationTokenSource.Cancel();
};

try
{
    await Task.Delay(Timeout.Infinite, cancellationTokenSource.Token);
}
catch (TaskCanceledException)
{
    Console.WriteLine("[.NET Consumer] Shutting down...");
}

channel.Close();
connection.Close();
