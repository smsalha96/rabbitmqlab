using System.Runtime.Serialization;
using System.ServiceModel;

namespace SoapProducer.Models;

// SOAP Service Contract
[ServiceContract]
public interface IIncidentService
{
    [OperationContract]
    Task<IncidentResponse> CreateTicket(CreateTicketRequest request);

    [OperationContract]
    Task<IncidentResponse> TestPubSub(TestMessageRequest request);

    [OperationContract]
    Task<IncidentResponse> TestRouting(string severity, TestMessageRequest request);

    [OperationContract]
    Task<IncidentResponse> TestTopic(string routingKey, TestMessageRequest request);

    [OperationContract]
    Task<IncidentResponse> TestDLQ(bool forceFail, TestMessageRequest request);

    [OperationContract]
    Task<IncidentResponse> TestTTL(int ttlSeconds, TestMessageRequest request);

    [OperationContract]
    Task<IncidentResponse> TestPriority(int priority, TestMessageRequest request);

    [OperationContract]
    Task<IncidentResponse> TestAckModes(string mode, TestMessageRequest request);
}

// Data Contracts
[DataContract]
public class CreateTicketRequest
{
    [DataMember]
    public string Title { get; set; } = string.Empty;

    [DataMember]
    public string Description { get; set; } = string.Empty;

    [DataMember]
    public string Severity { get; set; } = "medium";
}

[DataContract]
public class TestMessageRequest
{
    [DataMember]
    public string Message { get; set; } = string.Empty;
}

[DataContract]
public class IncidentResponse
{
    [DataMember]
    public bool Success { get; set; }

    [DataMember]
    public string Message { get; set; } = string.Empty;

    [DataMember]
    public string? TicketId { get; set; }

    [DataMember]
    public DateTime Timestamp { get; set; }
}
