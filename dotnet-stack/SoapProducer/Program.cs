using SoapCore;
using SoapProducer.Models;
using SoapProducer.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services
builder.Services.AddSingleton<IIncidentService, IncidentService>();
builder.Services.AddSoapCore();

var app = builder.Build();

// Configure SOAP endpoint (cast to IApplicationBuilder to resolve ambiguity)
((IApplicationBuilder)app).UseSoapEndpoint<IIncidentService>(
    path: "/IncidentService.asmx",
    encoder: new SoapEncoderOptions(),
    serializer: SoapSerializer.DataContractSerializer
);

// Health check endpoint
app.MapGet("/health", () => new { status = "healthy", service = "SOAP Producer (.NET)" });

// WSDL endpoint info
app.MapGet("/", () => Results.Redirect("/IncidentService.asmx?wsdl"));

app.Run();
