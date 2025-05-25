using AnaliseRpc;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// Configurar Kestrel para suportar HTTP/2 sem TLS na porta 50052
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(50052, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;  // Apenas HTTP/2
    });
});

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<AnaliseService>();
app.MapGet("/", () => "Analise RPC Service is running.");

app.Run();
