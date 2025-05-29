using PreProcessamentoRpc;
using Grpc.Core;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// Configurar Kestrel para ouvir na porta 50051 usando HTTP/2 (necessário para gRPC)
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(50051, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<PreProcessamentoService>();
app.MapGet("/", () => "PreProcessamento RPC Service");

app.Run();
