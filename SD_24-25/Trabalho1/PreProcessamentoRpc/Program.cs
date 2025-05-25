using PreProcessamentoRpc;
using Grpc.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<PreProcessamentoService>();
app.MapGet("/", () => "PreProcessamento RPC Service");

app.Run();