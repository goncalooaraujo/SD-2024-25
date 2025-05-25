using AnaliseRpc;
using Grpc.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<AnaliseService>();
app.MapGet("/", () => "Analise RPC Service");

app.Run();