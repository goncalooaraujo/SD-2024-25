using AnaliseRpc;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// ⚙️ Configurar Kestrel para escutar na porta 50052 usando HTTP/2 (gRPC)
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(50052, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

// ✅ Adicionar suporte a gRPC
builder.Services.AddGrpc();

// 🛡️ (Opcional) CORS para permitir chamadas externas no futuro
builder.Services.AddCors(o => o.AddPolicy("AllowAll", policy =>
{
    policy.AllowAnyOrigin()
          .AllowAnyMethod()
          .AllowAnyHeader();
}));

var app = builder.Build();

// ✅ Mapear o serviço gRPC para análise
app.MapGrpcService<AnaliseService>();

// 🔎 Endpoint simples para debugging
app.MapGet("/", () => "✅ Serviço Analise RPC ativo em /grpc na porta 50052.");

// (Opcional) Aplicar política de CORS
app.UseCors("AllowAll");

app.Run();
