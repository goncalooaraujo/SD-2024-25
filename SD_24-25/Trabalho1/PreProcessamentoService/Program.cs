using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

// Adiciona suporte a endpoints + Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "Serviço de Pré-Processamento",
        Version = "v1"
    });
});

var app = builder.Build();

// Ativa Swagger em todos os ambientes (opcional)
app.UseSwagger();
app.UseSwaggerUI(options =>
{
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "Pré-Processamento v1");
    options.RoutePrefix = string.Empty; // Swagger na root
});

app.MapPost("/preprocessar", (PreprocessamentoRequest request) =>
{
    var mensagensTransformadas = request.Mensagens.Select(m =>
        new Mensagem(
            request.PreProcessamento switch
            {
                "uppercase" => m.Caracteristica?.ToUpper(),
                "lowercase" => m.Caracteristica?.ToLower(),
                _ => m.Caracteristica
            },
            m.Hora
        )).ToList();

    return Results.Ok(mensagensTransformadas);
})
.WithName("Preprocessar")
.WithOpenApi(); // Necessário para Swagger reconhecer o endpoint

app.Run();

// Tipos
public record PreprocessamentoRequest(string PreProcessamento, List<Mensagem> Mensagens);
public record Mensagem(string? Caracteristica, string? Hora);
