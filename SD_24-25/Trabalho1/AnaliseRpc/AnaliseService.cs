using Grpc.Core;
using MongoDB.Driver;

namespace AnaliseRpc
{
    public class AnaliseService : Analise.AnaliseBase
    {
        public override Task<ResultadoAnalise> AnalisarDados(DadosParaAnalise request, ServerCallContext context)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("sd");
            var collection = database.GetCollection<Modelo>("dados");

            var filter = Builders<Modelo>.Filter.Eq(x => x.WavyId, request.WavyId);
            var dados = collection.Find(filter).ToList();

            double soma = 0;
            int contador = 0;

            foreach (var doc in dados)
            {
                foreach (var msg in doc.Mensagens)
                {
                    if (double.TryParse(msg.Caracteristica.Split(' ')[0], out var valor))
                    {
                        soma += valor;
                        contador++;
                    }
                }
            }

            double media = contador > 0 ? soma / contador : 0;

            return Task.FromResult(new ResultadoAnalise
            {
                Media = media,
                TotalAmostras = contador,
                Resumo = $"Média calculada para {request.WavyId}: {media:F2}"
            });
        }
    }

    public class Modelo
    {
        public string? Id { get; set; }
        public string? WavyId { get; set; }
        public string? Tipo { get; set; }
        public List<Mensagem>? Mensagens { get; set; }
    }

    public class Mensagem
    {
        public string? Caracteristica { get; set; }
        public string? Hora { get; set; }
    }
}