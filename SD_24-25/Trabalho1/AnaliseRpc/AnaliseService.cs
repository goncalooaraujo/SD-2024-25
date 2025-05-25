using Grpc.Core;
using MongoDB.Driver;

namespace AnaliseRpc
{
    public class AnaliseService : Analise.AnaliseBase
    {
        public override Task<ResultadoAnalisePorTipo> AnalisarDadosPorTipo(DadosParaAnalise request, ServerCallContext context)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("sd");
            var collection = database.GetCollection<Modelo>("dados");

            // Pega todos os documentos para o WavyId (ignorar filtro tipo para agrupar depois)
            var filter = Builders<Modelo>.Filter.Eq(x => x.WavyId, request.WavyId);
            var dados = collection.Find(filter).ToList();

            // Dicionário para armazenar soma e contagem por tipo
            var dict = new Dictionary<string, (double soma, int count)>();

            foreach (var doc in dados)
            {
                if (doc.Tipo == null) continue;

                foreach (var msg in doc.Mensagens ?? new List<Mensagem>())
                {
                    if (double.TryParse(msg.Caracteristica?.Split(' ')[0], out var valor))
                    {
                        if (!dict.ContainsKey(doc.Tipo))
                            dict[doc.Tipo] = (0, 0);

                        var atual = dict[doc.Tipo];
                        dict[doc.Tipo] = (atual.soma + valor, atual.count + 1);
                    }
                }
            }

            var resultado = new ResultadoAnalisePorTipo();

            foreach (var par in dict)
            {
                var media = par.Value.count > 0 ? par.Value.soma / par.Value.count : 0;

                resultado.MediasPorTipo.Add(new TipoMedia
                {
                    Tipo = par.Key,
                    Media = media,
                    TotalAmostras = par.Value.count
                });
            }

            return Task.FromResult(resultado);
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