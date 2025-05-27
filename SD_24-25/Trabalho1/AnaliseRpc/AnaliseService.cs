using Grpc.Core;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace AnaliseRpc
{
    public class AnaliseService : Analise.AnaliseBase
    {
        public override Task<ResultadoAnalisePorTipo> AnalisarDadosPorTipo(DadosParaAnalise request, ServerCallContext context)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("sd");
            var collection = database.GetCollection<Modelo>("dados");

            var filter = Builders<Modelo>.Filter.Regex(x => x.WavyId, new BsonRegularExpression($"^{request.WavyId}$", "i"));
            var dados = collection.Find(filter).ToList();

            var dict = new Dictionary<string, (double soma, int count)>();

            foreach (var doc in dados)
            {
                if (string.IsNullOrEmpty(doc.Tipo)) continue;

                // Acumula por tipo
                if (!dict.ContainsKey(doc.Tipo))
                    dict[doc.Tipo] = (0, 0);

                dict[doc.Tipo] = (dict[doc.Tipo].soma + doc.Valor, dict[doc.Tipo].count + 1);
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
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("wavyId")]
        public string? WavyId { get; set; }

        [BsonElement("tipo")]
        public string? Tipo { get; set; }

        [BsonElement("valor")]
        public double Valor { get; set; }

        [BsonElement("unidade")]
        public string? Unidade { get; set; }

        [BsonElement("hora")]
        public string? Hora { get; set; }
    }
}
