using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.IO;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Threading.Tasks;

namespace Servidor
{
    class Modelo
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

    static class MongoHelper
    {
        private static readonly IMongoCollection<Modelo> collection;

        static MongoHelper()
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("sd");
            collection = database.GetCollection<Modelo>("dados");
        }

        public static void GuardarDados(string mensagem)
        {
            try
            {
                Console.WriteLine($"[MongoDB] Recebido: {mensagem}");

                var modelo = ExtrairModeloJSON(mensagem);
                if (modelo == null)
                {
                    Console.WriteLine("[MongoDB ERRO] Não foi possível interpretar a mensagem JSON.");
                    return;
                }

                collection.InsertOne(modelo);
                Console.WriteLine($"[MongoDB] Dados inseridos para WAVY: {modelo.WavyId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Ao guardar dados: {ex.Message}");
            }
        }

        // SIMPLIFIED: Only expects JSON from Agregador
        private static Modelo? ExtrairModeloJSON(string msg)
        {
            try
            {
                msg = msg.Trim();

                // Should always be JSON from Agregador
                if (!msg.StartsWith("{") || !msg.EndsWith("}"))
                {
                    Console.WriteLine("[MongoDB ERRO] Mensagem não é JSON válido");
                    return null;
                }

                var jObject = JObject.Parse(msg);
                return new Modelo
                {
                    WavyId = jObject["wavyId"]?.ToString(),
                    Tipo = jObject["tipo"]?.ToString(),
                    Valor = jObject["valor"]?.ToObject<double>() ?? 0,
                    Unidade = jObject["unidade"]?.ToString(),
                    Hora = jObject["hora"]?.ToString(),
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Erro ao parsear JSON: {ex.Message}");
                return null;
            }
        }
    }

    class Program
    {
        private static readonly Mutex ficheiroMutex = new Mutex();

        static async Task Main()
        {
            try
            {
                var mongoClient = new MongoClient("mongodb://localhost:27017");
                var database = mongoClient.GetDatabase("sd");
                await database.RunCommandAsync((Command<BsonDocument>)"{ping:1}");
                Console.WriteLine("[MongoDB] Conexão bem-sucedida!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Falha na conexão: {ex.Message}");
                return;
            }

            int port = 6000;
            TcpListener server = new TcpListener(IPAddress.Any, port);
            server.Start();
            Console.WriteLine("SERVIDOR DE ARMAZENAMENTO Pronto (porta 6000)");
            Console.WriteLine("Para análise, use o AnaliseService na porta 50052");

            while (true)
            {
                TcpClient client = await server.AcceptTcpClientAsync();
                Thread clientThread = new Thread(() => HandleClient(client));
                clientThread.Start();
            }
        }

        static void HandleClient(object obj)
        {
            TcpClient client = (TcpClient)obj;
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[2048];

            try
            {
                var remoteEndPoint = client.Client.RemoteEndPoint.ToString();

                while (true)
                {
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break;

                    string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[STORAGE] Dados recebidos do AGREGADOR ({remoteEndPoint}): {message}");

                    ficheiroMutex.WaitOne();
                    try
                    {
                        File.AppendAllText("dados_recebidos.json", message + "\n");
                    }
                    finally
                    {
                        ficheiroMutex.ReleaseMutex();
                    }

                    MongoHelper.GuardarDados(message);

                    string resposta = $"Dados armazenados com sucesso";
                    byte[] respostaBytes = Encoding.ASCII.GetBytes(resposta);
                    stream.Write(respostaBytes, 0, respostaBytes.Length);

                    if (message.Contains("fim")) break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[STORAGE] Erro no HandleClient: {ex.Message}");
            }
            finally
            {
                client.Close();
            }
        }
    }
}