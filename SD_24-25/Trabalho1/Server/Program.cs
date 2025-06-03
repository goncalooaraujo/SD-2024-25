using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
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

        [BsonElement("data")]
        public string? Data { get; set; }
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

        public static void GuardarDados(string mensagemJson)
        {
            try
            {
                Console.WriteLine($"[MongoDB] Recebido: {mensagemJson}");

                var jObject = JObject.Parse(mensagemJson);
                var modelo = new Modelo
                {
                    WavyId = jObject["wavyId"]?.ToString(),
                    Tipo = jObject["tipo"]?.ToString(),
                    Valor = jObject["valor"]?.ToObject<double>() ?? 0,
                    Unidade = jObject["unidade"]?.ToString(),
                    Data = jObject["data"]?.ToString(),
                };

                collection.InsertOne(modelo);
                Console.WriteLine($"[MongoDB] Dados inseridos para WAVY: {modelo.WavyId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Ao guardar dados: {ex.Message}");
            }
        }
    }

    class Program
    {
        static async Task Main()
        {
            Console.WriteLine("=== SERVIDOR DE ARMAZENAMENTO ===");

            // Test MongoDB connection
            try
            {
                var mongoClient = new MongoClient("mongodb://localhost:27017");
                var database = mongoClient.GetDatabase("sd");
                await database.RunCommandAsync((Command<BsonDocument>)"{ping:1}");
                Console.WriteLine("✅ [MongoDB] Conexão bem-sucedida!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [MongoDB ERRO] Falha na conexão: {ex.Message}");
                Console.WriteLine("   Certifique-se que o MongoDB está rodando na porta 27017");
                return;
            }

            // Start TCP server for receiving data
            await IniciarServidorTCP();
        }

        static async Task IniciarServidorTCP()
        {
            int port = 6000;
            TcpListener server = new TcpListener(IPAddress.Any, port);
            server.Start();
            Console.WriteLine($"✅ [TCP] Servidor pronto na porta {port}");

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
                var remoteEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "Unknown";

                while (true)
                {
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break;

                    string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"📨 Dados recebidos do AGREGADOR ({remoteEndPoint}): {message}");

                    // Save to MongoDB (expecting JSON format only)
                    MongoHelper.GuardarDados(message);

                    // Send confirmation
                    string resposta = $"✅ Dados armazenados com sucesso";
                    byte[] respostaBytes = Encoding.ASCII.GetBytes(resposta);
                    stream.Write(respostaBytes, 0, respostaBytes.Length);

                    if (message.Contains("fim")) break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro no HandleClient: {ex.Message}");
            }
            finally
            {
                client.Close();
            }
        }
    }
}
