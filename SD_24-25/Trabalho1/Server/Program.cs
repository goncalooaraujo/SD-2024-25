using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.IO;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Grpc.Net.Client;
using AnaliseRpc;

namespace Servidor
{
    class Mensagem
    {
        [BsonElement("caracteristica")]
        public string? Caracteristica { get; set; }

        [BsonElement("hora")]
        public string? Hora { get; set; }
    }

    class Modelo
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("wavyId")]
        public string? WavyId { get; set; }

        [BsonElement("tipo")]
        public string? Tipo { get; set; }

        [BsonElement("mensagens")]
        public List<Mensagem>? Mensagens { get; set; }
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

        public static void GuardarDados(string json)
        {
            try
            {
                Console.WriteLine($"[MongoDB] Recebido JSON: {json}"); // Log do JSON recebido

                var jObject = JObject.Parse(json);
                var modelo = new Modelo
                {
                    WavyId = jObject["wavyId"]?.ToString(),
                    Tipo = jObject["tipo"]?.ToString(),
                    Mensagens = new List<Mensagem>()
                };

                foreach (var msg in jObject["mensagens"])
                {
                    modelo.Mensagens.Add(new Mensagem
                    {
                        Caracteristica = msg["Caracteristica"]?.ToString(),
                        Hora = msg["Hora"]?.ToString()
                    });
                }

                var client = new MongoClient("mongodb://localhost:27017");
                var database = client.GetDatabase("sd");
                var collection = database.GetCollection<Modelo>("dados");

                collection.InsertOne(modelo);
                Console.WriteLine($"[MongoDB] Dados inseridos para WAVY: {modelo.WavyId}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Ao guardar dados: {ex.Message}");
            }
        }

        public static List<Modelo> ObterDadosPorWavy(string wavyId)
        {
            return collection.Find(x => x.WavyId == wavyId).ToList();
        }
    }

    class Program
    {
        private static readonly Mutex ficheiroMutex = new Mutex();
        private static GrpcChannel rpcChannel;
        private static Analise.AnaliseClient rpcClient;

        static async Task Main()
        {
            try
            {
                var mongoClient = new MongoClient("mongodb://localhost:27017");
                var database = mongoClient.GetDatabase("sd");
                database.RunCommandAsync((Command<BsonDocument>)"{ping:1}").Wait();
                Console.WriteLine("[MongoDB] Conexão bem-sucedida!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Falha na conexão: {ex.Message}");
                return;
            }

            // Configurar cliente RPC para análise
            rpcChannel = GrpcChannel.ForAddress("http://localhost:50052");
            rpcClient = new Analise.AnaliseClient(rpcChannel);

            // Iniciar thread para análise sob demanda
            ThreadPool.QueueUserWorkItem(async _ => await IniciarInterfaceAnalise());

            // Iniciar servidor TCP
            int port = 6000;
            TcpListener server = new TcpListener(IPAddress.Any, port);
            server.Start();
            Console.WriteLine("SERVIDOR Pronto (RPC ativado)");

            while (true)
            {
                TcpClient client = await server.AcceptTcpClientAsync();
                Thread clientThread = new Thread(() => HandleClient(client));
                clientThread.Start();
            }
        }

        static async Task IniciarInterfaceAnalise()
        {
            while (true)
            {
                Console.WriteLine("\n=== MENU DE ANÁLISE ===");
                Console.WriteLine("1. analise WAVY_ID - Analisar dados de uma WAVY específica");
                Console.WriteLine("2. listar - Listar todas as WAVYs com dados registrados");
                Console.WriteLine("3. sair - Encerrar o servidor");
                Console.Write("\nDigite sua opção: ");

                var input = Console.ReadLine()?.Trim().ToLower();

                if (input == "sair" || input == "3")
                {
                    Console.WriteLine("Encerrando o servidor...");
                    Environment.Exit(0);
                }
                else if (input == "listar" || input == "2")
                {
                    Console.WriteLine("\n[LISTANDO WAVYs REGISTRADAS]");
                    try
                    {
                        var client = new MongoClient("mongodb://localhost:27017");
                        var database = client.GetDatabase("sd");
                        var collection = database.GetCollection<Modelo>("dados");

                        var wavyIds = await collection.DistinctAsync<string>("wavyId", FilterDefinition<Modelo>.Empty);

                        if (!wavyIds.Any())
                        {
                            Console.WriteLine("Nenhuma WAVY encontrada no banco de dados.");
                            continue;
                        }

                        Console.WriteLine("WAVYs registradas:");
                        await wavyIds.ForEachAsync(id => Console.WriteLine($"- {id}"));

                        var wavyList = await wavyIds.ToListAsync();
                        Console.WriteLine($"\nTotal: {wavyList.Count} WAVY(s) encontrada(s)");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERRO] Ao listar WAVYs: {ex.Message}");
                    }
                }
                else if ((input?.StartsWith("analise ") ?? false) || (input?.StartsWith("1 ") ?? false))
                {
                    var wavyId = input.StartsWith("1 ") ? input.Substring(2) : input.Substring(8);

                    if (string.IsNullOrWhiteSpace(wavyId))
                    {
                        Console.WriteLine("[AVISO] Por favor, especifique o ID da WAVY");
                        continue;
                    }

                    Console.WriteLine($"\n[INICIANDO ANÁLISE PARA {wavyId}]");

                    try
                    {
                        Console.WriteLine($"Conectando ao serviço RPC de análise...");
                        using var channel = GrpcChannel.ForAddress("http://localhost:50052");
                        var client = new Analise.AnaliseClient(channel);

                        Console.WriteLine($"Solicitando análise para {wavyId}...");
                        var resposta = await client.AnalisarDadosAsync(new DadosParaAnalise
                        {
                            WavyId = wavyId
                        });

                        Console.WriteLine("\n=== RESULTADOS DA ANÁLISE ===");
                        Console.WriteLine($"WAVY ID: {wavyId}");
                        Console.WriteLine($"Média calculada: {resposta.Media:F2}");
                        Console.WriteLine($"Total de amostras: {resposta.TotalAmostras}");
                        Console.WriteLine($"Resumo: {resposta.Resumo}");
                        Console.WriteLine("=============================");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERRO] Na análise: {ex.Message}");
                        Console.WriteLine("Verifique se:");
                        Console.WriteLine("- O serviço de análise está rodando");
                        Console.WriteLine("- O WAVY_ID está correto");
                        Console.WriteLine("- Há dados no MongoDB para esta WAVY");
                    }
                }
                else
                {
                    Console.WriteLine("[AVISO] Comando não reconhecido. Tente novamente.");
                    Console.WriteLine("Dica: Use 'analise WAVY_001' ou '1 WAVY_001'");
                }
            }
        }

        static void HandleClient(object obj)
        {
            TcpClient client = (TcpClient)obj;
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[1024];

            try
            {
                while (true)
                {
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break;

                    string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine("Dados recebidos do AGREGADOR: " + message);

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

                    string resposta = "Dados recebidos com sucesso";
                    byte[] respostaBytes = Encoding.ASCII.GetBytes(resposta);
                    stream.Write(respostaBytes, 0, respostaBytes.Length);

                    if (message.Contains("fim")) break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro no HandleClient: {ex.Message}");
            }
            finally
            {
                client.Close();
            }
        }
    }
}