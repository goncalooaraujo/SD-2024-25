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
using Grpc.Net.Client;
using AnaliseRpc;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Xml;
using System.Text.RegularExpressions;

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

                var modelo = ExtrairModelo(mensagem);
                if (modelo == null)
                {
                    Console.WriteLine("[MongoDB ERRO] Não foi possível interpretar a mensagem.");
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

        private static Modelo? ExtrairModelo(string msg)
        {
            try
            {
                msg = msg.Trim();

                // JSON
                if (msg.StartsWith("{") && msg.EndsWith("}"))
                {
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

                // XML
                if (msg.StartsWith("<") && msg.EndsWith(">"))
                {
                    var xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(msg);
                    var root = xmlDoc.DocumentElement;

                    if (root == null) return null;

                    return new Modelo
                    {
                        WavyId = root.SelectSingleNode("wavyId")?.InnerText,
                        Tipo = root.SelectSingleNode("tipo")?.InnerText,
                        Valor = double.TryParse(root.SelectSingleNode("valor")?.InnerText?.Replace(",", "."), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double val) ? val : 0,
                        Unidade = root.SelectSingleNode("unidade")?.InnerText,
                        Hora = root.SelectSingleNode("hora")?.InnerText,
                    };
                }

                // CSV (wavyId,tipo,valor,unidade,hora)
                var partes = msg.Split(',');
                if (partes.Length == 5)
                {
                    return new Modelo
                    {
                        WavyId = partes[0],
                        Tipo = partes[1],
                        Valor = double.TryParse(partes[2].Replace(",", "."), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double val) ? val : 0,
                        Unidade = partes[3],
                        Hora = partes[4]
                    };
                }

                // TXT (WAVY ID: WAVY_003 | Tipo: salinidade | Valor: 34.50 PSU | Hora: 12:31:45)
                if (msg.Contains("WAVY ID:") && msg.Contains("Tipo:") && msg.Contains("Valor:") && msg.Contains("Hora:"))
                {
                    var wavyMatch = Regex.Match(msg, @"WAVY ID:\s*(\S+)");
                    var tipoMatch = Regex.Match(msg, @"Tipo:\s*([^|]+)");
                    var valorMatch = Regex.Match(msg, @"Valor:\s*([\d.,]+)");
                    var unidadeMatch = Regex.Match(msg, @"Valor:\s*[\d.,]+\s*([^\s|]+)");
                    var horaMatch = Regex.Match(msg, @"Hora:\s*([^\s|]+)");

                    return new Modelo
                    {
                        WavyId = wavyMatch.Groups[1].Value.Trim(),
                        Tipo = tipoMatch.Groups[1].Value.Trim(),
                        Valor = double.TryParse(valorMatch.Groups[1].Value.Replace(",", "."), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double val) ? val : 0,
                        Unidade = unidadeMatch.Groups[1].Value.Trim(),
                        Hora = horaMatch.Groups[1].Value.Trim()
                    };
                }

                return null;
            }
            catch
            {
                return null;
            }
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
                await database.RunCommandAsync((Command<BsonDocument>)"{ping:1}");
                Console.WriteLine("[MongoDB] Conexão bem-sucedida!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Falha na conexão: {ex.Message}");
                return;
            }

            rpcChannel = GrpcChannel.ForAddress("http://localhost:50052");
            rpcClient = new Analise.AnaliseClient(rpcChannel);

            ThreadPool.QueueUserWorkItem(async _ => await IniciarInterfaceAnalise());

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

                        var wavyList = await collection.Distinct<string>("wavyId", FilterDefinition<Modelo>.Empty).ToListAsync();

                        if (wavyList.Count == 0)
                        {
                            Console.WriteLine("Nenhuma WAVY encontrada no banco de dados.");
                            continue;
                        }

                        foreach (var id in wavyList)
                            Console.WriteLine($"- {id}");

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
                        var resposta = await rpcClient.AnalisarDadosPorTipoAsync(new DadosParaAnalise { WavyId = wavyId });

                        Console.WriteLine("\n=== RESULTADOS DA ANÁLISE POR TIPO ===");
                        Console.WriteLine($"WAVY ID: {wavyId}");

                        foreach (var mediaTipo in resposta.MediasPorTipo)
                        {
                            Console.WriteLine($"Tipo: {mediaTipo.Tipo}");
                            Console.WriteLine($"  Média: {mediaTipo.Media:F2}");
                            Console.WriteLine($"  Total de amostras: {mediaTipo.TotalAmostras}");
                        }

                        Console.WriteLine("=============================");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[ERRO] Na análise: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine("[AVISO] Comando não reconhecido.");
                }
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
                    Console.WriteLine($"Dados recebidos do AGREGADOR ({remoteEndPoint}): {message}");

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

                    // Do not send back the original message, just a status confirmation
                    string resposta = $"Mensagem recebida com sucesso de {remoteEndPoint}";
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
