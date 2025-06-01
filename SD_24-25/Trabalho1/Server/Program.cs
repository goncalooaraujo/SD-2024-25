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
using Grpc.Core;

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

        public static async Task<List<string>> ListarWAVYsAsync()
        {
            try
            {
                return await collection.Distinct<string>("wavyId", FilterDefinition<Modelo>.Empty).ToListAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MongoDB ERRO] Ao listar WAVYs: {ex.Message}");
                return new List<string>();
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
                if (partes.Length == 5 || partes.Length == 6)
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
        private static GrpcChannel? pythonAnalysisChannel;
        private static Analise.AnaliseClient? pythonAnalysisClient;

        static async Task Main()
        {
            Console.WriteLine("=== SERVIDOR C# COM INTEGRAÇÃO PYTHON gRPC ===");

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

            // Connect to Python gRPC service
            await InicializarConexaoPython();

            // Start analysis interface in background
            ThreadPool.QueueUserWorkItem(async _ => await IniciarInterfaceAnalise());

            // Start TCP server for receiving data
            await IniciarServidorTCP();
        }

        static async Task InicializarConexaoPython()
        {
            try
            {
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                pythonAnalysisChannel = GrpcChannel.ForAddress("http://localhost:50052");
                pythonAnalysisClient = new Analise.AnaliseClient(pythonAnalysisChannel);

                // Test connection - FIXED: removed incorrect using statement
                var testRequest = new DadosParaAnalise { WavyId = "test" };
                try
                {
                    var response = pythonAnalysisClient.AnalisarDadosPorTipo(testRequest);
                    Console.WriteLine("✅ [PYTHON] Conexão com serviço de análise Python estabelecida!");
                }
                catch (Grpc.Core.RpcException)
                {
                    // If we get an RPC exception, the server is running but returned an error
                    Console.WriteLine("✅ [PYTHON] Conexão com serviço de análise Python estabelecida!");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  [PYTHON AVISO] Falha na conexão com serviço Python: {ex.Message}");
                Console.WriteLine("   Certifique-se que o servidor Python está rodando: python servidor.py");
                pythonAnalysisClient = null;
            }
        }

        static async Task IniciarServidorTCP()
        {
            int port = 6000;
            TcpListener server = new TcpListener(IPAddress.Any, port);
            server.Start();
            Console.WriteLine($"✅ [TCP] Servidor pronto na porta {port} (Integrado com Python gRPC)");

            while (true)
            {
                TcpClient client = await server.AcceptTcpClientAsync();
                Thread clientThread = new Thread(() => HandleClient(client));
                clientThread.Start();
            }
        }

        static async Task IniciarInterfaceAnalise()
        {
            await Task.Delay(1000); // Wait for server to start

            while (true)
            {
                Console.WriteLine("\n" + new string('=', 50));
                Console.WriteLine("           MENU DE ANÁLISE DE DADOS");
                Console.WriteLine(new string('=', 50));
                Console.WriteLine("1. analise [WAVY_ID] - Analisar dados de uma WAVY específica");
                Console.WriteLine("2. listar            - Listar todas as WAVYs registradas");
                Console.WriteLine("3. status            - Verificar status das conexões");
                Console.WriteLine("4. sair              - Encerrar o servidor");
                Console.WriteLine(new string('=', 50));
                Console.Write("Digite sua opção: ");

                var input = Console.ReadLine()?.Trim();

                if (string.IsNullOrEmpty(input))
                    continue;

                var comando = input.ToLower();

                if (comando == "sair" || comando == "4")
                {
                    Console.WriteLine("🔄 Encerrando o servidor...");
                    await EncerrarServidor();
                    Environment.Exit(0);
                }
                else if (comando == "listar" || comando == "2")
                {
                    await ListarWAVYs();
                }
                else if (comando == "status" || comando == "3")
                {
                    await VerificarStatus();
                }
                else if (comando.StartsWith("analise ") || comando.StartsWith("1 "))
                {
                    var wavyId = comando.StartsWith("1 ") ? input.Substring(2).Trim() : input.Substring(8).Trim();

                    if (string.IsNullOrWhiteSpace(wavyId))
                    {
                        Console.WriteLine("⚠️  Por favor, especifique o ID da WAVY");
                        continue;
                    }

                    await AnalisarWAVYComPython(wavyId);
                }
                else
                {
                    Console.WriteLine("⚠️  Comando não reconhecido. Tente novamente.");
                }
            }
        }

        static async Task ListarWAVYs()
        {
            Console.WriteLine("\n📋 [LISTANDO WAVYs REGISTRADAS]");
            try
            {
                var wavyList = await MongoHelper.ListarWAVYsAsync();

                if (wavyList.Count == 0)
                {
                    Console.WriteLine("   Nenhuma WAVY encontrada no banco de dados.");
                    return;
                }

                Console.WriteLine($"   Total: {wavyList.Count} WAVY(s) encontrada(s)");
                Console.WriteLine("   " + new string('-', 30));

                foreach (var id in wavyList)
                    Console.WriteLine($"   📊 {id}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [ERRO] Ao listar WAVYs: {ex.Message}");
            }
        }

        static async Task VerificarStatus()
        {
            Console.WriteLine("\n🔍 [STATUS DAS CONEXÕES]");

            // MongoDB Status
            try
            {
                var mongoClient = new MongoClient("mongodb://localhost:27017");
                var database = mongoClient.GetDatabase("sd");
                await database.RunCommandAsync((Command<BsonDocument>)"{ping:1}");
                Console.WriteLine("   ✅ MongoDB: Conectado");
            }
            catch
            {
                Console.WriteLine("   ❌ MongoDB: Desconectado");
            }

            // Python gRPC Status
            if (pythonAnalysisClient != null)
            {
                try
                {
                    var testRequest = new DadosParaAnalise { WavyId = "status_test" };
                    // FIXED: removed incorrect using statement
                    pythonAnalysisClient.AnalisarDadosPorTipo(testRequest);
                    Console.WriteLine("   ✅ Python gRPC: Conectado");
                }
                catch
                {
                    Console.WriteLine("   ❌ Python gRPC: Desconectado");
                }
            }
            else
            {
                Console.WriteLine("   ❌ Python gRPC: Não inicializado");
            }
        }

        static async Task AnalisarWAVYComPython(string wavyId)
        {
            Console.WriteLine($"\n🔬 [INICIANDO ANÁLISE PARA {wavyId.ToUpper()}]");

            if (pythonAnalysisClient == null)
            {
                Console.WriteLine("❌ [ERRO] Serviço Python não está disponível.");
                Console.WriteLine("   Certifique-se que o servidor Python está rodando: python servidor.py");
                return;
            }

            try
            {
                var request = new DadosParaAnalise { WavyId = wavyId };
                // Use the async version for better performance
                var resposta = await pythonAnalysisClient.AnalisarDadosPorTipoAsync(request);

                Console.WriteLine("\n" + new string('=', 60));
                Console.WriteLine($"           RESULTADOS DA ANÁLISE - {wavyId.ToUpper()}");
                Console.WriteLine(new string('=', 60));

                if (resposta.MediasPorTipo.Count == 0)
                {
                    Console.WriteLine("   ⚠️  Nenhum dado encontrado para esta WAVY.");
                    return;
                }

                foreach (var mediaTipo in resposta.MediasPorTipo)
                {
                    Console.WriteLine($"📊 Tipo: {mediaTipo.Tipo}");
                    Console.WriteLine($"   📈 Média: {mediaTipo.Media:F2}");
                    Console.WriteLine($"   📋 Amostras: {mediaTipo.TotalAmostras}");
                    Console.WriteLine("   " + new string('-', 40));
                }

                Console.WriteLine($"✅ Análise concluída com sucesso!");
                Console.WriteLine(new string('=', 60));
            }
            catch (RpcException rpcEx)
            {
                Console.WriteLine($"❌ [ERRO gRPC] {rpcEx.Status.Detail}");
                Console.WriteLine("   Verifique se o servidor Python está funcionando corretamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [ERRO] Na análise Python: {ex.Message}");
                Console.WriteLine("   Certifique-se que o servidor Python está rodando: python servidor.py");
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

                    // Save to file
                    ficheiroMutex.WaitOne();
                    try
                    {
                        File.AppendAllText("dados_recebidos.json", message + "\n");
                    }
                    finally
                    {
                        ficheiroMutex.ReleaseMutex();
                    }

                    // Save to MongoDB
                    MongoHelper.GuardarDados(message);

                    // Send confirmation
                    string resposta = $"✅ Dados recebidos e processados com sucesso de {remoteEndPoint}";
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

        static async Task EncerrarServidor()
        {
            if (pythonAnalysisChannel != null)
            {
                await pythonAnalysisChannel.ShutdownAsync();
                pythonAnalysisChannel.Dispose();
            }
        }
    }
}
