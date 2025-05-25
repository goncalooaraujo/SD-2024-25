using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using PreProcessamentoRpc;

namespace Agregador
{
    class MensagemInfo
    {
        public string? Caracteristica { get; set; }
        public string? Hora { get; set; }
    }

    class ConfiguracaoWavy
    {
        public string PreProcessamento { get; set; } = "none";
        public int VolumeDados { get; set; } = 3;
        public string Servidor { get; set; } = "localhost";
    }

    class Program
    {
        static Dictionary<string, ConfiguracaoWavy> configuracoes = new();
        static Dictionary<string, List<MensagemInfo>> mensagensPorWavy = new();
        static readonly object configLock = new();
        static readonly object mensagensLock = new();
        static Timer reloadTimer;
        static GrpcChannel rpcChannel;
        static PreProcessamento.PreProcessamentoClient rpcClient;

        static async Task Main()
        {
            // Configurar cliente RPC
            rpcChannel = GrpcChannel.ForAddress("http://localhost:50051");
            rpcClient = new PreProcessamento.PreProcessamentoClient(rpcChannel);

            LerConfiguracoes("config.csv");
            reloadTimer = new Timer(_ => LerConfiguracoes("config.csv"), null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

            int port = 5000;
            TcpListener server = new TcpListener(IPAddress.Any, port);
            server.Start();
            Console.WriteLine("AGREGADOR Pronto (RPC ativado)");

            while (true)
            {
                TcpClient client = await server.AcceptTcpClientAsync();
                Thread clientThread = new Thread(async () => await HandleClient(client));
                clientThread.Start();
            }
        }

        static void LerConfiguracoes(string caminho)
        {
            try
            {
                if (!File.Exists(caminho)) return;

                var novasConfigs = new Dictionary<string, ConfiguracaoWavy>();

                foreach (var linha in File.ReadAllLines(caminho))
                {
                    var partes = linha.Split(':');
                    if (partes.Length == 4)
                    {
                        string id = partes[0];
                        string proc = partes[1];
                        int volume = int.TryParse(partes[2], out var v) ? v : 3;
                        string servidor = partes[3];
                        novasConfigs[id] = new ConfiguracaoWavy
                        {
                            PreProcessamento = proc,
                            VolumeDados = volume,
                            Servidor = servidor
                        };
                    }
                }

                lock (configLock)
                {
                    configuracoes = novasConfigs;
                }
                Console.WriteLine("[CONFIG] Ficheiro de configuração recarregado");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CONFIG] Erro ao recarregar configurações: {ex.Message}");
            }
        }

        static async Task HandleClient(object obj)
        {
            TcpClient client = (TcpClient)obj;
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[1024];
            string wavyId = "";

            try
            {
                while (true)
                {
                    int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead == 0) break;

                    string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine("Recebido da WAVY: " + message);

                    if (message.StartsWith("HELLO:"))
                    {
                        wavyId = message.Substring(6);
                        if (!configuracoes.ContainsKey(wavyId))
                        {
                            Console.WriteLine($"[WARNING] WAVY_ID '{wavyId}' não está na configuração!");
                        }
                        continue;
                    }

                    if (string.IsNullOrEmpty(wavyId)) continue;

                    ConfiguracaoWavy config;
                    lock (configLock)
                    {
                        config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();
                    }

                    var (tipo, carac, hora) = ExtrairDados(message);
                    if (tipo == null || carac == null || hora == null) continue;

                    // Chamada RPC para pré-processamento
                    try
                    {
                        var respostaRpc = await rpcClient.ProcessarDadosAsync(new DadosBrutos
                        {
                            Dados = carac,
                            TipoProcessamento = config.PreProcessamento
                        });
                        carac = respostaRpc.Dados;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Erro no RPC: {ex.Message}");
                        // Fallback para processamento local se RPC falhar
                        if (config.PreProcessamento == "uppercase")
                            carac = carac.ToUpper();
                        else if (config.PreProcessamento == "lowercase")
                            carac = carac.ToLower();
                    }

                    lock (mensagensLock)
                    {
                        if (!mensagensPorWavy.ContainsKey(wavyId))
                            mensagensPorWavy[wavyId] = new List<MensagemInfo>();

                        mensagensPorWavy[wavyId].Add(new MensagemInfo
                        {
                            Caracteristica = carac,
                            Hora = hora
                        });

                        if (mensagensPorWavy[wavyId].Count >= config.VolumeDados)
                        {
                            EnviarParaServidor(tipo, mensagensPorWavy[wavyId], config, wavyId);
                            mensagensPorWavy[wavyId].Clear();
                        }
                    }

                    string response = "Mensagem recebida e processada";
                    byte[] responseBytes = Encoding.ASCII.GetBytes(response);
                    await stream.WriteAsync(responseBytes, 0, responseBytes.Length);
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

        static (string? tipo, string? carac, string? hora) ExtrairDados(string msg)
        {
            try
            {
                var partes = msg.Split('|');
                string? tipo = null;
                string? carac = null;
                string? hora = null;

                foreach (var parte in partes)
                {
                    if (parte.StartsWith("TIPO:")) tipo = parte[5..];
                    else if (parte.StartsWith("CARAC:")) carac = parte[6..];
                    else if (parte.StartsWith("HORA:")) hora = parte[5..];
                }

                return (tipo, carac, hora);
            }
            catch
            {
                return (null, null, null);
            }
        }

        static void EnviarParaServidor(string tipo, List<MensagemInfo> mensagens, ConfiguracaoWavy config, string wavyId)
        {
            try
            {
                var jsonObj = new
                {
                    tipo = tipo,
                    wavyId = wavyId,
                    mensagens = mensagens
                };

                string json = JsonSerializer.Serialize(jsonObj);
                Console.WriteLine($"[DEBUG] JSON a ser enviado: {json}"); // Log do JSON

                string serverIp = config.Servidor == "localhost" ? "127.0.0.1" : config.Servidor;
                int serverPort = 6000;

                using TcpClient serverClient = new TcpClient(serverIp, serverPort);
                NetworkStream stream = serverClient.GetStream();
                byte[] dataBytes = Encoding.UTF8.GetBytes(json);
                stream.Write(dataBytes, 0, dataBytes.Length);

                // Ler resposta do servidor
                byte[] responseBuffer = new byte[1024];
                int bytesRead = stream.Read(responseBuffer, 0, responseBuffer.Length);
                string resposta = Encoding.ASCII.GetString(responseBuffer, 0, bytesRead);
                Console.WriteLine($"[DEBUG] Resposta do Servidor: {resposta}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"[ERRO] Ao enviar para servidor: {e.Message}");
            }
        }
    }
}