using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using PreProcessamentoRpc;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Agregador
{
    class MensagemInfo { public string? Dados { get; set; } public string? Hora { get; set; } }

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
        static int mensagensRecebidas = 0;
        static int mensagensEnviadas = 0;

        static async Task Main()
        {
            Console.WriteLine("=== AGREGADOR INICIANDO ===");

            try
            {
                rpcChannel = GrpcChannel.ForAddress("http://localhost:50051");
                rpcClient = new PreProcessamento.PreProcessamentoClient(rpcChannel);
                Console.WriteLine("[RPC] Conexão estabelecida com pré-processamento");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RPC] AVISO: Não foi possível conectar ao pré-processamento: {ex.Message}");
                Console.WriteLine("[RPC] Continuando sem pré-processamento...");
            }

            LerConfiguracoes("config.csv");
            reloadTimer = new Timer(_ => LerConfiguracoes("config.csv"), null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                using var connection = factory.CreateConnection();
                using var channel = connection.CreateModel();

                channel.ExchangeDeclare(exchange: "sensores", type: ExchangeType.Topic);
                string queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(queue: queueName, exchange: "sensores", routingKey: "#");

                Console.WriteLine($"[RABBITMQ] Conectado - Fila: {queueName}");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    try
                    {
                        string mensagem = Encoding.UTF8.GetString(ea.Body.ToArray());
                        Interlocked.Increment(ref mensagensRecebidas);

                        Console.WriteLine($"[AGREGADOR] #{mensagensRecebidas} Mensagem recebida: {mensagem}");

                        string wavyId = ExtrairWavyId(mensagem);
                        if (!string.IsNullOrEmpty(wavyId))
                        {
                            await ProcessarMensagem(wavyId, mensagem);
                        }
                        else
                        {
                            Console.WriteLine("[AGREGADOR] AVISO: Não foi possível extrair wavyId da mensagem");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[AGREGADOR] ERRO ao processar mensagem: {ex.Message}");
                    }
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine("AGREGADOR a escutar mensagens RabbitMQ...");

                // Status timer
                Timer statusTimer = new Timer(_ =>
                {
                    Console.WriteLine($"[STATUS] Recebidas: {mensagensRecebidas} | Enviadas: {mensagensEnviadas}");
                }, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

                await Task.Delay(Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RABBITMQ] ERRO: {ex.Message}");
            }
        }

        static void LerConfiguracoes(string caminho)
        {
            try
            {
                if (!File.Exists(caminho))
                {
                    Console.WriteLine("[CONFIG] Arquivo config.csv não encontrado, usando configurações padrão");
                    return;
                }

                var novasConfigs = new Dictionary<string, ConfiguracaoWavy>();
                foreach (var linha in File.ReadAllLines(caminho))
                {
                    var partes = linha.Split(':');
                    if (partes.Length == 4)
                    {
                        novasConfigs[partes[0]] = new ConfiguracaoWavy
                        {
                            PreProcessamento = partes[1],
                            VolumeDados = int.TryParse(partes[2], out int v) ? v : 3,
                            Servidor = partes[3]
                        };
                    }
                }
                lock (configLock) configuracoes = novasConfigs;
                Console.WriteLine($"[CONFIG] Recarregado - {novasConfigs.Count} configurações");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CONFIG] Erro: {ex.Message}");
            }
        }

        static async Task ProcessarMensagem(string wavyId, string message)
        {
            ConfiguracaoWavy config;
            lock (configLock) config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();

            string dadosProcessados = message;

            // Tentar pré-processamento se disponível
            if (rpcClient != null)
            {
                try
                {
                    var respostaRpc = await rpcClient.ProcessarDadosAsync(new DadosBrutos { Dados = message, TipoProcessamento = config.PreProcessamento });
                    dadosProcessados = respostaRpc.Dados;
                    Console.WriteLine($"[RPC] Dados processados para {wavyId}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RPC] Erro para {wavyId}: {ex.Message} - usando dados originais");
                    dadosProcessados = message;
                }
            }

            lock (mensagensLock)
            {
                if (!mensagensPorWavy.ContainsKey(wavyId))
                    mensagensPorWavy[wavyId] = new List<MensagemInfo>();

                mensagensPorWavy[wavyId].Add(new MensagemInfo { Dados = dadosProcessados, Hora = DateTime.Now.ToString("HH:mm:ss") });

                Console.WriteLine($"[AGREGADOR] {wavyId}: {mensagensPorWavy[wavyId].Count}/{config.VolumeDados} mensagens");

                if (mensagensPorWavy[wavyId].Count >= config.VolumeDados)
                {
                    var mensagensParaEnviar = new List<MensagemInfo>(mensagensPorWavy[wavyId]);
                    Task.Run(() => EnviarParaServidor(mensagensParaEnviar, config, wavyId));
                    mensagensPorWavy[wavyId].Clear();
                }
            }
        }

        static async void EnviarParaServidor(List<MensagemInfo> mensagens, ConfiguracaoWavy config, string wavyId)
        {
            Console.WriteLine($"[ENVIAR] Iniciando envio de {mensagens.Count} mensagens de {wavyId} para {config.Servidor}:6000");

            int sucessos = 0;
            foreach (var mensagem in mensagens)
            {
                TcpClient client = null;
                NetworkStream stream = null;

                try
                {
                    client = new TcpClient();
                    client.ReceiveTimeout = 5000;
                    client.SendTimeout = 5000;

                    await client.ConnectAsync(config.Servidor, 6000);
                    stream = client.GetStream();

                    byte[] data = Encoding.ASCII.GetBytes(mensagem.Dados);
                    await stream.WriteAsync(data, 0, data.Length);

                    // Ler resposta
                    byte[] buffer = new byte[1024];
                    int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        string resposta = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                        Console.WriteLine($"[SERVIDOR] Resposta: {resposta}");
                        sucessos++;
                        Interlocked.Increment(ref mensagensEnviadas);
                    }

                    await Task.Delay(50); // Pequena pausa
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO] Falha ao enviar para {config.Servidor}: {ex.Message}");
                }
                finally
                {
                    try
                    {
                        stream?.Close();
                        client?.Close();
                    }
                    catch { }
                }
            }

            Console.WriteLine($"[ENVIAR] {wavyId}: {sucessos}/{mensagens.Count} mensagens enviadas com sucesso");
        }

        static string ExtrairWavyId(string mensagem)
        {
            try
            {
                // Tentar JSON primeiro
                if (mensagem.Trim().StartsWith("{"))
                {
                    using var doc = JsonDocument.Parse(mensagem);
                    return doc.RootElement.GetProperty("wavyId").GetString() ?? "";
                }

                // Tentar CSV
                if (mensagem.Contains(","))
                {
                    var partes = mensagem.Split(',');
                    if (partes.Length >= 1) return partes[0];
                }

                // Tentar TXT
                if (mensagem.Contains("WAVY ID:"))
                {
                    var inicio = mensagem.IndexOf("WAVY ID:") + 8;
                    var fim = mensagem.IndexOf(" ", inicio);
                    if (fim == -1) fim = mensagem.IndexOf("|", inicio);
                    if (fim > inicio) return mensagem.Substring(inicio, fim - inicio).Trim();
                }

                // Tentar XML
                if (mensagem.Contains("<wavyId>"))
                {
                    var inicio = mensagem.IndexOf("<wavyId>") + 8;
                    var fim = mensagem.IndexOf("</wavyId>", inicio);
                    if (fim > inicio) return mensagem.Substring(inicio, fim - inicio);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERRO] Ao extrair wavyId: {ex.Message}");
            }
            return "";
        }
    }
}