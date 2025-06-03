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
    class MensagemInfo
    {
        public string? Dados { get; set; }
        public string? Data { get; set; }
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
        static PreProcessamento.PreProcessamentoClient? rpcClient;
        static int mensagensRecebidas = 0;
        static int mensagensEnviadas = 0;

        static async Task Main()
        {
            Console.WriteLine("=== AGREGADOR INICIANDO ===");

            // Initialize gRPC client
            try
            {
                var rpcChannel = GrpcChannel.ForAddress("http://localhost:50051");
                rpcClient = new PreProcessamento.PreProcessamentoClient(rpcChannel);
                Console.WriteLine("[RPC] Conexão estabelecida com pré-processamento");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RPC] AVISO: {ex.Message}");
            }

            // Load configurations
            LerConfiguracoes();

            // Auto-flush timer (every 30 seconds)
            var flushTimer = new Timer(_ => EnviarMensagensPendentes(), null,
                TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

            // Start RabbitMQ consumer
            await IniciarConsumidor();
        }

        static async Task IniciarConsumidor()
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

                    Console.WriteLine($"[AGREGADOR] #{mensagensRecebidas} Mensagem: {mensagem}");
                    await ProcessarMensagem(mensagem);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[AGREGADOR] ERRO: {ex.Message}");
                }
            };

            channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
            Console.WriteLine("AGREGADOR escutando mensagens...");

            // Status timer
            var statusTimer = new Timer(_ =>
                Console.WriteLine($"[STATUS] Recebidas: {mensagensRecebidas} | Enviadas: {mensagensEnviadas}"),
                null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));

            await Task.Delay(Timeout.Infinite);
        }

        static async Task ProcessarMensagem(string mensagem)
        {
            if (rpcClient == null)
            {
                Console.WriteLine("[AGREGADOR] Pré-processamento não disponível");
                return;
            }

            try
            {
                var respostaRpc = await rpcClient.ProcessarDadosAsync(new DadosBrutos
                {
                    Dados = mensagem,
                    TipoProcessamento = "none"
                });

                using JsonDocument doc = JsonDocument.Parse(respostaRpc.Dados);
                var root = doc.RootElement;

                if (!root.TryGetProperty("success", out var successProp) || !successProp.GetBoolean())
                {
                    Console.WriteLine("[AGREGADOR] Erro no pré-processamento");
                    return;
                }

                if (!root.TryGetProperty("wavyId", out var wavyIdProp))
                {
                    Console.WriteLine("[AGREGADOR] wavyId não encontrado");
                    return;
                }

                string wavyId = wavyIdProp.GetString() ?? "";
                if (string.IsNullOrEmpty(wavyId)) return;

                // Get configuration for this wavyId
                ConfiguracaoWavy config;
                lock (configLock)
                {
                    config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();
                }

                // Reprocess with specific configuration if needed
                if (config.PreProcessamento != "none")
                {
                    respostaRpc = await rpcClient.ProcessarDadosAsync(new DadosBrutos
                    {
                        Dados = mensagem,
                        TipoProcessamento = config.PreProcessamento
                    });
                }

                await AdicionarAoBatch(wavyId, respostaRpc.Dados, config);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AGREGADOR] Erro ao processar: {ex.Message}");
            }
        }

        static async Task AdicionarAoBatch(string wavyId, string dadosProcessados, ConfiguracaoWavy config)
        {
            lock (mensagensLock)
            {
                if (!mensagensPorWavy.ContainsKey(wavyId))
                    mensagensPorWavy[wavyId] = new List<MensagemInfo>();

                mensagensPorWavy[wavyId].Add(new MensagemInfo
                {
                    Dados = dadosProcessados,
                    Data = DateTime.Now.ToString("dd/MM/yyyy")
                });

                Console.WriteLine($"[AGREGADOR] {wavyId}: {mensagensPorWavy[wavyId].Count}/{config.VolumeDados} mensagens");

                // Send batch if volume reached
                if (mensagensPorWavy[wavyId].Count >= config.VolumeDados)
                {
                    var mensagensParaEnviar = new List<MensagemInfo>(mensagensPorWavy[wavyId]);
                    Task.Run(() => EnviarParaServidor(mensagensParaEnviar, config, wavyId));
                    mensagensPorWavy[wavyId].Clear();
                }
            }
        }

        static void EnviarMensagensPendentes()
        {
            lock (mensagensLock)
            {
                foreach (var kvp in mensagensPorWavy.ToList())
                {
                    if (kvp.Value.Count > 0)
                    {
                        var config = configuracoes.ContainsKey(kvp.Key) ? configuracoes[kvp.Key] : new ConfiguracaoWavy();
                        Console.WriteLine($"[FLUSH] Enviando {kvp.Value.Count} mensagens de {kvp.Key}");

                        var mensagensParaEnviar = new List<MensagemInfo>(kvp.Value);
                        Task.Run(() => EnviarParaServidor(mensagensParaEnviar, config, kvp.Key));
                        kvp.Value.Clear();
                    }
                }
            }
        }

        static void LerConfiguracoes()
        {
            try
            {
                string configFile = "config.csv";
                if (!File.Exists(configFile))
                {
                    CriarArquivoConfigPadrao(configFile);
                    return;
                }

                var novasConfigs = new Dictionary<string, ConfiguracaoWavy>();
                var linhas = File.ReadAllLines(configFile);

                foreach (var linha in linhas)
                {
                    if (string.IsNullOrWhiteSpace(linha) || linha.StartsWith("#"))
                        continue;

                    var partes = linha.Split(':');
                    if (partes.Length == 4)
                    {
                        string wavyId = partes[0].Trim();
                        novasConfigs[wavyId] = new ConfiguracaoWavy
                        {
                            PreProcessamento = partes[1].Trim(),
                            VolumeDados = int.TryParse(partes[2].Trim(), out int v) ? v : 3,
                            Servidor = partes[3].Trim()
                        };
                    }
                }

                lock (configLock)
                    configuracoes = novasConfigs;

                Console.WriteLine($"[CONFIG] {configuracoes.Count} configurações carregadas");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CONFIG] Erro: {ex.Message}");
            }
        }

        static void CriarArquivoConfigPadrao(string caminho)
        {
            var configPadrao = new StringBuilder();
            configPadrao.AppendLine("# Formato: wavyId:preProcessamento:volumeDados:servidor");
            configPadrao.AppendLine("WAVY_001:none:3:localhost");
            configPadrao.AppendLine("WAVY_002:uppercase:5:localhost");
            configPadrao.AppendLine("WAVY_003:normalize:2:localhost");

            File.WriteAllText(caminho, configPadrao.ToString());
            Console.WriteLine($"[CONFIG] Arquivo padrão criado: {caminho}");
            LerConfiguracoes();
        }

        static async void EnviarParaServidor(List<MensagemInfo> mensagens, ConfiguracaoWavy config, string wavyId)
        {
            int sucessos = 0;
            foreach (var mensagem in mensagens)
            {
                try
                {
                    using var client = new TcpClient();
                    client.ReceiveTimeout = 5000;
                    client.SendTimeout = 5000;

                    await client.ConnectAsync(config.Servidor, 6000);
                    using var stream = client.GetStream();

                    byte[] data = Encoding.ASCII.GetBytes(mensagem.Dados);
                    await stream.WriteAsync(data, 0, data.Length);

                    // Read response
                    byte[] buffer = new byte[1024];
                    int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        sucessos++;
                        Interlocked.Increment(ref mensagensEnviadas);
                    }

                    await Task.Delay(50);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO] Falha ao enviar: {ex.Message}");
                }
            }

            Console.WriteLine($"[ENVIAR] {wavyId}: {sucessos}/{mensagens.Count} enviadas");
        }
    }
}
