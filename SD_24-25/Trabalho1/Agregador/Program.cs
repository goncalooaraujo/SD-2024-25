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

    class ConfiguracaoSistema
    {
        public ConfiguracaoWavy ConfiguracaoPadrao { get; set; } = new();
        public Dictionary<string, ConfiguracaoWavy> ConfiguracoesEspecificas { get; set; } = new();
    }

    class Program
    {
        static ConfiguracaoSistema configuracaoSistema = new();
        static Dictionary<string, List<MensagemInfo>> mensagensPorWavy = new();
        static readonly object configLock = new();
        static readonly object mensagensLock = new();
        static PreProcessamento.PreProcessamentoClient? rpcClient;
        static int mensagensRecebidas = 0;
        static int mensagensEnviadas = 0;

        // Configuration for message types per aggregator
        static readonly Dictionary<int, List<string>> tiposPorAgregador = new()
        {
            { 1, new List<string> { "pressao", "temperatura" } },
            { 2, new List<string> { "salinidade", "corrente" } }
        };

        static async Task Main()
        {
            Console.WriteLine("=== SISTEMA DE AGREGADORES INICIANDO ===");

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

            // Show aggregator configuration
            Console.WriteLine("\n=== CONFIGURAÇÃO DOS AGREGADORES ===");
            foreach (var kvp in tiposPorAgregador)
            {
                Console.WriteLine($"[AGREGADOR {kvp.Key}] Tipos: {string.Join(", ", kvp.Value)}");
            }

            Console.WriteLine("\n=== CONFIGURAÇÃO PADRÃO PARA TODOS OS WAVYs ===");
            Console.WriteLine($"[DEFAULT] Pré-processamento: {configuracaoSistema.ConfiguracaoPadrao.PreProcessamento}");
            Console.WriteLine($"[DEFAULT] Volume de dados: {configuracaoSistema.ConfiguracaoPadrao.VolumeDados}");
            Console.WriteLine($"[DEFAULT] Servidor: {configuracaoSistema.ConfiguracaoPadrao.Servidor}");

            if (configuracaoSistema.ConfiguracoesEspecificas.Count > 0)
            {
                Console.WriteLine($"[DEFAULT] Configurações específicas: {configuracaoSistema.ConfiguracoesEspecificas.Count} WAVYs");
            }
            Console.WriteLine("=====================================\n");

            // Auto-flush timer (every 30 seconds)
            var flushTimer = new Timer(_ => EnviarMensagensPendentes(), null,
                TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

            // Status timer
            var statusTimer = new Timer(_ =>
                Console.WriteLine($"[STATUS] Recebidas: {mensagensRecebidas} | Enviadas: {mensagensEnviadas}"),
                null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));

            // Configuration reload timer (every 5 minutes)
            var configTimer = new Timer(_ => LerConfiguracoes(), null,
                TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));

            // Start multiple aggregators
            List<Task> tasks = new();

            for (int i = 1; i <= 2; i++)
            {
                int agregadorId = i;
                tasks.Add(Task.Run(() => IniciarAgregador(agregadorId)));
            }

            await Task.WhenAll(tasks);
        }

        static async Task IniciarAgregador(int agregadorId)
        {
            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                using var connection = factory.CreateConnection();
                using var channel = connection.CreateModel();

                channel.ExchangeDeclare(exchange: "sensores", type: ExchangeType.Topic);
                string queueName = $"agregador_{agregadorId}";
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: true);

                // Bind only to specific message types for this aggregator
                if (tiposPorAgregador.ContainsKey(agregadorId))
                {
                    foreach (string tipo in tiposPorAgregador[agregadorId])
                    {
                        channel.QueueBind(queue: queueName, exchange: "sensores", routingKey: tipo);
                        Console.WriteLine($"[AGREGADOR {agregadorId}] Binding para tipo: {tipo}");
                    }
                }
                else
                {
                    Console.WriteLine($"[AGREGADOR {agregadorId}] AVISO: Nenhum tipo configurado");
                    return;
                }

                Console.WriteLine($"[AGREGADOR {agregadorId}] Conectado - Fila: {queueName}");
                Console.WriteLine($"[AGREGADOR {agregadorId}] Processando tipos: {string.Join(", ", tiposPorAgregador[agregadorId])}");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += async (model, ea) =>
                {
                    try
                    {
                        string mensagem = Encoding.UTF8.GetString(ea.Body.ToArray());
                        string routingKey = ea.RoutingKey;
                        Interlocked.Increment(ref mensagensRecebidas);

                        Console.WriteLine($"[AGREGADOR {agregadorId}] #{mensagensRecebidas} Tipo: {routingKey} | Mensagem: {mensagem}");

                        // Verify if this aggregator should process this type
                        if (!tiposPorAgregador[agregadorId].Contains(routingKey))
                        {
                            Console.WriteLine($"[AGREGADOR {agregadorId}] IGNORANDO tipo {routingKey} - não é responsabilidade deste agregador");
                            return;
                        }

                        await ProcessarMensagem(mensagem, agregadorId, routingKey);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[AGREGADOR {agregadorId}] ERRO: {ex.Message}");
                    }
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine($"[AGREGADOR {agregadorId}] Aguardando mensagens...");

                await Task.Delay(Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AGREGADOR {agregadorId}] ERRO: {ex.Message}");
            }
        }

        static async Task ProcessarMensagem(string mensagem, int agregadorId, string tipoMensagem)
        {
            if (rpcClient == null)
            {
                Console.WriteLine($"[AGREGADOR {agregadorId}] Pré-processamento não disponível");
                return;
            }

            try
            {
                Console.WriteLine($"[AGREGADOR {agregadorId}] Processando mensagem tipo '{tipoMensagem}'");

                var respostaRpc = await rpcClient.ProcessarDadosAsync(new DadosBrutos
                {
                    Dados = mensagem,
                    TipoProcessamento = "none"
                });

                using JsonDocument doc = JsonDocument.Parse(respostaRpc.Dados);
                var root = doc.RootElement;

                if (!root.TryGetProperty("success", out var successProp) || !successProp.GetBoolean())
                {
                    Console.WriteLine($"[AGREGADOR {agregadorId}] Erro no pré-processamento");
                    return;
                }

                if (!root.TryGetProperty("wavyId", out var wavyIdProp))
                {
                    Console.WriteLine($"[AGREGADOR {agregadorId}] wavyId não encontrado");
                    return;
                }

                string wavyId = wavyIdProp.GetString() ?? "";
                if (string.IsNullOrEmpty(wavyId)) return;

                // Get configuration for this wavyId (uses default if not specifically configured)
                ConfiguracaoWavy config = ObterConfiguracaoParaWavy(wavyId);

                // Show which configuration is being used
                bool isDefault = !configuracaoSistema.ConfiguracoesEspecificas.ContainsKey(wavyId);
                Console.WriteLine($"[AGREGADOR {agregadorId}] {wavyId} usando configuração {(isDefault ? "PADRÃO" : "ESPECÍFICA")}");

                // Reprocess with specific configuration if needed
                if (config.PreProcessamento != "none")
                {
                    respostaRpc = await rpcClient.ProcessarDadosAsync(new DadosBrutos
                    {
                        Dados = mensagem,
                        TipoProcessamento = config.PreProcessamento
                    });
                }

                await AdicionarAoBatch(wavyId, respostaRpc.Dados, config, agregadorId, tipoMensagem);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AGREGADOR {agregadorId}] Erro ao processar: {ex.Message}");
            }
        }

        static ConfiguracaoWavy ObterConfiguracaoParaWavy(string wavyId)
        {
            lock (configLock)
            {
                // Check if there's a specific configuration for this WAVY
                if (configuracaoSistema.ConfiguracoesEspecificas.ContainsKey(wavyId))
                {
                    return configuracaoSistema.ConfiguracoesEspecificas[wavyId];
                }

                // Return default configuration for any WAVY not specifically configured
                return configuracaoSistema.ConfiguracaoPadrao;
            }
        }

        static async Task AdicionarAoBatch(string wavyId, string dadosProcessados, ConfiguracaoWavy config, int agregadorId, string tipoMensagem)
        {
            // Create unique key combining wavyId, message type, and aggregator ID
            string chaveUnica = $"{wavyId}_{tipoMensagem}_{agregadorId}";

            lock (mensagensLock)
            {
                if (!mensagensPorWavy.ContainsKey(chaveUnica))
                    mensagensPorWavy[chaveUnica] = new List<MensagemInfo>();

                mensagensPorWavy[chaveUnica].Add(new MensagemInfo
                {
                    Dados = dadosProcessados,
                    Data = DateTime.Now.ToString("dd/MM/yyyy")
                });

                Console.WriteLine($"[AGREGADOR {agregadorId}] {wavyId} ({tipoMensagem}): {mensagensPorWavy[chaveUnica].Count}/{config.VolumeDados} mensagens");

                // Send batch if volume reached
                if (mensagensPorWavy[chaveUnica].Count >= config.VolumeDados)
                {
                    var mensagensParaEnviar = new List<MensagemInfo>(mensagensPorWavy[chaveUnica]);
                    Task.Run(() => EnviarParaServidor(mensagensParaEnviar, config, wavyId, agregadorId, tipoMensagem));
                    mensagensPorWavy[chaveUnica].Clear();
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
                        // Extract wavyId from the unique key (format: wavyId_tipoMensagem_agregadorId)
                        string[] partes = kvp.Key.Split('_');
                        string wavyId = partes.Length > 0 ? partes[0] : kvp.Key;
                        string tipoMensagem = partes.Length > 1 ? partes[1] : "unknown";
                        int agregadorId = partes.Length > 2 && int.TryParse(partes[2], out int id) ? id : 0;

                        ConfiguracaoWavy config = ObterConfiguracaoParaWavy(wavyId);
                        Console.WriteLine($"[FLUSH] Enviando {kvp.Value.Count} mensagens de {wavyId} ({tipoMensagem}) - Agregador {agregadorId}");

                        var mensagensParaEnviar = new List<MensagemInfo>(kvp.Value);
                        Task.Run(() => EnviarParaServidor(mensagensParaEnviar, config, wavyId, agregadorId, tipoMensagem));
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
                var novaConfiguracao = new ConfiguracaoSistema();

                if (!File.Exists(configFile))
                {
                    CriarArquivoConfigPadrao(configFile);
                    return;
                }

                var linhas = File.ReadAllLines(configFile);
                bool defaultConfigSet = false;

                foreach (var linha in linhas)
                {
                    if (string.IsNullOrWhiteSpace(linha) || linha.StartsWith("#"))
                        continue;

                    var partes = linha.Split(':');
                    if (partes.Length == 4)
                    {
                        string wavyId = partes[0].Trim();
                        var config = new ConfiguracaoWavy
                        {
                            PreProcessamento = partes[1].Trim(),
                            VolumeDados = int.TryParse(partes[2].Trim(), out int v) ? v : 3,
                            Servidor = partes[3].Trim()
                        };

                        // If wavyId is "DEFAULT", use it as the default configuration
                        if (wavyId.Equals("DEFAULT", StringComparison.OrdinalIgnoreCase))
                        {
                            novaConfiguracao.ConfiguracaoPadrao = config;
                            defaultConfigSet = true;
                            Console.WriteLine($"[CONFIG] Configuração padrão definida: {config.PreProcessamento}, {config.VolumeDados}, {config.Servidor}");
                        }
                        else
                        {
                            // Specific configuration for a particular WAVY
                            novaConfiguracao.ConfiguracoesEspecificas[wavyId] = config;
                            Console.WriteLine($"[CONFIG] Configuração específica para {wavyId}: {config.PreProcessamento}, {config.VolumeDados}, {config.Servidor}");
                        }
                    }
                }

                // If no default configuration was set in the file, use the built-in default
                if (!defaultConfigSet)
                {
                    Console.WriteLine("[CONFIG] Usando configuração padrão built-in");
                }

                lock (configLock)
                    configuracaoSistema = novaConfiguracao;

                Console.WriteLine($"[CONFIG] Sistema configurado - Padrão: OK, Específicas: {configuracaoSistema.ConfiguracoesEspecificas.Count}");
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
            configPadrao.AppendLine("# Use 'DEFAULT' como wavyId para configuração padrão aplicada a todos os WAVYs");
            configPadrao.AppendLine("# Configurações específicas sobrescrevem a configuração padrão");
            configPadrao.AppendLine("");
            configPadrao.AppendLine("# Configuração padrão para TODOS os WAVYs");
            configPadrao.AppendLine("DEFAULT:none:3:localhost");
            configPadrao.AppendLine("");
            configPadrao.AppendLine("# Configurações específicas (opcionais)");
            configPadrao.AppendLine("# WAVY_001:uppercase:5:localhost");
            configPadrao.AppendLine("# WAVY_002:normalize:2:192.168.1.100");

            File.WriteAllText(caminho, configPadrao.ToString());
            Console.WriteLine($"[CONFIG] Arquivo padrão criado: {caminho}");
            LerConfiguracoes();
        }

        static async void EnviarParaServidor(List<MensagemInfo> mensagens, ConfiguracaoWavy config, string wavyId, int agregadorId = 0, string tipoMensagem = "")
        {
            int sucessos = 0;
            string identificador = string.IsNullOrEmpty(tipoMensagem) ? wavyId : $"{wavyId} ({tipoMensagem})";

            Console.WriteLine($"[AGREGADOR {agregadorId}] Enviando {mensagens.Count} mensagens de {identificador} para {config.Servidor}:6000");

            foreach (var mensagem in mensagens)
            {
                try
                {
                    using var client = new TcpClient();
                    client.ReceiveTimeout = 5000;
                    client.SendTimeout = 5000;

                    await client.ConnectAsync(config.Servidor, 6000);
                    using var stream = client.GetStream();

                    byte[] data = Encoding.ASCII.GetBytes(mensagem.Dados ?? "");
                    await stream.WriteAsync(data, 0, data.Length);

                    // Read response
                    byte[] buffer = new byte[1024];
                    int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead > 0)
                    {
                        string resposta = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                        Console.WriteLine($"[SERVIDOR] Resposta para {identificador}: {resposta}");
                        sucessos++;
                        Interlocked.Increment(ref mensagensEnviadas);
                    }

                    await Task.Delay(50);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[AGREGADOR {agregadorId}] ERRO ao enviar: {ex.Message}");
                }
            }

            Console.WriteLine($"[AGREGADOR {agregadorId}] {identificador}: {sucessos}/{mensagens.Count} enviadas com sucesso");
        }
    }
}
