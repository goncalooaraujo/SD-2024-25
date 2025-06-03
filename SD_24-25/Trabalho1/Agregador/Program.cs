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
        static Timer flushTimer; // Timer para enviar mensagens pendentes
        static GrpcChannel rpcChannel;
        static PreProcessamento.PreProcessamentoClient rpcClient;
        static int mensagensRecebidas = 0;
        static int mensagensEnviadas = 0;
        static int errosProcessamento = 0;
        static Dictionary<string, int> errosPorFormato = new()
        {
            { "json", 0 },
            { "xml", 0 },
            { "csv", 0 },
            { "txt", 0 },
            { "outro", 0 }
        };

        static async Task Main()
        {
            Console.WriteLine("=== AGREGADOR INICIANDO ===");

            // Debug: mostrar informações do diretório
            MostrarInformacoesDiretorio();

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

            // Timer para enviar mensagens pendentes a cada 30 segundos
            flushTimer = new Timer(_ => EnviarMensagensPendentes(), null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

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

                        // Detectar formato para logging
                        string formato = DetectarFormato(mensagem);
                        Console.WriteLine($"[AGREGADOR] #{mensagensRecebidas} Mensagem recebida ({formato}): {mensagem}");

                        // Enviar diretamente para pré-processamento
                        await ProcessarMensagemComPreProcessamento(mensagem, formato);
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
                    int pendentes = ContarMensagensPendentes();
                    Console.WriteLine($"[STATUS] Recebidas: {mensagensRecebidas} | Enviadas: {mensagensEnviadas} | Erros: {errosProcessamento} | Pendentes: {pendentes}");
                }, null, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(30));

                // Comando manual para forçar envio
                Task.Run(() => MonitorarComandos());

                await Task.Delay(Timeout.Infinite);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RABBITMQ] ERRO: {ex.Message}");
            }
        }

        static string DetectarFormato(string mensagem)
        {
            mensagem = mensagem.Trim();
            if (mensagem.StartsWith("{") && mensagem.EndsWith("}"))
                return "json";
            else if (mensagem.StartsWith("<") && mensagem.EndsWith(">"))
                return "xml";
            else if (mensagem.Split(',').Length == 5 || mensagem.Split(',').Length == 6) // Accept both 5 and 6 fields
                return "csv";
            else if (mensagem.Contains("WAVY ID:") && mensagem.Contains("Tipo:"))
                return "txt";
            else
                return "outro";
        }

        static async Task ProcessarMensagemComPreProcessamento(string mensagem, string formato)
        {
            if (rpcClient == null)
            {
                Console.WriteLine("[AGREGADOR] Pré-processamento não disponível, ignorando mensagem");
                return;
            }

            try
            {
                // Enviar para pré-processamento com tipo padrão (será substituído depois)
                Console.WriteLine($"[AGREGADOR] Enviando mensagem formato {formato} para pré-processamento");

                var respostaRpc = await rpcClient.ProcessarDadosAsync(
                    new DadosBrutos
                    {
                        Dados = mensagem,
                        TipoProcessamento = "none" // Valor temporário
                    });

                Console.WriteLine($"[AGREGADOR] Resposta do pré-processamento: {respostaRpc.Dados}");

                // Verificar se o pré-processamento foi bem-sucedido
                using JsonDocument doc = JsonDocument.Parse(respostaRpc.Dados);
                var root = doc.RootElement;

                // Verificar se houve erro no pré-processamento
                if (root.TryGetProperty("success", out var successProp) && !successProp.GetBoolean())
                {
                    string errorMsg = root.TryGetProperty("error", out var errorProp) ?
                                     errorProp.GetString() ?? "Unknown error" : "Unknown error";
                    Console.WriteLine($"[AGREGADOR] Erro no pré-processamento ({formato}): {errorMsg}");
                    Interlocked.Increment(ref errosProcessamento);

                    // Incrementar contador de erros por formato
                    lock (errosPorFormato)
                    {
                        if (errosPorFormato.ContainsKey(formato))
                            errosPorFormato[formato]++;
                        else
                            errosPorFormato["outro"]++;
                    }

                    // Tentar processar manualmente se for CSV (fallback)
                    if (formato == "csv")
                    {
                        await ProcessarCSVManualmente(mensagem);
                    }

                    return;
                }

                // Extrair wavyId do resultado processado
                if (!root.TryGetProperty("wavyId", out var wavyIdProp))
                {
                    Console.WriteLine("[AGREGADOR] Resposta do pré-processamento não contém wavyId");
                    Interlocked.Increment(ref errosProcessamento);
                    return;
                }

                string wavyId = wavyIdProp.GetString() ?? "";
                if (string.IsNullOrEmpty(wavyId))
                {
                    Console.WriteLine("[AGREGADOR] wavyId vazio na resposta do pré-processamento");
                    Interlocked.Increment(ref errosProcessamento);
                    return;
                }

                // Obter configuração específica para este wavyId
                ConfiguracaoWavy config;
                lock (configLock)
                {
                    config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();
                }

                // Se o tipo de processamento for diferente do padrão, reprocessar com o tipo correto
                if (config.PreProcessamento != "none")
                {
                    respostaRpc = await rpcClient.ProcessarDadosAsync(
                        new DadosBrutos
                        {
                            Dados = mensagem,
                            TipoProcessamento = config.PreProcessamento
                        });
                }

                // Adicionar à fila de mensagens para este wavyId
                await AdicionarAoBatch(wavyId, respostaRpc.Dados);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AGREGADOR] Erro ao processar com pré-processamento: {ex.Message}");
                Interlocked.Increment(ref errosProcessamento);
            }
        }

        // Método de fallback para processar CSV manualmente
        static async Task ProcessarCSVManualmente(string mensagem)
        {
            try
            {
                Console.WriteLine("[AGREGADOR] Tentando processar CSV manualmente");
                var partes = mensagem.Split(',');
                if (partes.Length != 5)
                {
                    Console.WriteLine("[AGREGADOR] CSV inválido (não tem 5 campos)");
                    return;
                }

                string wavyId = partes[0].Trim();
                string tipo = partes[1].Trim();
                string valor = partes[2].Trim();
                string unidade = partes[3].Trim();
                string hora = partes[4].Trim();

                if (string.IsNullOrEmpty(wavyId))
                {
                    Console.WriteLine("[AGREGADOR] CSV com wavyId vazio");
                    return;
                }

                // Criar JSON manualmente
                var jsonObj = new
                {
                    wavyId = wavyId,
                    tipo = tipo,
                    valor = valor,
                    unidade = unidade,
                    hora = hora,
                    processedAt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                    success = true,
                    processedManually = true
                };

                string jsonResult = JsonSerializer.Serialize(jsonObj, new JsonSerializerOptions
                {
                    WriteIndented = true
                });

                Console.WriteLine($"[AGREGADOR] CSV processado manualmente: {jsonResult}");

                // Obter configuração específica para este wavyId
                ConfiguracaoWavy config;
                lock (configLock)
                {
                    config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();
                }

                // Aplicar transformação conforme configuração
                if (config.PreProcessamento != "none")
                {
                    switch (config.PreProcessamento)
                    {
                        case "uppercase":
                            valor = valor.ToUpper();
                            break;
                        case "lowercase":
                            valor = valor.ToLower();
                            break;
                        case "normalize":
                            valor = valor.Trim().ToLower();
                            break;
                    }

                    // Atualizar o JSON com o valor processado
                    jsonObj = new
                    {
                        wavyId = wavyId,
                        tipo = tipo,
                        valor = valor,
                        unidade = unidade,
                        hora = hora,
                        processedAt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                        success = true,
                        processedManually = true
                    };

                    jsonResult = JsonSerializer.Serialize(jsonObj, new JsonSerializerOptions
                    {
                        WriteIndented = true
                    });
                }

                // Adicionar à fila de mensagens para este wavyId
                await AdicionarAoBatch(wavyId, jsonResult);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AGREGADOR] Erro ao processar CSV manualmente: {ex.Message}");
            }
        }

        static async Task AdicionarAoBatch(string wavyId, string dadosProcessados)
        {
            ConfiguracaoWavy config;
            lock (configLock)
            {
                config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();
            }

            lock (mensagensLock)
            {
                if (!mensagensPorWavy.ContainsKey(wavyId))
                    mensagensPorWavy[wavyId] = new List<MensagemInfo>();

                mensagensPorWavy[wavyId].Add(new MensagemInfo
                {
                    Dados = dadosProcessados,
                    Hora = DateTime.Now.ToString("HH:mm:ss")
                });

                Console.WriteLine($"[AGREGADOR] {wavyId}: {mensagensPorWavy[wavyId].Count}/{config.VolumeDados} mensagens");

                // Se atingiu o volume de dados configurado, enviar o lote
                if (mensagensPorWavy[wavyId].Count >= config.VolumeDados)
                {
                    var mensagensParaEnviar = new List<MensagemInfo>(mensagensPorWavy[wavyId]);
                    Task.Run(() => EnviarParaServidor(mensagensParaEnviar, config, wavyId));
                    mensagensPorWavy[wavyId].Clear();
                }
            }
        }

        static void MonitorarComandos()
        {
            while (true)
            {
                try
                {
                    Console.WriteLine("\n[COMANDO] Digite 'flush' para enviar mensagens pendentes ou 'status' para ver estatísticas:");
                    var comando = Console.ReadLine()?.Trim().ToLower();

                    switch (comando)
                    {
                        case "flush":
                            Console.WriteLine("[COMANDO] Forçando envio de mensagens pendentes...");
                            EnviarMensagensPendentes();
                            break;
                        case "status":
                            MostrarStatus();
                            break;
                        case "exit":
                        case "quit":
                            Environment.Exit(0);
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[COMANDO] Erro: {ex.Message}");
                }
            }
        }

        static void MostrarStatus()
        {
            lock (mensagensLock)
            {
                Console.WriteLine("\n=== STATUS DETALHADO ===");
                Console.WriteLine($"Mensagens recebidas: {mensagensRecebidas}");
                Console.WriteLine($"Mensagens enviadas: {mensagensEnviadas}");
                Console.WriteLine($"Erros de processamento: {errosProcessamento}");

                Console.WriteLine("\nErros por formato:");
                foreach (var kvp in errosPorFormato)
                {
                    Console.WriteLine($"  {kvp.Key}: {kvp.Value}");
                }

                Console.WriteLine($"\nMensagens pendentes: {ContarMensagensPendentes()}");
                Console.WriteLine("\nMensagens por WAVY:");

                foreach (var kvp in mensagensPorWavy)
                {
                    var config = configuracoes.ContainsKey(kvp.Key) ? configuracoes[kvp.Key] : new ConfiguracaoWavy();
                    Console.WriteLine($"  {kvp.Key}: {kvp.Value.Count}/{config.VolumeDados} mensagens");
                }
                Console.WriteLine("========================\n");
            }
        }

        static int ContarMensagensPendentes()
        {
            lock (mensagensLock)
            {
                int total = 0;
                foreach (var lista in mensagensPorWavy.Values)
                {
                    total += lista.Count;
                }
                return total;
            }
        }

        static void EnviarMensagensPendentes()
        {
            Console.WriteLine("[FLUSH] Verificando mensagens pendentes...");

            lock (mensagensLock)
            {
                var wavysComMensagens = new List<string>();
                foreach (var kvp in mensagensPorWavy)
                {
                    if (kvp.Value.Count > 0)
                    {
                        wavysComMensagens.Add(kvp.Key);
                    }
                }

                foreach (var wavyId in wavysComMensagens)
                {
                    var mensagens = mensagensPorWavy[wavyId];
                    if (mensagens.Count > 0)
                    {
                        var config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();
                        Console.WriteLine($"[FLUSH] Enviando {mensagens.Count} mensagens pendentes de {wavyId}");

                        var mensagensParaEnviar = new List<MensagemInfo>(mensagens);
                        Task.Run(() => EnviarParaServidor(mensagensParaEnviar, config, wavyId));
                        mensagens.Clear();
                    }
                }
            }
        }

        static void MostrarInformacoesDiretorio()
        {
            try
            {
                string diretorioAtual = Directory.GetCurrentDirectory();
                string diretorioExecutavel = AppDomain.CurrentDomain.BaseDirectory;

            }
            catch (Exception ex)
            {
            }
        }

        static void LerConfiguracoes(string nomeArquivo)
        {
            try
            {
                // Tentar múltiplos caminhos
                string[] caminhosPossiveis = {
                    nomeArquivo, // Diretório atual
                    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, nomeArquivo), // Diretório do executável
                    Path.Combine(Directory.GetCurrentDirectory(), nomeArquivo), // Diretório de trabalho
                    Path.Combine(Environment.CurrentDirectory, nomeArquivo) // Diretório do ambiente
                };

                string caminhoEncontrado = null;
                foreach (var caminho in caminhosPossiveis)
                {
                    if (File.Exists(caminho))
                    {
                        caminhoEncontrado = caminho;
                        break;
                    }
                }

                if (caminhoEncontrado == null)
                {
                    CriarArquivoConfigPadrao(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, nomeArquivo));
                    return;
                }

                var novasConfigs = new Dictionary<string, ConfiguracaoWavy>();
                var linhas = File.ReadAllLines(caminhoEncontrado);


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
                    else
                    {
                    }
                }

                lock (configLock) configuracoes = novasConfigs;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CONFIG] Erro: {ex.Message}");
            }
        }

        static void CriarArquivoConfigPadrao(string caminho)
        {
            try
            {
                var configPadrao = new StringBuilder();
                configPadrao.AppendLine("# Arquivo de configuração do Agregador");
                configPadrao.AppendLine("# Formato: wavyId:preProcessamento:volumeDados:servidor");
                configPadrao.AppendLine("WAVY_001:none:3:localhost");
                configPadrao.AppendLine("WAVY_002:uppercase:5:localhost");
                configPadrao.AppendLine("WAVY_003:normalize:2:localhost");
                configPadrao.AppendLine("WAVY_004:none:4:localhost");
                configPadrao.AppendLine("WAVY_005:lowercase:3:localhost");

                File.WriteAllText(caminho, configPadrao.ToString());
                Console.WriteLine($"[CONFIG] Arquivo padrão criado em: {caminho}");

                // Recarregar as configurações
                LerConfiguracoes(Path.GetFileName(caminho));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CONFIG] Erro ao criar arquivo padrão: {ex.Message}");
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
    }
}