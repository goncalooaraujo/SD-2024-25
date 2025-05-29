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
                    int pendentes = ContarMensagensPendentes();
                    Console.WriteLine($"[STATUS] Recebidas: {mensagensRecebidas} | Enviadas: {mensagensEnviadas} | Pendentes: {pendentes}");
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
                Console.WriteLine($"Mensagens pendentes: {ContarMensagensPendentes()}");
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

                Console.WriteLine($"[DEBUG] Diretório atual: {diretorioAtual}");
                Console.WriteLine($"[DEBUG] Diretório do executável: {diretorioExecutavel}");

                Console.WriteLine("[DEBUG] Arquivos no diretório atual:");
                var arquivos = Directory.GetFiles(diretorioAtual);
                foreach (var arquivo in arquivos)
                {
                    Console.WriteLine($"[DEBUG] - {Path.GetFileName(arquivo)}");
                }

                if (diretorioAtual != diretorioExecutavel)
                {
                    Console.WriteLine("[DEBUG] Arquivos no diretório do executável:");
                    var arquivosExec = Directory.GetFiles(diretorioExecutavel);
                    foreach (var arquivo in arquivosExec)
                    {
                        Console.WriteLine($"[DEBUG] - {Path.GetFileName(arquivo)}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DEBUG] Erro ao listar arquivos: {ex.Message}");
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
                    Console.WriteLine($"[CONFIG] Tentando: {caminho}");
                    if (File.Exists(caminho))
                    {
                        caminhoEncontrado = caminho;
                        Console.WriteLine($"[CONFIG] Arquivo encontrado em: {caminho}");
                        break;
                    }
                }

                if (caminhoEncontrado == null)
                {
                    Console.WriteLine($"[CONFIG] Arquivo {nomeArquivo} não encontrado em nenhum local, criando arquivo padrão...");
                    CriarArquivoConfigPadrao(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, nomeArquivo));
                    return;
                }

                var novasConfigs = new Dictionary<string, ConfiguracaoWavy>();
                var linhas = File.ReadAllLines(caminhoEncontrado);

                Console.WriteLine($"[CONFIG] Lendo {linhas.Length} linhas do arquivo");

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
                        Console.WriteLine($"[CONFIG] {wavyId}: PreProc={partes[1].Trim()}, Volume={partes[2].Trim()}, Servidor={partes[3].Trim()}");
                    }
                    else
                    {
                        Console.WriteLine($"[CONFIG] Linha inválida ignorada: {linha}");
                    }
                }

                lock (configLock) configuracoes = novasConfigs;
                Console.WriteLine($"[CONFIG] Recarregado com sucesso - {novasConfigs.Count} configurações ativas");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CONFIG] Erro: {ex.Message}");
                Console.WriteLine($"[CONFIG] StackTrace: {ex.StackTrace}");
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
                configPadrao.AppendLine("WAVY_002:filtro:5:localhost");
                configPadrao.AppendLine("WAVY_003:normalizacao:2:localhost");
                configPadrao.AppendLine("WAVY_004:none:4:localhost");
                configPadrao.AppendLine("WAVY_005:none:3:localhost");

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

        static async Task ProcessarMensagem(string wavyId, string message)
        {
            ConfiguracaoWavy config;
            lock (configLock)
            {
                config = configuracoes.ContainsKey(wavyId) ? configuracoes[wavyId] : new ConfiguracaoWavy();
                if (!configuracoes.ContainsKey(wavyId))
                {
                    Console.WriteLine($"[CONFIG] Usando configuração padrão para {wavyId}");
                }
            }

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