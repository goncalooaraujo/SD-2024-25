using System;
using System.Collections.Generic;
using System.IO;
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

        static async Task Main()
        {
            rpcChannel = GrpcChannel.ForAddress("http://localhost:50051");
            rpcClient = new PreProcessamento.PreProcessamentoClient(rpcChannel);
            LerConfiguracoes("config.csv");
            reloadTimer = new Timer(_ => LerConfiguracoes("config.csv"), null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.ExchangeDeclare(exchange: "sensores", type: ExchangeType.Topic);
            string queueName = channel.QueueDeclare().QueueName;
            channel.QueueBind(queue: queueName, exchange: "sensores", routingKey: "#");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                string mensagem = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine($"[AGREGADOR] Mensagem recebida: {mensagem}");
                string wavyId = ExtrairWavyId(mensagem);
                if (!string.IsNullOrEmpty(wavyId))
                {
                    await ProcessarMensagem(wavyId, mensagem);
                }
            };

            channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
            Console.WriteLine("AGREGADOR a escutar mensagens RabbitMQ...");
            await Task.Delay(Timeout.Infinite);
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
                        novasConfigs[partes[0]] = new ConfiguracaoWavy
                        {
                            PreProcessamento = partes[1],
                            VolumeDados = int.TryParse(partes[2], out int v) ? v : 3,
                            Servidor = partes[3]
                        };
                    }
                }
                lock (configLock) configuracoes = novasConfigs;
                Console.WriteLine("[CONFIG] Recarregado com sucesso.");
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
            try
            {
                var respostaRpc = await rpcClient.ProcessarDadosAsync(new DadosBrutos { Dados = message, TipoProcessamento = config.PreProcessamento });
                dadosProcessados = respostaRpc.Dados;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RPC] Erro: {ex.Message}");
                return;
            }

            string tipo = "";
            try
            {
                using var doc = JsonDocument.Parse(dadosProcessados);
                tipo = doc.RootElement.GetProperty("tipo").GetString() ?? "";
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[JSON] Erro: {ex.Message}");
                return;
            }

            lock (mensagensLock)
            {
                if (!mensagensPorWavy.ContainsKey(wavyId))
                    mensagensPorWavy[wavyId] = new List<MensagemInfo>();

                mensagensPorWavy[wavyId].Add(new MensagemInfo { Dados = dadosProcessados, Hora = DateTime.Now.ToString("HH:mm:ss") });

                if (mensagensPorWavy[wavyId].Count >= config.VolumeDados)
                {
                    EnviarParaServidor(tipo, mensagensPorWavy[wavyId], config, wavyId);
                    mensagensPorWavy[wavyId].Clear();
                }
            }
        }

        static void EnviarParaServidor(string tipo, List<MensagemInfo> mensagens, ConfiguracaoWavy config, string wavyId)
        {
            try
            {
                var jsonObj = new { tipo, wavyId, mensagens };
                string json = JsonSerializer.Serialize(jsonObj, new JsonSerializerOptions { WriteIndented = true });
                Console.WriteLine($"[ENVIAR] JSON:{json}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"[ERRO] Envio: {e.Message}");
            }
        }

        static string ExtrairWavyId(string mensagem)
        {
            try
            {
                using var doc = JsonDocument.Parse(mensagem);
                return doc.RootElement.GetProperty("wavyId").GetString() ?? "";
            }
            catch
            {
                return "";
            }
        }
    }
}