using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using System.Collections.Generic;

namespace Wavy
{
    class Program
    {
        static readonly string[] tipos = { "temperatura", "pressao", "salinidade", "corrente" };
        static readonly string[] formatos = { "json", "csv", "xml", "txt" };
        static readonly Random random = new Random();

        // Dicionário para manter estado dos sensores (simular tendências)
        static Dictionary<string, Dictionary<string, double>> estadoSensores = new Dictionary<string, Dictionary<string, double>>();
        static readonly object estadoLock = new object();

        static readonly DateTime dataInicial = new DateTime(2000, 1, 1);
        static readonly DateTime dataFinal = DateTime.Now;

        static void Main(string[] args)
        {
            Console.Write("Quantas WAVYs queres simular? ");
            if (!int.TryParse(Console.ReadLine(), out int quantidade) || quantidade <= 0)
            {
                Console.WriteLine("Valor inválido.");
                return;
            }

            Console.WriteLine($"Iniciando {quantidade} WAVY(s)...");

            for (int i = 0; i < quantidade; i++)
            {
                string wavyId = $"WAVY_{i + 1:D3}";
                InicializarEstadoSensor(wavyId);
                Thread wavyThread = new Thread(() => IniciarWavy(wavyId));
                wavyThread.Start();

                // Pequena pausa para evitar sobrecarga inicial
                Thread.Sleep(100);
            }

        }

        static void InicializarEstadoSensor(string wavyId)
        {
            lock (estadoLock)
            {
                estadoSensores[wavyId] = new Dictionary<string, double>
                {
                    ["temperatura"] = 18 + random.NextDouble() * 6, // 18-24°C inicial
                    ["pressao"] = 1010 + random.Next(30), // 1010-1040 hPa inicial
                    ["salinidade"] = 32 + random.NextDouble() * 3, // 32-35 PSU inicial
                    ["corrente"] = random.NextDouble() * 1.5 // 0-1.5 m/s inicial
                };
            }
        }

        static void IniciarWavy(string wavyId)
        {
            try
            {
                var factory = new ConnectionFactory() { HostName = "localhost" };
                using var connection = factory.CreateConnection();
                using var channel = connection.CreateModel();

                channel.ExchangeDeclare(exchange: "sensores", type: ExchangeType.Topic);

                Console.WriteLine($"[{wavyId}] Iniciado - enviando dados realistas...");

                for (int i = 0; i < 10; i++)
                {
                    string tipo = tipos[random.Next(tipos.Length)];
                    (double valor, string unidade) = GerarCaracteristicaRealista(wavyId, tipo);

                    // Gerar data aleatória dentro do intervalo configurado
                    DateTime dataAleatoria = GerarDataAleatoria();
                    string dataFormatada = dataAleatoria.ToString("dd/MM/yyyy");

                    string formato = formatos[random.Next(formatos.Length)];
                    string mensagem = GerarMensagem(formato, wavyId, tipo, valor, unidade, dataFormatada, dataAleatoria);

                    var body = Encoding.UTF8.GetBytes(mensagem);
                    channel.BasicPublish(exchange: "sensores", routingKey: tipo, basicProperties: null, body: body);

                    Console.WriteLine($"[{wavyId}] #{i + 1} Publicado ({formato}) {tipo}: {valor:F2} {unidade} em {dataFormatada}");

                    // Intervalo aleatório entre 800ms e 3000ms para simular variação real
                    Thread.Sleep(random.Next(800, 3000));
                }

                Console.WriteLine($"[{wavyId}] Concluído - 10 mensagens enviadas");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{wavyId}] ERRO: {ex.Message}");
            }
        }

        static DateTime GerarDataAleatoria()
        {
            // Calcular intervalo total em ticks
            long intervaloTicks = dataFinal.Ticks - dataInicial.Ticks;

            // Gerar um número aleatório de ticks dentro do intervalo
            long ticksAleatorios = (long)(random.NextDouble() * intervaloTicks);

            // Criar data aleatória
            DateTime dataAleatoria = new DateTime(dataInicial.Ticks + ticksAleatorios);

            // Retornar apenas a data (sem a hora)
            return dataAleatoria.Date;
        }

        static (double, string) GerarCaracteristicaRealista(string wavyId, string tipo)
        {
            lock (estadoLock)
            {
                double valorAtual = estadoSensores[wavyId][tipo];
                double novoValor;
                string unidade;

                switch (tipo)
                {
                    case "temperatura":
                        // Temperatura oceânica: 5°C a 30°C com flutuações graduais
                        double deltaTemp = (random.NextDouble() - 0.5) * 2; // -1 a +1°C de variação
                        novoValor = Math.Max(5, Math.Min(30, valorAtual + deltaTemp));

                        // Ocasionalmente, mudanças mais bruscas (correntes, profundidade)
                        if (random.NextDouble() < 0.1) // 10% chance
                        {
                            novoValor += (random.NextDouble() - 0.5) * 8; // -4 a +4°C adicional
                            novoValor = Math.Max(5, Math.Min(30, novoValor));
                        }

                        unidade = "°C";
                        break;

                    case "pressao":
                        // Pressão atmosférica: 980 a 1050 hPa
                        double deltaPressao = (random.NextDouble() - 0.5) * 6; // -3 a +3 hPa
                        novoValor = Math.Max(980, Math.Min(1050, valorAtual + deltaPressao));

                        // Sistemas meteorológicos podem causar mudanças maiores
                        if (random.NextDouble() < 0.05) // 5% chance
                        {
                            novoValor += (random.NextDouble() - 0.5) * 40; // -20 a +20 hPa adicional
                            novoValor = Math.Max(980, Math.Min(1050, novoValor));
                        }

                        unidade = "hPa";
                        break;

                    case "salinidade":
                        // Salinidade oceânica: 28 a 38 PSU
                        double deltaSalinidade = (random.NextDouble() - 0.5) * 0.8; // -0.4 a +0.4 PSU
                        novoValor = Math.Max(28, Math.Min(38, valorAtual + deltaSalinidade));

                        // Influência de chuva ou evaporação intensa
                        if (random.NextDouble() < 0.08) // 8% chance
                        {
                            novoValor += (random.NextDouble() - 0.5) * 4; // -2 a +2 PSU adicional
                            novoValor = Math.Max(28, Math.Min(38, novoValor));
                        }

                        unidade = "PSU";
                        break;

                    case "corrente":
                        // Velocidade da corrente: 0 a 3 m/s
                        double deltaCorrente = (random.NextDouble() - 0.5) * 0.6; // -0.3 a +0.3 m/s
                        novoValor = Math.Max(0, Math.Min(3, valorAtual + deltaCorrente));

                        // Correntes podem ter picos devido a marés ou tempestades
                        if (random.NextDouble() < 0.12) // 12% chance
                        {
                            novoValor += random.NextDouble() * 1.5; // 0 a +1.5 m/s adicional
                            novoValor = Math.Min(3, novoValor);
                        }

                        unidade = "m/s";
                        break;

                    default:
                        novoValor = 0;
                        unidade = "";
                        break;
                }

                // Atualizar estado do sensor
                estadoSensores[wavyId][tipo] = novoValor;

                // Adicionar pequeno ruído de sensor (±0.1% do valor)
                double ruido = novoValor * (random.NextDouble() - 0.5) * 0.002;
                novoValor += ruido;

                return (novoValor, unidade);
            }
        }

        static string GerarMensagem(string formato, string wavyId, string tipo, double valor, string unidade, string data, DateTime timestamp)
        {
            // Adicionar algumas variações no formato dos dados
            string valorFormatado = valor.ToString("F2").Replace(",", ".");

            // Ocasionalmente usar mais ou menos casas decimais
            if (random.NextDouble() < 0.3) // 30% chance
            {
                valorFormatado = valor.ToString("F3").Replace(",", ".");
            }
            else if (random.NextDouble() < 0.1) // 10% chance
            {
                valorFormatado = valor.ToString("F1").Replace(",", ".");
            }

            // Usar timestamp da data aleatória em vez da data atual
            long unixTimestamp = new DateTimeOffset(timestamp).ToUnixTimeSeconds();

            return formato.ToLower() switch
            {
                "json" => $@"{{""wavyId"":""{wavyId}"",""tipo"":""{tipo}"",""valor"":{valorFormatado},""unidade"":""{unidade}"",""data"":""{data}"",""timestamp"":{unixTimestamp}}}",

                "csv" => $"{wavyId},{tipo},{valorFormatado},{unidade},{data}",

                "xml" => $@"<mensagem><wavyId>{wavyId}</wavyId><tipo>{tipo}</tipo><valor>{valorFormatado}</valor><unidade>{unidade}</unidade><data>{data}</data></mensagem>",

                "txt" => $"WAVY ID: {wavyId} | Tipo: {tipo} | Valor: {valorFormatado} {unidade} | Data: {data}",

                _ => $@"{{""wavyId"":""{wavyId}"",""tipo"":""{tipo}"",""valor"":{valorFormatado},""unidade"":""{unidade}"",""data"":""{data}""}}"
            };
        }
    }
}
