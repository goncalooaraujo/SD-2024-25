using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;

namespace Wavy
{
    class Program
    {
        static readonly string[] tipos = { "temperatura", "pressao", "salinidade", "corrente" };
        static readonly string[] formatos = { "json", "csv", "xml", "txt" };
        static readonly Random random = new Random();

        static void Main(string[] args)
        {
            Console.Write("Quantas WAVYs queres simular? ");
            if (!int.TryParse(Console.ReadLine(), out int quantidade) || quantidade <= 0)
            {
                Console.WriteLine("Valor inválido.");
                return;
            }

            for (int i = 0; i < quantidade; i++)
            {
                string wavyId = $"WAVY_{i + 1:D3}";
                Thread wavyThread = new Thread(() => IniciarWavy(wavyId));
                wavyThread.Start();
            }
        }

        static void IniciarWavy(string wavyId)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.ExchangeDeclare(exchange: "sensores", type: ExchangeType.Topic);

            for (int i = 0; i < 10; i++)
            {
                string tipo = tipos[random.Next(tipos.Length)];
                (double valor, string unidade) = GerarCaracteristica(tipo);
                string hora = DateTime.Now.ToString("HH:mm:ss");
                string formato = formatos[random.Next(formatos.Length)];
                string mensagem = GerarMensagem(formato, wavyId, tipo, valor, unidade, hora);

                var body = Encoding.UTF8.GetBytes(mensagem);
                channel.BasicPublish(exchange: "sensores", routingKey: tipo, basicProperties: null, body: body);

                Console.WriteLine($"[{wavyId}] Publicado no tópico '{tipo}':\n{mensagem}\n");
                Thread.Sleep(1000);
            }
        }

        static string GerarMensagem(string formato, string wavyId, string tipo, double valor, string unidade, string hora)
        {
            return formato.ToLower() switch
            {
                "json" => $@"{{
                ""wavyId"": ""{wavyId}"",
                ""tipo"": ""{tipo}"",
                ""valor"": {valor.ToString("F2").Replace(",", ".")},
                ""unidade"": ""{unidade}"",
                ""hora"": ""{hora}""
                }}",

                "csv" => $"{wavyId},{tipo},{valor.ToString("F2").Replace(",", ".")},{unidade},{hora}",

                "xml" => $@"<mensagem>
                <wavyId>{wavyId}</wavyId>
                <tipo>{tipo}</tipo>
                <valor>{valor.ToString("F2").Replace(",", ".")}</valor>
                <unidade>{unidade}</unidade>
                <hora>{hora}</hora>
                </mensagem>",

                "txt" => $"WAVY ID: {wavyId} | Tipo: {tipo} | Valor: {valor.ToString("F2").Replace(",", ".")} {unidade} | Hora: {hora}",

                _ => "{}" // fallback
            };
        }

        static (double, string) GerarCaracteristica(string tipo)
        {
            return tipo switch
            {
                "temperatura" => (15 + random.NextDouble() * 10, "C"),
                "pressao" => (1000 + random.Next(50), "hPa"),
                "salinidade" => (30 + random.NextDouble() * 5, "PSU"),
                "corrente" => (random.NextDouble() * 2, "m/s"),
                _ => (0, "")
            };
        }
    }
}