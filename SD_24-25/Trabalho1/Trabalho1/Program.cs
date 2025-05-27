using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;

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
                Thread wavyThread = new Thread(() => IniciarWavy(wavyId, ""));
                wavyThread.Start();
            }
        }

        static void IniciarWavy(string wavyId, string _)
        {
            string serverIp = "127.0.0.1";
            int port = 6000;

            try
            {
                Console.WriteLine($"[{wavyId}] Conectando ao servidor {serverIp}:{port}...");
                using TcpClient client = new TcpClient(serverIp, port);
                using NetworkStream stream = client.GetStream();
                Console.WriteLine($"[{wavyId}] Conectado com sucesso.");

                for (int i = 0; i < 10; i++)
                {
                    string tipo = tipos[random.Next(tipos.Length)];
                    (double valor, string unidade) = GerarCaracteristica(tipo);
                    string hora = DateTime.Now.ToString("HH:mm:ss");

                    string formato = formatos[random.Next(formatos.Length)]; // format per message
                    string mensagem = GerarMensagem(formato, wavyId, tipo, valor, unidade, hora);

                    byte[] data = Encoding.UTF8.GetBytes(mensagem);
                    Console.WriteLine($"[{wavyId}] Enviando mensagem {i + 1} ({formato.ToUpper()}):\n{mensagem}\n");
                    stream.Write(data, 0, data.Length);

                    byte[] buffer = new byte[512];
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[{wavyId}] Resposta: {response}");

                    Thread.Sleep(1000);
                }

                // Mensagem de término — pode continuar em JSON
                string fimMensagem = GerarMensagem("json", wavyId, "fim", 0, "", DateTime.Now.ToString("HH:mm:ss"));
                byte[] fimData = Encoding.UTF8.GetBytes(fimMensagem);
                stream.Write(fimData, 0, fimData.Length);
                Console.WriteLine($"[{wavyId}] Mensagem de término enviada.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"[{wavyId}] Erro: {e.Message}");
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

                "txt" => $"WAVY ID: {wavyId} | Tipo: {tipo} | Valor: {valor:F2} {unidade} | Hora: {hora}",

                _ => "{}" // fallback
            };
        }

        static (double valor, string unidade) GerarCaracteristica(string tipo)
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
