using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Wavy
{
    class Program
    {
        static readonly string[] tipos = { "temperatura", "pressao", "salinidade", "corrente" };
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
            string serverIp = "127.0.0.1";
            int port = 6000; // ✅ Corrigido para bater com o servidor

            try
            {
                Console.WriteLine($"[{wavyId}] Tentando conectar ao servidor {serverIp}:{port}...");
                TcpClient client = new TcpClient(serverIp, port);
                Console.WriteLine($"[{wavyId}] Conexão estabelecida");

                NetworkStream stream = client.GetStream();

                for (int i = 0; i < 10; i++)
                {
                    string tipo = tipos[random.Next(tipos.Length)];
                    string carac = GerarCaracteristica(tipo);
                    string hora = DateTime.Now.ToString("HH:mm:ss");

                    string json = $@"{{
    ""wavyId"": ""{wavyId}"",
    ""tipo"": ""{tipo}"",
    ""mensagens"": [
        {{
            ""Caracteristica"": ""{carac}"",
            ""Hora"": ""{hora}""
        }}
    ]
}}";

                    byte[] data = Encoding.ASCII.GetBytes(json);
                    Console.WriteLine($"[{wavyId}] Enviando JSON {i + 1}:\n{json}");
                    stream.Write(data, 0, data.Length);

                    byte[] buffer = new byte[256];
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    string response = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[{wavyId}] Resposta: {response}");

                    Thread.Sleep(1000);
                }

                // Enviar mensagem final (pode ser ignorada ou usada como sinal de término)
                string fimMessage = $@"{{
    ""wavyId"": ""{wavyId}"",
    ""tipo"": ""fim"",
    ""mensagens"": []
}}";
                byte[] fimData = Encoding.ASCII.GetBytes(fimMessage);
                stream.Write(fimData, 0, fimData.Length);
                Console.WriteLine($"[{wavyId}] Enviou mensagem de término.");

                client.Close();
                Console.WriteLine($"[{wavyId}] Comunicação encerrada");
            }
            catch (Exception e)
            {
                Console.WriteLine($"[{wavyId}] Erro: {e.Message}");
            }
        }

        static string GerarCaracteristica(string tipo)
        {
            return tipo switch
            {
                "temperatura" => (15 + random.NextDouble() * 10).ToString("F2") + " ºC",
                "pressao" => (1000 + random.Next(50)).ToString() + " hPa",
                "salinidade" => (30 + random.NextDouble() * 5).ToString("F2") + " PSU",
                "corrente" => (random.NextDouble() * 2).ToString("F2") + " m/s",
                _ => "0"
            };
        }
    }
}
