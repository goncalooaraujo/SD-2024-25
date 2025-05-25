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
            int port = 5000;

            try
            {
                Console.WriteLine($"[{wavyId}] Tentando conectar ao agregador {serverIp}:{port}...");
                TcpClient client = new TcpClient(serverIp, port);
                Console.WriteLine($"[{wavyId}] Conexão estabelecida");

                NetworkStream stream = client.GetStream();

                string startMessage = "HELLO:" + wavyId;
                byte[] startData = Encoding.ASCII.GetBytes(startMessage);
                Console.WriteLine($"[{wavyId}] Enviando mensagem HELLO: {startMessage}");
                stream.Write(startData, 0, startData.Length);

                // Ler resposta do HELLO
                byte[] helloBuffer = new byte[256];
                int helloBytes = stream.Read(helloBuffer, 0, helloBuffer.Length);
                string helloResponse = Encoding.ASCII.GetString(helloBuffer, 0, helloBytes);
                Console.WriteLine($"[{wavyId}] Resposta ao HELLO: {helloResponse}");

                for (int i = 0; i < 10; i++)
                {
                    string tipo = tipos[random.Next(tipos.Length)];
                    string carac = GerarCaracteristica(tipo);
                    string hora = DateTime.Now.ToString("HH:mm:ss");

                    string message = $"WAVY_ID:{wavyId}|TIPO:{tipo}|CARAC:{carac}|HORA:{hora}";
                    byte[] data = Encoding.ASCII.GetBytes(message);
                    Console.WriteLine($"[{wavyId}] Enviando mensagem {i + 1}: {message}");
                    stream.Write(data, 0, data.Length);

                    byte[] buffer = new byte[256];
                    int bytesRead = stream.Read(buffer, 0, buffer.Length);
                    string response = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                    Console.WriteLine($"[{wavyId}] Resposta: {response}");

                    Thread.Sleep(1000);
                }

                string endMessage = ":" + wavyId;
                Console.WriteLine($"[{wavyId}] Enviando mensagem de término: {endMessage}");
                stream.Write(Encoding.ASCII.GetBytes(endMessage));
                client.Close();

                Console.WriteLine($"[{wavyId}] Comunicação terminada");
            }
            catch (Exception e)
            {
                Console.WriteLine($"[{wavyId}] Erro: " + e.Message);
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