using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.IO;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Servidor
{
    class Mensagem
    {
        [BsonElement("caracteristica")]
        public string? Caracteristica { get; set; }

        [BsonElement("hora")]
        public string? Hora { get; set; }
    }

    class Modelo
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonElement("wavyId")]
        public string? WavyId { get; set; }

        [BsonElement("tipo")]
        public string? Tipo { get; set; }

        [BsonElement("mensagens")]
        public List<Mensagem>? Mensagens { get; set; }
    }

    static class MongoHelper
    {
        private static readonly IMongoCollection<Modelo> collection;

        static MongoHelper()
        {
            var client = new MongoClient("localhost:27017");
            var database = client.GetDatabase("sd");
            collection = database.GetCollection<Modelo>("dados");
        }

        public static void GuardarDados(string json)
        {
            try
            {
                var jObject = JObject.Parse(json);

                var modelo = new Modelo
                {
                    WavyId = jObject["wavyId"]?.ToString(),
                    Tipo = jObject["tipo"]?.ToString(),
                    Mensagens = new List<Mensagem>()
                };

                foreach (var msg in jObject["mensagens"])
                {
                    modelo.Mensagens.Add(new Mensagem
                    {
                        Caracteristica = msg["Caracteristica"]?.ToString(),
                        Hora = msg["Hora"]?.ToString()
                    });
                }

                collection.InsertOne(modelo);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro ao guardar no MongoDB: " + ex.Message);
            }
        }
    }

    class Program
    {
        private static readonly Mutex ficheiroMutex = new Mutex();

        static void Main()
        {
            int port = 6000;
            TcpListener server = new TcpListener(IPAddress.Any, port);
            server.Start();
            Console.WriteLine("SERVIDOR Pronto");

            while (true)
            {
                TcpClient client = server.AcceptTcpClient();
                Thread clientThread = new Thread(() => HandleClient(client));
                clientThread.Start();
            }
        }

        static void HandleClient(object obj)
        {
            TcpClient client = (TcpClient)obj;
            NetworkStream stream = client.GetStream();
            byte[] buffer = new byte[1024];
            while (true)
            {
                int bytesRead = stream.Read(buffer, 0, buffer.Length);
                if (bytesRead == 0) break;

                string message = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                Console.WriteLine("Dados recebidos do AGREGADOR: " + message);

                ficheiroMutex.WaitOne();
                try
                {
                    File.AppendAllText("dados_recebidos.json", message + "\n");
                }
                finally
                {
                    ficheiroMutex.ReleaseMutex();
                }

                MongoHelper.GuardarDados(message);

                string resposta = "Dados recebidos com sucesso";
                byte[] respostaBytes = Encoding.ASCII.GetBytes(resposta);
                stream.Write(respostaBytes, 0, respostaBytes.Length);

                if (message.Contains("fim")) break;
            }
            client.Close();
        }
    }
}
