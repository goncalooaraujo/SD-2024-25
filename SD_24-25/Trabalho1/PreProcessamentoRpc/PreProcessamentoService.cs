using Grpc.Core;
using System.Text.Json;
using System.Xml;

namespace PreProcessamentoRpc
{
    public class PreProcessamentoService : PreProcessamento.PreProcessamentoBase
    {
        public override Task<MensagemNormalizada> NormalizarMensagem(MensagemBruta request, ServerCallContext context)
        {
            try
            {
                string conteudo = request.Conteudo?.Trim() ?? "";

                // Inicializa valores padrão
                string tipo = "desconhecido";
                string caracteristica = "";
                string hora = "";

                // Detectar JSON
                if (conteudo.StartsWith("{") && conteudo.EndsWith("}"))
                {
                    var doc = JsonDocument.Parse(conteudo);
                    var root = doc.RootElement;

                    tipo = root.GetProperty("tipo").GetString() ?? tipo;
                    var valor = root.GetProperty("valor").GetRawText();
                    var unidade = root.GetProperty("unidade").GetString() ?? "";
                    caracteristica = $"{valor} {unidade}".Trim();
                    hora = root.GetProperty("hora").GetString() ?? "";
                }
                // Detectar XML
                else if (conteudo.StartsWith("<") && conteudo.EndsWith(">"))
                {
                    var xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(conteudo);

                    var root = xmlDoc.DocumentElement;
                    tipo = root.SelectSingleNode("tipo")?.InnerText ?? tipo;
                    var valor = root.SelectSingleNode("valor")?.InnerText ?? "";
                    var unidade = root.SelectSingleNode("unidade")?.InnerText ?? "";
                    caracteristica = $"{valor} {unidade}".Trim();
                    hora = root.SelectSingleNode("hora")?.InnerText ?? "";
                }
                else
                {
                    // Tratamento fallback (pode adaptar para TXT ou CSV)
                    // Exemplo: mensagem simples separada por '|'
                    var partes = conteudo.Split('|');
                    foreach (var parte in partes)
                    {
                        if (parte.StartsWith("TIPO:")) tipo = parte[5..].Trim();
                        else if (parte.StartsWith("CARAC:")) caracteristica = parte[6..].Trim();
                        else if (parte.StartsWith("HORA:")) hora = parte[5..].Trim();
                    }
                }

                return Task.FromResult(new MensagemNormalizada
                {
                    Tipo = tipo,
                    Caracteristica = caracteristica,
                    Hora = hora
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[PreProcessamentoService] Erro ao normalizar mensagem: {ex.Message}");
                throw new RpcException(new Status(StatusCode.Internal, "Erro interno no serviço PreProcessamento"));
            }
        }
    }
}
