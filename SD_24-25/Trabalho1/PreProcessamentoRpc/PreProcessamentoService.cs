using Grpc.Core;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml;

namespace PreProcessamentoRpc
{
    public class PreProcessamentoService : PreProcessamento.PreProcessamentoBase
    {
        public override Task<DadosProcessados> ProcessarDados(DadosBrutos request, ServerCallContext context)
        {
            try
            {
                // Extract data from the raw message
                var (tipo, valor, unidade, hora, wavyId) = ExtrairDados(request.Dados);

                if (string.IsNullOrEmpty(wavyId) || tipo == null || valor == null || hora == null)
                {
                    // Return error with more details
                    return Task.FromResult(new DadosProcessados
                    {
                        Dados = "{\"error\": \"Could not parse message\", \"success\": false}"
                    });
                }

                // Apply preprocessing to the valor if needed
                string valorProcessado = valor;
                switch (request.TipoProcessamento)
                {
                    case "uppercase":
                        valorProcessado = valor.ToUpper();
                        break;
                    case "lowercase":
                        valorProcessado = valor.ToLower();
                        break;
                    case "normalize":
                        valorProcessado = valor.Trim().ToLower();
                        break;
                    default:
                        // No processing
                        break;
                }

                // Convert to standardized JSON format
                var jsonObj = new
                {
                    wavyId = wavyId,
                    tipo = tipo,
                    valor = valorProcessado,
                    unidade = unidade,
                    hora = hora,
                    processedAt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                    success = true
                };

                string jsonResult = JsonSerializer.Serialize(jsonObj, new JsonSerializerOptions
                {
                    WriteIndented = true
                });

                return Task.FromResult(new DadosProcessados { Dados = jsonResult });
            }
            catch (Exception ex)
            {
                // Enhanced error handling
                var errorObj = new
                {
                    error = $"Processing error: {ex.Message}",
                    success = false
                };

                string errorJson = JsonSerializer.Serialize(errorObj);
                return Task.FromResult(new DadosProcessados { Dados = errorJson });
            }
        }

        private static (string? tipo, string? valor, string? unidade, string? hora, string? wavyId) ExtrairDados(string msg)
        {
            msg = msg.Trim();

            // JSON
            if (msg.StartsWith("{") && msg.EndsWith("}"))
            {
                try
                {
                    using JsonDocument doc = JsonDocument.Parse(msg);
                    var root = doc.RootElement;

                    string wavyId = root.TryGetProperty("wavyId", out var wavyIdProp) ? wavyIdProp.GetString() ?? "" : "";
                    string tipo = root.GetProperty("tipo").GetString() ?? "";
                    string valorStr = root.GetProperty("valor").GetRawText();
                    string unidade = root.GetProperty("unidade").GetString() ?? "";
                    string hora = root.GetProperty("hora").GetString() ?? "";

                    return (tipo, valorStr, unidade, hora, wavyId);
                }
                catch
                {
                    return (null, null, null, null, null);
                }
            }

            // XML
            if (msg.StartsWith("<") && msg.EndsWith(">"))
            {
                try
                {
                    XmlDocument xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(msg);
                    var root = xmlDoc.DocumentElement;

                    if (root == null) return (null, null, null, null, null);

                    string wavyId = root.SelectSingleNode("wavyId")?.InnerText ?? "";
                    string tipo = root.SelectSingleNode("tipo")?.InnerText ?? "";
                    string valor = root.SelectSingleNode("valor")?.InnerText ?? "";
                    string unidade = root.SelectSingleNode("unidade")?.InnerText ?? "";
                    string hora = root.SelectSingleNode("hora")?.InnerText ?? "";

                    return (tipo, valor, unidade, hora, wavyId);
                }
                catch
                {
                    return (null, null, null, null, null);
                }
            }

            // CSV - assumindo formato: wavyId,tipo,valor,unidade,hora
            var csvParts = msg.Split(',');
            if (csvParts.Length == 5 || csvParts.Length == 6)
            {
                try
                {
                    string wavyId = csvParts[0].Trim();
                    string tipo = csvParts[1].Trim();
                    string valor = csvParts[2].Trim();
                    string unidade = csvParts[3].Trim();
                    string hora = csvParts[4].Trim();
                    // Ignore the 6th field (data) if present

                    return (tipo, valor, unidade, hora, wavyId);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CSV PARSE ERROR] {ex.Message}");
                    return (null, null, null, null, null);
                }
            }

            // TXT estilo: WAVY ID: WAVY_003 | Tipo: salinidade | Valor: 34.50 PSU | Hora: 12:31:45
            if (msg.Contains("WAVY ID:") && msg.Contains("Tipo:") && msg.Contains("Valor:") && msg.Contains("Hora:"))
            {
                try
                {
                    var wavyIdMatch = Regex.Match(msg, @"WAVY ID:\s*([^\s|]+)");
                    var tipoMatch = Regex.Match(msg, @"Tipo:\s*([^|]+)");
                    var valorMatch = Regex.Match(msg, @"Valor:\s*([\d.,]+)");
                    var unidadeMatch = Regex.Match(msg, @"Valor:\s*[\d.,]+\s*([^\s|]+)");
                    var horaMatch = Regex.Match(msg, @"Hora:\s*([^\s|]+)");

                    string wavyId = wavyIdMatch.Success ? wavyIdMatch.Groups[1].Value.Trim() : "";
                    string tipo = tipoMatch.Success ? tipoMatch.Groups[1].Value.Trim() : "";
                    string valor = valorMatch.Success ? valorMatch.Groups[1].Value.Trim() : "";
                    string unidade = unidadeMatch.Success ? unidadeMatch.Groups[1].Value.Trim() : "";
                    string hora = horaMatch.Success ? horaMatch.Groups[1].Value.Trim() : "";

                    return (tipo, valor, unidade, hora, wavyId);
                }
                catch
                {
                    return (null, null, null, null, null);
                }
            }

            // Fallback ao formato antigo (TIPO:xxx|CARAC:yyy|HORA:zzz)
            try
            {
                var partes = msg.Split('|');
                string? tipo = null;
                string? valor = null;
                string? hora = null;
                string? wavyId = null;

                foreach (var parte in partes)
                {
                    if (parte.StartsWith("TIPO:")) tipo = parte[5..].Trim();
                    else if (parte.StartsWith("CARAC:")) valor = parte[6..].Trim();
                    else if (parte.StartsWith("HORA:")) hora = parte[5..].Trim();
                    else if (parte.StartsWith("WAVY:")) wavyId = parte[5..].Trim();
                }

                return (tipo, valor, "", hora, wavyId);
            }
            catch
            {
                return (null, null, null, null, null);
            }
        }
    }
}