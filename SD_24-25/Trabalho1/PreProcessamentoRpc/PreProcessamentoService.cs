using Grpc.Core;

namespace PreProcessamentoRpc
{
    public class PreProcessamentoService : PreProcessamento.PreProcessamentoBase
    {
        public override Task<DadosProcessados> ProcessarDados(DadosBrutos request, ServerCallContext context)
        {
            string dadosProcessados = request.Dados;

            switch (request.TipoProcessamento)
            {
                case "uppercase":
                    dadosProcessados = dadosProcessados.ToUpper();
                    break;
                case "lowercase":
                    dadosProcessados = dadosProcessados.ToLower();
                    break;
                case "normalize":
                    dadosProcessados = dadosProcessados.Trim().ToLower();
                    break;
                default:
                    // Nenhum processamento
                    break;
            }

            return Task.FromResult(new DadosProcessados { Dados = dadosProcessados });
        }
    }
}