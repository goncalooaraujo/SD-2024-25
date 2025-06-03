import grpc
from concurrent import futures
from pymongo import MongoClient
import analise_pb2
import analise_pb2_grpc

class AnaliseService(analise_pb2_grpc.AnaliseServicer):
    def AnalisarDadosPorTipo(self, request, context):
        client = MongoClient("mongodb://localhost:27017")
        db = client.sd
        collection = db.dados

        filtro = {"wavyId": {"$regex": f"^{request.wavy_id}$", "$options": "i"}}
        dados = list(collection.find(filtro))

        acumulador = {}

        for doc in dados:
            tipo = doc.get("tipo")
            valor = doc.get("valor", 0)
            if not tipo or valor is None:
                continue

            if tipo not in acumulador:
                acumulador[tipo] = {
                    "soma": 0, 
                    "count": 0, 
                    "min": float('inf'), 
                    "max": float('-inf')
                }
            
            acumulador[tipo]["soma"] += valor
            acumulador[tipo]["count"] += 1
            acumulador[tipo]["min"] = min(acumulador[tipo]["min"], valor)
            acumulador[tipo]["max"] = max(acumulador[tipo]["max"], valor)

        resultado = analise_pb2.ResultadoAnalisePorTipo()

        for tipo, stats in acumulador.items():
            media = stats["soma"] / stats["count"] if stats["count"] > 0 else 0
            
            # Handle edge case where no valid data was found
            min_valor = stats["min"] if stats["min"] != float('inf') else 0
            max_valor = stats["max"] if stats["max"] != float('-inf') else 0
            
            # Create TipoMedia message with all fields
            tipo_media = analise_pb2.TipoMedia()
            tipo_media.tipo = tipo
            tipo_media.media = media
            tipo_media.min_valor = min_valor
            tipo_media.max_valor = max_valor
            tipo_media.total_amostras = stats["count"]
            
            # Add to result
            resultado.medias_por_tipo.append(tipo_media)

        return resultado

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    analise_pb2_grpc.add_AnaliseServicer_to_server(AnaliseService(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    print("✅ Servidor de Análise RPC em Python ativo na porta 50052")
    print("📊 Calculando média, mínimo, máximo por tipo de sensor")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()