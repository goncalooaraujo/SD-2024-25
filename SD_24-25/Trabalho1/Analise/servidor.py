import grpc
from concurrent import futures
from pymongo import MongoClient
import analise_pb2
import analise_pb2_grpc
from datetime import datetime

class AnaliseService(analise_pb2_grpc.AnaliseServicer):
    def AnalisarDadosPorTipo(self, request, context):
        client = MongoClient("mongodb://localhost:27017")
        db = client.sd
        collection = db.dados

        # Parse the search parameters from the wavy_id field
        # Format: "both:search_value|date_start:YYYY-MM-DD|date_end:YYYY-MM-DD"
        wavy_id_input = request.wavy_id
        
        # Parse parameters
        params = {}
        if "|" in wavy_id_input:
            parts = wavy_id_input.split("|")
            main_search = parts[0]
            for part in parts[1:]:
                if ":" in part:
                    key, value = part.split(":", 1)
                    params[key] = value
        else:
            main_search = wavy_id_input
        
        # Parse main search
        if ":" in main_search and main_search.split(":")[0] in ["wavy", "tipo", "both"]:
            search_type, search_value = main_search.split(":", 1)
        else:
            search_type = "both"  # Default to both for smart search
            search_value = main_search

        # Build filter based on search type
        filtro = {}
        
        if search_type == "wavy":
            filtro = {"wavyId": {"$regex": f"^{search_value}$", "$options": "i"}}
        elif search_type == "tipo":
            filtro = {"tipo": {"$regex": f"^{search_value}$", "$options": "i"}}
        elif search_type == "both":
            # Search for documents that match either wavy_id OR tipo
            filtro = {
                "$or": [
                    {"wavyId": {"$regex": f"^{search_value}$", "$options": "i"}},
                    {"tipo": {"$regex": f"^{search_value}$", "$options": "i"}}
                ]
            }
        else:
            # Default to both search
            filtro = {
                "$or": [
                    {"wavyId": {"$regex": f"^{search_value}$", "$options": "i"}},
                    {"tipo": {"$regex": f"^{search_value}$", "$options": "i"}}
                ]
            }

        # Add date filters if provided
        # Note: MongoDB documents have "data" field with format "dd/MM/yyyy"
        date_conditions = []
        
        if "date_start" in params:
            try:
                start_date = datetime.strptime(params["date_start"], "%Y-%m-%d")
                start_date_str = start_date.strftime("%d/%m/%Y")
                print(f"🗓️ Filtering from date: {start_date_str}")
                
                # For string date comparison, we need to convert to comparable format
                # We'll use a different approach - convert the date field to date for comparison
                date_conditions.append({
                    "$expr": {
                        "$gte": [
                            {"$dateFromString": {
                                "dateString": "$data",
                                "format": "%d/%m/%Y"
                            }},
                            start_date
                        ]
                    }
                })
            except ValueError:
                print(f"❌ Invalid start date format: {params['date_start']}")
                pass  # Invalid date format, ignore
        
        if "date_end" in params:
            try:
                end_date = datetime.strptime(params["date_end"], "%Y-%m-%d")
                end_date_str = end_date.strftime("%d/%m/%Y")
                print(f"🗓️ Filtering until date: {end_date_str}")
                
                # Add one day to include the entire end date
                end_date = end_date.replace(hour=23, minute=59, second=59)
                
                date_conditions.append({
                    "$expr": {
                        "$lte": [
                            {"$dateFromString": {
                                "dateString": "$data",
                                "format": "%d/%m/%Y"
                            }},
                            end_date
                        ]
                    }
                })
            except ValueError:
                print(f"❌ Invalid end date format: {params['date_end']}")
                pass  # Invalid date format, ignore
        
        # Combine all conditions
        if date_conditions:
            if len(date_conditions) == 1:
                final_filter = {"$and": [filtro, date_conditions[0]]}
            else:
                final_filter = {"$and": [filtro] + date_conditions}
        else:
            final_filter = filtro

        print(f"🔍 Filtro MongoDB: {final_filter}")  # Debug log
        
        try:
            dados = list(collection.find(final_filter))
            print(f"📊 Documentos encontrados: {len(dados)}")  # Debug log
        except Exception as e:
            print(f"❌ Erro na consulta MongoDB: {e}")
            # Fallback to basic filter without date if date parsing fails
            dados = list(collection.find(filtro))
            print(f"📊 Documentos encontrados (sem filtro de data): {len(dados)}")

        acumulador = {}

        for doc in dados:
            tipo = doc.get("tipo")
            valor = doc.get("valor", 0)
            wavy_id = doc.get("wavyId", "")
            data_doc = doc.get("data", "")
            
            if not tipo or valor is None:
                continue

            # Create a unique key combining tipo and wavyId for better organization
            if search_type in ["both", "tipo"]:
                key = f"{tipo} (WAVY: {wavy_id})"
            else:
                key = tipo
            
            if key not in acumulador:
                acumulador[key] = {
                    "soma": 0, 
                    "count": 0, 
                    "min": float('inf'), 
                    "max": float('-inf'),
                    "tipo": tipo,
                    "wavy_id": wavy_id,
                    "dates": [],  # Store all dates for this group
                    "first_date": None,
                    "last_date": None
                }
            
            # Convert valor to float if it's a string
            try:
                if isinstance(valor, str):
                    valor = float(valor.replace(",", "."))
                valor = float(valor)
            except (ValueError, TypeError):
                print(f"⚠️ Valor inválido ignorado: {valor}")
                continue
            
            acumulador[key]["soma"] += valor
            acumulador[key]["count"] += 1
            acumulador[key]["min"] = min(acumulador[key]["min"], valor)
            acumulador[key]["max"] = max(acumulador[key]["max"], valor)
            
            # Track dates
            if data_doc:
                acumulador[key]["dates"].append(data_doc)
                
                # Convert date string to datetime for comparison
                try:
                    date_obj = datetime.strptime(data_doc, "%d/%m/%Y")
                    if acumulador[key]["first_date"] is None or date_obj < acumulador[key]["first_date"]:
                        acumulador[key]["first_date"] = date_obj
                    if acumulador[key]["last_date"] is None or date_obj > acumulador[key]["last_date"]:
                        acumulador[key]["last_date"] = date_obj
                except ValueError:
                    pass  # Invalid date format, ignore

        resultado = analise_pb2.ResultadoAnalisePorTipo()

        for key, stats in acumulador.items():
            media = stats["soma"] / stats["count"] if stats["count"] > 0 else 0
            
            # Handle edge case where no valid data was found
            min_valor = stats["min"] if stats["min"] != float('inf') else 0
            max_valor = stats["max"] if stats["max"] != float('-inf') else 0
            
            # Format date range
            date_range = ""
            if stats["first_date"] and stats["last_date"]:
                if stats["first_date"] == stats["last_date"]:
                    date_range = stats["first_date"].strftime("%d/%m/%Y")
                else:
                    date_range = f"{stats['first_date'].strftime('%d/%m/%Y')} - {stats['last_date'].strftime('%d/%m/%Y')}"
            elif stats["dates"]:
                # If we have dates but couldn't parse them, show unique dates
                unique_dates = list(set(stats["dates"]))
                if len(unique_dates) == 1:
                    date_range = unique_dates[0]
                else:
                    date_range = f"{len(unique_dates)} datas diferentes"
            
            # Create TipoMedia message with all fields
            tipo_media = analise_pb2.TipoMedia()
            tipo_media.tipo = f"{key} | 📅 {date_range}" if date_range else key
            tipo_media.media = media
            tipo_media.min_valor = min_valor
            tipo_media.max_valor = max_valor
            tipo_media.total_amostras = stats["count"]
            
            # Add to result
            resultado.medias_por_tipo.append(tipo_media)

        print(f"✅ Análise concluída: {len(resultado.medias_por_tipo)} tipos encontrados")
        return resultado

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    analise_pb2_grpc.add_AnaliseServicer_to_server(AnaliseService(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    print("✅ Servidor de Análise RPC em Python ativo na porta 50052")
    print("📊 Calculando média, mínimo, máximo por tipo de sensor")
    print("🔍 Busca inteligente por WAVY ID e Tipo")
    print("📅 Suporte para filtros de data (campo 'data' formato dd/MM/yyyy)")
    print("🗓️ Incluindo informações de data nos resultados")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
