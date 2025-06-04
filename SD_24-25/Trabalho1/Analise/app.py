import streamlit as st
import grpc
import analise_pb2
import analise_pb2_grpc
from datetime import datetime, date

# Função para chamar o serviço gRPC
def chamar_analise(search_value, date_start=None, date_end=None):
    with grpc.insecure_channel("localhost:50052") as channel:
        stub = analise_pb2_grpc.AnaliseStub(channel)
        
        # Format the search string with date filters
        search_string = f"both:{search_value}"
        
        if date_start:
            search_string += f"|date_start:{date_start}"
        if date_end:
            search_string += f"|date_end:{date_end}"
        
        resposta = stub.AnalisarDadosPorTipo(
            analise_pb2.DadosParaAnalise(wavy_id=search_string)
        )
        
        return resposta

# Interface
st.title("🔍 Análise de Dados por WAVY")

# Main search bar
search_value = st.text_input(
    "🔍 Buscar", 
    value="wavy_001", 
    help="Digite qualquer termo - busca automaticamente em WAVY ID e Tipo de sensor",
    placeholder="Ex: wavy_001, temperatura, sensor..."
)

# Date filter section
with st.expander("📅 Filtrar por Data", expanded=False):
    col1, col2 = st.columns(2)
    
    with col1:
        date_start = st.date_input(
            "Data Início",
            value=None,
            min_value=date(2000, 1, 1),  # Set minimum date to year 2000
            max_value=date.today(),
            help="Deixe vazio para não filtrar por data inicial (mínimo: 01/01/2000)"
        )
    
    with col2:
        date_end = st.date_input(
            "Data Fim", 
            value=None,
            min_value=date(2000, 1, 1),  # Set minimum date to year 2000
            max_value=date.today(),
            help="Deixe vazio para não filtrar por data final (mínimo: 01/01/2000)"
        )
    
    # Validate date range
    if date_start and date_end and date_start > date_end:
        st.error("❌ Data de início deve ser anterior à data de fim")


if st.button("🔍 Analisar", type="primary"):
    if not search_value.strip():
        st.error("Por favor, digite um valor para buscar.")
    elif date_start and date_end and date_start > date_end:
        st.error("❌ Corrija o intervalo de datas antes de continuar.")
    else:
        try:
            with st.spinner("Analisando dados..."):
                # Convert dates to strings if they exist
                date_start_str = date_start.strftime("%Y-%m-%d") if date_start else None
                date_end_str = date_end.strftime("%Y-%m-%d") if date_end else None
                
                resultado = chamar_analise(search_value.strip(), date_start_str, date_end_str)
            
            # Display search info
            search_info = f"Termo: '{search_value}'"
            if date_start_str or date_end_str:
                if date_start_str and date_end_str:
                    search_info += f" | Período: {date_start.strftime('%d/%m/%Y')} até {date_end.strftime('%d/%m/%Y')}"
                elif date_start_str:
                    search_info += f" | A partir de: {date_start.strftime('%d/%m/%Y')}"
                elif date_end_str:
                    search_info += f" | Até: {date_end.strftime('%d/%m/%Y')}"
            
            st.success(f"✅ Busca realizada - {search_info}")
            
            if len(resultado.medias_por_tipo) == 0:
                st.warning("Nenhum dado encontrado para esta busca.")
                st.info("💡 Dicas:")
                st.markdown("""
                - Verifique se o termo de busca está correto
                - Tente expandir o intervalo de datas
                - Use termos mais genéricos (ex: 'wavy' em vez de 'wavy_001')
                """)
            else:
                st.subheader(f"📈 Resultados ({len(resultado.medias_por_tipo)} encontrados):")
                
                # Sort results by total samples (descending)
                sorted_results = sorted(
                    resultado.medias_por_tipo, 
                    key=lambda x: x.total_amostras, 
                    reverse=True
                )
                
                # Summary metrics
                total_samples = sum(r.total_amostras for r in sorted_results)
                st.metric("📊 Total de Amostras", total_samples)
                
                st.divider()
                
                # Create metrics in columns
                for i, tipo in enumerate(sorted_results):
                    with st.container():
                        # Extract date info from tipo name if present
                        tipo_name = tipo.tipo
                        if " | 📅 " in tipo_name:
                            sensor_info, date_info = tipo_name.split(" | 📅 ", 1)
                            st.markdown(f"### 📊 {sensor_info}")
                            st.markdown(f"**📅 Período dos dados:** {date_info}")
                        else:
                            st.markdown(f"### 📊 {tipo_name}")
                        
                        # Create metrics in columns
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Média", f"{tipo.media:.2f}")
                        
                        with col2:
                            st.metric("Mínimo", f"{tipo.min_valor:.2f}")
                        
                        with col3:
                            st.metric("Máximo", f"{tipo.max_valor:.2f}")
                        
                        with col4:
                            st.metric("Total Amostras", tipo.total_amostras)
                        
                        # Add a progress bar for visual representation
                        if tipo.max_valor != tipo.min_valor:
                            range_val = tipo.max_valor - tipo.min_valor
                            media_normalized = (tipo.media - tipo.min_valor) / range_val
                            st.progress(media_normalized, text=f"Média relativa: {media_normalized:.1%}")
                        
                        # Show data distribution info
                        with st.expander("📊 Detalhes dos Dados", expanded=False):
                            st.markdown(f"""
                            - **Amplitude:** {tipo.max_valor - tipo.min_valor:.2f}
                            - **Desvio da média:** ±{(tipo.max_valor - tipo.min_valor) / 2:.2f}
                            - **Densidade de amostras:** {tipo.total_amostras} medições
                            """)
                        
                        if i < len(sorted_results) - 1:  # Don't add divider after last item
                            st.divider()
                        
        except grpc.RpcError as e:
            st.error(f"❌ Erro ao conectar com o servidor: {e}")
            st.info("Verifique se o servidor está rodando na porta 50052")
        except Exception as e:
            st.error(f"❌ Erro inesperado: {e}")
