import streamlit as st
import grpc
import analise_pb2
import analise_pb2_grpc

# Função para chamar o serviço gRPC
def chamar_analise(wavy_id):
    with grpc.insecure_channel("localhost:50052") as channel:
        stub = analise_pb2_grpc.AnaliseStub(channel)
        resposta = stub.AnalisarDadosPorTipo(
            analise_pb2.DadosParaAnalise(wavy_id=wavy_id)
        )
        return resposta

# Interface
st.title("🔍 Análise de Dados por WAVY")

wavy_id = st.text_input("ID da WAVY", value="wavy123")

if st.button("Analisar"):
    resultado = chamar_analise(wavy_id)
    st.subheader(f"Resultados para '{wavy_id}':")
    for tipo in resultado.medias_por_tipo:
        st.write(f"📊 Tipo: {tipo.tipo} | Média: {tipo.media:.2f} | Total: {tipo.total_amostras}")
