import streamlit as st
import pandas as pd
import psycopg2
import altair as alt

# Função para carregar os dados do PostgreSQL
@st.cache_resource
def load_data():
    # Conectar ao banco de dados PostgreSQL
    conn = psycopg2.connect(
        host="dpg-cqi9s8ogph6c738lqiqg-a.ohio-postgres.render.com",
        database="dbname_7b7d",
        user="dbname_7b7d_user",
        password="Uq6UvyiWIXnpzAjGNGhG3DQakRYscGs1"
    )
    
    query = """
    SELECT
        n_nota,
        data_de_pregao,
        qted,
        mercadoria,
        txop,
        tx_corretagem,
        cotacao,
        movimentacao
    FROM
        gold_fatura;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Carregar os dados
df = load_data()

# Mostrar o DataFrame na interface do Streamlit
st.title("KPIs e Gráficos Financeiros - Gold Fatura")
st.write("Tabela de dados:")
st.dataframe(df)

# Calcular KPIs
total_movimentacao = df['movimentacao'].sum()
media_cotacao = df['cotacao'].mean()
total_qted = df['qted'].sum()

# Mostrar KPIs
st.header("KPIs")
st.metric(label="Total Movimentação", value=f"R${total_movimentacao:,.2f}")
st.metric(label="Média de Cotação", value=f"R${media_cotacao:,.2f}")
st.metric(label="Total Quantidade (qted)", value=f"{total_qted:,}")

# Gráfico de movimentação ao longo do tempo
st.header("Movimentação ao longo do tempo")
movimentacao_chart = alt.Chart(df).mark_line().encode(
    x='data_de_pregao:T',
    y='movimentacao:Q',
    color='mercadoria:N'
).properties(
    title='Movimentação por Mercadoria ao Longo do Tempo'
)
st.altair_chart(movimentacao_chart, use_container_width=True)

# Gráfico de cotação por mercadoria
st.header("Cotação por Mercadoria")
cotacao_chart = alt.Chart(df).mark_bar().encode(
    x='mercadoria:N',
    y='cotacao:Q',
    color='mercadoria:N'
).properties(
    title='Cotação Média por Mercadoria'
)
st.altair_chart(cotacao_chart, use_container_width=True)

# Gráfico de quantidade (qted) por mercadoria
st.header("Quantidade por Mercadoria")
qted_chart = alt.Chart(df).mark_bar().encode(
    x='mercadoria:N',
    y='qted:Q',
    color='mercadoria:N'
).properties(
    title='Quantidade Total por Mercadoria'
)
st.altair_chart(qted_chart, use_container_width=True)
