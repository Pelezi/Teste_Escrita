import streamlit as st
import mysql.connector
import decimal
import pandas as pd
import time

config = {
    'user': 'root',
    'password': '20042005db',
    'host': 'localhost',
    'database': 'test_phoenix_joao_pessoa'
}
def bd_phoenix(vw_name):
    # Parametros de Login AWS
    # Conexão às Views
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT * FROM {vw_name}'

    # Script MySQL para requests
    cursor.execute(request_name)
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas os cabeçalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e converte decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def atualizar_banco_dados(df_exportacao):
    # Conexão ao banco de dados
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()
    
    # Coluna para armazenar o status da atualização
    df_exportacao['Status Serviço'] = ''
    df_exportacao['Status Auditoria'] = ''
    
    # Placeholder para exibir o DataFrame e atualizar em tempo real
    placeholder = st.empty()
    for idx, row in df_exportacao.iterrows():
        id_reserva = row['Id Reserva']
        id_servico = row['Id Servico']
        currentPresentationHour = str(row['Horario de apresentação atual'])
        newPresentationHour = str(row['Novo horario de apresentação'])
        
        data = '{"presentation_hour":["' + currentPresentationHour + '","' + newPresentationHour + ' Roteirizador"]}'
        
        #Horário atual em string
        current_timestamp = str(int(time.time()))
        
        try:
            # Atualizar o banco de dados se o ID já existir
            query = "UPDATE reserve_service SET presentation_hour = %s WHERE id = %s"
            cursor.execute(query, (newPresentationHour, id_servico))
            conexao.commit()
            df_exportacao.at[idx, 'Status Serviço'] = 'Atualizado com sucesso'
            
        except Exception as e:
            df_exportacao.at[idx, 'Status Serviço'] = f'Erro: {e}'
        
        try:
            # Adicionar registro de edição na tabela de auditoria
            query = "INSERT INTO changelogs (relatedObjectType, relatedObjectId, parentId, data, createdAt, type, userId, module, hostname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, null)"
            cursor.execute(query, ('ReserveService', id_servico, id_reserva, data, current_timestamp, 'update', st.query_params["userId"], 'router'))
            conexao.commit()
            df_exportacao.at[idx, 'Status Auditoria'] = 'Atualizado com sucesso'
        except Exception as e:
            df_exportacao.at[idx, 'Status Auditoria'] = f'Erro: {e}'
            
        # Define o estilo para coloração condicional
        styled_df = df_exportacao.style.applymap(
            lambda val: 'background-color: green; color: white' if val == 'Atualizado com sucesso' 
            else ('background-color: red; color: white' if val != '' else ''),
            subset=['Status Serviço', 'Status Auditoria']
        )
        
        # Atualiza o DataFrame na interface em tempo real
        placeholder.dataframe(styled_df, hide_index=True, use_container_width=True)
        # time.sleep(0.5)
    
    cursor.close()
    conexao.close()
    return df_exportacao

def getUser(userId):
    # Cria uma cópia do config e sobrescreve o campo database
    config_general = config.copy()
    config_general['database'] = 'test_phoenix_general'
    
    # Conexão às Views usando o config modificado
    conexao = mysql.connector.connect(**config_general)
    cursor = conexao.cursor()

    request_name = f'SELECT * FROM user WHERE ID = {userId}'

    # Script MySQL para requests
    cursor.execute(request_name)
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas os cabeçalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e converte decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

# Configuração da página Streamlit
st.set_page_config(layout='wide')

if not st.query_params or not st.query_params["userId"]:
    st.error("Usuário não autenticado")
    st.stop()

if not 'df_vehicle_occupation' in st.session_state:
    # Carrega os dados da view `vw_vehicle_ocupation`
    st.session_state.df_vehicle_occupation = bd_phoenix('vw_vehicle_occupation')

if not 'df_user' in st.session_state:
    # Carrega os dados da tabela 'user`
    st.session_state.df_user = getUser(st.query_params["userId"])

st.title('Ocupação Média por Veículo')

st.divider()

st.write('Olá Usuário ' + st.session_state.df_user.nickname[0] + '!')

# Input de intervalo de data
row0 = st.columns(4)
with row0[0]:
    periodo = st.date_input('Período', value=[], format='DD/MM/YYYY')

# Filtra os dados conforme o intervalo de tempo informado
if len(periodo) == 2:  # Verifica se o intervalo está completo
    data_inicial, data_final = periodo

    df_filtrado = st.session_state.df_vehicle_occupation[
        (st.session_state.df_vehicle_occupation['Data Execucao'] >= data_inicial) &
        (st.session_state.df_vehicle_occupation['Data Execucao'] <= data_final)
    ].reset_index(drop=True)

    # Calcula a ocupação média por veículo, agrupando por Veículo e Escala
    ocupacao_por_escala = df_filtrado.groupby(['Tipo de Veiculo', 'Veiculo', 'Capacidade', 'Escala'])[['Total ADT', 'Total CHD']].sum().reset_index()

    # Adiciona uma coluna para a soma de ADT e CHD
    ocupacao_por_escala['Ocupação Total'] = ocupacao_por_escala['Total ADT'] + ocupacao_por_escala['Total CHD']

    # Agora, calculamos a média de ocupação total (ADT + CHD) por veículo
    ocupacao_media_tipo_veiculo = ocupacao_por_escala.groupby('Tipo de Veiculo')['Ocupação Total'].mean().reset_index()
    ocupacao_media_veiculo = ocupacao_por_escala.groupby(['Tipo de Veiculo', 'Veiculo', 'Capacidade'])['Ocupação Total'].mean().reset_index()
    ocupacao_por_escala_veiculo = ocupacao_por_escala.groupby(['Tipo de Veiculo', 'Veiculo'])['Ocupação Total'].mean().reset_index()
    
    ocupacao_media_veiculo['Ocupação Total'] = ocupacao_media_veiculo['Ocupação Total'].round(1)
    #Adiciona coluna de ocupação média percentual
    ocupacao_media_veiculo['Ocupação Média (%)'] = (ocupacao_media_veiculo['Ocupação Total'] / ocupacao_media_veiculo['Capacidade']) * 100
    
    #Adiciona "/" + capacidade do veículo e limite
    ocupacao_media_veiculo['Ocupação Total'] = ocupacao_media_veiculo['Ocupação Total'].astype(str) + "/" + ocupacao_media_veiculo['Capacidade'].astype(str)
    

    # Renomeia a coluna para 'Ocupação Média'
    ocupacao_media_tipo_veiculo = ocupacao_media_tipo_veiculo.rename(columns={'Ocupação Total': 'Ocupação Média Nominal'}) 
    ocupacao_media_veiculo = ocupacao_media_veiculo.rename(columns={'Ocupação Total': 'Ocupação Média Nominal'})
    
    ocupacao_media_percentual_tipo_veiculo = ocupacao_media_veiculo.groupby('Tipo de Veiculo')['Ocupação Média (%)'].mean().reset_index()
    ocupacao_media_tipo_veiculo = ocupacao_media_tipo_veiculo.merge(ocupacao_media_percentual_tipo_veiculo, on='Tipo de Veiculo')

    

    # Exibe a lista de tipos de veículos para seleção
    lista_veiculos = ocupacao_por_escala['Tipo de Veiculo'].unique().tolist()
    with row0[1]:
        tipo_veiculo_selecionado = st.selectbox('Selecionar Tipo de Veículo', sorted(lista_veiculos), index=None)

    # Se um tipo de veículo for selecionado, exibe a ocupação média
    if tipo_veiculo_selecionado:
        ocupacao_veiculo = ocupacao_media_tipo_veiculo[ocupacao_media_tipo_veiculo['Tipo de Veiculo'] == tipo_veiculo_selecionado]
        ocupacao_media_veiculo = ocupacao_media_veiculo[ocupacao_media_veiculo['Tipo de Veiculo'] == tipo_veiculo_selecionado]
        #Remove a coluna de Tipo de Veículo
        ocupacao_media_veiculo = ocupacao_media_veiculo.drop(columns=['Tipo de Veiculo'])
        ocupacao = ocupacao_veiculo['Ocupação Média Nominal'].values[0]  # Acessa a ocupação média

        # Filtra as escalas do veículo selecionado
        df_filtrado_veiculo = ocupacao_por_escala_veiculo[ocupacao_por_escala_veiculo['Tipo de Veiculo'] == tipo_veiculo_selecionado]
        #Remove a coluna de Tipo de Veículo
        df_filtrado_veiculo = df_filtrado_veiculo.drop(columns=['Tipo de Veiculo'])
        
        # Exibe o dataframe das escalas e adiciona um seletor de escala
        
        #ecibe um selectbox para escolher o veículo dentro do dataframe
        with row0[2]:
            veiculo_selecionado = st.selectbox('Selecionar Veículo', df_filtrado_veiculo['Veiculo'].unique().tolist(), index=None)
        
        if veiculo_selecionado:
            # Filtra os serviços associados ao tipo de veículo e veículo selecionados
            df_servicos_veiculo = ocupacao_por_escala[ocupacao_por_escala['Tipo de Veiculo'] == tipo_veiculo_selecionado]
            df_servicos_veiculo = df_servicos_veiculo[df_servicos_veiculo['Veiculo'] == veiculo_selecionado]
            
            #Adiciona a coluna de ocupação percentual
            df_servicos_veiculo['Ocupação Total (%)'] = (df_servicos_veiculo['Ocupação Total'] / df_servicos_veiculo['Capacidade']) * 100
            
            # Lista de escalas para o veículo selecionado
            escalas_disponiveis = df_servicos_veiculo['Escala'].unique().tolist()
            # Exibe um seletor para escolher a escala dentro do dataframe
            with row0[3]:
                escala_selecionada = st.selectbox('Selecionar Escala', escalas_disponiveis, index=None)

            if escala_selecionada:
                # Filtra os serviços associados à escala selecionada
                df_servicos_escala = df_filtrado[
                    (df_filtrado['Tipo de Veiculo'] == tipo_veiculo_selecionado) &
                    (df_filtrado['Escala'] == escala_selecionada)
                ]
                
            
                st.divider()
                # Botão para abrir o "dialog" de exportação
                with st.expander('Aplicar dados nos serviços', expanded=False):
                    df_para_exportacao = df_servicos_escala[['Id Reserva', 'Id Servico', 'Reserva', 'Tipo de Servico', 'Servico', 'Horario de apresentacao']]
                    df_para_exportacao = df_para_exportacao.rename(columns={'Horario de apresentacao': 'Horario de apresentação atual'})
                    # Adiciona nova coluna observação com "Teste de escrita no banco"
                    df_para_exportacao['Novo horario de apresentação'] = '2050-10-31 14:14:14'
                    df_para_exportacao['Status Serviço'] = ''
                    df_para_exportacao['Status Auditoria'] = ''
                    
                    
                    # Botão para confirmar a exportação
                    if st.button("Confirmar Exportação"):
                        try:
                            df_exportacao = atualizar_banco_dados(df_para_exportacao)
                            st.success("Exportação confirmada!")
                            st.session_state.df_vehicle_occupation = bd_phoenix('vw_vehicle_occupation')
                        except Exception as e:
                            st.error(f"Erro ao exportar: {e}")
                    else:
                        # Exibe o dataframe para exportação como uma pré-visualização
                        st.dataframe(df_para_exportacao, hide_index=True, use_container_width=True)
                        
                    
                    
                        
                if len(df_servicos_escala) > 1:
                    st.subheader(f'{len(df_servicos_escala)} serviços associados a Escala {escala_selecionada}')
                else:
                    st.subheader(f'{len(df_servicos_escala)} serviço associado a Escala {escala_selecionada}')
                st.dataframe(df_servicos_escala, hide_index=True, use_container_width=True)
                
            st.divider()
            if len(df_servicos_veiculo) > 1:
                st.subheader(f'{len(df_servicos_veiculo)} escalas associadas ao veículo {veiculo_selecionado}')
            else:
                st.subheader(f'{len(df_servicos_veiculo)} escala associada ao veículo {veiculo_selecionado}')
            st.dataframe(df_servicos_veiculo, hide_index=True, use_container_width=True)
        
        st.divider()
        st.subheader(f'Ocupação Média do Tipo de Veículo {tipo_veiculo_selecionado}: {ocupacao:.2f} passageiros')
        st.dataframe(ocupacao_media_veiculo, hide_index=True, use_container_width=True)
            
    # Exibe o dataframe completo
    st.divider()
    st.subheader('Ocupação Média por Tipo de Veículo')
    st.dataframe(ocupacao_media_tipo_veiculo, hide_index=True, use_container_width=True)
