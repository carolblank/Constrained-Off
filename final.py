import os
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap

# URL da página
url = "https://dados.ons.org.br/dataset/restricao_coff_eolica_usi"

# Diretório onde você deseja salvar os arquivos CSV
diretorio_destino = "arquivos"

# Verifique se o diretório de destino existe, se não, crie-o
if not os.path.exists(diretorio_destino):
    os.makedirs(diretorio_destino)

# Função para fazer o download de um arquivo CSV
def download_csv(csv_url):
    try:
        # Fazendo uma requisição GET para baixar o arquivo CSV
        with requests.get(csv_url, stream=True) as response:
            # Verificando se a requisição foi bem-sucedida
            if response.status_code == 200:
                # Obtendo o nome do arquivo do URL
                nome_arquivo = os.path.join(diretorio_destino, csv_url.split("/")[-1])
                # Escrevendo o conteúdo do arquivo no disco em partes
                with open(nome_arquivo, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Arquivo {nome_arquivo} baixado com sucesso!")
            else:
                print(f"Falha ao baixar o arquivo {csv_url}. Status code: {response.status_code}")
    except Exception as e:
        print(f"Falha ao baixar o arquivo {csv_url}: {e}")

# Fazendo uma requisição GET para obter o conteúdo da página
response = requests.get(url)

# Verificando se a requisição foi bem-sucedida
if response.status_code == 200:
    # Criando o objeto BeautifulSoup para analisar o HTML
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Inicializando uma lista vazia para armazenar os URLs dos arquivos CSV
    urls = []
    
    # Encontrando todos os links na página
    links = soup.find_all("a", href=True)
    
    # Iterando sobre os links encontrados
    for link in links:
        # Verificando se o link aponta para um arquivo CSV
        if link["href"].endswith(".csv"):
            # Adicionando o link do arquivo CSV à lista de URLs
            urls.append(link["href"])
    
    # Agora, temos a lista de URLs dos arquivos CSV, podemos fazer o download deles em paralelo
    with ThreadPoolExecutor(max_workers=5) as executor:  # Número máximo de threads de download
        executor.map(download_csv, urls)
else:
    print("Falha ao acessar a página:", response.status_code)


# Lista para armazenar os DataFrames de cada arquivo CSV
dataframes = []

# Iterar sobre os arquivos no diretório
for arquivo in os.listdir(diretorio_destino):
    if arquivo.endswith(".csv"):
        # Ler o arquivo CSV e armazenar em um DataFrame
        df = pd.read_csv(os.path.join(diretorio_destino, arquivo), delimiter=';')
        # Adicionar o DataFrame à lista
        dataframes.append(df)

# Concatenar todos os DataFrames em um único DataFrame
df_final = pd.concat(dataframes, ignore_index=True)

df_final['din_instante'] = pd.to_datetime(df_final['din_instante'])

# Exibir informações sobre o DataFrame final
print(df_final.info())

# Exibir as primeiras linhas do DataFrame final
print(df_final.head())

# Importar a nova base de dados
df_usinas = pd.read_csv("8 - fator de capacidade Mapa - ODS_data.csv")

df_usinas['id_ons'] = 'CJU_' + df_usinas['id_usina']

# Fazer a junção entre os DataFrames usando a coluna "id_ons" como chave
df_final = pd.merge(df_final, df_usinas[['id_ons', 'Val Latitude Sindat', 'Val Longitude Sindat']], on='id_ons', how='left')

# Renomear as colunas de latitude e longitude
df_final.rename(columns={"Val Latitude Sindat": "latitude", "Val Longitude Sindat": "longitude"}, inplace=True)

# Exibir as primeiras linhas do DataFrame final com as novas colunas de latitude e longitude
print(df_final.tail())

#########################################################

# Criar um mapa centrado em uma localização inicial
m = folium.Map(location=[-15.793889, -47.882778], zoom_start=4,tiles='OpenStreetMap')  # Centralizado no Brasil

# Filtrar as linhas com valores não nulos de latitude e longitude
df_final_filtered = df_final.dropna(subset=['latitude', 'longitude'])

# Agrupar o DataFrame filtrado por latitude e longitude
grupos = df_final_filtered.groupby(['latitude', 'longitude'])

# Iterar sobre os grupos únicos
for (latitude, longitude), grupo in grupos:
    # Obter o nome da usina do primeiro registro do grupo
    nom_usina = grupo['nom_usina'].iloc[0]
    # Adicionar um marcador para cada coordenada única
    folium.Marker([latitude, longitude], popup=nom_usina).add_to(m)

# Exibir o mapa
m
m.save('mapa_usinas.html')

#########################################################

# Selecionar apenas os registros do último mês
ultimo_mes = df_final_filtered['din_instante'].max().to_period('M')
df_ultimo_mes = df_final_filtered[df_final_filtered['din_instante'].dt.to_period('M') == ultimo_mes]

# Agrupar por latitude e longitude e calcular a média do var_geracao
media_val_geracao = df_ultimo_mes.groupby(['latitude', 'longitude','id_ons'])['val_geracao'].mean().reset_index()

# Criar o mapa
m = folium.Map(location=[-15.793889, -47.882778], zoom_start=4)

# Adicionar marcadores ao mapa
for index, row in media_val_geracao.iterrows():
    folium.CircleMarker(location=[row['latitude'], row['longitude']],
                        radius=10,  # Tamanho fixo dos pontos
                        color=None,  # Sem contorno
                        fill=True,
                        fill_color='deeppink',  # Esquema de cores
                        fill_opacity=row['val_geracao']/media_val_geracao['val_geracao'].max(),  # Opacidade baseada no valor de geração
                        popup=f"Variação de Geração: {row['val_geracao']}").add_to(m)

# Salvar o mapa como um arquivo HTML
m.save('mapa_pontos_coloridos.html')

# Exibir o mapa
m

######################################################

# Selecionar apenas os registros do último mês
ultimo_mes = df_final_filtered['din_instante'].max().to_period('M')
df_ultimo_mes = df_final_filtered[df_final_filtered['din_instante'].dt.to_period('M') == ultimo_mes]

# Agrupar por latitude e longitude e calcular a média do var_geracao
media_var_geracao = df_ultimo_mes.groupby(['latitude', 'longitude'])['val_geracao'].mean().reset_index()

# Criar o mapa
m = folium.Map(location=[-15.793889, -47.882778], zoom_start=4)

# Converter os dados para o formato de lista de tuplas (latitude, longitude, peso)
heat_data = [[row['latitude'], row['longitude'], row['val_geracao']] for index, row in media_var_geracao.iterrows()]

# Adicionar o mapa de calor ao mapa
HeatMap(heat_data).add_to(m)

# Salvar o mapa como um arquivo HTML
m.save('mapa_calor.html')