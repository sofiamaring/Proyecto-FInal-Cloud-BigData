import socket
import pandas as pd
import re
import emoji
from tqdm import tqdm
import time
from datetime import datetime

# Configuración del servidor
host = "0.0.0.0"  # Dirección para permitir conexiones externas
port = 9999

# Funciones de limpieza
def strip_emoji(text):
    return emoji.replace_emoji(text, "") if hasattr(emoji, 'replace_emoji') else text

def strip_all_entities(text): 
    if not isinstance(text, str):
        return ""
    text = text.replace('\r', '').replace('\n', ' ').lower()
    text = re.sub(r"(?:\@|https?\://|www.)\S+", "", text)
    text = re.sub(r'[^\x00-\x7f]', r'', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_text(text):
    if not isinstance(text, str):
        return ""
    cleaned_text = strip_emoji(text)
    cleaned_text = strip_all_entities(cleaned_text)
    return cleaned_text

# Cargar los datos desde un archivo CSV
file_path = 'Corona_NLP_train.csv'
data = pd.read_csv(file_path, sep=',', encoding='latin1', quotechar='"')

# Limpiar los datos
tqdm.pandas()
data['OriginalTweet'] = data['OriginalTweet'].progress_apply(clean_text)
data['Sentiment'] = data['Sentiment'].str.strip()
data = data[data['OriginalTweet'].str.strip() != '']

# Filtrar columnas necesarias para el streaming
filtered_data = data[['OriginalTweet', 'Sentiment']]

# Iniciar el servidor de streaming
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host, port))
server_socket.listen(1)
print(f"Esperando conexión del cliente en {host}:{port}...")

client_socket, client_address = server_socket.accept()
print(f"Cliente conectado desde {client_address}")

# Transmitir los datos fila por fila
for _, row in filtered_data.iterrows():
    data_to_send = f"{row['OriginalTweet']},{row['Sentiment']}\n"
    client_socket.send(data_to_send.encode("utf-8"))
    print(f"Enviando: {data_to_send.strip()}")
    time.sleep(0.1)  # Simular transmisión de datos en tiempo real

# Enviar la señal de finalización
client_socket.send("END_OF_STREAM\n".encode("utf-8"))
print("Señal de finalización enviada.")

# Cerrar la conexión
client_socket.close()
server_socket.close()