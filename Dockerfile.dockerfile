# Dockerfile:
# Usar una imagen base con OpenJDK 11 y Python
FROM openjdk:11-jdk-slim

# Actualizar e instalar herramientas necesarias y dependencias de Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    bash \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Crear enlace simbólico para 'python' si es necesario
RUN ln -s /usr/bin/python3 /usr/bin/python

# Configurar directorio de trabajo
WORKDIR /app

# Copiar el código de la aplicación
COPY app.py /app/

# Copiar el modelo al contenedor
COPY sentiment_analysis_model /app/sentiment_analysis_model

# Instalar dependencias de Python necesarias para la aplicación
RUN pip3 install --no-cache-dir flask pyspark numpy

# Exponer el puerto en el que se ejecutará Flask
EXPOSE 5000

# Comando para ejecutar la aplicación
CMD ["python", "app.py"]