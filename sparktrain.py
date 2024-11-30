from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StringType

# Configurar la sesión de Spark
spark = SparkSession.builder \
    .appName("SentimentAnalysisStreamingTraining") \
    .getOrCreate()

# Definir esquema para los datos transmitidos
schema = StructType() \
    .add("OriginalTweet", StringType()) \
    .add("Sentiment", StringType())

# Leer datos del socket
host = "18.206.151.201"  # Dirección IP del servidor (Cloud9)
port = 9999
raw_stream = spark.readStream.format("socket") \
    .option("host", host) \
    .option("port", port) \
    .load()

# Parsear el flujo de datos
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as csv") \
    .selectExpr("split(csv, ',') as fields") \
    .select(
        col("fields")[0].alias("OriginalTweet"),
        col("fields")[1].alias("Sentiment")
    )

# Preprocesamiento para Spark ML
tokenizer = Tokenizer(inputCol="OriginalTweet", outputCol="words")
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="text_features")
label_indexer = StringIndexer(inputCol="Sentiment", outputCol="label")
assembler = VectorAssembler(inputCols=["text_features"], outputCol="features")

# Modelo de regresión logística
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Pipeline para procesamiento y modelo
pipeline = Pipeline(stages=[
    tokenizer,
    stopwords_remover,
    vectorizer,
    label_indexer,
    assembler,
    lr
])

# Acumulador para los datos de entrenamiento
batch_accumulator = []

# Ruta del bucket S3 para guardar el modelo
s3_model_path = "s3://covid-cloud-bigdata/sentiment_analysis_model"

# Función para procesar cada batch de datos
def train_and_save(batch_df, batch_id):
    global batch_accumulator

    # Verificar si el batch contiene la señal de finalización
    if not batch_df.isEmpty():
        rows = batch_df.collect()
        for row in rows:
            if row["OriginalTweet"] == "END_OF_STREAM":
                print("Señal de finalización detectada. Terminando el proceso.")
                query.stop()
                return

    # Acumular datos del batch actual
    if not batch_df.isEmpty():
        batch_accumulator.append(batch_df)

    # Combinar datos acumulados
    if len(batch_accumulator) > 0:
        combined_df = batch_accumulator[0]
        for additional_batch in batch_accumulator[1:]:
            combined_df = combined_df.union(additional_batch)

        # Si no hay suficientes datos, esperar más
        if combined_df.count() < 50:  # Ajusta el umbral según sea necesario
            print(f"Batch {batch_id}: Esperando más datos para entrenamiento.")
            return

        print(f"Entrenando modelo con datos acumulados para batch {batch_id}...")
        train_data = combined_df
        model = pipeline.fit(train_data)

        # Guardar el modelo entrenado en S3
        print(f"Guardando modelo entrenado en {s3_model_path}...")
        model.write().overwrite().save(s3_model_path)
        print("Modelo guardado exitosamente.")

        # Limpiar el acumulador después de entrenar
        batch_accumulator.clear()

# Configurar el streaming para entrenar el modelo en cada batch
query = parsed_stream.writeStream \
    .foreachBatch(train_and_save) \
    .outputMode("append") \
    .start()

query.awaitTermination()