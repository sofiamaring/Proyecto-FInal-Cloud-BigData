from flask import Flask, request, jsonify
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession

app = Flask(__name__)

# Ruta del modelo descargado
MODEL_PATH = './sentiment_analysis_model'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("SentimentAnalysisAPI") \
    .getOrCreate()

# Cargar el modelo desde el sistema de archivos
model = PipelineModel.load(MODEL_PATH)

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()  # Recibir datos JSON desde la solicitud
    input_text = data.get('text', '')

    if not input_text:
        return jsonify({'error': 'No se proporcionó texto para la predicción'}), 400

    # Crear un DataFrame de Spark para la predicción
    df = spark.createDataFrame([(input_text,)], ['OriginalTweet'])

    # Realizar predicción
    predictions = model.transform(df)
    prediction_result = predictions.select('prediction').collect()[0][0]

    return jsonify({'text': input_text, 'sentiment': prediction_result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)from flask import Flask, request, jsonify
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession

app = Flask(__name__)

# Ruta del modelo descargado
MODEL_PATH = './sentiment_analysis_model'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("SentimentAnalysisAPI") \
    .getOrCreate()

# Cargar el modelo desde el sistema de archivos
model = PipelineModel.load(MODEL_PATH)

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()  # Recibir datos JSON desde la solicitud
    input_text = data.get('text', '')

    if not input_text:
        return jsonify({'error': 'No se proporcionó texto para la predicción'}), 400

    # Crear un DataFrame de Spark para la predicción
    df = spark.createDataFrame([(input_text,)], ['OriginalTweet'])

    # Realizar predicción
    predictions = model.transform(df)
    prediction_result = predictions.select('prediction').collect()[0][0]

    return jsonify({'text': input_text, 'sentiment': prediction_result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)from flask import Flask, request, jsonify
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession

app = Flask(__name__)

# Ruta del modelo descargado
MODEL_PATH = './sentiment_analysis_model'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("SentimentAnalysisAPI") \
    .getOrCreate()

# Cargar el modelo desde el sistema de archivos
model = PipelineModel.load(MODEL_PATH)

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()  # Recibir datos JSON desde la solicitud
    input_text = data.get('text', '')

    if not input_text:
        return jsonify({'error': 'No se proporcionó texto para la predicción'}), 400

    # Crear un DataFrame de Spark para la predicción
    df = spark.createDataFrame([(input_text,)], ['OriginalTweet'])

    # Realizar predicción
    predictions = model.transform(df)
    prediction_result = predictions.select('prediction').collect()[0][0]

    return jsonify({'text': input_text, 'sentiment': prediction_result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)from flask import Flask, request, jsonify
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql import SparkSession

app = Flask(__name__)

# Ruta del modelo descargado
MODEL_PATH = './sentiment_analysis_model'

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("SentimentAnalysisAPI") \
    .getOrCreate()

# Cargar el modelo desde el sistema de archivos
model = PipelineModel.load(MODEL_PATH)

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()  # Recibir datos JSON desde la solicitud
    input_text = data.get('text', '')

    if not input_text:
        return jsonify({'error': 'No se proporcionó texto para la predicción'}), 400

    # Crear un DataFrame de Spark para la predicción
    df = spark.createDataFrame([(input_text,)], ['OriginalTweet'])

    # Realizar predicción
    predictions = model.transform(df)
    prediction_result = predictions.select('prediction').collect()[0][0]

    return jsonify({'text': input_text, 'sentiment': prediction_result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)