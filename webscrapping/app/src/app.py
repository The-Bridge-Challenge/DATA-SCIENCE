from flask import Flask, render_template, request, jsonify
from funciones import *
from variables import *

app = Flask(__name__)
app.config['DEBUG'] = True

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/result', methods=['POST'])
def result():
    cups = request.form.get('cups')
    df = webscrape(cups)
    cups_m = df.iloc[0]['cups']
    cups = cups_m
    if df is not None:
        df = convertir_df(df, columnas_a_convertir)
        insertar_datos(df)
        mensaje = 'Datos ingresados correctamente'
    else:
        mensaje = 'No se obtuvieron datos para el CUPS proporcionado'

    return jsonify({'mensaje': mensaje, 'cups': cups})

@app.route('/datos/<cups>', methods=['GET'])
def obtener_datos(cups):
    engine = create_engine(servidor)
    query = "SELECT * FROM sips WHERE cups = %(cups)s"
    df = pd.read_sql(query, engine, params={'cups': cups})

    if df.empty:
        return jsonify({"error": "No se encontraron datos para el CUPS proporcionado"}), 404
    else:
        return jsonify(df.to_dict(orient='records'))


if __name__ == '__main__':
    app.run(port=5000, host='0.0.0.0')