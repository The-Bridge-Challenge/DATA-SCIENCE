from flask import Flask, render_template, request
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
    # cliente = datos.values.tolist()  # Convierte el DataFrame a una lista de listas
    # return render_template('respuesta.html', cliente=cliente)
    df = convertir_df(df,columnas_a_convertir)
    insertar_datos(df)
    return render_template('ingesta.html')

if __name__ == '__main__':
    app.run(port=5050, host='0.0.0.0')
