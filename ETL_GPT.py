import pandas as pd
import click


def extract(input_file):
    # Función para extraer datos de un archivo
    return pd.read_csv(input_file)


def transform(data):
    # Función para transformar los datos
    data['Transformed_Column'] = data['Original_Column'] * 2
    return data


def load(data, output_file):
    # Función para cargar datos en un nuevo archivo
    data.to_csv(output_file, index=False)


# Función principal que llama a las funciones extract, transform y load
click.echo('ETL en progreso...')

input = "in.csv"
output = "out.csv"

# Extraer datos
data = extract(input)

# Transformar datos
data_transformed = transform(data)

# Cargar datos en un nuevo archivo
load(data_transformed, output)

click.echo('ETL completado con éxito.')
