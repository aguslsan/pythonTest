import csv

import yaml

def leer_archivo(archivo_path, campos, delimiter):
    with open(archivo_path, "r", encoding="UTF-8") as archivo:
        reader = csv.reader(archivo, delimiter=delimiter)
        lista_input = []
        for registro in reader:
            if len(registro) == len(campos):
                # Crear un diccionario con los campos y valores correspondientes
                registro_dict = {campo: valor for campo, valor in zip(campos, registro)}
                lista_input.append(registro_dict)
            else:
                print(f"Advertencia: Registro incompleto en línea {reader.line_num}. Ignorando.")

    return lista_input

def load_config(transformations_file):
    with open(transformations_file, 'r') as f:
        transformations = yaml.safe_load(f)
    return transformations


def read_files(transformations):
    for x in transformations["files"]:
        print (x)
        globals()[x["variable_name"]] = leer_archivo(x["file_name"], x["schema"].split("|"), "|")