import csv
from builtins import map

import ETL_Abinitio_Stile_COMMON as COMMON
import yaml
import ETL_JOINS


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
        globals()[x["variable_name"]] = leer_archivo(x["file_name"], x["schema"].split("|"), "|")


# ________________________________________________________

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


def escribir_archivo(lista, path, delimitador):
    with open(path, "w", encoding="UTF-8", newline="") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=lista[0].keys(), delimiter=delimitador)
        writer.writeheader()
        writer.writerows(lista)


# ________________________________________________________

def execute_filters(transformations):
    for filt in transformations["filters"]:
        temp_list = []
        for registro in globals()[filt["variable_name"]]:
            if not eval(filt["code"]):
                print("true")
                temp_list.append(registro)
            else:
                print("false")
        globals()[filt["variable_name"]] = temp_list
        #print(globals()[filt["variable_name"]])

def execute_joins(transformations):
    for join in transformations["joins"]:
        globals()["main"] = left_join(globals()[join["input_main"]], globals()[join["input_lookup"]],
                                      join["key_main"], join["key_lookup"], join["prefix"])


def execute_transformations(transformations):
    execute_filters(transformations)
    execute_joins(transformations)
    output_list = []
    for registro in globals()["main"]:
        output_list.append(transform(registro, transformations))
    return output_list


def transform(registro, transformations):
    output_record = {}
    for mapeo in transformations["mappings"]:
        if mapeo["type"] == "hardcode":
            output_record.update({mapeo["output_field_name"]: mapeo["parameters"]["value"]})

        if mapeo["type"] == "direct":
            output_record.update({mapeo["output_field_name"]: registro[mapeo["parameters"]["input_field_name"]]})

        if mapeo["type"] == "transformation":
            if mapeo["parameters"]["transformation_type"] == "manual":
                output_record.update({mapeo['output_field_name']: mapeo['parameters']['code']})

                exec("output_record.update({mapeo['output_field_name'] : " + mapeo['parameters']['code'] + "})")

    return output_record


# __________________________________________________________

def renombrar_claves(diccionario, prefijo):
    diccionario_renombrado = {prefijo + antigua_clave: valor for antigua_clave, valor in diccionario.items()}
    return diccionario_renombrado


def left_join(list1, list2, list1_key, list2_key, prefix):
    result = []
    for item1 in list1:
        joined_items = item1.copy()
        key1 = item1.get(list1_key)
        match_found = False
        item2 = {}

        for item2 in list2:
            key2 = item2.get(list2_key)

            if key1 == key2:
                joined_items.update(renombrar_claves(item2.copy(), prefix))
                match_found = True
                break  # Salir del bucle una vez que se encuentra una coincidencia

        if not match_found:
            # Si no hay coincidencia, agregar campos vacíos del diccionario con prefijo
            empty_dict = {f"{prefix}{k}": "" for k in item2.keys()}
            joined_items.update(empty_dict)

        result.append(joined_items)

    return result


def inner_join(list1, list2, list1_key, list2_key, prefix):
    result = []
    for item1 in list1:
        key1 = item1.get(list1_key)

        for item2 in list2:
            key2 = item2.get(list2_key)

            if key1 == key2:
                joined_items = item1.copy()
                joined_items.update(renombrar_claves(item2.copy(), prefix))
                result.append(joined_items)

    return result


# __________________________________________________________

mappings = load_config("mappings.yaml")

read_files(mappings)

lista_output = execute_transformations(mappings)

for x in lista_output:
    print(x)

escribir_archivo(lista=lista_output, path="out.csv", delimitador="|")
