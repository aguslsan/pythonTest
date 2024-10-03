import csv
import datetime

ct = datetime.datetime.now()
print("Empieza la ejecucion:", ct)

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


def filtrar(filtro, lista):
    return list(filter(filtro, lista))


# Escribir archivo de prueba

datos_lista = [
    ["paco", "42", 3],
    ["agu", "4", 2],
    ["messi", "420", 69]
]

with open("in.csv", "w", encoding="UTF-8", newline="") as archivo:
    writer = csv.writer(archivo, delimiter="|")
    # writer.writeheader()
    writer.writerows(datos_lista)

# ________________________________________________________

archivo_path = "in.csv"
esquema_entrada = ["nombre", "numero", "divisor"]

lista_input = leer_archivo(archivo_path=archivo_path, campos=esquema_entrada, delimiter="|")

lista_filtrada = filtrar(filtro=lambda elemento: elemento["nombre"] != "messi" or elemento["numero"] != "420",
                         lista=lista_input)

lista_output = []

for registro in lista_filtrada:

    registro_output = {"nombre_y_numero": registro.get("nombre") + registro.get("numero")[:1] if registro.get(
        "nombre") == "agu" else "gato",
                       "divido": float(registro.get("numero")) / float(registro.get("divisor")),
                       "palabra": "",
                       "palabra2": "SOY_UN_HARDCODE"
                       }
    lista_output.append(registro_output)


escribir_archivo(lista=lista_output, path="out.csv", delimitador="|")

# ________________________________________________________


ct = datetime.datetime.now()
print("ejecucion finalizada: ", ct)


