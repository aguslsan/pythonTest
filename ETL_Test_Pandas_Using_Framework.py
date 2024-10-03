import csv
import pandas as pd
import ETL_Pandas_Framework as ETL

# Escribir archivo de prueba delimitado

datos_lookup = [
    ["lautaro", "0 goles"],
    ["julian", "4 goles"],
    ["messi", "7 goles"]
]

with open("in2.txt", "w", encoding="UTF-8", newline="") as archivo:
    writer = csv.writer(archivo, delimiter="|")
    # writer.writeheader()
    writer.writerows(datos_lookup)

datos_lista = [
    ["paco", "42", 3],
    ["agu", "4", 2],
    ["messi", "420", 69]
]

with open("in.csv", "w", encoding="UTF-8", newline="") as archivo:
    writer = csv.writer(archivo, delimiter="|")
    # writer.writeheader()
    writer.writerows(datos_lista)

# Escribir archivo de prueba posicional

datos_lista = [
    ["paco       420      69"],
    ["agu        123      456"],
    ["messi        789      987"]
]

with open("in_positional.csv", "w", encoding="UTF-8", newline="") as archivo:
    writer = csv.writer(archivo)
    # writer.writeheader()
    writer.writerows(datos_lista)













# ________________________________________________________


# Leo archivo de entrada delimitado como dataFrame

input_dataFrame = ETL.leerArchivoDelimitado("in.csv", "|", ["nombre", "numero", "divisor"])

# Leo archivo de entrada delimitado como dataFrame

input_dataFrame_lookup = ETL.leerArchivoDelimitado("in2.txt", "|", ["nombre", "goles"])

# Leo archivo de entrada posicional como dataFrame

input_dataFrame_positional = ETL.leerArchivoPosicional("in_positional.csv", [11, 9, 3], ["nombre", "numero", "divisor"])

# Creo dataFrame de salida y le agrego columnas transformadas

output_dataFrame = pd.DataFrame()

output_dataFrame["nombre_y_numero"] = input_dataFrame["nombre"]+input_dataFrame["numero"]

output_dataFrame["divido"] = pd.to_numeric(input_dataFrame["numero"], downcast="float")/pd.to_numeric(input_dataFrame["divisor"], downcast="float")
output_dataFrame["divido"] = output_dataFrame["divido"].astype("string")

# Escribo archivo de salida

output_dataFrame.to_csv("out2.csv", "|", index=False)

# Creo dataFrame de salida y le agregu los espacios

output_positional_dataFrame = pd.DataFrame()

output_positional_dataFrame["columna"] = (output_dataFrame["nombre_y_numero"].transform(lambda x: str(x).ljust(25))+
                                          output_dataFrame["divido"].transform(lambda x: str(x).ljust(10)))

# print(output_positional_dataFrame)

# Escribo archivo de salida

output_positional_dataFrame.to_csv("out_positional.csv", "|", index=False, header=False)

# ________________________________________________________

# Transformo dataFrame mergeado

def transformacion(elemento_serie1, elemento_serie2):
    return elemento_serie1 + elemento_serie2

merged_dataFrame = pd.merge(input_dataFrame, input_dataFrame_lookup, left_on="nombre", right_on="nombre", how="left")

print(merged_dataFrame)

output_dataFrame_merged = pd.DataFrame()
output_dataFrame_merged["nombre_y_numero"] = merged_dataFrame["nombre"]+merged_dataFrame["numero"]

output_dataFrame_merged["divido"] = pd.to_numeric(merged_dataFrame["numero"], downcast="float")/pd.to_numeric(merged_dataFrame["divisor"], downcast="float")
output_dataFrame_merged["divido"] = output_dataFrame_merged["divido"].astype("string")

output_dataFrame_merged["nombre_y_goles"] = merged_dataFrame["nombre"]+merged_dataFrame["goles"]

# print(output_dataFrame_merged)

# Escribo dataFrame mergeado

output_dataFrame_merged.to_csv("out_merged.csv", "|", index=False)
