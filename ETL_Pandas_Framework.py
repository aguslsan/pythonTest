import csv
import pandas as pd

def leerArchivoDelimitado(file, delimitador, nombreCampos):
    tiposDeDato={}
    for elemento in nombreCampos:
        tiposDeDato.update({elemento: "string"})

    return pd.read_csv(file, delimiter=delimitador, names=nombreCampos, dtype=tiposDeDato)

def leerArchivoPosicional(file, longitudes, nombreCampos):
    tiposDeDato={}
    for elemento in nombreCampos:
        tiposDeDato.update({elemento: "string"})

    return pd.read_fwf(file, widths=longitudes, names=nombreCampos, dtype=tiposDeDato)

# def merge(tabla, tabla2, )