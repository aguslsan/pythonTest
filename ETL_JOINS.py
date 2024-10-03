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


# Ejemplo de uso
lista1 = [{'id': 1, 'nombre': 'Alice'}, {'id': 2, 'nombre': 'Bob'}, {'id': 3, 'nombre': 'Charlie'}]
lista2 = [{'id_otro_nombre': 3, 'edad': 25}, {'id_otro_nombre': 5, 'edad': 30}, {'id_otro_nombre': 6, 'edad': 22}]
lista3 = [{"edad": 25, "tipo": "joven"}, {"edad": 30, "tipo": ""}]

resultado = inner_join(lista1, lista2, 'id', 'id_otro_nombre', "LOOKUP_")
resultado_2 = inner_join(resultado, lista3, "LOOKUP_edad", "edad", "LOOKUP2_")

# for elemento in resultado_2:
    # print(elemento)
