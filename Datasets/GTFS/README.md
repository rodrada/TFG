# Directorio de datasets

Aquí deben almacenarse todos los conjuntos de datos en formato GTFS que se quieran utilizar, cada uno con su propio subdirectorio.

Para el nombre del subdirectorio, se recomienda **no** emplear espacios. Por ejemplo: RioDeJaneiro.

Dentro del subdirectorio han de ponerse todos los ficheros en formato GTFS (agency.txt, calendar.txt, stops.txt, etc.).

Una vez colocado el dataset, debemos asegurarnos de que el formato es correcto, y corregirlo en caso contrario. Para ello, se proporciona el script process\_dataset.py en el directorio de scripts del proyecto. Simplemente nos desplazamos al directorio correspondiente y ejecutamos el siguiente comando:

```
./process_dataset.py <ruta-dataset>
```

Por ejemplo:

```
./process_dataset.py /home/dani/TFG/Datasets/GTFS/RioDeJaneiro
```
