from .sqlalchemy_multiconnector import SQLConnector, BASE, manage_session

use_filterparams_binding_doc = """
El atributo 'binding' de la clase QueryBinding acepta varias opciones para mapear los campos de la consulta a los atributos de la entidad.
 A continuación se describen las opciones disponibles:
- 'param': especifica el nombre del parámetro de la consulta HTTP que se utilizará para obtener el valor del campo. Por ejemplo, 'filter[param][title]'.

- 'join': especifica el nombre de la relación de SQLAlchemy que se utilizará para realizar la unión con la tabla asociada. Por ejemplo, 'categories' si se quiere unir la tabla Article con la tabla Category.

- 'op': especifica el operador que se utilizará para comparar el valor del campo con el valor proporcionado en la consulta. Por ejemplo, eq para igualdad, gt para mayor que, in_ para pertenencia a un conjunto, etc.

- 'val': especifica un valor fijo que se utilizará en lugar del valor proporcionado en la consulta. Por ejemplo, 'John' para seleccionar todos los registros que tengan el valor 'John' en el campo.

- 'convert': especifica una función que se utilizará para convertir el valor proporcionado en la consulta antes de compararlo con el valor del campo. Por ejemplo, una función que convierta una cadena en un objeto datetime.

- 'cast': especifica el tipo de datos al que se debe convertir el valor proporcionado en la consulta antes de compararlo con el valor del campo. Por ejemplo, Integer para convertir una cadena en un entero.

- 'default': especifica un valor predeterminado que se utilizará si no se proporciona ningún valor en la consulta para el campo.

- 'nullable': especifica si se debe permitir valores nulos para el campo. Los valores válidos son True o False.

- 'formatter': especifica una función que se utilizará para dar formato al valor del campo en la respuesta HTTP. Por ejemplo, una función que convierta un objeto datetime en una cadena.

- 'desc': especifica si la columna correspondiente al campo debe ordenarse en orden descendente. Los valores válidos son True o False.

- 'label': especifica el nombre que se utilizará para etiquetar la columna correspondiente al campo en la consulta SQL. Por defecto, se utiliza el nombre del campo.

- 'filter': especifica un filtro que se aplicará a la consulta SQL. Por ejemplo, un filtro que seleccione únicamente los registros que cumplan una condición específica.
"""
