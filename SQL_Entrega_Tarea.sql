-- Asunto: Tarea entregable SQL Máster Big Data & Data Science UCM
-- Autor: Tamara Infante
-- Fecha: 27/04/2023
-- Plataforma utilizada: Snowflake




---  EJERCICIO 1: 

/*
	A continuación, vamos a realizar las siguientes consultas y para ello vamos a necesitar los archivos incluidos en el comprimido operaciones_ucm.zip 	disponibles en la plataforma del máster:

	- Crear una base de datos con el nombre tarea_ucm.
	- Crear un esquema de base de datos con el nombre operaciones_ucm.
	- Creamos las tres tablas correspondientes a los 3 archivos: orders, refunds y merchants.
	Recuerda seleccionar el tipo de dato más adecuado para cada uno de los campos de las tres tablas.
	- Opcional: Si estamos realizando los ejercicios sobre Snowflake, rellenamos las tablas a partir de los datos incluidos en los archivos *.csv.
*/

-- (*) "Show SQL" que aparece en las pantallas de Snowflake al crear la BBDD y las tablas

-- Crear base de datos
CREATE DATABASE tarea_ucm;

-- Crear esquema
CREATE SCHEMA "TAREA_UCM"."OPERACIONES_UCM";

-- Crear file format para leer csv separado por punto y coma
CREATE FILE FORMAT "TAREA_UCM"."OPERACIONES_UCM".csv_punto_coma TYPE = 'CSV' COMPRESSION = 'AUTO' FIELD_DELIMITER = ';' RECORD_DELIMITER = '\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');

-- Crear tabla orders
CREATE TABLE "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" ("ORDER_ID" STRING, "CREATED_AT" DATE, "STATUS" STRING, "AMOUNT" FLOAT, "REFUNDED_AT" DATE, "MERCHANT_ID" STRING, "COUNTRY" STRING);
PUT file://<file_path>/orders.csv @ORDERS/ui1682585910611

COPY INTO "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" FROM @/ui1682585910611 FILE_FORMAT = '"TAREA_UCM"."OPERACIONES_UCM"."CSV_PUNTO_COMA"' ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;

-- Crear tabla refunds
CREATE TABLE "TAREA_UCM"."OPERACIONES_UCM"."REFUNDS" ("ORDER_ID" STRING, "REFUNDED_AT" DATE, "AMOUNT" FLOAT);
PUT file://<file_path>/refunds.csv @REFUNDS/ui1681555531767

COPY INTO "TAREA_UCM"."OPERACIONES_UCM"."REFUNDS" FROM @/ui1681555531767 FILE_FORMAT = '"TAREA_UCM"."OPERACIONES_UCM"."CSV_PUNTO_COMA"' ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;

-- Crear tabla merchants
CREATE TABLE "TAREA_UCM"."OPERACIONES_UCM"."MERCHANTS" ("MERCHANT_ID" STRING, "NAME" STRING);
PUT file://<file_path>/merchants.csv @MERCHANTS/ui1681555715638

COPY INTO "TAREA_UCM"."OPERACIONES_UCM"."MERCHANTS" FROM @/ui1681555715638 FILE_FORMAT = '"TAREA_UCM"."OPERACIONES_UCM"."CSV_PUNTO_COMA"' ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;







---  EJERCICIO 2: A partir de las tablas incluidas en la base de datos tarea_ucm vamos a realizar las siguientes consultas:

/* 
   1. Realizamos una consulta donde obtengamos por país y estado de operación, el total de operaciones y su importe promedio. La consulta debe cumplir las 	siguientes condiciones:
		a. Operaciones posteriores al 01-07-2015.
		b. Operaciones realizadas en Francia, Portugal y España.
		c. Operaciones con un valor mayor de 100 € y menor de 1500€. 
      Ordenamos los resultados por el promedio del importe de manera descendente. 
*/


USE "TAREA_UCM"."OPERACIONES_UCM";

SELECT country,
       status,
       COUNT(order_id) AS total_operaciones,
       AVG(amount) AS importe_promedio
FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS"
WHERE created_at > '2015-07-01' AND
      country IN ('Francia', 'Portugal', 'España') AND
      amount > 100 AND amount < 1500
GROUP BY 1, 2
ORDER BY 4 DESC;





/* 
    2. Realizamos una consulta donde obtengamos los 3 países con el mayor número de operaciones, el total de operaciones, la operación con un valor máximo y 
       la operación con el valor mínimo para cada país. La consulta debe cumplir las siguientes condiciones:
		a. Excluimos aquellas operaciones con el estado “Delinquent” y “Cancelled”.
		b. Operaciones con un valor mayor de 100 €.
*/


--- NOTA: No sabía si con operación con valor máximo te referías a order_id con max y min amount o solamente preguntabas por el amount, asique he sacado los order_id también.

SELECT orders.country,
       COUNT(orders.order_id) AS total_operaciones, 
       t3.order_id AS order_max_amount,
       MAX(orders.amount) AS max_amount,
       t4.order_id AS order_min_amount, 
       MIN(orders.amount) AS min_amount

FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS orders

--- unimos con la tabla en la que tenemos country - order_id con max amount de los top 3 países en número operaciones
JOIN (
        SELECT t1.country, t1.order_id
        FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS t1
              INNER JOIN (
                            SELECT country, MAX(amount) AS max_amount
                            FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS"
                            --- mismos filtros que en consulta principal
                            WHERE status NOT IN ('DELINQUENT','CANCELLED') AND   
                                  amount > 100
                            GROUP BY country
                            ORDER BY COUNT(order_id) DESC   --- número de operaciones
                            LIMIT 3                         --- top 3 países
                         ) AS t2 ON t1.country = t2.country AND t1.amount = t2.max_amount
     ) AS t3 ON orders.country = t3.country

--- unimos con la tabla en la que tenemos country - order_id con min amount de los top 3 países en número operaciones
JOIN (
        SELECT t1.country, t1.order_id
        FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS t1
              INNER JOIN (
                            SELECT country, MIN(amount) AS min_amount
                            FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS"
                            --- mismos filtros que en consulta principal
                            WHERE status NOT IN ('DELINQUENT','CANCELLED') AND   
                                  amount > 100
                            GROUP BY country
                            ORDER BY COUNT(order_id) DESC   --- número de operaciones
                            LIMIT 3                         --- top 3 países
                         ) AS t2 ON t1.country = t2.country AND t1.amount = t2.min_amount
     ) AS t4 ON orders.country = t4.country

WHERE status NOT IN ('DELINQUENT','CANCELLED') AND
        amount > 100
GROUP BY 1, 3, 5
ORDER BY 2 DESC
LIMIT 3; 







---  EJERCICIO 3 : A partir de las tablas incluidas en la base de datos tarea_ucm vamos a realizar las siguientes consultas:

/*   
    1. Realizamos una consulta donde obtengamos, por país y comercio, el total de operaciones, su valor promedio y el total de devoluciones. La consulta 		 debe cumplir las siguientes condiciones:
		a. Se debe mostrar el nombre y el id del comercio.
		b. Comercios con más de 10 ventas.
		c. Comercios de Marruecos, Italia, España y Portugal.
		d. Creamos un campo que identifique si el comercio acepta o no devoluciones. Si no acepta (total de devoluciones es igual a cero) el campo debe 		   contener el valor “No” y si sí lo acepta (total de devoluciones es mayor que cero) el campo debe contener el valor “Sí”. Llamaremos al campo 		   “acepta_devoluciones”.
	Ordenamos los resultados por el total de operaciones de manera ascendente.
*/ 


SELECT orders.country,
       merchants.name,
       merchants.merchant_id,
       COUNT(orders.order_id) AS total_operaciones,
       AVG(orders.amount) AS importe_promedio,
       COUNT(refunds.order_id) AS total_devoluciones,
       CASE WHEN total_devoluciones > 0 THEN  'Si'
            ELSE 'No'
       END AS acepta_devoluciones
       
FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS orders
    JOIN "TAREA_UCM"."OPERACIONES_UCM"."MERCHANTS" AS merchants ON orders.merchant_id = merchants.merchant_id
    JOIN "TAREA_UCM"."OPERACIONES_UCM"."REFUNDS" AS refunds ON orders.order_id = refunds.order_id
WHERE orders.country IN ('Marruecos', 'Italia', 'España', 'Portugal') AND 
      --- Comercios con más de 10 operaciones en total (no por país)
      merchants.name IN
                          (
                            SELECT merchants.name
                            FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS orders
                              JOIN "TAREA_UCM"."OPERACIONES_UCM"."MERCHANTS" AS merchants ON orders.merchant_id = merchants.merchant_id
                            GROUP BY 1
                            HAVING COUNT(orders.order_id) > 10
                          )
GROUP BY 1,2,3
ORDER BY 4 ASC;





/* 
   2. Realizamos una consulta donde vamos a traer todos los campos de las tablas operaciones y comercios. De la tabla devoluciones vamos a traer el conteo  	de devoluciones por operación y la suma del valor de las devoluciones. 
	
	Una vez tengamos la consulta anterior, creamos una vista con el nombre orders_view dentro del esquema tarea_ucm con esta consulta.
  	
	Nota: La tabla refunds contiene más de una devolución por operación por lo que para hacer el cruce es muy importante que agrupemos las devoluciones

*/

SELECT orders.*,
       merchants.name,  -- para no duplicar merchant_id
       refunds.refunds,
       refunds.amount_refunds
FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS orders
   JOIN "TAREA_UCM"."OPERACIONES_UCM"."MERCHANTS" AS merchants ON orders.merchant_id = merchants.merchant_id
   JOIN (
            SELECT orders.order_id,
                    COUNT(refunds.order_id) AS refunds,
                    SUM(refunds.amount) AS amount_refunds
            FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS orders
                FULL OUTER JOIN "TAREA_UCM"."OPERACIONES_UCM"."REFUNDS" AS refunds ON orders.order_id = refunds.order_id
            GROUP BY 1
        )  AS refunds ON orders.order_id = refunds.order_id
ORDER BY 2;


---      Crear vista con el nombre orders_view dentro del esquema operaciones_ucm con esta consulta

CREATE VIEW "TAREA_UCM"."OPERACIONES_UCM"."orders_view"
AS 
        SELECT orders.*,
               merchants.name,
               refunds.refunds,
               refunds.amount_refunds
        FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS orders
           JOIN "TAREA_UCM"."OPERACIONES_UCM"."MERCHANTS" AS merchants ON orders.merchant_id = merchants.merchant_id
           JOIN (
                    SELECT orders.order_id,
                            COUNT(refunds.order_id) AS refunds,
                            SUM(refunds.amount) AS amount_refunds
                    FROM "TAREA_UCM"."OPERACIONES_UCM"."ORDERS" AS orders
                        FULL OUTER JOIN "TAREA_UCM"."OPERACIONES_UCM"."REFUNDS" AS refunds ON orders.order_id = refunds.order_id
                    GROUP BY 1
                )  AS refunds ON orders.order_id = refunds.order_id
        ORDER BY 2;
 