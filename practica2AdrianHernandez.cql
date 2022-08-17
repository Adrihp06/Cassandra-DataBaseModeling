CREATE KEYSPACE adrianhernandez 
WITH replication = {
	'class':'SimpleStrategy',
	'replication_factor':1
};

CREATE TABLE tabla1 ( 
	cliente_ciudad text, 
	cliente_dni text, 
	cliente_nombre text, 
	cliente_calle text, 
	 PRIMARY KEY (cliente_ciudad, cliente_dni)
);

CREATE TABLE tabla2 (
	prestamo_numero int, 
	prestamo_cantidad float, 
	cliente_nombre text, 
	cliente_dni text, 
	PRIMARY KEY(prestamo_numero)
);

CREATE TABLE tabla3 ( 
	sucursal_nombre text, 
	cliente_dni text, 
	cuenta_numero int, 
	cliente_nombre text, 
	PRIMARY KEY(sucursal_nombre, cliente_dni, cuenta_numero)
);

CREATE TABLE tabla4 ( 
	tarjeta_tipo text, 
	cuenta_numero int, 
	cliente_dni text, 
	cliente_nombre text, 
	PRIMARY KEY(tarjeta_tipo, cuenta_numero, cliente_dni)
);

CREATE TABLE tabla5 (
	cliente_dni text, 
	saldosum counter, PRIMARY KEY (cliente_dni)
);

CREATE TABLE tabla6 ( 
	sucursal_ciudad text, 
	sucursal_nombre text, 
	sucursal_id text, 
	sucursal_activo boolean, 
	PRIMARY KEY ((sucursal_ciudad, sucursal_nombre), sucursal_id)
);

CREATE TABLE tabla7 ( 
	detalletar_limite float, 
	cuenta_numero int, 
	tarjeta_nombre text, 
	tarjeta_tipo text, 
	tarjeta_servicios set<text>, 
	PRIMARY KEY (detalletar_limite, cuenta_numero, tarjeta_nombre)
);

CREATE TABLE tabla8 (
	tarjeta_servicio text,
	tarjeta_tipo text, 
	tarjeta_nombre text, 
	tarjeta_servicios set<text>, 
	PRIMARY KEY(tarjeta_servicio, tarjeta_tipo)
);

CREATE TABLE SoporteCliente (
	cliente_dni text, 
	cliente_ciudad text,  
	cliente_nombre text, 
	cliente_calle text,  
	PRIMARY KEY (cliente_dni)
);

CREATE TABLE SoporteTarjeta ( 
	tarjeta_nombre text, 
	tarjeta_tipo text, 
	tarjeta_servicios set<text>, 
	PRIMARY KEY (tarjeta_nombre)
);



