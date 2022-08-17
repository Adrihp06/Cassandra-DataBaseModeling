from cassandra.cluster import Cluster
import pandas as pd
import numpy as np
#se siguieron los pasos de la practica en todo momento, no se crearon aquellas tablas que no se exigieran en el ejercicio
#ni tampoco las funciones de consulta en relación a dichas tablas
#el código usado no difiere mucho con el que se presentó en las clases.


#vamos a comenzar creando las clases de cada atributo y de cada relación
class Cliente:
    def __init__(self, DNI, Nombre, Calle, Ciudad):
        self.DNI = DNI
        self.Nombre=Nombre
        self.Calle = Calle
        self.Ciudad = Ciudad
class Prestamo:
    def __init__(self, Numero, Cantidad):
        self.Numero = Numero
        self.Cantidad = Cantidad
    def __init__(self, Numero, Cantidad, Id):
        self.Numero = Numero
        self.Cantidad = Cantidad
        self.Id = Id
class Sucursal:
    def __init__(self, Id, Nombre, Ciudad, Activo):
        self.Id = Id
        self.Nombre = Nombre
        self.Ciudad = Ciudad
        self.Activo = Activo
class Cuenta:
    def __init__(self, Numero, Saldo):
        self.Numero = Numero
        self.Saldo = Saldo
    def __init__(self, Numero, Saldo, Id):
        self.Numero = Numero
        self.Saldo = Saldo       
        self.Id = Id
class Tarjeta:
    def __init__(self, Nombre, Servicios, Tipo):
        self.Nombre = Nombre
        self.Servicios = Servicios
        self.Tipo = Tipo
class Beneficiario:
    def __init__(self, DNI, Nombre):
        self.DNI = DNI
        self.Nombre = Nombre
#relaciones n:m
class DetalleTar:
    def __init__(self, Limite, Nombre, Numero):
        self.Limite = Limite
        self.Nombre = Nombre
        self.Numero = Numero
class CuBen:
    def __init__(self, Numero, DNI):
        self.Numero = Numero
        self.DNI = DNI
class Depositante:
    def __init__(self, Numero, DNI):
        self.Numero = Numero
        self.DNI = DNI
class Prestatario:
    def __init__(self, DNI, Numero):
        self.DNI = DNI
        self.Numero = Numero

#Funcion para pedir datos de un cliente e insertarlos en la Base de Datos
def insertTabla1SoporteCliente():
    dni = input ("Dame dni del cliente: ")
    nombre = input ("Dame nombre del cliente: ")
    ciudad = input ("Dame la ciudad del cliente: ")
    calle = input("Dame la calle del cliente: ") 

    #Ahora insertamos los datos en la tabla 1
    c = Cliente (dni, nombre, calle, ciudad)
    insertStatement = session.prepare ("INSERT INTO tabla1 (cliente_ciudad, cliente_dni, cliente_calle, cliente_nombre) VALUES (?, ?, ?, ?)")
    session.execute (insertStatement, [c.Ciudad, c.DNI, c.Calle, c.Nombre])

    #Insertamos los datos en la tabla SoporteCliente
    insertStatementSop = session.prepare ("INSERT INTO SoporteCliente (cliente_dni, cliente_calle, cliente_ciudad, cliente_nombre) VALUES (?, ?, ?, ?)")
    session.execute (insertStatementSop, [c.DNI, c.Calle, c.Ciudad, c.Nombre ])



def insertTabla8SoporteTarjeta():
    nombre = input ("Dame nombre de la tarjeta: ")
    tipo = input ("Dame el tipo de tarjeta: ")
    #iniciamos la colección (set)
    servicios = set()  
    servicio = input ("Introduzca un servicio, vacío para parar: ")
    while (servicio != ""):
        servicios.add(servicio)
        servicio = input("Introduzca una servicio, vacío para parar: ")

    t = Tarjeta (nombre, servicios, tipo)
    insertStatement = session.prepare ("INSERT INTO tabla8 (tarjeta_servicio, tarjeta_tipo, tarjeta_nombre, tarjeta_servicios) VALUES (?, ?, ?, ?)")
    for value in servicios:
        session.execute(insertStatement, [value, t.Tipo, t.Nombre, t.Servicios])
    
    insertStatementSop = session.prepare ("INSERT INTO SoporteTarjeta (tarjeta_nombre, tarjeta_servicios, tarjeta_tipo) VALUES (?, ?, ?)")
    session.execute(insertStatementSop, [t.Nombre, t.Servicios, t.Tipo])

#Vamos ahora a crear las relaciones
#La relación depositante unicamente involucra a la tabla5, es la columna de agregacion por lo que tenemos que colocar el update
def insertRelDepositante():
    dni = input ("Dame dni del cliente: ")
    saldo = int(input("Dame el saldo de la cuenta: "))
    #Tabla5
    insertStatement = session.prepare ("UPDATE tabla5 SET saldosum = saldosum + ? WHERE cliente_dni = ?")
    session.execute(insertStatement, [saldo, dni])



#Esta relación solo involucra a la tabla 2
def insertRelPrestario():
    dni = input ("Dame dni del cliente: ")
    numero = int(input("Dame el número del préstamo: "))
    cantidad = float(input("Dame la cantidad del préstamo: "))
    cliente = extraerDatosCliente (dni)
    if (cliente != None):
        insertStatement = session.prepare("INSERT INTO tabla2 (prestamo_numero, prestamo_cantidad, cliente_nombre, cliente_dni) VALUES (?,?,?,?)")
        session.execute(insertStatement, [numero, cantidad, cliente.Nombre, dni])
    else:
        print ("Este cliente no tiene información asociada, rellene la información correspondiente primero")


#Esta relación trabaja solamente con la tabla 7, con ayuda de la tabla soporte extraemos los datos necesarios
def insertRelDetalleTar():
    limite = float(input("Dame el límite de la tarjeta: "))
    numero = int(input("Dame el número de cuenta: "))
    nombre = input ("Dame nombre de la tarjeta: ")
    tarjeta = extraerDatosTarjeta(nombre)
    if (tarjeta != None):
        insertStatement = session.prepare ("INSERT INTO tabla7 (detalletar_limite, cuenta_numero, tarjeta_nombre, tarjeta_servicios, tarjeta_tipo) VALUES (?, ?, ?, ?, ?)")
        session.execute(insertStatement, [limite, numero, nombre, tarjeta.Servicios, tarjeta.Tipo])
    #añado un print que me de información sobre el error
    else:
        print ("Este nombre de tarjeta no tiene información asociada, rellene la información correspondiente primero")


#Tabla de extracción de datos de las tablas soporte
def extraerDatosCliente(DNI):
    select = session.prepare ("SELECT * FROM SoporteCliente WHERE cliente_dni = ?") 
    filas = session.execute (select, [DNI,])
    for fila in filas:
        #creamos instancia del cliente
        c = Cliente (DNI,  fila.cliente_nombre, fila.cliente_calle, fila.cliente_ciudad) 
        return c




def extraerDatosTarjeta(Nombre):
    select = session.prepare ("SELECT * FROM SoporteTarjeta WHERE tarjeta_nombre = ?") 
    filas = session.execute (select, [Nombre,])
    for fila in filas:
        t = Tarjeta (Nombre, fila.tarjeta_servicios, fila.tarjeta_tipo) 
        return t



#Vamos a crear funciones de actualizacion, necesitas la primary key para poder hacerlo y nos ayudamos del update
def actualizarCalle():
    ciudad = input("Indique la ciudad de la que quiera actualizar la calle: ")
    dni = input("Indique el DNI: ")
    calle = input("Indique la nueva calle: ")
    updateCalleCliente = session.prepare ("UPDATE tabla1 SET cliente_calle = ? WHERE cliente_ciudad = ? AND cliente_dni= ?")
    session.execute(updateCalleCliente,[calle, ciudad, dni])



#Es necesario colocar tanto la partition como la clustering key para buscar la columna deseada, despues con ayuda de la tabla soporte insertamos los datos ya actualizados
def actualizarLimite():
    oldlimite = float(input("Indique limite actual de la tarjeta: "))
    newlimit = float(input("Indique el nuevo limite de la tarjeta: "))
    numero = int(input("Indique el numero de la cuenta: "))
    nombre = input("Indique el nombre de la tarjeta: ")
    tarjeta = extraerDatosTarjeta(nombre)
    if (tarjeta != None):
    #usamos la tabla soporte para introducir de nuevo los datos correspondiente, al tener que modificar una primary key
    # vamos a tener que hacer un delete e insert
        borrarLimite= session.prepare ("DELETE FROM tabla7 WHERE detalletar_limite = ? AND cuenta_numero = ? AND tarjeta_nombre = ?")
        session.execute(borrarLimite, [oldlimite,numero,nombre])
        insertStatement = session.prepare ("INSERT INTO tabla7 (detalletar_limite, cuenta_numero, tarjeta_nombre, tarjeta_servicios, tarjeta_tipo) VALUES (?, ?, ?, ?, ?)")
        session.execute(insertStatement, [newlimit, numero, nombre, tarjeta.Servicios, tarjeta.Tipo])
  

def consultaTabla1():
    ciudad= input("Introduzca la ciudad por la que se quiere buscar: ")
    consulta = session.prepare ("SELECT * FROM tabla1 WHERE cliente_ciudad = ?")
    filas = session.execute(consulta, [ciudad,])
    for fila in filas:
        print("Ciudad: " + fila.cliente_ciudad)
        print("DNI: " + fila.cliente_dni)
        print("Calle:  " + fila.cliente_calle)
        print("Nombre: " + fila.cliente_nombre)
        print("")



def consultaTabla2():
    numero= int(input("Introduzca el número de préstamo por el que se quiere buscar: "))
    consulta = session.prepare ("SELECT * FROM tabla2 WHERE prestamo_numero = ?")
    filas = session.execute(consulta, [numero,])
    for fila in filas:
        print("Número de préstamo: " + str(fila.prestamo_numero))
        print("Cantidad:  " + str(fila.prestamo_cantidad))
        print("Nombre: " + fila.cliente_nombre)
        print("DNI: " + fila.cliente_dni)
        print("")



def consultaTabla5():
    dni= input("Introduzca el dni por el que se quiere buscar: ")
    consulta = session.prepare ("SELECT * FROM tabla5 WHERE cliente_dni = ?")
    filas = session.execute(consulta, [dni,])
    for fila in filas:
        print ("DNI: " + fila.cliente_dni)
        print ("Saldo sumado: " + str(fila.saldosum))
        print("")





def consultaTabla7():
    limite= float(input("Introduzca el limite por el que se quiere buscar: "))
    consulta = session.prepare ("SELECT * FROM tabla7 WHERE detalletar_limite = ?")
    filas = session.execute(consulta, [limite,])
    for fila in filas:
        print ("Límite de la tarjeta: " + str(fila.detalletar_limite))
        print ("Número de cuenta: " + str(fila.cuenta_numero))
        print ("Nombre de la tarjeta: " + fila.tarjeta_nombre)
        print ("Tipo de tarjeta: " + fila.tarjeta_tipo)
        print ("Servicios: " + str(fila.tarjeta_servicios))
        print ("")




def consultaTabla8():
    servicio= input("Introduzca el servicio por el que se quiere buscar: ")
    consulta = session.prepare ("SELECT * FROM tabla8 WHERE tarjeta_servicio = ?")
    filas = session.execute(consulta, [servicio,])
    for fila in filas:
        print ("Servicio: " + fila.tarjeta_servicio)
        print ("Nombre de la tarjeta: " + fila.tarjeta_nombre)
        print ("Tipo de tarjeta: " + fila.tarjeta_tipo)
        print ("Servicios: " + str(fila.tarjeta_servicios))
        print ("")



#Conexión con Cassandra
cluster = Cluster()
session = cluster.connect('adrianhernandez')
numero = -1

while (numero != 0):
    print ("Introduzca un número para ejecutar una de las siguientes operaciones:")
    print ("1. Insertar un cliente")
    print ("2. Insertar una tarjeta")
    print ("3. Relación Depositante(tabla8)")
    print ("4. Relación Prestario(tabla2)")
    print ("5. Relación DetalleTar(tabla7)")
    print ("6. Cambiar la calle en función de la ciudad")
    print ("7. Cambiar el limite de la tarjeta segun el numero de cuenta y el nombre de la tarjeta")
    print ("8. Consulta tabla 1")
    print ("9. Consulta tabla 2")
    print ("10. Consulta tabla 5")
    print ("11. Consulta tabla 7")
    print ("12. Consulta tabla 8")
    numero = int (input())
    if (numero == 1):
        insertTabla1SoporteCliente()
    elif (numero == 2):
        insertTabla8SoporteTarjeta()
    elif (numero == 3):
        insertRelDepositante()
    elif (numero == 4):
        insertRelPrestario()
    elif (numero == 5):
        insertRelDetalleTar()
    elif (numero == 6):
        actualizarCalle()
    elif (numero == 7):
        actualizarLimite()
    elif (numero == 8):
        consultaTabla1()
    elif (numero == 8):
        consultaTabla1()
    elif (numero == 9):
        consultaTabla2()
    elif (numero == 10):
        consultaTabla5()
    elif (numero == 11):
        consultaTabla7()
    elif (numero == 12):
        consultaTabla8()        
    else:
        print ("Número incorrecto")
cluster.shutdown() 