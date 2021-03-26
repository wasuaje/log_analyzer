#!/usr/bin/python
# -*- encoding: utf-8 -*-

import commands, os, glob
import dbdatapym
import datetime, time
import threading

def run_cmd(comando):
	out = commands.getoutput(comando)
	return out

def get_month(txtmonth):
	if txtmonth=='Jan': month = 1
	if txtmonth=='Feb': month = 2
	if txtmonth=='Mar': month = 3
	if txtmonth=='Apr': month = 4
	if txtmonth=='May': month = 5
	if txtmonth=='Jun': month = 6
	if txtmonth=='Jul': month = 7
	if txtmonth=='Aug': month = 8
	if txtmonth=='Sep': month = 9
	if txtmonth=='Oct': month = 10
	if txtmonth=='Nov': month = 11
	if txtmonth=='Dec': month = 12	
	return month
	
def parse_nginx(output):
	line=output.split(' - ')
	ip=line[0]
	fecha=line[2].replace('[', '').replace(']', '').replace('/', ':').replace(' -0430', '')
	fecha=fecha.split(':')
	fecha=datetime.datetime(int(fecha[2]), get_month(fecha[1]), int(fecha[0]), int(fecha[3]) , int(fecha[4]), int(fecha[5]))
	fecha=fecha.isoformat()
	metodo=line[3][1:4]
	request=line[3][5:len(line[3])-1]
	cod_retorno=line[8]
	init=line[12].find('resolution')
	resolution=line[12][init+11:init+11+7]
	useragent=line[10].replace('"', '').replace('http://m.eluniversal.com/', '')
	useragent=useragent.split(' -')
	if len(useragent) > 1:
		useragent=useragent[1]
	else:
		useragent=useragent[0]
	datos=[ip, fecha, metodo, request, cod_retorno, useragent , resolution]
	return datos
	
def parse_dyn(output):
	line=output.split(' ')	
	ip=line[0]
	fecha=line[3].replace('[', '').replace(']', '').replace('/', ':').replace(' -0430', '')
	fecha=fecha.split(':')
	fecha=datetime.datetime(int(fecha[2]), get_month(fecha[1]), int(fecha[0]), int(fecha[3]) , int(fecha[4]), int(fecha[5]))
	metodo=line[5][1:4]
	request=line[6]
	cod_retorno=line[9]
	useragent=''
	for a in range(12, len(line)):
		useragent+=line[a]+' '
	datos=[ip, fecha, metodo, request, cod_retorno, useragent.replace('"',"")]
	return datos
	
def parse_static(output):
	line=output.split(' ')	
	ip=line[0]
	fecha=line[1].replace('[', '').replace(']', '').replace('/', ':').replace(' -0430', '')
	fecha=fecha.split(':')
	fecha=datetime.datetime(int(fecha[2]), get_month(fecha[1]), int(fecha[0]), int(fecha[3]) , int(fecha[4]), int(fecha[5]))
	metodo=line[3][1:4]
	request=line[4]
	cod_retorno=line[6]
	referrer=line[9]
	datos=[ip, fecha, metodo, request, cod_retorno, referrer]
	return datos
	

def man_files(filename, tipo, logfile):
	file = open(filename, "r")
	comando='parse_'+tipo+'(line)'
	#a = dbdata.Dbdata()
	print 'Ejecutando => '+comando
	for line in file:
		datos = eval(comando)		
		datos.append(logfile)		
		a.insertar(datos, tipo)
	a.cerrar()
	file.close()
	
def run():		
	path = 'data/'
	for infile in glob.glob( os.path.join(path, '*.*') ):		
		thefile=infile.split('_')
		tipo=thefile[0].split('/')[1]
		logfile=thefile[0]
		filename=infile		
		t = threading.Thread(target=man_files, args=(infile,tipo,logfile))		#creo un thread para cada log guardado = 7 hilos simultaneos
		t.start()
		time.sleep(2)
				
def write_file(newLine):
	file = open("resultado.txt", "a")
	file.write(newLine)
	file.close()

if __name__ == '__main__':	
	a = dbdatapym.Dbdata()
	feci=datetime.datetime.today()	
	print 'vaciando tabla...'
	a.vaciar()
	print 'eliminando indices para facilitar insercion...'
	#a.indices('drop')		#elimino los indices para hacer mas rapida la insercion
	print 'cargando y parseando logs'	
	run()
	
	# no continuar hasta que se cierren los threads abiertos
	active_count = threading.activeCount()
	while active_count >  1:				#mientras haya mas de 1 thread no continua
		active_count = threading.activeCount()
		
	print 'creando indices...'
#	a.indices('create')	#los creo de nuevo para hacer rapidos las busquedas
#	a.cerrar()
	fecf=datetime.datetime.today()
	diferencia=fecf-feci
	minutos=diferencia.seconds/60
	segundos=diferencia.seconds-(minutos*60)
	print 'Finalizando carga en en '+str(minutos)+ ' minutos y '+str(segundos)+' segundos'
	os.remove("resultado.txt")
	write_file('Finalizando carga en en '+str(minutos)+ ' minutos y '+str(segundos)+' segundos \n')
