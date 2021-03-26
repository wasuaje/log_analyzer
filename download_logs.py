#!/usr/bin/python
# -*- encoding: utf-8 -*-

import dbdata, datetime, commands, os,  time
import threading

def run_cmd(cmd):
			output = commands.getoutput(cmd)

def write_file(newLine):
	file = open("resultado.txt", "a")
	file.write(newLine)
	file.close()

if __name__ == '__main__':
	feci=datetime.datetime.today()
	a = dbdata.Dbdata()
	servers=a.get_servers()
	h=1
	for srv in servers.keys():		
		logfiles=a.get_logfiles(srv)		
		for log in logfiles.keys():			
			if not os.path.exists("data/"+logfiles[log][0]+'_'+str(log)+'_log.txt'):				
				cmd=" scp root@"+servers[srv][1]+':'+logfiles[log][1]+" data/"+logfiles[log][0]+'_'+str(log)+'_log.txt'
				print 'Ejecutando => '+cmd		
				t = threading.Thread(target=run_cmd, args=(cmd,))		#creo un hilo por cada log para hacer 7 descargas simultaneas
				t.start()
				time.sleep(2)			
	active_count = threading.activeCount()
	while active_count >  1:																#mientras haya + de un hilo (el principal) sigo contando hilos y queda solo el ppal continuo
		active_count = threading.activeCount()
	fecf=datetime.datetime.today()
	diferencia=fecf-feci
	minutos=diferencia.seconds/60
	segundos=diferencia.seconds-(minutos*60)
	print 'Finalizando descarga de logs en '+str(minutos)+ ' minutos y '+str(segundos)+' segundos'
	fin = 'Finalizando descarga de logs en '+str(minutos)+ ' minutos y '+str(segundos)+' segundos'
	write_file(fin)
