#!/usr/bin/python
# -*- encoding: utf-8 -*-

import dbdata
from socket import gethostbyaddr 
import operator, os
import datetime, time

dirs={}
dirs["sysadmin"]="wasuaje@eluniversal.com"

def send_mail():
	import smtplib
	# Import the email modules we'll need
	#from email.mime.text import MIMEText
	from email import MIMEText			#2.4 compat
	# Open a plain text file for reading.  For this example, assume that
	# the text file contains only ASCII characters.
	fp = open("resultado.txt", 'rb')
	# Create a text/plain message
	#msg = MIMEText(fp.read())
        msg = MIMEText.MIMEText(fp.read())		#2.4 compat

	fp.close()


	for mail in dirs.keys():
		msg['Subject'] = "Estadisticas de Analisis de logfiles WEB"
		msg['From'] = "Sysadmin@localhost"
		msg['To'] = dirs[mail]
		prueba = dirs[mail]
		# Send the message via our own SMTP server, but don't include the envelope header.
		s = smtplib.SMTP('localhost')		
                s.sendmail(msg['From'], msg['To'],  msg.as_string())
		s.quit()

def write_file(newLine):
	file = open("resultado.txt", "a")
	file.write(newLine)
	file.close()
	
def nslooky(ip):
      try: 
           output = gethostbyaddr(ip)
           return output[0]
      except: 
           output = "not found" 
           return output

def get_topten():	
	a=dbdata.Dbdata()	
	b = a.top_ten()
	hosts={}
	for clave in b.keys():
		hosts.update({clave:nslooky(clave)})
	#print hosts
	sorted_b = sorted(b.iteritems(), key=operator.itemgetter(1), reverse=True)
	#print sorted_b
	#print hosts
	c= "*******************************************\n"
	c+="**      IPS CON MAS CONEXIONES          **\n"
	c+= "*******************************************\n"
	c+= 'IP'+'\t\t'+'Conex.'+'\t'+'DNS'+'\t\t\t'+'Req/seg'+'\n'
	for ip in sorted_b:
		try:
			reqbysec=float(ip[1]/get_seconds(ip[0]))
		except:
			reqbysec=1
		c += ip[0]+'\t'+str(ip[1])+'\t'+hosts[ip[0]]+'\t'+str(reqbysec)+'\n'
	return c
	
def get_seconds(ip):
	a=dbdata.Dbdata()	
	b = a.max_min_dates(ip)
	#print datetime.datetime.strptime(b[1])
	#mytime = b[1]
	#time_format = "%Y-%m-%d %H:%M:%S"
	#print b
	#max_date=(datetime.datetime.fromtimestamp(time.mktime(time.strptime(b[0], time_format))))
	#min_date=(datetime.datetime.fromtimestamp(time.mktime(time.strptime(b[1], time_format))))
	#diferencia=max_date-min_date
	return b[0] #diferencia.seconds
	
	#c=datetime.datetime(b[0])
	#print c
def get_wierd_agents():
	a=dbdata.Dbdata()
	b= a.wierd_agents()
	sorted_b = sorted(b.iteritems(), key=operator.itemgetter(1), reverse=True)
	c=  "*******************************************\n"
	c+= "** USER AGENTS NO CONVENCIONALES (MOBIL) **\n"
	c+= "*******************************************\n"
	c+= 'IP'+'\t\t'+'Conex.'+'\t'+'User_Agent \n'
	for ip in sorted_b:		
		#print ip
		c += ip[0]+'\t'+str(ip[1][0])+'\t'+ip[1][1]+'\n'
	return c
	
		
if __name__ == '__main__':		
	feci=datetime.datetime.today()
	print "obteniendo top ten"
	write_file(get_topten())
	write_file('\n\n')
	#get_max_min_dates('67.202.47.68')
	print "obteniendo user agents"
	write_file(get_wierd_agents())
	send_mail()	
	fecf=datetime.datetime.today()
	diferencia=fecf-feci
	minutos=diferencia.seconds/60
	segundos=diferencia.seconds-(minutos*60)
	print 'Finalizando reporte en '+str(minutos)+ ' minutos y '+str(segundos)+' segundos'
	fin='Finalizando reporte en '+str(minutos)+ ' minutos y '+str(segundos)+' segundos \n'
	write_file(fin)
