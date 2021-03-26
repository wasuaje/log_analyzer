#!/usr/bin/python
# -*- encoding: utf-8 -*-

import MySQLdb

class Dbdata:
	def __init__(self):
		self.conn = MySQLdb.connect(host='localhost',user='root', passwd='www4214',db='dbdata2')
		                            
	def insertar(self, datos, tipo):		
		#tipo nginx,dyn,static						
		if tipo == 'nginx':
			sql='insert delayed into registro ( ip, fecha, metodo, request, cod_retorno, user_agent , resolution, logfile) values (%s,%s,%s,%s,%s,%s,%s,%s)'
		elif tipo == 'dyn': 
			sql='insert delayed into registro ( ip, fecha, metodo, request, cod_retorno, user_agent, logfile) values (%s,%s,%s,%s,%s,%s,%s)'
		elif tipo == 'static': 
			sql='insert delayed into registro ( ip, fecha, metodo, request, cod_retorno, referrer, logfile) values (%s,%s,%s,%s,%s,%s,%s)'			
		else:
			print "no se pudo insertar"
		c = self.conn.cursor()
		try:
			#print sql, datos
			c.execute(sql,datos)
		except MySQLdb.Error, e:
			print "An error occurred:", e.args[0]
			print  "Mientras se insertaba la linea: ",  datos
	
	def bulk_insert(self, filename,tipo,logfile):
		
		if tipo == 'nginx':
			sql="load data local infile '"+filename+"' into table registro fields terminated by '||' ( ip, fecha, metodo, request, cod_retorno, user_agent , resolution, logfile);" 
		elif tipo == 'dyn': 
			sql="load data local infile '"+filename+"' into table registro fields terminated by '||' (ip, fecha, metodo, request, cod_retorno, user_agent, logfile); "
		elif tipo == 'static': 
			sql="load data local infile '"+filename+"' into table registro fields terminated by '||' (ip, fecha, metodo, request, cod_retorno, referrer, logfile);" 
		else:
			print "no se pudo insertar"
		c = self.conn.cursor()
		try:
			#print sql
			c.execute(sql)
		except MySQLdb.Error, e:
			print "Ha habido un Error %d: %s" % (e.args[0], e.args[1])
			#print  "Mientras se insertaba la linea: ",  datos
		
	
	def cerrar(self):
		self.conn.commit()
		self.conn.close()
	
	def vaciar(self):
		c = self.conn.cursor()
		sql='delete from registro'
		c.execute(sql)
		#c.close()
		
	def indices(self, accion):		
		c = self.conn.cursor()
		if accion == 'drop':			
			sql = ['drop index idxregistro_ip on registro', 'drop index idxregistro_fecha on registro', 'drop index idxregistro_logfile on registro']
			#sql = ['drop index if exists idxregistro_ip, idxregistro_fecha, idxregistro_logfile']
		elif accion == 'create':
			sql = ['create index idxregistro_ip on registro (ip)','create index idxregistro_fecha on registro (fecha)', 
			'create index idxregistro_logfile on registro (logfile)']
			#sql=['create index if not exists idxregistro on registro(ip, logfile,  fecha)']
		for i in sql:
			try:
				c.execute(i)
			except MySQLdb.Error, e:
				print "Ha habido un Error %d: %s" % (e.args[0], e.args[1])
		self.conn.commit()
		#self.conn.close()		
		#c.close()

	def get_servers(self):		
		c = self.conn.cursor()
		c.execute('select * from servidor ')  #where id = 1
		diccionario={}
		for line in c:
			diccionario[line[0]]=[line[1], line[2]]
		c.close()
		return diccionario
		
	def get_logfiles(self, srv_id):
		c = self.conn.cursor()
		c.execute('select * from logfile where server = '+ str(srv_id))	#+ ' and id=1 limit 1000'
		diccionario={}
		for line in c:
			#print line
			diccionario[line[0]]= [line[1], line[2]]
		c.close()
		return diccionario
	
	def top_ten(self):
		c = self.conn.cursor()
		sql='SELECT  ip,count(*) from registro group by 1 order by 2 desc limit 10'
		c.execute(sql)
		diccionario={}
		for line in c:
			#print line
			diccionario.update({line[0]:line[1]})
		c.close()
		return diccionario
	
	def max_min_dates(self, ip):
		c = self.conn.cursor()
		#sql="select max(fecha), min(fecha) from registro where ip='"+ip+"'"
		sql="select second(TIMEDIFF(max(fecha),min(fecha))) from registro where ip='"+ip+"'"
		#print sql
		c.execute(sql)
		for line in c:
			c.close()
		#self.conn.close()
		return line
	
	def wierd_agents(self):
		c = self.conn.cursor()
		sql="select ip, count(ip),user_agent from registro where length(user_agent)<20 or  user_agent like '%.com%' and logfile = 1 group by 3 order by 2 desc limit 100"
		#print sql
		c.execute(sql)
		diccionario={}
		for line in c:
			#print line
			diccionario.update({line[0]:[line[1], line[2]] })
		c.close()
		return diccionario
