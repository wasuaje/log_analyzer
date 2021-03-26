#!/usr/bin/python
# -*- encoding: utf-8 -*-

import sqlite3

class Dbdata:
	def __init__(self):
		self.conn = sqlite3.connect('dbdata.sqlite')
					
	def insertar(self, datos, tipo):		
		#tipo nginx,dyn,static						
		if tipo == 'nginx':
			sql='insert into registro (id, ip, fecha, metodo, request, cod_retorno, user_agent , resolution, logfile) values (null,?,?,?,?,?,?,?,?)'
		elif tipo == 'dyn': 
			sql='insert into registro (id, ip, fecha, metodo, request, cod_retorno, user_agent, logfile) values (null,?,?,?,?,?,?,?)'
		elif tipo == 'static': 
			sql='insert into registro (id, ip, fecha, metodo, request, cod_retorno, referrer, logfile) values (null,?,?,?,?,?,?,?)'			
		else:
			print "no se pudo insertar"
		c = self.conn.cursor()
		try:
			c.execute(sql,datos)
		except sqlite3.Error, e:
			print "An error occurred:", e.args[0]
			print  "Mientras se insertaba la linea: ",  datos
			
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
			sql = ['drop index if exists idxregistro_ip', 'drop index if exists idxregistro_fecha', 'drop index if exists idxregistro_logfile']
			#sql = ['drop index if exists idxregistro_ip, idxregistro_fecha, idxregistro_logfile']
		elif accion == 'create':
			sql = ['create index if not exists idxregistro_ip on registro (ip)','create index if not exists idxregistro_fecha on registro (fecha)', 
			'create index if not exists idxregistro_logfile on registro (logfile)']
			#sql=['create index if not exists idxregistro on registro(ip, logfile,  fecha)']
		for i in sql:
			c.execute(i)
		self.conn.commit()
		#self.conn.close()		
		#c.close()

	def get_servers(self):		
		c = self.conn.cursor()
		c.execute('select * from servidor ') #where id = 1
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
		sql="select max(fecha), min(fecha) from registro where ip='"+ip+"'"
		#print sql
		c.execute(sql)
		for line in c:
			c.close()
		#self.conn.close()
		return line
	
	def wierd_agents(self):
		c = self.conn.cursor()
		sql="select ip, count(ip),user_agent from registro where length(user_agent)<20 or  user_agent like '%.com%'	and logfile = 1 group by 3 order by 2 desc limit 100"
		#print sql
		c.execute(sql)
		diccionario={}
		for line in c:
			#print line
			diccionario.update({line[0]:[line[1], line[2]] })
		c.close()
		return diccionario
