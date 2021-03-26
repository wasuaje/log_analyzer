#!/usr/bin/python
# -*- encoding: utf-8 -*-

#import MySQLdb
import pymongo
from operator import itemgetter

class Dbdata:
	def __init__(self):
		#self.conn = MySQLdb.connect(host='localhost',user='root', passwd='www4214',db='dbdata2')
		self.conn = pymongo.Connection('10.3.1.59')
		self.db = self.conn['analyzer']
		# Returns Collection Object
		self.coll = self.db['registro']                   
		
	def insertar(self, datos, tipo):
		if tipo  == 'nginx':
			data={"ip":datos[0], "fecha":datos[1], "metodo":datos[2], "request":datos[3], "cod_retorno":datos[4], "user_agent":datos[5] , "resolution":datos[6], "logfile":datos[7]}
		elif tipo == 'dyn': 
			data={"ip":datos[0], "fecha":datos[1], "metodo":datos[2], "request":datos[3], "cod_retorno":datos[4], "user_agent":datos[5], "logfile":datos[6]}
		elif tipo == 'static': 
			data={"ip":datos[0], "fecha":datos[1], "metodo":datos[2], "request":datos[3], "cod_retorno":datos[4], "referrer":datos[5], "logfile":datos[6]}
		else: 
			print "no se pudo insertar"				
		idinsert = self.coll.insert(data)
		print datos,tipo,idinsert
		

	def top_ten(self):
		iplist=self.coll.distinct("ip")
		ipcnt=[]
		for ip in iplist:
			cnt = self.coll.find({'ip':ip},{'ip':1}).count()
			ipcnt.append((ip,cnt))

		diccionario={}			
		ordipcnt=sorted(ipcnt, key=itemgetter(1),reverse=True)
		
		for i in range(0,20):
			print ordipcnt[i]	
			diccionario[ordipcnt[i][0]]=ordipcnt[i][1]
					
		return diccionario


	
	def cerrar(self):
		self.conn.commit()
		self.conn.close()
	
	def vaciar(self):
		self.coll.remove({})
		
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
		#c = self.conn.cursor()
		sql="select ip, count(ip),user_agent from registro where length(user_agent)<20 or  user_agent like '%.com%' and logfile = 1 group by 3 order by 2 desc limit 100"
		#print sql
		c.execute(sql)
		diccionario={}
		for line in c:
			#print line
			diccionario.update({line[0]:[line[1], line[2]] })
		c.close()
		return diccionario
