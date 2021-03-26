import pymongo
from operator import itemgetter

conn = pymongo.Connection('10.3.1.59')
db = conn['analyzer']
coll = db['registro']                   
#iplist=coll.distinct("ip")
#ipcnt=[]

#coll.ensure_index( "ip" )

#for ip in iplist:
#	cnt = coll.find({'ip':ip},{'ip':1}).count()
#	ipcnt.append((ip,cnt))
#
#diccionario={}			
#ordipcnt=sorted(ipcnt, key=itemgetter(1),reverse=True)
#print len(ordipcnt)
#for i in range(0,20):
#	print ordipcnt[i]
#	diccionario[ordipcnt[i][0]]=ordipcnt[i][1]
			
#print diccionario
##// use all fields
keys = {"ip" : 1}

##// set intial values
initial = {"count" : 0}

##// JavaScript function to perform
reduce = "function(obj, prev) { prev.count+=1; }";

##// only use documents where the "a" field is greater than 1
condition = {"cod_retorno":"304"}

g = coll.group(  keys, condition,initial, reduce )

print g
