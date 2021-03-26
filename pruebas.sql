--SELECT  ip,count(*) from registro group by 1 order by 2 desc limit 10
--select ip,MAX(fecha) from registro
-- where ip='67.195.115.41' 
--group by 1
--limit 5
--select count(ip) from registro where date(fecha)='2011-04-05'
--select ip,user_agent,logfile  from registro 
	--select ip, count(ip),user_agent from registro where length(user_agent)<20 or  user_agent like '%.com%'
	--and logfile = 1
	--group by 3 order by 2 desc

--select ip,user_agent from registro where length(user_agent)<20 or  user_agent like '%.com%'
 select * from sqlite_master where type='index'