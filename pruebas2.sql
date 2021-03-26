--  select count(*),now() from registro ;
-- select ip, count(ip),referrer from registro where length(user_agent)<20 or  referrer not like '%eluniversal%' and logfile = 3 group by 3 order by 2 desc limit 100
-- delete  from registro
select * from registro where ip = '190.202.82.98'
-- cod_retorno='404';
-- SELECT  ip,count(*) from registro group by 1 order by 2 desc limit 10
-- describe registro