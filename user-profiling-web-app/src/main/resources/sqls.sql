-- 昨日带看量, 60080
SELECT count(*) from customer_touring where creation_date between '20160530T000000+0000' and '20160601T000000+0000'
SELECT count(*) from customer_touring where creation_date >= '20160530T000000+0000' and creation_date <= '20160601T000000+0000'


--
