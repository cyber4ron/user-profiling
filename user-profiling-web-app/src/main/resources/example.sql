select count(*) as count,
avg(delegations.price_max) as avg_price_max
from customer
where nested('delegations', delegations.biz_circle = '六里桥' and delegations.price_max > 100000 and delegations.price_max < 5000000)
group by nested(delegations.biz_circle)

select * from customer
select delegations from customer where nested('delegations', delegations.del_id is not null) limit 3

 limit 1000
order by avg_price_max desc

select *
from customer
where nested('delegations', delegations.biz_circle = '六里桥')
and nested('delegations', delegations.city_id = 110000)
and nested('delegations', delegations.price_max > 100000)
and nested('delegations', delegations.price_max < 5000000) limit 1000
order by nested(delegations.area_min) desc

select count(*) as count,
avg(delegations.price_max) as avg_price_max
from customer
where nested('delegations', delegations.biz_circle like '%桥') -- x
and nested('delegations', delegations.city_id = 110000)
and nested('delegations', delegations.price_max > 100000)
and nested('delegations', delegations.price_max < 5000000)


select count(*), avg(list_price) from house where list_price > 100000 and list_price < 10000000 group by resblock_id

{
    "sql" : "select * from online_user_20160516 where _type = 'srh' and dtl_id = 'BJSJ92018676' order by ts desc"
}

