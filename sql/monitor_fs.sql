select * from recorded
order by folder_name desc;

SELECT pg_size_pretty( pg_database_size('financial_statements') );

SELECT pg_size_pretty( pg_total_relation_size('tablename') );

select count(1) from fs_num;