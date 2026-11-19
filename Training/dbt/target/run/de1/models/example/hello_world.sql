
  create view "mydb"."mart"."hello_world__dbt_tmp"
    
    
  as (
    select
    'Hello, dbt!' as message,
    now() as generated_at
  );