DO 
$do$
declare __table_name character varying(100);
begin
   __table_name := 'ctennismappingplayers';

   if not exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      raise notice 'The table <%> does not exist. It will be created.',__table_name;

      create table ctennismappingplayers
     	(
        ctcode varchar(200) not null  constraint uniq_ctennismappingplayers_ctcode unique,
        ctcodeofficial varchar(200) not null,
        ctcreatedon timestamp,
        ctupdatedon timestamp,

     		primary key(ctcode,ctcodeofficial)
     	);

      raise notice 'The table <%> has been successfully created.',__table_name;
   else
      raise notice 'The table <%> already exists.',__table_name;
   end if;
end
$do$;
