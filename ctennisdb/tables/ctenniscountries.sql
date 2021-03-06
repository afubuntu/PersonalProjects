DO 
$do$
declare __table_name character varying(100);
begin
   __table_name := 'ctenniscountries';

   if not exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      raise notice 'The table <%> does not exist. It will be created.',__table_name;

      create table ctenniscountries
     	(
     		ctcode varchar(3) not null,
        ctlabel varchar(100),
        ctcreatedon timestamp,
        ctupdatedon timestamp,
        
        primary key(ctcode)
     	);

      raise notice 'The table <%> has been successfully created.',__table_name;
   else
      raise notice 'The table <%> already exists.',__table_name;
   end if;
end
$do$;
