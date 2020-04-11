DO 
$do$
declare 
      __table_name character varying(100);
      __foreign_key_name character varying(100);
begin
   __table_name := 'ctennisplayers';
   __foreign_key_name :='ctennisplayers_ctcountries_ctcode_fkey';

   if not exists (select 1 from information_schema.table_constraints where table_schema='public' and table_name=__table_name and constraint_name=__foreign_key_name) then

      raise notice 'The foreign key <%> does not exist. It will be created.',__foreign_key_name;

      execute 'alter table ' || __table_name || ' add constraint ' || __foreign_key_name || ' foreign key (ctcountry) references ctenniscountries(ctcode);'; 

      raise notice 'The foreign key <%> has been successfully created.',__foreign_key_name;
   else
      raise notice 'The foreign key <%> already exists.',__foreign_key_name;
   end if;  
end
$do$;
