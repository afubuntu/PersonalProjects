DO 
$do$
declare __table_name character varying(100);
begin
   __table_name := 'ctennistournamentscategories';

   if not exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      raise notice 'The table <%> does not exist. It will be created.',__table_name;

      create table ctennistournamentscategories
     	(
        ctdid integer not null,
        ctcode varchar(100) not null constraint uniq_ctennistournamentscategories_ctcode unique,
        ctlabel varchar(200),
        ctpoints integer,
        ctcreatedon timestamp,
        ctupdatedon timestamp,

        primary key(ctdid)    		
     	);

      raise notice 'The table <%> has been successfully created.',__table_name;
   else
      raise notice 'The table <%> already exists.',__table_name;
   end if;
end
$do$;
