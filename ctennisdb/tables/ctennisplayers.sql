DO 
$do$
declare __table_name character varying(100);
begin
   __table_name := 'ctennisplayers';

   if not exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      raise notice 'The table <%> does not exist. It will be created.',__table_name;

      create table ctennisplayers
     	(
     		ctdid serial,
        ctcode varchar(200) not null constraint uniq_ctennisplayers_ctcode unique,
     		ctname varchar(200) not null,
     		ctcountry varchar(3), 
        ctgender varchar(1),       
     		ctbirthdate date,
     		ctweight varchar(20),
     		ctheight varchar(20),
     		ctbeginpro date,
     		cthandstyle varchar(200),
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
