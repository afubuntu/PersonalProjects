DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennismappingsurfaces';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennismappingsurfaces
     	(
     		ctlabel,
     		ctlabelofficial,
        ctcreatedon 
     	)
      select 
        tmp.ctlabel,
        tmp.ctlabelofficial,
        current_timestamp
      from     
      (           select 'I-Hard' as ctlabel, 'Indoor Hard' as ctlabelofficial
        union all select 'O-Hard' as ctlabel, 'Outdoor Hard' as ctlabelofficial
        union all select 'O-Clay' as ctlabel, 'Outdoor Clay' as ctlabelofficial
        union all select 'I-Clay' as ctlabel, 'Indoor Clay' as ctlabelofficial
        union all select 'O-Grass' as ctlabel, 'Outdoor Grass' as ctlabelofficial
        union all select 'I-Grass' as ctlabel, 'Indoor Grass' as ctlabelofficial
      ) as tmp
      left outer join ctennismappingsurfaces as c 
           on  lower(c.ctlabel)=lower(tmp.ctlabel) and lower(c.ctlabelofficial)=lower(tmp.ctlabelofficial)
      where c.ctlabel is null and c.ctlabelofficial is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;
