DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctenniscountries';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctenniscountries
     	(
     		ctcode,
     		ctlabel,
        ctcreatedon 
     	)
      select 
        tmp.ctcode,
        tmp.ctlabel,
        current_timestamp 
      from    
      (           select 'USA' as ctcode,  'United States' as ctlabel
        union all select 'UAE' as ctcode,  'United Arab Emirates' as ctlabel
        union all select 'AUS' as ctcode,  'Australia' as ctlabel
        union all select 'BRA' as ctcode,  'Brazil' as ctlabel
        union all select 'TUR' as ctcode,  'Turkey' as ctlabel
        union all select 'ZZZ' as ctcode,  'Not defined' as ctlabel
      ) as tmp
      left outer join ctenniscountries as co 
           on  co.ctcode=tmp.ctcode
      where co.ctcode is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;
