DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennismappingcountries';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennismappingcountries
     	(
        ctcode,
        ctcodeofficial,
        ctcreatedon 
     	)
      select 
        tmp.ctcode,
        tmp.ctcodeofficial,
        current_timestamp 
      from    
      (           select 'U.S.A.' as ctcode, 'USA' as ctcodeofficial
        union all select 'U.S.A' as ctcode, 'USA'  as ctcodeofficial
        union all select 'USA' as ctcode, 'USA'  as ctcodeofficial
        union all select 'CT' as ctcode, 'USA'  as ctcodeofficial
        union all select 'U.A.E.' as ctcode, 'UAE'  as ctcodeofficial
        union all select 'U.A.E' as ctcode, 'UAE'  as ctcodeofficial
        union all select 'UAE' as ctcode, 'UAE'  as ctcodeofficial
        union all select 'Multiple Locations' as ctcode, 'ZZZ'  as ctcodeofficial
        union all select 'SGP' as ctcode, 'ZZZ'  as ctcodeofficial
      ) as tmp
      left outer join ctennismappingcountries as r 
           on  lower(r.ctcode)=lower(tmp.ctcode) and lower(r.ctcodeofficial)=lower(tmp.ctcodeofficial)
      where r.ctcode is null and r.ctcodeofficial is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;
