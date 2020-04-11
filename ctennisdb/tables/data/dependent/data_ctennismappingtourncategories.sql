DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennismappingtourncategories';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennismappingtourncategories
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
      (           select '250' as ctcode, 'atp250' as ctcodeofficial
        union all select '500' as ctcode, 'atp500'  as ctcodeofficial
        union all select '1000' as ctcode, 'atpmasters1000'  as ctcodeofficial
        union all select 'challenger' as ctcode, 'atpchallenger'  as ctcodeofficial
        union all select '125k' as ctcode, 'wtachallenger'  as ctcodeofficial
      ) as tmp
      left outer join ctennismappingtourncategories as c 
           on  lower(c.ctcode)=lower(tmp.ctcode) and lower(c.ctcodeofficial)=lower(tmp.ctcodeofficial)
      where c.ctcode is null and c.ctcodeofficial is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;
