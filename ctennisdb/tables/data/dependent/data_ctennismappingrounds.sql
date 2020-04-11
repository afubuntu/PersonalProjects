DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennismappingrounds';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennismappingrounds
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
      (           select '1st round' as ctcode, 'roundof128' as ctcodeofficial
        union all select '2nd round' as ctcode, 'roundof64'  as ctcodeofficial
        union all select '3rd round' as ctcode, 'roundof32'  as ctcodeofficial
        union all select '4th round' as ctcode, 'roundof16'  as ctcodeofficial
        union all select '1/4' as ctcode, 'quarterfinals' as ctcodeofficial
        union all select 'quarterfinal' as ctcode, 'quarterfinals' as ctcodeofficial
        union all select '1/2' as ctcode, 'semifinals' as ctcodeofficial
        union all select 'semifinal' as ctcode, 'semifinals' as ctcodeofficial
        union all select 'fin' as ctcode, 'finals' as ctcodeofficial
        union all select 'final' as ctcode, 'finals' as ctcodeofficial
        union all select 'rubber 1' as ctcode, 'roundrobin' as ctcodeofficial
        union all select 'groupstage' as ctcode, 'roundrobin' as ctcodeofficial
        union all select 'group stage' as ctcode, 'roundrobin' as ctcodeofficial
        union all select 'q 1' as ctcode, '1stroundqualifying' as ctcodeofficial
        union all select 'q 2' as ctcode, '2ndroundqualifying' as ctcodeofficial
        union all select 'qual.' as ctcode, 'lastroundqualifying' as ctcodeofficial
        union all select 'olympic bronze' as ctcode, 'roundbronze' as ctcodeofficial
        union all select 'olympicbronze' as ctcode, 'roundbronze' as ctcodeofficial
        union all select 'bronze' as ctcode, 'roundbronze' as ctcodeofficial
      ) as tmp
      left outer join ctennismappingrounds as r 
           on  lower(r.ctcode)=lower(tmp.ctcode) and lower(r.ctcodeofficial)=lower(tmp.ctcodeofficial)
      where r.ctcode is null and r.ctcodeofficial is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;
