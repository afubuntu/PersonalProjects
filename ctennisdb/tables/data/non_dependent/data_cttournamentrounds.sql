DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'cttournamentrounds';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into cttournamentrounds
     	(
        ctcodeofficial,
     		ctlabel,
        ctcreatedon 
     	)
      select 
        tmp.ctcodeofficial,
        tmp.ctlabel,
        current_timestamp 
      from    
      (           select 'roundof128' as ctcodeofficial, 'Round of 128' as ctlabel
        union all select 'roundof64'  as ctcodeofficial, 'Round of 64' as ctlabel
        union all select 'roundof32'  as ctcodeofficial, 'Round of 32' as ctlabel
        union all select 'roundof16'  as ctcodeofficial, 'Round of 16' as ctlabel
        union all select 'quarterfinals' as ctcodeofficial,'Quarter-Finals' as ctlabel
        union all select 'semifinals' as ctcodeofficial,  'Semi-Finals' as ctlabel
        union all select 'finals' as ctcodeofficial,'Finals' as ctlabel
        union all select 'roundrobin' as ctcodeofficial,'Round Robin' as ctlabel
        union all select '1stroundqualifying' as ctcodeofficial,'1st Round Qualifying' as ctlabel
        union all select '2ndroundqualifying' as ctcodeofficial,'2nd Round Qualifying' as ctlabel
        union all select '3rdroundqualifying' as ctcodeofficial,'3rd Round Qualifying' as ctlabel
        union all select 'lastroundqualifying' as ctcodeofficial,'Last Round Qualifying' as ctlabel
        union all select 'roundbronze' as ctcodeofficial,'Round Bronze' as ctlabel
      ) as tmp
      left outer join cttournamentrounds as r 
           on  lower(r.ctcodeofficial)=lower(tmp.ctcodeofficial)
      where r.ctcodeofficial is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;
