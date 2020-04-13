DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennistournamentscategories';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennistournamentscategories
     	(
        ctdid,
     		ctcode,
     		ctlabel,
        ctpoints,
        ctcreatedon
     	)
      select
        row_number() over(order by tmp.ctcode) + (select coalesce(max(ctdid),0) from ctennistournamentscategories where lower(ctcode) not in ('notdefined')) as ctdid,
        tmp.ctcode as ctcode,
        tmp.ctlabel as ctlabel,
        tmp.ctpoints as ctpoints,
        current_timestamp as ctcreatedon
      from     
      (           select 'atp250' as ctcode, 'ATP 250' as ctlabel, 250 as ctpoints
        union all select 'atp500' as ctcode, 'ATP 500' as ctlabel, 500 as ctpoints
        union all select 'atpmasters1000' as ctcode, 'ATP Masters 1000' as ctlabel, 1000 as ctpoints
        union all select 'finals' as ctcode, 'Masters' as ctlabel, 1500 as ctpoints
        union all select 'grandslam' as ctcode, 'Grand Slam' as ctlabel, 2000 as ctpoints
        union all select 'itf' as ctcode, 'Davis Cup' as ctlabel, 0 as ctpoints
        union all select 'atpcup' as ctcode, 'ATP Cup' as ctlabel, 750 as ctpoints
        union all select 'premiermandatory' as ctcode, 'WTA Premier Mandatory' as ctlabel, 1000 as ctpoints
        union all select 'premier5' as ctcode, 'WTA Premier 5' as ctlabel, 900 as ctpoints
        union all select 'premier' as ctcode, 'WTA Premier' as ctlabel, 470 as ctpoints
        union all select 'International' as ctcode, 'WTA International' as ctlabel, 280 as ctpoints
        union all select 'lvr' as ctcode, 'Laver Cup' as ctlabel, 0 as ctpoints
        union all select 'olympicgames' as ctcode, 'Olympic Games' as ctlabel, 750 as ctpoints
        union all select 'atpchallenger' as ctcode, 'ATP Challenger Tour' as ctlabel, 125 as ctpoints
        union all select 'wtachallenger' as ctcode, 'WTA Challenger Tour' as ctlabel, 125 as ctpoints
      ) as tmp
      left outer join ctennistournamentscategories as c 
           on  lower(c.ctcode)=lower(tmp.ctcode)
      where c.ctcode is null and lower(tmp.ctcode) not in ('notdefined')

      union all select tmp.ctdid,tmp.ctcode,tmp.ctlabel,tmp.ctpoints,current_timestamp as ctcreatedon
                from(select 100000 as  ctdid, 'notdefined' as ctcode, 'Not Defined' as ctlabel, 0 as ctpoints) as tmp
                left outer join ctennistournamentscategories as c on  lower(c.ctcode)=lower(tmp.ctcode)
                where c.ctcode is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;