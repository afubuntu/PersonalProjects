DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennissurfaces';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennissurfaces
     	(
     		ctcode,
     		ctlabel,
        ctcreatedon 
     	)
      select 
        row_number() over(order by tmp.ctlabel) + (select coalesce(max(ctcode),0) from ctennissurfaces where lower(ctlabel) not in ('not defined')) as ctcode,
        tmp.ctlabel as ctlabel,
        current_timestamp as ctcreatedon
      from     
      (           select 'Indoor Hard' as ctlabel
        union all select 'Outdoor Hard' as ctlabel
        union all select 'Outdoor Clay' as ctlabel
        union all select 'Indoor Clay' as ctlabel
        union all select 'Outdoor Grass' as ctlabel
        union all select 'Indoor Grass' as ctlabel
        union all select 'Carpet' as ctlabel
        union all select 'Hard' as ctlabel
        union all select 'Grass' as ctlabel
        union all select 'Clay' as ctlabel
      ) as tmp
      left outer join ctennissurfaces as s
           on lower(s.ctlabel)=lower(tmp.ctlabel)
      where s.ctlabel is null and lower(tmp.ctlabel) not in ('not defined')
      
      union all select tmp.ctcode,tmp.ctlabel,current_timestamp as ctcreatedon
                from (select 100000 as ctcode, 'Not defined' as ctlabel) as tmp
                left outer join ctennissurfaces as s on lower(s.ctlabel)=lower(tmp.ctlabel)
                where s.ctlabel is null; 
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;
