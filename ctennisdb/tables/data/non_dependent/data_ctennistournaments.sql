DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennistournaments';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennistournaments
     	(
        ctname,
        ctcategory,
        ctcountry,
        ctsurface,
        ctlabel,
        ctcreatedon
     	)
      select
        tmp.ctname,
        tmp.ctcategory,
        tmp.ctcountry,
        tmp.ctsurface,
        tmp.ctlabel,
        current_timestamp
      from 
      (           select 'Sydney International' as ctname, (select ctdid from ctennistournamentscategories where ctcode='atp250') as ctcategory, 'AUS' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Outdoor Hard')) as ctsurface, 'Sydney' as ctlabel
        union all select 'Hopman Cup' as ctname, (select ctdid from ctennistournamentscategories where ctcode='itf') as ctcategory, 'AUS' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Hard')) as ctsurface, 'Perth' as ctlabel
        union all select 'Indian Wells Challenger' as ctname, (select ctdid from ctennistournamentscategories where ctcode='atpchallenger') as ctcategory, 'USA' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Hard')) as ctsurface, 'Indian Wells Challenger' as ctlabel
        union all select 'Phoenix Challenger' as ctname, (select ctdid from ctennistournamentscategories where ctcode='atpchallenger') as ctcategory, 'USA' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Hard')) as ctsurface, 'Phoenix Challenger' as ctlabel
        union all select 'Brisbane International' as ctname, (select ctdid from ctennistournamentscategories where ctcode='atp250') as ctcategory, 'AUS' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Outdoor Hard')) as ctsurface, 'Brisbane' as ctlabel
        union all select 'Brasil Open - Sao Paulo' as ctname, (select ctdid from ctennistournamentscategories where ctcode='atp250') as ctcategory, 'BRA' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Indoor Clay')) as ctsurface, 'Sao Paulo' as ctlabel
        union all select 'TEB BNP Paribas Istanbul Open - Istanbul' as ctname, (select ctdid from ctennistournamentscategories where ctcode='atp250') as ctcategory, 'TUR' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Clay')) as ctsurface, 'Istanbul' as ctlabel
        union all select 'Mubadala World Tennis Championship' as ctname, (select ctdid from ctennistournamentscategories where ctcode='lvr') as ctcategory, 'ZZZ' as ctcountry, (select ctcode from ctennissurfaces where lower(ctlabel)=lower('Hard')) as ctsurface, 'Mubadala World Tennis Championship' as ctlabel
      ) as tmp
      left outer join ctennistournaments as t
           on  lower(t.ctname)=lower(tmp.ctname)
      where t.ctname is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;