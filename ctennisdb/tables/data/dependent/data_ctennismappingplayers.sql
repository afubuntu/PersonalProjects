DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennismappingplayers';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      insert into ctennismappingplayers
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
      (           select 'roger-federer' as ctcode, 'roger-federer-f324' as ctcodeofficial
        union all select 'novak-djokovic' as ctcode, 'novak-djokovic-d643'  as ctcodeofficial
        union all select 'christian-garin' as ctcode, 'cristian-garin-gd64'  as ctcodeofficial
        union all select 'jeff-wolf' as ctcode, 'j.j.-wolf-w09g'  as ctcodeofficial
        union all select 'teimuraz-gabashvili' as ctcode, 'teymuraz-gabashvili-g681'  as ctcodeofficial
        union all select 'khumoun-sultanov' as ctcode, 'khumoyun-sultanov-sr81'  as ctcodeofficial
        union all select 'aleksandr-nedovesov' as ctcode, 'aleksandr-nedovyesov-n503'  as ctcodeofficial
        union all select 'evgeny-tyurnev' as ctcode, 'evgenii-tiurnev-td47'  as ctcodeofficial
        union all select 'caty-mcnally' as ctcode, '325725-catherine-mcnally'  as ctcodeofficial
        union all select 'lesya-tsurenko' as ctcode, '315295-lesia-tsurenko'  as ctcodeofficial
        union all select 'laura-ioana-andrei' as ctcode, '312269-laura-ioana-paar'  as ctcodeofficial
        union all select 'raluca-georgiana-serban' as ctcode, '321280-raluka-serban'  as ctcodeofficial
        union all select 'dasha-lopatetskaya' as ctcode, '328592-daria-lopatetska'  as ctcodeofficial
        union all select 'ylona-georgiana-ghioroaie' as ctcode, '323440-ilona-georgiana-ghioroaie'  as ctcodeofficial
        union all select 'caroline-uebelhoer' as ctcode, '319444-caroline-werner'  as ctcodeofficial
      ) as tmp
      left outer join ctennismappingplayers as p 
           on  lower(p.ctcode)=lower(tmp.ctcode) and lower(p.ctcodeofficial)=lower(tmp.ctcodeofficial)
      where p.ctcode is null and p.ctcodeofficial is null;
    
      GET DIAGNOSTICS __rows_count=ROW_COUNT;
      raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
      raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;