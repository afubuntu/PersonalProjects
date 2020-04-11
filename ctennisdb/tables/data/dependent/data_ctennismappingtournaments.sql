DO 
$do$
declare __table_name character varying(100);
        __rows_count numeric;
begin
   __table_name := 'ctennismappingtournaments';

   if exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

     insert into ctennismappingtournaments
     (
          ctname,
          ctnameofficial,
          ctcreatedon 
     )
     select 
          tmp.ctname,
          tmp.ctnameofficial,
          current_timestamp 
     from    
     (              select 'ATP Cup - Brisbane, Perth, Sydney' as ctname, 'ATP Cup' as ctnameofficial
          union all select 'Brisbane, Perth, Sydney' as ctname, 'ATP Cup' as ctnameofficial
          union all select 'Qatar ExxonMobil Open - Doha' as ctname, 'Qatar ExxonMobil Open' as ctnameofficial
          union all select 'Doha' as ctname, 'Qatar ExxonMobil Open' as ctnameofficial
          union all select 'Adelaide International - Adelaide' as ctname, 'Adelaide International' as ctnameofficial
          union all select 'Adelaide' as ctname, 'Adelaide International' as ctnameofficial
          union all select 'ASB Classic - Auckland' as ctname, 'ASB Classic' as ctnameofficial
          union all select 'Auckland' as ctname, 'ASB Classic' as ctnameofficial
          union all select 'Australian Open - Melbourne' as ctname, 'Australian Open' as ctnameofficial
          union all select 'Cordoba Open - Cordoba' as ctname, 'Cordoba Open' as ctnameofficial
          union all select 'Cordoba' as ctname, 'Cordoba Open' as ctnameofficial
          union all select 'Tata Open Maharashtra - Pune' as ctname, 'Tata Open Maharashtra' as ctnameofficial
          union all select 'Pune' as ctname, 'Tata Open Maharashtra' as ctnameofficial
          union all select 'Open Sud de France - Montpellier' as ctname, 'Open Sud de France' as ctnameofficial
          union all select 'Montpellier' as ctname, 'Open Sud de France' as ctnameofficial
          union all select 'ABN AMRO World Tennis Tournament - Rotterdam' as ctname, 'ABN AMRO World Tennis Tournament' as ctnameofficial
          union all select 'Rotterdam' as ctname, 'ABN AMRO World Tennis Tournament' as ctnameofficial
          union all select 'New York Open - New York' as ctname, 'New York Open' as ctnameofficial
          union all select 'New York' as ctname, 'New York Open' as ctnameofficial
          union all select 'Argentina Open - Buenos Aires' as ctname, 'Argentina Open' as ctnameofficial
          union all select 'Buenos Aires' as ctname, 'Argentina Open' as ctnameofficial
          union all select 'Rio Open - Rio de Janeiro' as ctname, 'Rio Open presented by Claro' as ctnameofficial
          union all select 'Rio de Janeiro' as ctname, 'Rio Open presented by Claro' as ctnameofficial
          union all select 'Open 13 Provence - Marseille' as ctname, 'Open 13 Provence' as ctnameofficial
          union all select 'Marseille' as ctname, 'Open 13 Provence' as ctnameofficial
          union all select 'Delray Beach Open - Delray Beach' as ctname, 'Delray Beach Open by VITACOST.com' as ctnameofficial
          union all select 'Delray Beach' as ctname, 'Delray Beach Open by VITACOST.com' as ctnameofficial
          union all select 'Dubai Duty Free Tennis Championships - Dubai' as ctname, 'Dubai Duty Free Tennis Championships' as ctnameofficial
          union all select 'Dubai' as ctname, 'Dubai Duty Free Tennis Championships' as ctnameofficial
          union all select 'Abierto Mexicano Telcel presentado por HSBC - Acapulco' as ctname, 'Abierto Mexicano Telcel presentado por HSBC' as ctnameofficial
          union all select 'Abierto Mexicano Telcel - Acapulco' as ctname, 'Abierto Mexicano Telcel presentado por HSBC' as ctnameofficial
          union all select 'Acapulco' as ctname, 'Abierto Mexicano Telcel presentado por HSBC' as ctnameofficial
          union all select 'Santiago Open - Santiago' as ctname, 'Chile Dove Men+Care Open' as ctnameofficial
          union all select 'Santiago' as ctname, 'Chile Dove Men+Care Open' as ctnameofficial
          union all select 'Davis Cup Qualifiers - Multiple Locations' as ctname, 'Davis Cup Qualifiers' as ctnameofficial
          union all select 'BNP Paribas Open - Indian Wells' as ctname, 'BNP Paribas Open' as ctnameofficial
          union all select 'ATP Masters 1000 Indian Wells' as ctname, 'BNP Paribas Open' as ctnameofficial
          union all select 'Indian Wells' as ctname, 'Indian Wells Challenger' as ctnameofficial
          union all select 'Miami Open presented by Itau - Miami' as ctname, 'Miami Open presented by Itau' as ctnameofficial
          union all select 'Miami Open - Miami' as ctname, 'Miami Open presented by Itau' as ctnameofficial
          union all select 'ATP Masters 1000 Miami' as ctname, 'Miami Open presented by Itau' as ctnameofficial
          union all select 'US Men''s Clay Court Championship - Houston' as ctname, 'Fayez Sarofim & Co. U.S. Men''s Clay Court Championship' as ctnameofficial
          union all select 'Houston' as ctname, 'Fayez Sarofim & Co. U.S. Men''s Clay Court Championship' as ctnameofficial
          union all select 'Grand Prix Hassan II - Marrakech' as ctname, 'Grand Prix Hassan II' as ctnameofficial
          union all select 'Marrakech' as ctname, 'Grand Prix Hassan II' as ctnameofficial
          union all select 'Rolex Monte-Carlo Masters - Monte Carlo' as ctname, 'Rolex Monte-Carlo Masters' as ctnameofficial
          union all select 'Monte-Carlo Rolex Masters - Monte Carlo' as ctname, 'Rolex Monte-Carlo Masters' as ctnameofficial
          union all select 'ATP Masters 1000 Monte Carlo' as ctname, 'Rolex Monte-Carlo Masters' as ctnameofficial
          union all select 'ATP Masters 1000 Monte-Carlo' as ctname, 'Rolex Monte-Carlo Masters' as ctnameofficial
          union all select 'Barcelona Open Banc Sabadell - Barcelona' as ctname, 'Barcelona Open Banc Sabadell' as ctnameofficial
          union all select 'Barcelona' as ctname, 'Barcelona Open Banc Sabadell' as ctnameofficial
          union all select 'Gazprom Hungarian Open - Budapest' as ctname, 'Hungarian Open' as ctnameofficial
          union all select 'Hungarian Open - Budapest' as ctname, 'Hungarian Open' as ctnameofficial
          union all select 'Budapest' as ctname, 'Hungarian Open' as ctnameofficial
          union all select 'BMW Open by FWU - Munich' as ctname, 'BMW Open by FWU' as ctnameofficial
          union all select 'BMW Open - Munich' as ctname, 'BMW Open by FWU' as ctnameofficial
          union all select 'Munich' as ctname, 'BMW Open by FWU' as ctnameofficial
          union all select 'Millennium Estoril Open - Estoril' as ctname, 'Millennium Estoril Open' as ctnameofficial
          union all select 'Estoril' as ctname, 'Millennium Estoril Open' as ctnameofficial
          union all select 'Mutua Madrid Open - Madrid' as ctname, 'Mutua Madrid Open' as ctnameofficial
          union all select 'ATP Masters 1000 Madrid' as ctname, 'Mutua Madrid Open' as ctnameofficial
          union all select 'Internazionali BNL d''Italia - Rome' as ctname, 'Internazionali BNL d''Italia' as ctnameofficial
          union all select 'ATP Masters 1000 Rome' as ctname, 'Internazionali BNL d''Italia' as ctnameofficial
          union all select 'Geneva Open - Geneva' as ctname, 'Banque Eric Sturdza Geneva Open' as ctnameofficial
          union all select 'Geneva' as ctname, 'Banque Eric Sturdza Geneva Open' as ctnameofficial
          union all select 'Open Parc - Lyon' as ctname, 'Open Parc Auvergne-Rhone-Alpes Lyon' as ctnameofficial
          union all select 'Lyon' as ctname, 'Open Parc Auvergne-Rhone-Alpes Lyon' as ctnameofficial
          union all select 'Roland Garros - Paris' as ctname, 'Roland Garros' as ctnameofficial
          union all select 'French Open - Paris' as ctname, 'Roland Garros' as ctnameofficial
          union all select 'Championnats Internationaux de France' as ctname, 'Roland Garros' as ctnameofficial
          union all select 'MercedesCup - Stuttgart' as ctname, 'MercedesCup' as ctnameofficial
          union all select 'Mercedes Cup - Stuttgart' as ctname, 'MercedesCup' as ctnameofficial
          union all select 'Stuttgart' as ctname, 'MercedesCup' as ctnameofficial
          union all select 'Libema Open - s-Hertogenbosch' as ctname, 'Libema Open' as ctnameofficial
          union all select 'Libema Open - ''s-Hertogenbosch' as ctname, 'Libema Open' as ctnameofficial
          union all select 's-Hertogenbosch' as ctname, 'Libema Open' as ctnameofficial
          union all select 'Fever-Tree Championships - London' as ctname, 'Fever-Tree Championships' as ctnameofficial
          union all select 'London / Queen''s Club' as ctname, 'Fever-Tree Championships' as ctnameofficial
          union all select 'AEGON Championships - London' as ctname, 'Fever-Tree Championships' as ctnameofficial
          union all select 'NOVENTI OPEN - Halle' as ctname, 'NOVENTI OPEN' as ctnameofficial
          union all select 'Halle' as ctname, 'NOVENTI OPEN' as ctnameofficial
          union all select 'Mallorca Championships - Mallorca' as ctname, 'Mallorca Championships' as ctnameofficial
          union all select 'Mallorca' as ctname, 'Mallorca Championships' as ctnameofficial
          union all select 'Nature Valley International - Eastbourne' as ctname, 'Nature Valley International' as ctnameofficial
          union all select 'Nature Valley International Eastbourne' as ctname, 'Nature Valley International' as ctnameofficial
          union all select 'Eastbourne' as ctname, 'Nature Valley International' as ctnameofficial
          union all select 'Wimbledon - London' as ctname, 'Wimbledon' as ctnameofficial
          union all select 'The Championships' as ctname, 'Wimbledon' as ctnameofficial
          union all select 'Hamburg European Open - Hamburg' as ctname, 'Hamburg European Open' as ctnameofficial
          union all select 'German Tennis Championships - Hamburg' as ctname, 'Hamburg European Open' as ctnameofficial
          union all select 'Hamburg' as ctname, 'Hamburg European Open' as ctnameofficial
          union all select 'Hall of Fame Open - Newport' as ctname, 'Hall of Fame Open' as ctnameofficial
          union all select 'Newport' as ctname, 'Hall of Fame Open' as ctnameofficial
          union all select 'Nordea Open - Bastad' as ctname, 'Nordea Open' as ctnameofficial
          union all select 'Bastad' as ctname, 'Nordea Open' as ctnameofficial
          union all select 'Davis Cup Group II - Multiple Locations' as ctname, 'Davis Cup Group II' as ctnameofficial
          union all select 'Abierto Mexicano de Tenis Mifel - Los Cabos' as ctname, 'Abierto de Tenis Mifel presentado por Cinemex' as ctnameofficial
          union all select 'Los Cabos' as ctname, 'Abierto de Tenis Mifel presentado por Cinemex' as ctnameofficial
          union all select 'J. Safra Sarasin Swiss Open Gstaad - Gstaad' as ctname, 'J. Safra Sarasin Swiss Open Gstaad' as ctnameofficial
          union all select 'Swiss Open - Gstaad' as ctname, 'J. Safra Sarasin Swiss Open Gstaad' as ctnameofficial
          union all select 'Gstaad' as ctname, 'J. Safra Sarasin Swiss Open Gstaad' as ctnameofficial
          union all select 'Plava Laguna Croatia Open Umag - Umag' as ctname, 'Plava Laguna Croatia Open Umag' as ctnameofficial
          union all select 'Umag' as ctname, 'Plava Laguna Croatia Open Umag' as ctnameofficial
          union all select 'Tokyo Olympic Games - Tokyo' as ctname, 'Tokyo Olympic Games' as ctnameofficial
          union all select 'BB&T Atlanta Open - Atlanta' as ctname, 'BB&T Atlanta Open presented by First Data' as ctnameofficial
          union all select 'Atlanta' as ctname, 'BB&T Atlanta Open presented by First Data' as ctnameofficial
          union all select 'Generali Open - Kitzbuhel' as ctname, 'Generali Open' as ctnameofficial
          union all select 'Kitzbuhel' as ctname, 'Generali Open' as ctnameofficial
          union all select 'Kitzbühel' as ctname, 'Generali Open' as ctnameofficial
          union all select 'Citi Open - Washington' as ctname, 'Citi Open' as ctnameofficial
          union all select 'Washington' as ctname, 'Citi Open' as ctnameofficial
          union all select 'Rogers Cup - Toronto' as ctname, 'Rogers Cup' as ctnameofficial
          union all select 'Rogers Cup - Montreal' as ctname, 'Rogers Cup' as ctnameofficial
          union all select 'Coupe Rogers - Montreal' as ctname, 'Rogers Cup' as ctnameofficial
          union all select 'ATP Masters 1000 Canada' as ctname, 'Rogers Cup' as ctnameofficial
          union all select 'Rogers Cup presented by National Bank' as ctname, 'Rogers Cup' as ctnameofficial
          union all select 'Western & Southern Open - Cincinnati' as ctname, 'Western & Southern Open' as ctnameofficial
          union all select 'ATP Masters 1000 Cincinnati' as ctname, 'Western & Southern Open' as ctnameofficial
          union all select 'Winston-Salem Open - Winston-Salem' as ctname, 'Winston-Salem Open' as ctnameofficial
          union all select 'Winston-Salem' as ctname, 'Winston-Salem Open' as ctnameofficial
          union all select 'US Open - New York' as ctname, 'US Open' as ctnameofficial
          union all select 'U.S. Open - New York' as ctname, 'US Open' as ctnameofficial
          union all select '2016 US OPEN TENNIS CHAMPIONSHIPS' as ctname, 'US Open' as ctnameofficial
          union all select '2017 US OPEN TENNIS CHAMPIONSHIPS' as ctname, 'US Open' as ctnameofficial
          union all select '2018 US OPEN TENNIS CHAMPIONSHIPS' as ctname, 'US Open' as ctnameofficial
          union all select '2019 US OPEN TENNIS CHAMPIONSHIPS' as ctname, 'US Open' as ctnameofficial
          union all select '2020 US OPEN TENNIS CHAMPIONSHIPS' as ctname, 'US Open' as ctnameofficial
          union all select 'US OPEN TENNIS CHAMPIONSHIPS' as ctname, 'US Open' as ctnameofficial
          union all select 'Davis Cup Groups I and II - Multiple Locations' as ctname, 'Davis Cup Groups I and II' as ctnameofficial
          union all select 'St. Petersburg Open - St. Petersburg' as ctname, 'St. Petersburg Open' as ctnameofficial
          union all select 'St. Petersburg' as ctname, 'St. Petersburg Open' as ctnameofficial
          union all select 'Moselle Open - Metz' as ctname, 'Moselle Open' as ctnameofficial
          union all select 'Metz' as ctname, 'Moselle Open' as ctnameofficial
          union all select 'Laver Cup - Boston' as ctname, 'Laver Cup' as ctnameofficial
          union all select 'Laver Cup - Geneva' as ctname, 'Laver Cup' as ctnameofficial
          union all select 'Laver Cup - Chicago' as ctname, 'Laver Cup' as ctnameofficial
          union all select 'Chengdu Open - Chengdu' as ctname, 'Chengdu Open' as ctnameofficial
          union all select 'Chengdu' as ctname, 'Chengdu Open' as ctnameofficial
          union all select 'Zhuhai Open - Zhuhai' as ctname, 'Huajin Securities Zhuhai Championships' as ctnameofficial
          union all select 'Zhuhai' as ctname, 'Huajin Securities Zhuhai Championships' as ctnameofficial
          union all select 'Sofia Open - Sofia' as ctname, 'Sofia Open' as ctnameofficial
          union all select 'DIEMA XTRA Sofia Open - Sofia' as ctname, 'Sofia Open' as ctnameofficial
          union all select 'Sofia' as ctname, 'Sofia Open' as ctnameofficial
          union all select 'China Open - Beijing' as ctname, 'China Open' as ctnameofficial
          union all select 'Beijing' as ctname, 'China Open' as ctnameofficial
          union all select 'Rakuten Japan Open Tennis Championships - Tokyo' as ctname, 'Rakuten Japan Open Tennis Championships' as ctnameofficial
          union all select 'Rakuten Japan Open - Tokyo' as ctname, 'Rakuten Japan Open Tennis Championships' as ctnameofficial
          union all select 'Tokyo' as ctname, 'Rakuten Japan Open Tennis Championships' as ctnameofficial
          union all select 'Rolex Shanghai Masters - Shanghai' as ctname, 'Rolex Shanghai Masters' as ctnameofficial
          union all select 'Shanghai Rolex Masters - Shanghai' as ctname, 'Rolex Shanghai Masters' as ctnameofficial
          union all select 'ATP Masters 1000 Shanghai' as ctname, 'Rolex Shanghai Masters' as ctnameofficial
          union all select 'VTB Kremlin Cup - Moscow' as ctname, 'VTB Kremlin Cup' as ctnameofficial
          union all select 'Kremlin Cup - Moscow' as ctname, 'VTB Kremlin Cup' as ctnameofficial
          union all select 'Moscow' as ctname, 'VTB Kremlin Cup' as ctnameofficial
          union all select 'European Open - Antwerp' as ctname, 'European Open' as ctnameofficial
          union all select 'Antwerp' as ctname, 'European Open' as ctnameofficial
          union all select 'Stockholm Open - Stockholm' as ctname, 'Stockholm Open' as ctnameofficial
          union all select 'Stockholm' as ctname, 'Stockholm Open' as ctnameofficial
          union all select 'Erste Bank Open - Vienna' as ctname, 'Erste Bank Open' as ctnameofficial
          union all select 'Vienna' as ctname, 'Erste Bank Open' as ctnameofficial
          union all select 'Swiss Indoors Basel - Basel' as ctname, 'Swiss Indoors Basel' as ctnameofficial
          union all select 'Basel' as ctname, 'Swiss Indoors Basel' as ctnameofficial
          union all select 'Rolex Paris Masters - Paris' as ctname, 'Rolex Paris Masters' as ctnameofficial
          union all select 'BNP Paribas Masters - Paris' as ctname, 'Rolex Paris Masters' as ctnameofficial
          union all select 'ATP Masters 1000 Paris' as ctname, 'Rolex Paris Masters' as ctnameofficial
          union all select 'Nitto ATP Finals - London' as ctname, 'Nitto ATP Finals' as ctnameofficial
          union all select 'ATP Finals' as ctname, 'Nitto ATP Finals' as ctnameofficial
          union all select 'Davis Cup Finals - Madrid' as ctname, 'Davis Cup Finals' as ctnameofficial
          union all select 'Sydney' as ctname, 'Sydney International' as ctnameofficial
          union all select 'Sydney International - Sydney' as ctname, 'Sydney International' as ctnameofficial
          union all select 'Brisbane' as ctname, 'Brisbane International' as ctnameofficial
          union all select 'Brisbane International - Brisbane' as ctname, 'Brisbane International' as ctnameofficial          
          union all select 'Hopman Cup - Perth' as ctname, 'Hopman Cup' as ctnameofficial
          union all select 'Istanbul' as ctname, 'TEB BNP Paribas Istanbul Open - Istanbul' as ctnameofficial
          union all select 'Sao Paulo' as ctname, 'Brasil Open - Sao Paulo' as ctnameofficial
     ) as tmp
     left outer join ctennismappingtournaments as t 
      on  lower(t.ctname)=lower(tmp.ctname) and lower(t.ctnameofficial)=lower(tmp.ctnameofficial)
     where t.ctname is null and t.ctnameofficial is null;

     GET DIAGNOSTICS __rows_count=ROW_COUNT;
     raise notice '<%> row(s) has(ve) been successfully inserted.',__rows_count;
   else
     raise notice 'The table <%> does not exist. It should be created first before inserting records into it.',__table_name;
   end if;
end
$do$;