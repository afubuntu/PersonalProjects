DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_insert_cttournament') then
		drop function sp_insert_cttournament;
	end if;
end
$do$;

create or replace function sp_insert_cttournament
	(
		_ctname varchar(200),
		_ctcountry varchar(200),
		_category varchar(100),
		_surface varchar(200),
		_ctlabel varchar(200),
		_update varchar(1) default 'N'
	)
	returns void
	language plpgsql
	as $$
	declare
		_cat_id integer;
		_surf_id integer;
		_country_code varchar(3);
	begin
		if not exists(select 1 from ctennistournaments as to1 
			          left outer join ctenniscountries as co1 on to1.ctcountry=co1.ctcode
			          left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=to1.ctcountry
			          where (lower(mco1.ctcode)=lower(_ctcountry) or lower(co1.ctlabel)=lower(_ctcountry)) 
			          and lower(to1.ctname)=lower(_ctname)) then

			select min(coalesce(co1.ctcode,co2.ctcode)) into _country_code 
			from ctenniscountries as co1
			left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=co1.ctcode
			left outer join ctenniscountries as co2 on co2.ctcode=co1.ctcode
			where lower(co1.ctlabel)=lower(_ctcountry) or lower(mco1.ctcode)=lower(_ctcountry) or lower(co2.ctlabel)='not defined';
					
			select min(coalesce(tcat.ctdid,ca2.ctdid)) into _cat_id
			from ctennistournamentscategories as tcat
			left outer join ctennismappingtourncategories as mc1 on mc1.ctcodeofficial=tcat.ctcode
			left outer join ctennistournamentscategories as ca2 on ca2.ctdid=tcat.ctdid
			where lower(tcat.ctcode)=lower(_category) or lower(mc1.ctcode)=lower(_category) or lower(ca2.ctlabel)='not defined';

			select min(coalesce(tsu.ctcode,ts2.ctcode)) into _surf_id
			from ctennissurfaces as tsu
			left outer join ctennismappingsurfaces as ms1 on lower(ms1.ctlabelofficial)=lower(tsu.ctlabel)
			left outer join ctennissurfaces as ts2 on ts2.ctcode=tsu.ctcode
			where lower(tsu.ctlabel)=lower(_surface) or lower(ms1.ctlabel)=lower(_surface) or lower(ts2.ctlabel)='not defined';

			insert into ctennistournaments
			(
				ctname,
				ctcategory,
				ctcountry,
				ctsurface,
				ctlabel,
				ctcreatedon,
				ctupdatedon
			)
		    select 
		        _ctname,
		        _cat_id,
		        _country_code,
		        _surf_id,
		        _ctlabel,
		        current_timestamp,
		        null;
		else
			if _update='Y' then

				select min(coalesce(co1.ctcode,co2.ctcode)) into _country_code 
				from ctenniscountries as co1
				left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=co1.ctcode
				left outer join ctenniscountries as co2 on co2.ctcode=co1.ctcode
				where lower(co1.ctlabel)=lower(_ctcountry) or lower(mco1.ctcode)=lower(_ctcountry) or lower(co2.ctlabel)='not defined';
						
				select min(coalesce(tcat.ctdid,ca2.ctdid)) into _cat_id
				from ctennistournamentscategories as tcat
				left outer join ctennismappingtourncategories as mc1 on mc1.ctcodeofficial=tcat.ctcode
				left outer join ctennistournamentscategories as ca2 on ca2.ctdid=tcat.ctdid
				where lower(tcat.ctcode)=lower(_category) or lower(mc1.ctcode)=lower(_category) or lower(ca2.ctlabel)='not defined';

				select min(coalesce(tsu.ctcode,ts2.ctcode)) into _surf_id
				from ctennissurfaces as tsu
				left outer join ctennismappingsurfaces as ms1 on lower(ms1.ctlabelofficial)=lower(tsu.ctlabel)
				left outer join ctennissurfaces as ts2 on ts2.ctcode=tsu.ctcode
				where lower(tsu.ctlabel)=lower(_surface) or lower(ms1.ctlabel)=lower(_surface) or lower(ts2.ctlabel)='not defined';

				update ctennistournaments
					set ctcategory=_cat_id,
						ctsurface=_surf_id,
						ctlabel=_ctlabel,
						ctupdatedon=current_timestamp
				 where lower(ctname)=lower(_ctname) and ctcountry=_country_code;
			end if;			
		end if;
	end $$;