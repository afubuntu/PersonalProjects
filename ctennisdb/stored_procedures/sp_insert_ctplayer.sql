DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_insert_ctplayer') then
		drop function sp_insert_ctplayer;
	end if;
end
$do$;

create or replace function sp_insert_ctplayer
	(
		_ctcode varchar(200),
		_ctname varchar(200),
		_ctcountry varchar(3),
		_ctgender varchar(1),
		_ctbirthdate date,
		_ctweight varchar(20),
		_ctheight varchar(20),
		_ctbeginpro date,
		_cthandstyle varchar(200),
		_update varchar(1) default 'N'
	)
	returns void
	language plpgsql
	as $$
	declare
		_player_cou varchar(3);
		_player_id integer;
	begin
		if not exists(select 1 from ctennisplayers as pl
			          left outer join ctennismappingplayers as mpl1 on mpl1.ctcodeofficial=pl.ctcode 
			          where lower(pl.ctcode)=lower(_ctcode) or lower(mpl1.ctcode)=lower(_ctcode)) then

			if exists(select 1 from ctennisplayers 
				      where (ctcode like '%'||_ctcode||'%') 
				         or (_ctcode like '%'||ctcode||'%') 
				         or (lower(replace(ctname,',',''))=lower(replace(_ctname,',','')))
				     ) then

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
				(          
				    select  _ctcode as ctcode,  
				            (select max(ctcode) from ctennisplayers 
				             where (ctcode like '%'||_ctcode||'%') 
				                or (_ctcode like '%'||ctcode||'%')
				                or (lower(replace(ctname,',',''))=lower(replace(_ctname,',','')))) as ctcodeofficial
				) as tmp
				left outer join ctennismappingplayers as p 
				   on  lower(p.ctcode)=lower(tmp.ctcode) and lower(p.ctcodeofficial)=lower(tmp.ctcodeofficial)
				where p.ctcode is null and p.ctcodeofficial is null;

				return;
			end if;

			select min(coalesce(co1.ctcode,co2.ctcode)) into _player_cou from ctenniscountries as co1
			left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=co1.ctcode
			left outer join ctenniscountries as co2 on co2.ctcode=co1.ctcode
			where co1.ctcode=_ctcountry or lower(mco1.ctcode)=lower(_ctcountry) or lower(co2.ctlabel)='not defined';

			insert into ctennisplayers
			(
				ctcode,
				ctname,
				ctcountry,
				ctgender,
				ctbirthdate,
				ctweight,
				ctheight,
				ctbeginpro,
				cthandstyle,
				ctcreatedon,
				ctupdatedon
			)
		    select 
		        _ctcode,
		        _ctname,
		        _player_cou,
		        _ctgender,
		        _ctbirthdate,
		        _ctweight,
		        _ctheight,
		        _ctbeginpro,
		        _cthandstyle,
		        current_timestamp,
		        null;
		else
			if _update='Y' then

				select max(pl.ctdid) into _player_id from ctennisplayers as pl
				left outer join ctennismappingplayers as mpl1 on mpl1.ctcodeofficial=pl.ctcode
				where lower(pl.ctcode)=lower(_ctcode) or lower(mpl1.ctcode)=lower(_ctcode);

				select min(coalesce(co1.ctcode,co2.ctcode)) into _player_cou from ctenniscountries as co1
				left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=co1.ctcode
				left outer join ctenniscountries as co2 on co2.ctcode=co1.ctcode
				where co1.ctcode=_ctcountry or lower(mco1.ctcode)=lower(_ctcountry) or lower(co2.ctlabel)='not defined';

				update ctennisplayers
					set ctname=_ctname,
						ctcountry=_player_cou,
						ctbirthdate=_ctbirthdate,
						ctweight=_ctweight,
						ctheight=_ctheight,
						ctbeginpro=_ctbeginpro,
						cthandstyle=_cthandstyle,
						ctupdatedon=current_timestamp
				where ctdid=_player_id;
			end if;
		end if;
	end $$;