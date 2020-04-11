DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_insert_ctmstats') then
		drop function sp_insert_ctmstats;
	end if;
end
$do$;

create or replace function sp_insert_ctmstats
	(
		_ctstartdate date,
		_ctenddate date,
		_ctplayer1 varchar(200),
		_ctname1 varchar(200),
		_ctplayer2 varchar(200),
		_ctname2 varchar(200),
		_cttournament varchar(200),
		_cttourcategory varchar(100),
		_ctourncountry varchar(200),
		_ctsurface varchar(200),
		_ctround varchar(100),
		_ctscore varchar(50),
		_ctstat1stserv1 varchar(10),
		_ctstat1stserv2 varchar(10),
		_ctstat1stservwon1 varchar(10),
		_ctstat1stservwon2 varchar(10),
		_ctstat2ndservwon1 varchar(10),
		_ctstat2ndservwon2 varchar(10),
		_ctstatbrkwon1 varchar(10),
		_ctstatbrkwon2 varchar(10),
		_ctstatretwon1 varchar(10),
		_ctstatretwon2 varchar(10),
		_ctstatdble1 integer,
		_ctstatdble2 integer,
		_ctstataces1 integer,
		_ctstataces2 integer,
		_ctstattype varchar(1),
		_ctstatsource varchar(200)
	)
	returns void
	language plpgsql
	as $$
	declare
		_ctcode varchar(100);
		_tourn_id integer;
		_surf_id integer;
		_pl1_id integer;
		_pl2_id integer;
		_rnd_id integer;
	begin
		if not exists(	select 1 from ctennismatchesstats as ms
						left outer join ctennisplayers as pl1 on pl1.ctdid=ms.ctplayer1
						left outer join ctennisplayers as pl2 on pl2.ctdid=ms.ctplayer2
						left outer join cttournamentrounds as rd1 on rd1.ctcode=ms.ctround				
						where (ms.ctstartdate<=_ctstartdate) 
						  and (ms.ctenddate>=_ctenddate)
						  and (ms.ctscore=_ctscore)
						  and (pl1.ctcode=_ctplayer1 or  pl1.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer1))
						  and (pl2.ctcode=_ctplayer2 or pl2.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer2))
						  and (lower(rd1.ctcodeofficial)=lower(_ctround) or lower(rd1.ctcodeofficial)= (select lower(max(ctcodeofficial)) from ctennismappingrounds where  lower(ctcode)=lower(_ctround)))
						union all
						select 1 from ctennismatchesstats as ms
						left outer join ctennisplayers as pl1 on pl1.ctdid=ms.ctplayer1
						left outer join ctennisplayers as pl2 on pl2.ctdid=ms.ctplayer2
						left outer join cttournamentrounds as rd1 on rd1.ctcode=ms.ctround				
						where (ms.ctstartdate<=_ctstartdate) 
						  and (ms.ctenddate>=_ctenddate)
						  and (ms.ctscore=fn_rscore(_ctscore))
						  and (pl1.ctcode=_ctplayer2 or  pl1.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer2))
						  and (pl2.ctcode=_ctplayer1 or pl2.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer1))
						  and (lower(rd1.ctcodeofficial)=lower(_ctround) or lower(rd1.ctcodeofficial)= (select lower(max(ctcodeofficial)) from ctennismappingrounds where  lower(ctcode)=lower(_ctround)))
					  ) then

			select pl1.ctdid ,pl2.ctdid into _pl1_id,_pl2_id
			from ctennisplayers as pl1 , ctennisplayers as pl2
			where (pl1.ctcode=_ctplayer1 or pl1.ctcode=(select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer1))
			and   (pl2.ctcode=_ctplayer2 or pl2.ctcode=(select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer2));

			if _pl1_id is null or _pl2_id is null then
				perform sp_insert_ctplayer(_ctplayer1,_ctname1,null,_ctstattype,null,null,null,null,null);
				perform sp_insert_ctplayer(_ctplayer2,_ctname2,null,_ctstattype,null,null,null,null,null);

				select pl1.ctdid ,pl2.ctdid into _pl1_id,_pl2_id
				from ctennisplayers as pl1 , ctennisplayers as pl2
				where (pl1.ctcode=_ctplayer1 or pl1.ctcode=(select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer1))
				and   (pl2.ctcode=_ctplayer2 or pl2.ctcode=(select max(ctcodeofficial) from ctennismappingplayers where ctcode=_ctplayer2));
			end if;

			select max(tourn.ctcode) into _tourn_id
			from ctennistournaments as tourn
			left outer join ctennismappingtournaments as mt1 on mt1.ctnameofficial=tourn.ctname
			where lower(mt1.ctname)=lower(_cttournament) or lower(tourn.ctname)=lower(_cttournament);

			if _tourn_id is null then
				perform sp_insert_cttournament(_cttournament,_ctourncountry,_cttourcategory,_ctsurface,_cttournament);

				if _cttourcategory='challenger' then
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
					from (select _cttournament||' Challenger' as ctname, _cttournament as ctnameofficial) as tmp
					left outer join ctennismappingtournaments as t 
					 on  lower(t.ctname)=lower(tmp.ctname) and lower(t.ctnameofficial)=lower(tmp.ctnameofficial)
					where t.ctname is null and t.ctnameofficial is null;
				end if;

				select max(tourn.ctcode) into _tourn_id
				from ctennistournaments as tourn
				left outer join ctennismappingtournaments as mt1 on mt1.ctnameofficial=tourn.ctname
				where lower(mt1.ctname)=lower(_cttournament) or lower(tourn.ctname)=lower(_cttournament);				
			end if;

			select min(coalesce(tsu.ctcode,ts2.ctcode)) into _surf_id
			from ctennissurfaces as tsu
			left outer join ctennismappingsurfaces as ms1 on lower(ms1.ctlabelofficial)=lower(tsu.ctlabel)
			left outer join ctennissurfaces as ts2 on ts2.ctcode=tsu.ctcode
			where lower(tsu.ctlabel)=lower(_ctsurface) or lower(ms1.ctlabel)=lower(_ctsurface) or lower(ts2.ctlabel)='not defined';

			select max(rd.ctcode) into _rnd_id
			from cttournamentrounds as rd
			left outer join ctennismappingrounds as mr1 on mr1.ctcodeofficial=rd.ctcodeofficial
			where lower(rd.ctcodeofficial)=lower(_ctround) or lower(mr1.ctcode)=lower(_ctround);

			_ctcode:=to_char(_ctstartdate,'yyyymmdd')||to_char(_ctenddate,'yyyymmdd')||_pl1_id||_pl2_id||_tourn_id||_rnd_id||replace(replace(replace(replace(_ctscore,'[',''),']',''),'-',''),' ','');

			insert into ctennismatchesstats
			(
		        ctcode,
		        ctstartdate,
		        ctenddate,
		        ctplayer1,
		        ctplayer2,
		        cttournament,
		        ctsurface,
		        ctround,
		        ctscore,
		        ctstat1stserv1,
		        ctstat1stserv2,
		        ctstat1stservwon1,
		        ctstat1stservwon2,
		        ctstat2ndservwon1,
		        ctstat2ndservwon2,
		        ctstatbrkwon1,
		        ctstatbrkwon2,
		        ctstatretwon1,
		        ctstatretwon2,
		        ctstatdble1,
		        ctstatdble2,
		        ctstataces1,
		        ctstataces2,
		        ctstatsource,
		        ctcreatedon,
		        ctupdatedon
			)
		    select 
		        _ctcode,
		        _ctstartdate,
		        _ctenddate,
		        _pl1_id,
		        _pl2_id,
		        _tourn_id,
		        _surf_id,
		        _rnd_id,
		        _ctscore,
		        _ctstat1stserv1,
		        _ctstat1stserv2,
		        _ctstat1stservwon1,
		        _ctstat1stservwon2,
		        _ctstat2ndservwon1,
		        _ctstat2ndservwon2,
		        _ctstatbrkwon1,
		        _ctstatbrkwon2,
		        _ctstatretwon1,
		        _ctstatretwon2,
		        _ctstatdble1,
		        _ctstatdble2,
		        _ctstataces1,
		        _ctstataces2,
		        _ctstatsource,
		        current_timestamp,
		        null;
		end if;
	end $$;
