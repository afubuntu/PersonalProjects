DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_get_ctmstats') then
		drop function sp_get_ctmstats;
	end if;
end
$do$;

create or replace function sp_get_ctmstats
	(
		_ctcode varchar(200) default null,
		_ctstartdate date default null,
		_ctenddate date default null,
		_ctgender varchar(1) default 'M'
	)
	returns table 
	(
		startdate date,
		enddate date,
		player1 varchar(200),
		player2 varchar(200),
		tournament varchar(200),
		tournament_category varchar(100),
		surface varchar(200),
		round varchar(100),
		score varchar(50),
		player_sets_won1 integer,
		player_sets_won2 integer,
		player_games_won1 integer,
		player_games_won2 integer,
		total_serv1 integer,
		total_serv2 integer,       
		first_serv1 integer,
		first_serv2 integer,
		first_serv_won1 integer,
		first_serv_won2 integer,
		second_serv_won1 integer,
		second_serv_won2 integer,
		total_break_points1 integer,
		total_break_points2 integer,
		break_points_won1 integer,
		break_points_won2 integer,
		double_fault1 integer,
		double_fault2 integer,
		aces1 integer,
		aces2 integer,
		source varchar(200),
		createdon timestamp,
		updatedon timestamp
	)
	language plpgsql
	as $$
	declare
		_max_date date;
		_min_date date;
	begin
		select min(ctstartdate), max(ctenddate) into _min_date,_max_date from ctennismatchesstats;

		if _ctcode is null then
			return query
				with official_source as
				     (
				     	select m0.ctstartdate as startdate,
				     	       m0.ctenddate as enddate,
				     	       replace(p1.ctname,',','')::varchar(200) as player1,
				     	       replace(p2.ctname,',','')::varchar(200) as player2,
				     	       t0.ctname as tournament,
				     	       c0.ctcode as tournament_category,
				     	       s0.ctlabel as surface,
				     	       r0.ctlabel as round,
				     	       m0.ctscore as score,
				     	       fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
				     	       fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
				     	       fn_get_score(m0.ctscore,1,'g') as player_games_won1,
				     	       fn_get_score(m0.ctscore,2,'g') as player_games_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
				     	       m0.ctstatdble1 as double_fault1,
				     	       m0.ctstatdble2 as double_fault2,
				     	       m0.ctstataces1 as aces1,
				     	       m0.ctstataces2 as aces2,
				     	       m0.ctstatsource as source,
				     	       m0.ctcreatedon as createdon
				     	from ctennismatchesstats as m0
				     	left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
				     	left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
				     	left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
				     	left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
				     	left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
				     	left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
				     	where m0.ctstatsource in('https://www.atptour.com','https://www.wtatennis.com')
				     	  and m0.ctstartdate>=coalesce(_ctstartdate,_min_date)
				     	  and m0.ctenddate<=coalesce(_ctenddate,_max_date)
				     	  and p1.ctgender=_ctgender
				     	  and p2.ctgender=_ctgender
				     	  and m0.ctscore not like '%[]%'
				     ),
				     alternative_source as
				     (
				     	select distinct case when r0.ctlabel like '%Qualifying%' then 
				     					case when c0.ctcode='grandslam' then (m0.ctstartdate+interval '13 day') else (m0.ctstartdate+interval '2 day') end 
				     				else m0.ctstartdate 
				     		   end::date as startdate,
				     	       case when r0.ctlabel like '%Qualifying%' then 
				     	       			case when c0.ctcode='grandslam' then (m0.ctenddate+interval '13 day') else  (m0.ctenddate+interval '2 day') end 
				     	       		else m0.ctenddate 
				     	       end::date as enddate,
				     	       replace(p11.ctname,',','')::varchar(200) as player1,
				     	       replace(p22.ctname,',','')::varchar(200) as player2,
				     	       t0.ctname as tournament,
							   case when t0.ctname in('Davis Cup','Hopman Cup','Fed Cup') then 'itf' 
								    when t0.ctname ilike '%olympic%' then 'olympicgames' 
								    when t0.ctname ilike '%challenger%' then _str_challenger 
								    else c0.ctcode end::varchar(100) as tournament_category,
							   case when s0.ctlabel='Hard' then 'Outdoor Hard'
								    when s0.ctlabel='Clay' then 'Outdoor Clay' 
								    when s0.ctlabel='Grass' then 'Outdoor Grass' 
								    else s0.ctlabel end::varchar(200) as surface,
				     	       r0.ctlabel as round,
				     	       m0.ctscore as score,
				     	       fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
				     	       fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
				     	       fn_get_score(m0.ctscore,1,'g') as player_games_won1,
				     	       fn_get_score(m0.ctscore,2,'g') as player_games_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
				     	       m0.ctstatdble1 as double_fault1,
				     	       m0.ctstatdble2 as double_fault2,
				     	       m0.ctstataces1 as aces1,
				     	       m0.ctstataces2 as aces2,
				     	       m0.ctstatsource as source,
				     	       m0.ctcreatedon as createdon
				     	from ctennismatchesstats as m0
				     	left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
				     	left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
						left outer join ctennismappingplayers as mp1 on (mp1.ctcode=p1.ctcode or mp1.ctcodeofficial=p1.ctcode)
						left outer join ctennismappingplayers as mp2 on (mp2.ctcode=p2.ctcode or mp2.ctcodeofficial=p2.ctcode)
				     	left outer join ctennisplayers as p11 on p11.ctcode=mp1.ctcodeofficial
				     	left outer join ctennisplayers as p22 on p22.ctcode=mp2.ctcodeofficial						
				     	left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
				     	left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
				     	left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
				     	left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
				     	where m0.ctstatsource in('http://www.tennislive.net')
				     	  and m0.ctstartdate>=coalesce(_ctstartdate,_min_date)
				     	  and m0.ctenddate<=coalesce(_ctenddate,_max_date)
				     	  and p1.ctgender=_ctgender
				     	  and p2.ctgender=_ctgender	
						  and (mp1.ctcode is not null or mp1.ctcodeofficial is not null)
						  and (mp2.ctcode is not null or mp2.ctcodeofficial is not null)
				     	  and m0.ctscore not like '%retired%'
				     	  and m0.ctscore not like '%walk%'		     	  
						  and t0.ctname not in ('ATP Cup','Laver Cup')
						  and t0.ctname not like '%WTA Elite Trophy%'
						  and t0.ctname not like '%WTA Finals%'
						  and t0.ctname not like '%ATP Finals%'	
				     )
				     select o0.*,current_timestamp::timestamp as updatedon from official_source as o0
				     union all
				     select a0.*,current_timestamp::timestamp as updatedon from alternative_source as a0
				     left outer join official_source as o0 
				        on ((a0.player1=o0.player1 and a0.player2=o0.player2 and a0.score=o0.score)
				        or (a0.player1=o0.player2 and a0.player2=o0.player1 and a0.score=fn_rscore(o0.score)))
				       and ((a0.startdate>=o0.startdate and a0.enddate<=o0.enddate)
				       	or (a0.startdate+interval '1 day'>=o0.startdate and a0.enddate+interval '1 day'<=o0.enddate)
				       	or (a0.startdate+interval '-1 day'>=o0.startdate and a0.enddate+interval '-1 day'<=o0.enddate))
				     where o0.player1 is null 
				       and o0.player2 is null
				       and o0.score is null;
		else
			return query
				with official_source as
				     (
				     	select m0.ctstartdate as startdate,
				     	       m0.ctenddate as enddate,
				     	       replace(p1.ctname,',','')::varchar(200) as player1,
				     	       replace(p2.ctname,',','')::varchar(200) as player2,
				     	       t0.ctname as tournament,
				     	       c0.ctcode as tournament_category,
				     	       s0.ctlabel as surface,
				     	       r0.ctlabel as round,
				     	       m0.ctscore as score,
				     	       fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
				     	       fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
				     	       fn_get_score(m0.ctscore,1,'g') as player_games_won1,
				     	       fn_get_score(m0.ctscore,2,'g') as player_games_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
				     	       m0.ctstatdble1 as double_fault1,
				     	       m0.ctstatdble2 as double_fault2,
				     	       m0.ctstataces1 as aces1,
				     	       m0.ctstataces2 as aces2,
				     	       m0.ctstatsource as source,
				     	       m0.ctcreatedon as createdon
				     	from ctennismatchesstats as m0
				     	left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
				     	left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
				     	left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
				     	left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
				     	left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
				     	left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
				     	where m0.ctstatsource in('https://www.atptour.com','https://www.wtatennis.com')
				     	  and m0.ctstartdate>=coalesce(_ctstartdate,_min_date)
				     	  and m0.ctenddate<=coalesce(_ctenddate,_max_date)
				     	  and p1.ctgender=_ctgender
				     	  and p2.ctgender=_ctgender
				     	  and (p1.ctcode=_ctcode or p2.ctcode=_ctcode)
				     	  and m0.ctscore not like '%[]%'
				     ),
				     alternative_source as
				     (
				     	select distinct case when r0.ctlabel like '%Qualifying%' then 
				     					case when c0.ctcode='grandslam' then (m0.ctstartdate+interval '13 day') else (m0.ctstartdate+interval '2 day') end 
				     				else m0.ctstartdate 
				     		   end::date as startdate,
				     	       case when r0.ctlabel like '%Qualifying%' then 
				     	       			case when c0.ctcode='grandslam' then (m0.ctenddate+interval '13 day') else  (m0.ctenddate+interval '2 day') end 
				     	       		else m0.ctenddate 
				     	       end::date as enddate,
				     	       replace(p11.ctname,',','')::varchar(200) as player1,
				     	       replace(p22.ctname,',','')::varchar(200) as player2,
				     	       t0.ctname as tournament,
							   case when t0.ctname in('Davis Cup','Hopman Cup','Fed Cup') then 'itf' 
								    when t0.ctname ilike '%olympic%' then 'olympicgames' 
								    when t0.ctname ilike '%challenger%' then _str_challenger 
								    else c0.ctcode end::varchar(100) as tournament_category,
							   case when s0.ctlabel='Hard' then 'Outdoor Hard'
								    when s0.ctlabel='Clay' then 'Outdoor Clay' 
								    when s0.ctlabel='Grass' then 'Outdoor Grass' 
								    else s0.ctlabel end::varchar(200) as surface,
				     	       r0.ctlabel as round,
				     	       m0.ctscore as score,
				     	       fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
				     	       fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
				     	       fn_get_score(m0.ctscore,1,'g') as player_games_won1,
				     	       fn_get_score(m0.ctscore,2,'g') as player_games_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
				     	       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
				     	       m0.ctstatdble1 as double_fault1,
				     	       m0.ctstatdble2 as double_fault2,
				     	       m0.ctstataces1 as aces1,
				     	       m0.ctstataces2 as aces2,
				     	       m0.ctstatsource as source,
				     	       m0.ctcreatedon as createdon
				     	from ctennismatchesstats as m0
				     	left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
				     	left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
						left outer join ctennismappingplayers as mp1 on (mp1.ctcode=p1.ctcode or mp1.ctcodeofficial=p1.ctcode)
						left outer join ctennismappingplayers as mp2 on (mp2.ctcode=p2.ctcode or mp2.ctcodeofficial=p2.ctcode)
				     	left outer join ctennisplayers as p11 on p11.ctcode=mp1.ctcodeofficial
				     	left outer join ctennisplayers as p22 on p22.ctcode=mp2.ctcodeofficial						
				     	left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
				     	left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
				     	left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
				     	left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
				     	where m0.ctstatsource in('http://www.tennislive.net')
				     	  and m0.ctstartdate>=coalesce(_ctstartdate,_min_date)
				     	  and m0.ctenddate<=coalesce(_ctenddate,_max_date)
				     	  and p1.ctgender=_ctgender
				     	  and p2.ctgender=_ctgender
						  and (mp1.ctcode is not null or mp1.ctcodeofficial is not null)
						  and (mp2.ctcode is not null or mp2.ctcodeofficial is not null)
				     	  and (p11.ctcode=_ctcode or p22.ctcode=_ctcode)	 
				     	  and m0.ctscore not like '%retired%'
				     	  and m0.ctscore not like '%walk%' 
						  and t0.ctname not in ('ATP Cup','Laver Cup')
						  and t0.ctname not like '%WTA Elite Trophy%'
						  and t0.ctname not like '%WTA Finals%'
						  and t0.ctname not like '%ATP Finals%'		
				     )
				     select o0.*,current_timestamp::timestamp as updatedon from official_source as o0
				     union all
				     select a0.*,current_timestamp::timestamp as updatedon from alternative_source as a0
				     left outer join official_source as o0 
				        on ((a0.player1=o0.player1 and a0.player2=o0.player2 and a0.score=o0.score)
				        or (a0.player1=o0.player2 and a0.player2=o0.player1 and a0.score=fn_rscore(o0.score)))
				       and ((a0.startdate>=o0.startdate and a0.enddate<=o0.enddate)
				       	or (a0.startdate+interval '1 day'>=o0.startdate and a0.enddate+interval '1 day'<=o0.enddate)
				       	or (a0.startdate+interval '-1 day'>=o0.startdate and a0.enddate+interval '-1 day'<=o0.enddate))
				     where o0.player1 is null 
				       and o0.player2 is null
				       and o0.score is null;				       
		end if;
	end $$;