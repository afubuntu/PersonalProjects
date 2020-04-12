 DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_feed_ctcleanmstats') then
		drop function sp_feed_ctcleanmstats(varchar(200),date,date,varchar(1));
	end if;
end
$do$;

create or replace function sp_feed_ctcleanmstats
	(
		_ctcode varchar(200) default null,
		_ctstartdate date default null,
		_ctenddate date default null,
		_ctgender varchar(1) default 'M'
	)
	returns void
	language plpgsql
	as $$
	declare
		_max_date date;
		_min_date date;
		_alt_cursor refcursor;
		_stats_record record;
		_insert_count integer;
	begin
		select coalesce(_ctstartdate,min(ctstartdate)), coalesce(_ctenddate,max(ctenddate)) into _min_date,_max_date from ctennismatchesstats;

		if _ctcode is null then
			insert into ctenniscleanstats
			(
				startdate,
				enddate,
				player1,
				player2,
				tournament,
				tournament_category,
				surface,
				round,
				score,
				player_sets_won1,
				player_sets_won2,
				player_games_won1,
				player_games_won2,
				total_serv1,
				total_serv2,       
				first_serv1,
				first_serv2,
				first_serv_won1,
				first_serv_won2,
				second_serv_won1,
				second_serv_won2,
				total_break_points1,
				total_break_points2,
				break_points_won1,
				break_points_won2,
				double_fault1,
				double_fault2,
				aces1,
				aces2,
				source,
				createdon
			)
			select
				official_source.startdate,
				official_source.enddate,
				official_source.player1,
				official_source.player2,
				official_source.tournament,
				official_source.tournament_category,
				official_source.surface,
				official_source.round,
				official_source.score,
				official_source.player_sets_won1,
				official_source.player_sets_won2,
				official_source.player_games_won1,
				official_source.player_games_won2,
				official_source.total_serv1,
				official_source.total_serv2,       
				official_source.first_serv1,
				official_source.first_serv2,
				official_source.first_serv_won1,
				official_source.first_serv_won2,
				official_source.second_serv_won1,
				official_source.second_serv_won2,
				official_source.total_break_points1,
				official_source.total_break_points2,
				official_source.break_points_won1,
				official_source.break_points_won2,
				official_source.double_fault1,
				official_source.double_fault2,
				official_source.aces1,
				official_source.aces2,
				official_source.source,
				official_source.createdon
			from
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
				  and m0.ctstartdate>=_min_date
				  and m0.ctenddate<=_max_date
				  and p1.ctgender=_ctgender
				  and p2.ctgender=_ctgender
				  and m0.ctscore not like '%[]%'
			) as official_source
			left outer join ctenniscleanstats as cleanstats
				on  cleanstats.startdate=official_source.startdate
				and cleanstats.enddate=official_source.enddate
				and cleanstats.player1=official_source.player1
				and cleanstats.player2=official_source.player2
				and cleanstats.round=official_source.round
				and cleanstats.score=official_source.score
			where cleanstats.startdate is null
			  and cleanstats.enddate is null
			  and cleanstats.player1 is null
			  and cleanstats.player2 is null
			  and cleanstats.round is null
			  and cleanstats.score is null;

			GET DIAGNOSTICS _insert_count=ROW_COUNT;
			raise notice '<%> row(s) has(ve) been successfully inserted.',_insert_count;

			open _alt_cursor for select alternative_source.*
							 from
							 (select case when r0.ctlabel like '%Qualifying%' then 
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
									left outer join ctennismappingplayers as mp1 on (mp1.ctcode=p1.ctcode or mp1.ctcodeofficial=p1.ctcode)
									left outer join ctennismappingplayers as mp2 on (mp2.ctcode=p2.ctcode or mp2.ctcodeofficial=p2.ctcode)
									left outer join ctennisplayers as p11 on p11.ctcode=mp1.ctcodeofficial
									left outer join ctennisplayers as p22 on p22.ctcode=mp2.ctcodeofficial	
									left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
									left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
									left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
									left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
									where m0.ctstatsource in('http://www.tennislive.net')
									and m0.ctstartdate>=_min_date
									and m0.ctenddate<=_max_date
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
									) as alternative_source
									left outer join ctenniscleanstats as cleanstats
									on  cleanstats.startdate=alternative_source.startdate
									and cleanstats.enddate=alternative_source.enddate
									and cleanstats.player1=alternative_source.player1
									and cleanstats.player2=alternative_source.player2
									and cleanstats.round=alternative_source.round
									and cleanstats.score=alternative_source.score
									where cleanstats.startdate is null
									and cleanstats.enddate is null
									and cleanstats.player1 is null
									and cleanstats.player2 is null
									and cleanstats.round is null
									and cleanstats.score is null;

			_insert_count:=0;
			loop
				fetch _alt_cursor into _stats_record;
				exit when not found;

				if not exists(
					select 1 from ctenniscleanstats as c0
					where  ((c0.player1=_stats_record.player1 and c0.player2=_stats_record.player2 and c0.score=_stats_record.score)
						or (_stats_record.player1=c0.player2 and _stats_record.player2=c0.player1 and _stats_record.score=fn_rscore(c0.score)))
					   and ((_stats_record.startdate>=c0.startdate and _stats_record.enddate<=c0.enddate)
					   	or (_stats_record.startdate+interval '1 day'>=c0.startdate and _stats_record.enddate+interval '1 day'<=c0.enddate)
					   	or (_stats_record.startdate+interval '-1 day'>=c0.startdate and _stats_record.enddate+interval '-1 day'<=c0.enddate))
				       and c0.source!=_stats_record.source
				    union all
					select 1 from ctenniscleanstats as c0
					where  ((c0.player1=_stats_record.player1 and c0.player2=_stats_record.player2 and c0.score=_stats_record.score)
						or (_stats_record.player1=c0.player2 and _stats_record.player2=c0.player1 and _stats_record.score=fn_rscore(c0.score)))
					   and (_stats_record.startdate=c0.startdate and _stats_record.enddate=c0.enddate)
				       and c0.source=_stats_record.source
				       and _stats_record.round=c0.round			       
					         ) then

					_insert_count:=_insert_count+1;
					insert into ctenniscleanstats
					(
						startdate,
						enddate,
						player1,
						player2,
						tournament,
						tournament_category,
						surface,
						round,
						score,
						player_sets_won1,
						player_sets_won2,
						player_games_won1,
						player_games_won2,
						total_serv1,
						total_serv2,       
						first_serv1,
						first_serv2,
						first_serv_won1,
						first_serv_won2,
						second_serv_won1,
						second_serv_won2,
						total_break_points1,
						total_break_points2,
						break_points_won1,
						break_points_won2,
						double_fault1,
						double_fault2,
						aces1,
						aces2,
						source,
						createdon,
						updatedon
					)
					select
						_tmp_clean.startdate,
						_tmp_clean.enddate,
						_tmp_clean.player1,
						_tmp_clean.player2,
						_tmp_clean.tournament,
						_tmp_clean.tournament_category,
						_tmp_clean.surface,
						_tmp_clean.round,
						_tmp_clean.score,
						_tmp_clean.player_sets_won1,
						_tmp_clean.player_sets_won2,
						_tmp_clean.player_games_won1,
						_tmp_clean.player_games_won2,
						_tmp_clean.total_serv1,
						_tmp_clean.total_serv2,       
						_tmp_clean.first_serv1,
						_tmp_clean.first_serv2,
						_tmp_clean.first_serv_won1,
						_tmp_clean.first_serv_won2,
						_tmp_clean.second_serv_won1,
						_tmp_clean.second_serv_won2,
						_tmp_clean.total_break_points1,
						_tmp_clean.total_break_points2,
						_tmp_clean.break_points_won1,
						_tmp_clean.break_points_won2,
						_tmp_clean.double_fault1,
						_tmp_clean.double_fault2,
						_tmp_clean.aces1,
						_tmp_clean.aces2,
						_tmp_clean.source,
						_tmp_clean.createdon,
						current_timestamp
					from
					(	select
							_stats_record.startdate as startdate,
							_stats_record.enddate as enddate,
							_stats_record.player1 as player1,
							_stats_record.player2 as player2,
							_stats_record.tournament as tournament,
							_stats_record.tournament_category as tournament_category,
							_stats_record.surface as surface,
							_stats_record.round as round,
							_stats_record.score as score,
							_stats_record.player_sets_won1 as player_sets_won1,
							_stats_record.player_sets_won2 as player_sets_won2,
							_stats_record.player_games_won1 as player_games_won1,
							_stats_record.player_games_won2 as player_games_won2,
							_stats_record.total_serv1 as total_serv1,
							_stats_record.total_serv2 as total_serv2,       
							_stats_record.first_serv1 as first_serv1,
							_stats_record.first_serv2 as first_serv2,
							_stats_record.first_serv_won1 as first_serv_won1,
							_stats_record.first_serv_won2 as first_serv_won2,
							_stats_record.second_serv_won1 as second_serv_won1,
							_stats_record.second_serv_won2 as second_serv_won2,
							_stats_record.total_break_points1 as total_break_points1,
							_stats_record.total_break_points2 as total_break_points2,
							_stats_record.break_points_won1 as break_points_won1,
							_stats_record.break_points_won2 as break_points_won2,
							_stats_record.double_fault1 as double_fault1,
							_stats_record.double_fault2 as double_fault2,
							_stats_record.aces1 as aces1,
							_stats_record.aces2 as aces2,
							_stats_record.source as source,
							_stats_record.createdon as createdon
					) as _tmp_clean
					left outer join ctenniscleanstats as cleanstats
						on _tmp_clean.startdate=cleanstats.startdate
					   and _tmp_clean.enddate=cleanstats.enddate
					   and _tmp_clean.round=cleanstats.round
					   and _tmp_clean.player1=cleanstats.player1
					   and _tmp_clean.player2=cleanstats.player2
					   and _tmp_clean.score=cleanstats.score
					where  cleanstats.startdate is null
					   and cleanstats.enddate is null
					   and cleanstats.round is null
					   and cleanstats.player1 is null
					   and cleanstats.player2 is null
					   and cleanstats.score is null;
						
					raise notice 'Insertion - stats : % : {%; %; %; %; %}',_insert_count,_stats_record.startdate,_stats_record.player1,_stats_record.player2,_stats_record.tournament,_stats_record.score;
				end if;
			end loop;
			close _alt_cursor;
		else
			insert into ctenniscleanstats
			(
				startdate,
				enddate,
				player1,
				player2,
				tournament,
				tournament_category,
				surface,
				round,
				score,
				player_sets_won1,
				player_sets_won2,
				player_games_won1,
				player_games_won2,
				total_serv1,
				total_serv2,       
				first_serv1,
				first_serv2,
				first_serv_won1,
				first_serv_won2,
				second_serv_won1,
				second_serv_won2,
				total_break_points1,
				total_break_points2,
				break_points_won1,
				break_points_won2,
				double_fault1,
				double_fault2,
				aces1,
				aces2,
				source,
				createdon
			)
			select
				official_source.startdate,
				official_source.enddate,
				official_source.player1,
				official_source.player2,
				official_source.tournament,
				official_source.tournament_category,
				official_source.surface,
				official_source.round,
				official_source.score,
				official_source.player_sets_won1,
				official_source.player_sets_won2,
				official_source.player_games_won1,
				official_source.player_games_won2,
				official_source.total_serv1,
				official_source.total_serv2,       
				official_source.first_serv1,
				official_source.first_serv2,
				official_source.first_serv_won1,
				official_source.first_serv_won2,
				official_source.second_serv_won1,
				official_source.second_serv_won2,
				official_source.total_break_points1,
				official_source.total_break_points2,
				official_source.break_points_won1,
				official_source.break_points_won2,
				official_source.double_fault1,
				official_source.double_fault2,
				official_source.aces1,
				official_source.aces2,
				official_source.source,
				official_source.createdon
			from
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
				  and m0.ctstartdate>=_min_date
				  and m0.ctenddate<=_max_date
				  and p1.ctgender=_ctgender
				  and p2.ctgender=_ctgender
				  and (p1.ctcode=_ctcode or p2.ctcode=_ctcode)
				  and m0.ctscore not like '%[]%'
			) as official_source
			left outer join ctenniscleanstats as cleanstats
				on  cleanstats.startdate=official_source.startdate
				and cleanstats.enddate=official_source.enddate
				and cleanstats.player1=official_source.player1
				and cleanstats.player2=official_source.player2
				and cleanstats.round=official_source.round
				and cleanstats.score=official_source.score
			where cleanstats.startdate is null
			  and cleanstats.enddate is null
			  and cleanstats.player1 is null
			  and cleanstats.player2 is null
			  and cleanstats.round is null
			  and cleanstats.score is null;

			GET DIAGNOSTICS _insert_count=ROW_COUNT;
			raise notice '<%> row(s) has(ve) been successfully inserted.',_insert_count;

			open _alt_cursor for select alternative_source.*
							 from
							 (select case when r0.ctlabel like '%Qualifying%' then 
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
									left outer join ctennismappingplayers as mp1 on (mp1.ctcode=p1.ctcode or mp1.ctcodeofficial=p1.ctcode)
									left outer join ctennismappingplayers as mp2 on (mp2.ctcode=p2.ctcode or mp2.ctcodeofficial=p2.ctcode)
							     	left outer join ctennisplayers as p11 on p11.ctcode=mp1.ctcodeofficial
							     	left outer join ctennisplayers as p22 on p22.ctcode=mp2.ctcodeofficial										
									left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
									left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
									left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
									left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
									where m0.ctstatsource in('http://www.tennislive.net')
									and m0.ctstartdate>=_min_date
									and m0.ctenddate<=_max_date
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
									) as alternative_source
									left outer join ctenniscleanstats as cleanstats
									on  cleanstats.startdate=alternative_source.startdate
									and cleanstats.enddate=alternative_source.enddate
									and cleanstats.player1=alternative_source.player1
									and cleanstats.player2=alternative_source.player2
									and cleanstats.round=alternative_source.round
									and cleanstats.score=alternative_source.score
									where cleanstats.startdate is null
									and cleanstats.enddate is null
									and cleanstats.player1 is null
									and cleanstats.player2 is null
									and cleanstats.round is null
									and cleanstats.score is null;

			_insert_count:=0;
			loop
				fetch _alt_cursor into _stats_record;
				exit when not found;

				if not exists(
					select 1 from ctenniscleanstats as c0
					where  ((c0.player1=_stats_record.player1 and c0.player2=_stats_record.player2 and c0.score=_stats_record.score)
						or (_stats_record.player1=c0.player2 and _stats_record.player2=c0.player1 and _stats_record.score=fn_rscore(c0.score)))
					   and ((_stats_record.startdate>=c0.startdate and _stats_record.enddate<=c0.enddate)
					   	or (_stats_record.startdate+interval '1 day'>=c0.startdate and _stats_record.enddate+interval '1 day'<=c0.enddate)
					   	or (_stats_record.startdate+interval '-1 day'>=c0.startdate and _stats_record.enddate+interval '-1 day'<=c0.enddate))
				       and c0.source!=_stats_record.source
				    union all
					select 1 from ctenniscleanstats as c0
					where  ((c0.player1=_stats_record.player1 and c0.player2=_stats_record.player2 and c0.score=_stats_record.score)
						or (_stats_record.player1=c0.player2 and _stats_record.player2=c0.player1 and _stats_record.score=fn_rscore(c0.score)))
					   and (_stats_record.startdate=c0.startdate and _stats_record.enddate=c0.enddate)
				       and c0.source=_stats_record.source
				       and _stats_record.round=c0.round
					         ) then

					_insert_count:=_insert_count+1;
					insert into ctenniscleanstats
					(
						startdate,
						enddate,
						player1,
						player2,
						tournament,
						tournament_category,
						surface,
						round,
						score,
						player_sets_won1,
						player_sets_won2,
						player_games_won1,
						player_games_won2,
						total_serv1,
						total_serv2,       
						first_serv1,
						first_serv2,
						first_serv_won1,
						first_serv_won2,
						second_serv_won1,
						second_serv_won2,
						total_break_points1,
						total_break_points2,
						break_points_won1,
						break_points_won2,
						double_fault1,
						double_fault2,
						aces1,
						aces2,
						source,
						createdon,
						updatedon
					)
					select
						_tmp_clean.startdate,
						_tmp_clean.enddate,
						_tmp_clean.player1,
						_tmp_clean.player2,
						_tmp_clean.tournament,
						_tmp_clean.tournament_category,
						_tmp_clean.surface,
						_tmp_clean.round,
						_tmp_clean.score,
						_tmp_clean.player_sets_won1,
						_tmp_clean.player_sets_won2,
						_tmp_clean.player_games_won1,
						_tmp_clean.player_games_won2,
						_tmp_clean.total_serv1,
						_tmp_clean.total_serv2,       
						_tmp_clean.first_serv1,
						_tmp_clean.first_serv2,
						_tmp_clean.first_serv_won1,
						_tmp_clean.first_serv_won2,
						_tmp_clean.second_serv_won1,
						_tmp_clean.second_serv_won2,
						_tmp_clean.total_break_points1,
						_tmp_clean.total_break_points2,
						_tmp_clean.break_points_won1,
						_tmp_clean.break_points_won2,
						_tmp_clean.double_fault1,
						_tmp_clean.double_fault2,
						_tmp_clean.aces1,
						_tmp_clean.aces2,
						_tmp_clean.source,
						_tmp_clean.createdon,
						current_timestamp
					from
					(	select
							_stats_record.startdate as startdate,
							_stats_record.enddate as enddate,
							_stats_record.player1 as player1,
							_stats_record.player2 as player2,
							_stats_record.tournament as tournament,
							_stats_record.tournament_category as tournament_category,
							_stats_record.surface as surface,
							_stats_record.round as round,
							_stats_record.score as score,
							_stats_record.player_sets_won1 as player_sets_won1,
							_stats_record.player_sets_won2 as player_sets_won2,
							_stats_record.player_games_won1 as player_games_won1,
							_stats_record.player_games_won2 as player_games_won2,
							_stats_record.total_serv1 as total_serv1,
							_stats_record.total_serv2 as total_serv2,       
							_stats_record.first_serv1 as first_serv1,
							_stats_record.first_serv2 as first_serv2,
							_stats_record.first_serv_won1 as first_serv_won1,
							_stats_record.first_serv_won2 as first_serv_won2,
							_stats_record.second_serv_won1 as second_serv_won1,
							_stats_record.second_serv_won2 as second_serv_won2,
							_stats_record.total_break_points1 as total_break_points1,
							_stats_record.total_break_points2 as total_break_points2,
							_stats_record.break_points_won1 as break_points_won1,
							_stats_record.break_points_won2 as break_points_won2,
							_stats_record.double_fault1 as double_fault1,
							_stats_record.double_fault2 as double_fault2,
							_stats_record.aces1 as aces1,
							_stats_record.aces2 as aces2,
							_stats_record.source as source,
							_stats_record.createdon as createdon
					) as _tmp_clean
					left outer join ctenniscleanstats as cleanstats
						on _tmp_clean.startdate=cleanstats.startdate
					   and _tmp_clean.enddate=cleanstats.enddate
					   and _tmp_clean.round=cleanstats.round
					   and _tmp_clean.player1=cleanstats.player1
					   and _tmp_clean.player2=cleanstats.player2
					   and _tmp_clean.score=cleanstats.score
					where  cleanstats.startdate is null
					   and cleanstats.enddate is null
					   and cleanstats.round is null
					   and cleanstats.player1 is null
					   and cleanstats.player2 is null
					   and cleanstats.score is null;

					raise notice 'Insertion - stats : % : {%; %; %; %; %}',_insert_count,_stats_record.startdate,_stats_record.player1,_stats_record.player2,_stats_record.tournament,_stats_record.score;
				end if;
			end loop;
			close _alt_cursor;
		end if;
	end $$;
