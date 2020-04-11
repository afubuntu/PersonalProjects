DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_setupmapping_stats') then
		drop function sp_setupmapping_stats(integer,integer,varchar(1));
	end if;
end
$do$;

create or replace function sp_setupmapping_stats
	(
		_row_min integer default 0, 
		_row_max integer default -1,
		_ctgender varchar(1) default 'M'
	)
	returns void 
	language plpgsql
	as $$
	declare
		_count_off_table integer;
		_count_alt_table integer;
		_cur_row_off integer;
		_cur_row_alt integer;
		_ctcode_off varchar(200);
		_ctname_off varchar(200);
		_ctcode_alt varchar(200);
		_ctname_alt varchar(200);
	begin
		if _ctgender='M' then
			create temp table tmp_off_table as
			select off_table.*,row_number() over(order by off_table.ctname) as row_number
			from 
			(
				select p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p1 on p1.ctdid=m0.ctplayer1
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p1.ctcode
				where mp0.ctcodeofficial is null
				  and p1.ctgender=_ctgender
				  and m0.ctstatsource in('https://www.atptour.com') 
				union
				select p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p2 on p2.ctdid=m0.ctplayer2
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p2.ctcode
				where mp0.ctcodeofficial is null
				  and p2.ctgender=_ctgender
				  and m0.ctstatsource in('https://www.atptour.com')
			) as off_table;

			create temp table tmp_alt_table as
			select alt_table.*,row_number() over(order by alt_table.ctname) as row_number
			from 
			(
				select p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p1 on p1.ctdid=m0.ctplayer1
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p1.ctcode
				where mp0.ctcodeofficial is null
				  and p1.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net') 
				union
				select p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p2 on p2.ctdid=m0.ctplayer2
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p2.ctcode
				where mp0.ctcodeofficial is null
				  and p2.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net')
			) as alt_table;

			select count(*) into _count_alt_table from tmp_alt_table;
			if _row_max>=0 then
				_count_alt_table:=_row_max;
			end if;
			select min(row_number),max(row_number) into _cur_row_off,_count_off_table from tmp_off_table;

			loop
				exit when _cur_row_off>_count_off_table;
				select ctcode,ctname into _ctcode_off,_ctname_off from tmp_off_table where row_number=_cur_row_off;
				_cur_row_alt:=0;
				if _row_min>0 then
					_cur_row_alt:=_row_min;
				end if;

				loop
					exit when _cur_row_alt>_count_alt_table-1;
					select ctcode,ctname into _ctcode_alt,_ctname_alt from tmp_alt_table limit 1 offset _cur_row_alt;

					if length(fn_mappingplayer1st(_ctcode_off,_ctname_off,_ctcode_alt,_ctname_alt))>0 then
						raise notice '{% ==> %}',_ctcode_alt,_ctcode_off;

						begin
							perform sp_insert_ctplayermapping(_ctcode_alt,_ctcode_off);
						exception when others then
							update sp_insert_ctplayermapping
								set ctcodeofficial=_ctcode_off
							where ctcode=_ctcode_alt;
						end;

						delete from tmp_alt_table where ctcode=_ctcode_alt;
						_count_alt_table:=_count_alt_table-1;
						exit;
					end if;
					_cur_row_alt:=_cur_row_alt+1;
				end loop;
				_cur_row_off:=_cur_row_off+1;
			end loop;

			select count(alt_table.*) into _count_alt_table
			from 
			(
				select distinct p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p1 on p1.ctdid=m0.ctplayer1
				left outer join ctennismappingplayers as mp0 on (mp0.ctcode=p1.ctcode or mp0.ctcodeofficial=p1.ctcode)
				where (mp0.ctcode is null or mp0.ctcodeofficial is null)
				  and p1.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net') 
				union
				select distinct p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p2 on p2.ctdid=m0.ctplayer2
				left outer join ctennismappingplayers as mp0 on (mp0.ctcode=p2.ctcode or mp0.ctcodeofficial=p2.ctcode)
				where (mp0.ctcode is null or mp0.ctcodeofficial is null)
				  and p2.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net')
			) as alt_table;

			raise notice '% has(ve) not been mapped with the first algorithm. The matches associated to them should be removed from the analysis',_count_alt_table;
		elsif _ctgender='W' then
			create temp table tmp_off_table as
			select off_table.*,row_number() over(order by off_table.ctname) as row_number
			from 
			(
				select p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p1 on p1.ctdid=m0.ctplayer1
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p1.ctcode
				where mp0.ctcodeofficial is null
				  and p1.ctgender=_ctgender
				  and m0.ctstatsource in('https://www.wtatennis.com') 
				union
				select p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p2 on p2.ctdid=m0.ctplayer2
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p2.ctcode
				where mp0.ctcodeofficial is null
				  and p2.ctgender=_ctgender
				  and m0.ctstatsource in('https://www.wtatennis.com')
			) as off_table;

			create temp table tmp_alt_table as
			select alt_table.*,row_number() over(order by alt_table.ctname) as row_number
			from 
			(
				select p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p1 on p1.ctdid=m0.ctplayer1
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p1.ctcode
				where mp0.ctcodeofficial is null
				  and p1.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net') 
				union
				select p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p2 on p2.ctdid=m0.ctplayer2
				left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p2.ctcode
				where mp0.ctcodeofficial is null
				  and p2.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net')
			) as alt_table;

			select count(*) into _count_alt_table from tmp_alt_table;
			if _row_max>=0 then
				_count_alt_table:=_row_max;
			end if;
			select min(row_number),max(row_number) into _cur_row_off,_count_off_table from tmp_off_table;

			loop
				exit when _cur_row_off>_count_off_table;
				select ctcode,ctname into _ctcode_off,_ctname_off from tmp_off_table where row_number=_cur_row_off;
				_cur_row_alt:=0;
				if _row_min>0 then
					_cur_row_alt:=_row_min;
				end if;

				loop
					exit when _cur_row_alt>_count_alt_table-1;
					select ctcode,ctname into _ctcode_alt,_ctname_alt from tmp_alt_table limit 1 offset _cur_row_alt;

					if length(fn_mappingplayer1st(_ctcode_off,_ctname_off,_ctcode_alt,_ctname_alt))>0 then
						raise notice '{% ==> %}',_ctcode_alt,_ctcode_off;

						begin
							perform sp_insert_ctplayermapping(_ctcode_alt,_ctcode_off);
						exception when others then
							update sp_insert_ctplayermapping
								set ctcodeofficial=_ctcode_off
							where ctcode=_ctcode_alt;
						end;

						delete from tmp_alt_table where ctcode=_ctcode_alt;
						_count_alt_table:=_count_alt_table-1;
						exit;
					end if;
					_cur_row_alt:=_cur_row_alt+1;
				end loop;
				_cur_row_off:=_cur_row_off+1;
			end loop;

			select count(alt_table.*) into _count_alt_table
			from 
			(
				select distinct p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p1 on p1.ctdid=m0.ctplayer1
				left outer join ctennismappingplayers as mp0 on (mp0.ctcode=p1.ctcode or mp0.ctcodeofficial=p1.ctcode)
				where (mp0.ctcode is null or mp0.ctcodeofficial is null)
				  and p1.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net') 
				union
				select distinct p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
				from ctennismatchesstats as m0
				left outer join ctennisplayers 	as p2 on p2.ctdid=m0.ctplayer2
				left outer join ctennismappingplayers as mp0 on (mp0.ctcode=p2.ctcode or mp0.ctcodeofficial=p2.ctcode)
				where (mp0.ctcode is null or mp0.ctcodeofficial is null)
				  and p2.ctgender=_ctgender
				  and m0.ctstatsource in('http://www.tennislive.net')
			) as alt_table;

			raise notice '% has(ve) not been mapped with the first algorithm. The matches associated to them should be removed from the analysis',_count_alt_table;
		else
			raise notice 'The value provided for the argument <_ctgender:= %> is not supported.',_ctgender;
			return;
		end if;
	end $$;