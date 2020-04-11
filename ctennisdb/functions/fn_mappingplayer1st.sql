DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='fn_mappingplayer1st') then
		drop function fn_mappingplayer1st(varchar(200),varchar(200),varchar(200),varchar(200),varchar(1),varchar(3),integer);
	end if;
end
$do$;

create or replace function fn_mappingplayer1st
	(
		_ctcode1 varchar(200),
		_ctname1 varchar(200),
		_ctcode2 varchar(200),
		_ctname2 varchar(200),
		_ctgender varchar(1) default 'M',
		_mapping_type varchar(3) default '1st',
		_tolerance integer default 3
	)
	returns varchar(400)
	language plpgsql
	as $$
	declare
		_cur_name1 varchar(200);
		_cur_name2 varchar(200);
		_part_name varchar(100);
		_pos_space integer;
		_pos_space2 integer;
		_ii integer;
		_jj integer;
		_kk integer;
		_len1 integer;
		_len2 integer;
		_found boolean;
	begin
		if _mapping_type='1st' then
		/* 1- Look up of every part of _ctname1 in _ctname2 */
			_cur_name1:=_ctname1;
			_cur_name2:=_ctname2;
			_pos_space:=strpos(_cur_name1,' ');
			_found:=true;

			loop
				exit when _pos_space=0;

				_part_name:=left(_cur_name1,_pos_space-1);
				_pos_space2:=strpos(_cur_name2,_part_name);
				if _pos_space2>0 then
					_cur_name2:=left(_cur_name2,_pos_space2-1)||right(_cur_name2,length(_cur_name2)-_pos_space2-length(_part_name)+1);
					_cur_name1=right(_cur_name1,length(_cur_name1)-_pos_space);
					_pos_space:=strpos(_cur_name1,' ');
				else
					_found:=false;
					exit;
				end if;
			end loop;

			if _found then
				if strpos(_cur_name2,_cur_name1)>0 then
					return _ctcode2||';'||_ctcode1;
				end if;
			end if;
		/* 2- Look up of every part of _ctcode2 in _ctcode1 */
			_cur_name1:=_ctcode1;
			_cur_name2:=_ctcode2;
			_pos_space:=strpos(_cur_name2,'-');
			_found:=true;

			loop
				exit when _pos_space=0;

				_part_name:=left(_cur_name2,_pos_space-1);
				_pos_space2:=strpos(_cur_name1,_part_name);
				if _pos_space2>0 then
					_cur_name1:=left(_cur_name1,_pos_space2-1)||right(_cur_name1,length(_cur_name1)-_pos_space2-length(_part_name)+1);
					_cur_name2=right(_cur_name2,length(_cur_name2)-_pos_space);
					_pos_space:=strpos(_cur_name2,'-');
				else
					_found:=false;
					exit;
				end if;
			end loop;

			if _found then
				if strpos(_cur_name1,_cur_name2)>0 then
					return _ctcode2||';'||_ctcode1;
				end if;
			end if;			
		/* 3- Look up of every part of _ctcode1 in _ctcode2 for men */
			if _ctgender='M' then
				_cur_name1:=reverse(_ctcode1);
				_cur_name2:=reverse(_ctcode2);
				_pos_space:=strpos(_cur_name1,'-');
				_cur_name1:=right(_cur_name1,length(_cur_name1)-_pos_space);
				_pos_space:=strpos(_cur_name1,'-');
				_found:=true;

				loop
					exit when _pos_space=0;

					_part_name:=left(_cur_name1,_pos_space-1);
					_pos_space2:=strpos(_cur_name2,_part_name);
					if _pos_space2>0 then
						_cur_name2:=left(_cur_name2,_pos_space2-1)||right(_cur_name2,length(_cur_name2)-_pos_space2-length(_part_name)+1);
						_cur_name1=right(_cur_name1,length(_cur_name1)-_pos_space);
						_pos_space:=strpos(_cur_name1,'-');
					else
						_found:=false;
						exit;
					end if;
				end loop;

				if _found then
					if strpos(_cur_name2,_cur_name1)>0 then
						return _ctcode2||';'||_ctcode1;
					end if;
				end if;
			end if;	
		/* 4- Look up of every part of _ctcode1 in _ctcode2 for women */
			if _ctgender='W' then
				_cur_name1:=_ctcode1;
				_cur_name2:=_ctcode2;
				_pos_space:=strpos(_cur_name1,'-');
				_cur_name1=right(_cur_name1,length(_cur_name1)-_pos_space);
				_pos_space:=strpos(_cur_name1,'-');
				_found:=true;

				loop
					exit when _pos_space=0;

					_part_name:=left(_cur_name1,_pos_space-1);
					_pos_space2:=strpos(_cur_name2,_part_name);
					if _pos_space2>0 then
						_cur_name2:=left(_cur_name2,_pos_space2-1)||right(_cur_name2,length(_cur_name2)-_pos_space2-length(_part_name)+1);
						_cur_name1=right(_cur_name1,length(_cur_name1)-_pos_space);
						_pos_space:=strpos(_cur_name1,'-');
					else
						_found:=false;
						exit;
					end if;
				end loop;

				if _found then
					if strpos(_cur_name2,_cur_name1)>0 then
						return _ctcode2||';'||_ctcode1;
					end if;
				end if;
			end if;

			return '';
		elsif _mapping_type='2nd' then
		/* 5- Trying to mapp _ctname1 and _ctname2 assuming that there might be some characters that are different. _tolerence indicates the maximun of characters that different */
			_len1:=length(_ctname1);
			_len2:=length(_ctname2);

			if abs(_len1-_len2)>_tolerance then
				return '';
			end if;

			_ii:=0;
			_jj:=1;
			_kk:=_tolerance/2;

			loop
				exit when _jj>_len1;

				_part_name:=substring(_ctname1,_jj,1);
				if _part_name=' ' or _part_name='-' then
					_jj=_jj+1;
					continue;
				end if;

				if _jj=1 then
					_cur_name2:=substring(_ctname2,_jj,_kk+1);
				elsif _jj>1 and _jj<_len2 then
					_cur_name2:=substring(_ctname2,_jj-1,_tolerance);
				elsif _jj>=_len2 then
					_cur_name2:=substring(_ctname2,_len2-_kk,_kk+1);
				end if;

				if strpos(_cur_name2,_part_name)=0 then
					_ii=_ii+1;
				end if;

				if _ii>_tolerance then
					return '';
				end if;

				_jj=_jj+1;
			end loop;

			return _ctcode2||';'||_ctcode1;
		else
			return '';
		end if;
	end $$;