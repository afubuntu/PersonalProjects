DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='fn_get_score') then
		drop function fn_get_score(varchar(50),integer,varchar(1));
	end if;
end
$do$;

create or replace function fn_get_score
	(
		_score varchar(50),
		_player integer,
		_score_type varchar(1)
	)
	returns integer
	language plpgsql
	as $$
	declare
		_pos_space integer;
		_pos_tiebreak integer;
		_current_score varchar(50);
		_set_score varchar(50);
		_player1_set integer;
		_player2_set integer;
		_player1_sets_won integer;
		_player2_sets_won integer;
		_player1_game integer;
		_player2_game integer;
	begin
		if length(_score)<=0 or _score is null then
			return -1;
		end if;

		_current_score:=_score;
		_pos_space:=strpos(_current_score,' ');
		_player1_sets_won:=0;
		_player2_sets_won:=0;
		_player1_game:=0;
		_player2_game:=0;

		loop
			exit when _pos_space=0;

			_set_score:=left(_current_score,_pos_space-1);
			_pos_tiebreak:=strpos(_set_score,'[');

			if _pos_tiebreak>0 then
				_set_score:=left(_set_score,_pos_tiebreak-1);
			end if;

			begin
				_player1_set:=split_part(_set_score,'-',1)::integer;
				_player2_set:=split_part(_set_score,'-',2)::integer;
			exception when others then
				return -1;
			end;
			_player1_game:=_player1_game+_player1_set;
			_player2_game:=_player2_game+_player2_set;

			if _player1_set>_player2_set then
				_player1_sets_won:=_player1_sets_won+1;
			else
				_player2_sets_won:=_player2_sets_won+1;
			end if;

			_current_score:=right(_current_score,length(_current_score)-_pos_space);
			_pos_space:=strpos(_current_score,' ');
		end loop;

		if length(_current_score)>0 then
			_pos_tiebreak:=strpos(_current_score,'[');

			if _pos_tiebreak>0 then
				_current_score:=left(_current_score,_pos_tiebreak-1);
			end if;

			begin
				_player1_set:=split_part(_current_score,'-',1)::integer;
				_player2_set:=split_part(_current_score,'-',2)::integer;
				_player1_game:=_player1_game+_player1_set;
				_player2_game:=_player2_game+_player2_set;				
			exception when others then
				_player1_set:=-1;
			end;

			if _player1_set>=0 then
				if _player1_set>_player2_set then
					_player1_sets_won:=_player1_sets_won+1;
				else
					_player2_sets_won:=_player2_sets_won+1;
				end if;
			end if;
		end if;

		if _player=1 then
			if _score_type='s' then
				return _player1_sets_won;
			else
				if _score_type='g' then
					return _player1_game;
				end if;
				return -1;
			end if;
		else
			if _player=2 then
				if _score_type='s' then
					return _player2_sets_won;
				else
					if _score_type='g' then
						return _player2_game;
					end if;
					return -1;
				end if;
			end if;
			return -1;
		end if;
	end $$;