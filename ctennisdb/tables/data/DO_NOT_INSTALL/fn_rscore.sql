create or replace function fn_rscore
	(
		_score varchar(50)
	)
	returns varchar(50)
	language plpgsql
	as $$
	declare
		_return_score varchar(50);
		_pos_space integer;
		_pos_tiebreak integer;
		_current_score varchar(50);
		_set_score varchar(50);
		_set_tie_break varchar(50);
	begin
		_current_score:=_score;
		_pos_space:=strpos(_current_score,' ');
		_return_score:='';

		loop
			exit when _pos_space=0;

			_set_score:=left(_current_score,_pos_space-1);
			_pos_tiebreak:=strpos(_set_score,'[');
			_set_tie_break:='';

			if _pos_tiebreak>0 then
				_set_tie_break:=right(_set_score,length(_set_score)-_pos_tiebreak+1);
				_set_score:=left(_set_score,_pos_tiebreak-1);
			end if;

			_return_score:=_return_score||' '||split_part(_set_score,'-',2)||'-'||split_part(_set_score,'-',1)||_set_tie_break;
			_current_score:=right(_current_score,length(_current_score)-_pos_space);
			_pos_space:=strpos(_current_score,' ');
		end loop;

		if length(_current_score)>0 then
			_pos_tiebreak:=strpos(_current_score,'[');
			_set_tie_break:='';

			if _pos_tiebreak>0 then
				_set_tie_break:=right(_current_score,length(_current_score)-_pos_tiebreak+1);
				_current_score:=left(_current_score,_pos_tiebreak-1);
			end if;

			_return_score:=_return_score||' '||split_part(_current_score,'-',2)||'-'||split_part(_current_score,'-',1)||_set_tie_break;
		end if;

		return ltrim(_return_score);
	end $$;