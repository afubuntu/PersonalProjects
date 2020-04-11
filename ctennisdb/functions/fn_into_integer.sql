DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='fn_into_integer') then
		drop function fn_into_integer(varchar);
	end if;
end
$do$;

create or replace function fn_into_integer
	(
		_str varchar default ''
	)
	returns integer
	language plpgsql
	as $$
	begin
		begin
			return _str::integer;				
		exception when others then
			return -1;
		end;
	end $$;