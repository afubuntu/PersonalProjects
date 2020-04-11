DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_insert_ctplayermapping') then
		drop function sp_insert_ctplayermapping;
	end if;
end
$do$;

create or replace function sp_insert_ctplayermapping
	(
		_ctcode varchar(200),
		_ctcodeofficial varchar(200)
	)
	returns void
	language plpgsql
	as $$
	begin
		if not exists(select 1 from ctennismappingplayers 
			          where lower(ctcodeofficial)=lower(_ctcodeofficial) and lower(ctcode)=lower(_ctcode)) then

			insert into ctennismappingplayers
			(
				ctcode,
				ctcodeofficial,
				ctcreatedon
			)
		    select 
		        _ctcode,
		        _ctcodeofficial,
		        current_timestamp;
		end if;
	end $$;