DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_insert_ctcountry') then
		drop function sp_insert_ctcountry;
	end if;
end
$do$;

create or replace function sp_insert_ctcountry
	(
		_ctcode varchar(3),
		_ctlabel varchar(100),
		_update varchar(1) default 'N'
	)
	returns void
	language plpgsql
	as $$
	begin
		if not exists(select 1 from ctenniscountries where lower(ctcode)=lower(_ctcode)) then

			insert into ctenniscountries
				(
					ctcode,
					ctlabel,
					ctcreatedon,
					ctupdatedon
				)
		    select
		    		_ctcode,
		    		_ctlabel,
		    		current_timestamp,
		    		null;
		else
			if _update='Y' then

				update ctenniscountries
					set ctlabel=_ctlabel,
					    ctupdatedon=current_timestamp
				where lower(ctcode)=lower(_ctcode);
			end if;
		end if;
	end $$;