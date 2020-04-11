DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_insert_ctcategory') then
		drop function sp_insert_ctcategory;
	end if;
end
$do$;

create or replace function sp_insert_ctcategory
	(
		_ctcode varchar(100),
		_ctlabel varchar(100),
		_ctpoints integer,
		_update varchar(1) default 'N'
	)
	returns void
	language plpgsql
	as $$
	declare
		_ct_id integer;
	begin
		if not exists(select 1 from ctennistournamentscategories where lower(ctcode)=lower(_ctcode)) then	

			select coalesce(max(ctdid),0)+1 into _ct_id from ctennistournamentscategories where ctcode not in ('notdefined');

			insert into ctennistournamentscategories
				(
					ctdid,
					ctcode,
					ctlabel,
					ctpoints,
					ctcreatedon,
					ctupdatedon
				)
		    select
		    		_ct_id,
		    		_ctcode,
		    		_ctlabel,
		    		_ctpoints,
		    		current_timestamp,
		    		null;
		else
			if _update='Y' then

				update ctennistournamentscategories
					set ctlabel=_ctlabel,
						ctpoints=_ctpoints,
						ctupdatedon=current_timestamp
				where lower(ctcode)=lower(_ctcode);
			end if;		
		end if;
	end $$;