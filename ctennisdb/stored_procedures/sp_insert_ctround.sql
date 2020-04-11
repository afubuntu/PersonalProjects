DO 
$do$
begin
	if exists(select 1 from pg_proc where proname='sp_insert_ctround') then
		drop function sp_insert_ctround;
	end if;
end
$do$;

create or replace function sp_insert_ctround
	(
		_ctcodeofficial varchar(100),
		_ctlabel varchar(100),
		_update varchar(1) default 'N'
	)
	returns void
	language plpgsql
	as $$
	begin
		if not exists(select 1 from cttournamentrounds where lower(ctcodeofficial)=lower(_ctcodeofficial)) then	

			insert into cttournamentrounds
				(
					ctcodeofficial,
					ctlabel,
					ctcreatedon,
					ctupdatedon
				)
		    select
		    		_ctcodeofficial,
		    		_ctlabel,
		    		current_timestamp,
		    		null;
		else
			if _update='Y' then
				update cttournamentrounds
					set ctlabel=_ctlabel,
						ctupdatedon=current_timestamp
				where lower(ctcodeofficial)=lower(_ctcodeofficial);
			end if;	
		end if;
	end $$;