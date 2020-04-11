DO 
$do$
declare __table_name character varying(100);
begin
   __table_name := 'ctennismatchesstats';

   if not exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      raise notice 'The table <%> does not exist. It will be created.',__table_name;

      create table ctennismatchesstats
     	(
        ctcode varchar(100) not null constraint uniq_ctennismatchesstats_ctcode unique,
        ctstartdate date,
        ctenddate date,
        ctplayer1 integer,
        ctplayer2 integer,
        cttournament integer,
        ctsurface integer,
        ctround integer,
        ctscore varchar(50),
        ctstat1stserv1 varchar(10),
        ctstat1stserv2 varchar(10),
        ctstat1stservwon1 varchar(10),
        ctstat1stservwon2 varchar(10),
        ctstat2ndservwon1 varchar(10),
        ctstat2ndservwon2 varchar(10),
        ctstatbrkwon1 varchar(10),
        ctstatbrkwon2 varchar(10),
        ctstatretwon1 varchar(10),
        ctstatretwon2 varchar(10),
        ctstatdble1 integer,
        ctstatdble2 integer,
        ctstataces1 integer,
        ctstataces2 integer,
        ctstatsource varchar(200),
        ctcreatedon timestamp,
        ctupdatedon timestamp,

        primary key(ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore)   		
     	);

      raise notice 'The table <%> has been successfully created.',__table_name;
   else
      raise notice 'The table <%> already exists.',__table_name;
   end if;
end
$do$;
