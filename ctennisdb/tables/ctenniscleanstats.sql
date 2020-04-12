DO 
$do$
declare __table_name character varying(100);
begin
   __table_name := 'ctenniscleanstats';

   if not exists (select 1 from information_schema.tables where table_schema='public' and table_name=__table_name and table_type='BASE TABLE') then

      raise notice 'The table <%> does not exist. It will be created.',__table_name;

      create table ctenniscleanstats
     	(
        startdate date,
        enddate date,
        player1 varchar(200),
        player2 varchar(200),
        gender varchar(1),
        tournament varchar(200),
        tournament_category varchar(100),
        surface varchar(200),
        round varchar(100),
        score varchar(50),
        player_sets_won1 integer,
        player_sets_won2 integer,
        player_games_won1 integer,
        player_games_won2 integer,
        total_serv1 integer,
        total_serv2 integer,       
        first_serv1 integer,
        first_serv2 integer,
        first_serv_won1 integer,
        first_serv_won2 integer,
        second_serv_won1 integer,
        second_serv_won2 integer,
        total_break_points1 integer,
        total_break_points2 integer,
        break_points_won1 integer,
        break_points_won2 integer,
        double_fault1 integer,
        double_fault2 integer,
        aces1 integer,
        aces2 integer,
        source varchar(200),
        createdon timestamp,
        updatedon timestamp,

        primary key(startdate,enddate,player1,player2,round,score)   		
     	);

      raise notice 'The table <%> has been successfully created.',__table_name;
   else
      raise notice 'The table <%> already exists.',__table_name;
   end if;
end
$do$;
