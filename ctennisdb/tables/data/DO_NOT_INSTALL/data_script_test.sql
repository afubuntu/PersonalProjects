/*DO 
$do$
declare __officialCode character varying(3);
        __mappingCode character varying(3);
        __coalesce_value character varying(3);
begin
      select co11.ctcode,mco1.ctcodeofficial,coalesce(co11.ctcode,mco1.ctcodeofficial) into __officialCode,__mappingCode,__coalesce_value
      from ctenniscountries as co1
      left outer join ctenniscountries as co11 on co11.ctcode=co1.ctcode
      left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=co1.ctcode
      where  lower(mco1.ctcode)= 'united arab emirates' or lower(co11.ctlabel)='united arab emirates';

      raise notice '[__officialCode = % ];[__mappingCode = % ];[__coalesce_value = % ]',__officialCode,__mappingCode,__coalesce_value;
end
$do$;

*//*
select co11.ctcode,mco1.ctcodeofficial,coalesce(co11.ctcode,mco1.ctcodeofficial) from ctenniscountries as co1 left outer join ctenniscountries as co11 on co11.ctcode=co1.ctcode left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=co1.ctcode where  coalesce(lower(mco1.ctcode),lower(co11.ctlabel))='u.s.a.';*/

/*
select * from ctennistournaments as to1
left outer join ctenniscountries as co1 on to1.ctcountry=co1.ctcode
left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=to1.ctcountry
where (lower(mco1.ctcode)='brazil' or lower(co1.ctlabel)='brazil') 
and lower(to1.ctname)=lower('Rio Open presented by Claro');
*/
/*
update ctennistournaments
set ctcountry='BRA'
where ctname='Rio Open presented by Claro';
*/

/*select ctdid,lower(replace(ctname,',','')) as ctname, ctcountry,ctbirthdate,ctbeginpro from ctennisplayers order by ctdid limit 200 offset 200;
*/
/*select * from ctennismappingtournaments ;*/
/*

	select pl1.ctdid as pl_id1,pl2.ctdid as pl_id2	
	from ctennisplayers as pl
	left outer join ctennisplayers as pl1 on pl1.ctdid=pl.ctdid
	left outer join ctennisplayers as pl2 on pl2.ctdid=pl.ctdid
	left outer join ctennismappingplayers as mp1 on mp1.ctcodeofficial=pl1.ctcode
	left outer join ctennismappingplayers as mp2 on mp2.ctcodeofficial=pl2.ctcode
	where (pl1.ctcode='roger-federer-f324' or mp1.ctcode='roger-federer-f324')
	  or (pl2.ctcode='novak-djokovic-d643' or mp2.ctcode='novak-djokovic-d643');
*/

/*
	select ctdid,ctname,ctbirthdate
	from ctennisplayers
	where ctgender='W' and ctcode=ctname;
*/
/*
select ctdid,ctname from ctennisplayers order by ctdid limit 10;
select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=1 or ctplayer2=1;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=2 or ctplayer2=2;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=3 or ctplayer2=3;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=4 or ctplayer2=4;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=5 or ctplayer2=5;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=6 or ctplayer2=6;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=7 or ctplayer2=7;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=8 or ctplayer2=8;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=9 or ctplayer2=9;

select ctstartdate,ctenddate,ctplayer1,ctplayer2,cttournament,ctround,ctscore
from ctennismatchesstats
where ctplayer1=10 or ctplayer2=10;
*/
/*
select cts.ctstartdate,cts.ctenddate,p1.ctname as player1,p2.ctname as player2,t1.ctname as tournament,cts.ctsurface,cts.ctround,cts.ctscore,cts.ctstatsource
from ctennismatchesstats cts
left outer join ctennisplayers p1 on p1.ctdid=cts.ctplayer1
left outer join ctennisplayers p2 on p2.ctdid=cts.ctplayer2
left outer join ctennistournaments t1 on cts.cttournament=t1.ctcode
where (p1.ctcode like '%rafael-nadal%' or p2.ctcode like '%rafael-nadal%')
--and cts.ctstatsource='http://www.tennislive.net'
order by p1.ctname,cts.cttournament,cts.ctround;
*/
/*
select ctdid,ctname from ctennisplayers order by ctname;
*/

/*
select * from ctennismatchesstats as ms
left outer join ctennisplayers as pl1 on pl1.ctdid=ms.ctplayer1
left outer join ctennisplayers as pl2 on pl2.ctdid=ms.ctplayer2
left outer join ctennistournaments as to1 on to1.ctcode=ms.cttournament
left outer join cttournamentrounds as rd1 on rd1.ctcode=ms.ctround
left outer join ctennissurfaces as su1 on su1.ctcode=ms.ctsurface					
where ms.ctstartdate='2019-02-25' 
  and ms.ctenddate='2019-03-03'
  and ms.ctscore='3-6 6-1 6-2'
  and (pl1.ctcode='roberto-carballes-baena-cf59' or  pl1.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode='roberto-carballes-baena-cf59'))
  and (pl2.ctcode='maximilian-marterer-mn13' or pl2.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode='maximilian-marterer-mn13'))
  and (lower(to1.ctname)=lower(_cttournament) or lower(to1.ctname)= (select lower(max(ctnameofficial)) from ctennismappingtournaments where lower(ctname)=lower(_cttournament)))
  and (lower(rd1.ctcodeofficial)=lower(_ctround) or lower(rd1.ctcodeofficial)= (select lower(max(ctcodeofficial)) from ctennismappingrounds where  lower(ctcode)=lower(_ctround)))
  and (lower(su1.ctlabel)=lower(_ctsurface) or lower(su1.ctlabel)= (select lower(max(ctlabelofficial)) from ctennismappingsurfaces where lower(ctlabel)=lower(_ctsurface)));
*/

/*
select ms.ctplayer1,ms.ctplayer2,ms.ctscore,ms.ctstartdate,ms.ctenddate,ms.ctstatsource*1 from ctennismatchesstats as ms
left outer join ctennisplayers as pl1 on pl1.ctdid=ms.ctplayer1
left outer join ctennisplayers as pl2 on pl2.ctdid=ms.ctplayer2
left outer join ctennistournaments as to1 on to1.ctcode=ms.cttournament
left outer join cttournamentrounds as rd1 on rd1.ctcode=ms.ctround
left outer join ctennissurfaces as su1 on su1.ctcode=ms.ctsurface					
where ms.ctstartdate>='2020-01-20' 
and ms.ctenddate<='2020-02-03'
and ms.ctscore='7-6[1] 6-4 6-3'
and (pl1.ctcode='novak-djokovic' or  pl1.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode='novak-djokovic'))
and (pl2.ctcode='roger-federer' or pl2.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode='roger-federer'))
and (lower(to1.ctname)=lower('Australian Open') or lower(to1.ctname)= (select lower(max(ctnameofficial)) from ctennismappingtournaments where lower(ctname)=lower('Australian Open')))
and (lower(rd1.ctcodeofficial)=lower('semifinals') or lower(rd1.ctcodeofficial)= (select lower(max(ctcodeofficial)) from ctennismappingrounds where  lower(ctcode)=lower('semifinals')))
union all
select ms.ctplayer1,ms.ctplayer2,ms.ctscore,ms.ctstartdate,ms.ctenddate,ms.ctstatsource 1 from ctennismatchesstats as ms
left outer join ctennisplayers as pl1 on pl1.ctdid=ms.ctplayer1
left outer join ctennisplayers as pl2 on pl2.ctdid=ms.ctplayer2
left outer join ctennistournaments as to1 on to1.ctcode=ms.cttournament
left outer join cttournamentrounds as rd1 on rd1.ctcode=ms.ctround
left outer join ctennissurfaces as su1 on su1.ctcode=ms.ctsurface					
where ms.ctstartdate>='2020-01-20' 
and ms.ctenddate<='2020-02-03'
and ms.ctscore='7-6[1] 6-4 6-3'
and (pl1.ctcode='roger-federer' or  pl1.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode='roger-federer'))
and (pl2.ctcode='novak-djokovic' or pl2.ctcode= (select max(ctcodeofficial) from ctennismappingplayers where ctcode='novak-djokovic'))
and (lower(to1.ctname)=lower('Australian Open') or lower(to1.ctname)= (select lower(max(ctnameofficial)) from ctennismappingtournaments where lower(ctname)=lower('Australian Open')))
and (lower(rd1.ctcodeofficial)=lower('semifinals') or lower(rd1.ctcodeofficial)= (select lower(max(ctcodeofficial)) from ctennismappingrounds where  lower(ctcode)=lower('semifinals')));
*/

/*
select * from ctennistournaments as to1 
			          left outer join ctenniscountries as co1 on to1.ctcountry=co1.ctcode
			          left outer join ctennismappingcountries as mco1 on mco1.ctcodeofficial=to1.ctcountry
			          where (lower(mco1.ctcode)=lower('USA') or lower(co1.ctlabel)=lower('USA')) 
			          and lower(to1.ctname)=lower('BNP Paribas Open');
			          */


/*
delete 
from ctennismatchesstats
where ctstatsource='http://www.tennislive.net';
*/
/*
delete 
from ctennismappingplayers;

delete 
from ctennisplayers;
*/
/*
select count(*)
from ctennismatchesstats csts
inner join ctennisplayers p1 on p1.ctdid=csts.ctplayer1
inner join ctennisplayers p2 on p2.ctdid=csts.ctplayer2
where p1.ctgender='W' and p2.ctgender='W';
*/
/*

with official_source as
     (
     	select m0.ctstartdate as startdate,
     	       m0.ctenddate as enddate,
     	       replace(p1.ctname,',','') as player1,
     	       replace(p2.ctname,',','') as player2,
     	       t0.ctname as tournament,
     	       s0.ctlabel as surface,
     	       r0.ctlabel as round,
     	       m0.ctscore as score,
     	       split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)::integer as total_serv1,
     	       split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)::integer as total_serv2,
     	       split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)::integer as first_serv1,
     	       split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)::integer as first_serv2,
     	       split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)::integer as first_serv_won1,
     	       split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)::integer as first_serv_won2,
     	       split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)::integer as second_serv_won1,
     	       split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)::integer as second_serv_won2,
     	       split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)::integer as total_break_points1,
     	       split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)::integer as total_break_points2,
     	       split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)::integer as break_points_won1,
     	       split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)::integer as break_points_won2,
     	       m0.ctstatdble1 as double_fault1,
     	       m0.ctstatdble2 as double_fault2,
     	       m0.ctstataces1 as aces1,
     	       m0.ctstataces2 as aces2,
     	       m0.ctstatsource as source,
     	       m0.ctcreatedon as createdon,
     	       m0.ctupdatedon as updatedon
     	from ctennismatchesstats as m0
     	left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
     	left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
     	left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
     	left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
     	left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
     	where m0.ctstatsource in('https://www.atptour.com','https://www.wtatennis.com')
     	  and m0.ctstartdate>=coalesce(null,(select min(ctstartdate) from ctennismatchesstats))
     	  and m0.ctenddate<=coalesce(null,(select max(ctenddate) from ctennismatchesstats))
     	  and p1.ctgender='M'
     	  and p2.ctgender='M'
     	  and (p1.ctname='Rafael, Nadal' or p2.ctname='Rafael, Nadal')
     ),
     alternative_source as
     (
     	select m0.ctstartdate as startdate,
     	       m0.ctenddate as enddate,
     	       replace(p1.ctname,',','') as player1,
     	       replace(p2.ctname,',','') as player2,
     	       t0.ctname as tournament,
     	       s0.ctlabel as surface,
     	       r0.ctlabel as round,
     	       m0.ctscore as score,
     	       split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)::integer as total_serv1,
     	       split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)::integer as total_serv2,
     	       split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)::integer as first_serv1,
     	       split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)::integer as first_serv2,
     	       split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)::integer as first_serv_won1,
     	       split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)::integer as first_serv_won2,
     	       split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)::integer as second_serv_won1,
     	       split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)::integer as second_serv_won2,
     	       split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)::integer as total_break_points1,
     	       split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)::integer as total_break_points2,
     	       split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)::integer as break_points_won1,
     	       split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)::integer as break_points_won2,
     	       m0.ctstatdble1 as double_fault1,
     	       m0.ctstatdble2 as double_fault2,
     	       m0.ctstataces1 as aces1,
     	       m0.ctstataces2 as aces2,
     	       m0.ctstatsource as source,
     	       m0.ctcreatedon as createdon,
     	       m0.ctupdatedon as updatedon
     	from ctennismatchesstats as m0
     	left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
     	left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
     	left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
     	left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
     	left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
     	where m0.ctstatsource in('http://www.tennislive.net')
     	  and m0.ctstartdate>=coalesce(null,(select min(ctstartdate) from ctennismatchesstats))
     	  and m0.ctenddate<=coalesce(null,(select max(ctenddate) from ctennismatchesstats))
     	  and p1.ctgender='M'
     	  and p2.ctgender='M'	
     	  and (p1.ctname='Rafael, Nadal' or p2.ctname='Rafael, Nadal')		     	
     )
     select o0.startdate,o0.enddate,o0.player1,o0.player2,o0.tournament,o0.round,o0.score,o0.source from official_source as o0
     union all
     select a0.startdate,a0.enddate,a0.player1,a0.player2,a0.tournament,a0.round,a0.score,a0.source from alternative_source as a0
     left outer join official_source as o0 
        on ((a0.player1=o0.player1 and a0.player2=o0.player2 and a0.score=o0.score)
        or (a0.player1=o0.player2 and a0.player2=o0.player1 and a0.score=fn_rscore(o0.score)))
       and (a0.startdate>=o0.startdate and a0.enddate<=o0.enddate)
     where o0.player1 is null 
       and o0.player2 is null
       and o0.score is null
       and a0.tournament not in ('Hopman Cup','ATP Cup','Laver Cup');
*/

/*
       select m0.ctstartdate as startdate,
               m0.ctenddate as enddate,
               replace(p1.ctname,',','') as player1,
               replace(p2.ctname,',','') as player2,
               t0.ctname as tournament,
               s0.ctlabel as surface,
               r0.ctlabel as round,
               m0.ctscore as score*//*,
               split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)::integer as total_serv1,
               split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)::integer as total_serv2,
               split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)::integer as first_serv1,
               split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)::integer as first_serv2,
               split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)::integer as first_serv_won1,
               split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)::integer as first_serv_won2,
               split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)::integer as second_serv_won1,
               split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)::integer as second_serv_won2,
               split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)::integer as total_break_points1,
               split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)::integer as total_break_points2,
               split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)::integer as break_points_won1,
               split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)::integer as break_points_won2,
               m0.ctstatdble1 as double_fault1,
               m0.ctstatdble2 as double_fault2,
               m0.ctstataces1 as aces1,
               m0.ctstataces2 as aces2,
               m0.ctstatsource as source,
               m0.ctcreatedon as createdon,
               m0.ctupdatedon as updatedon*/
/*        from ctennismatchesstats as m0
        left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
        left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
        left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
        left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
        left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
        where m0.ctstatsource in('https://www.atptour.com')
          and m0.ctstartdate>=coalesce(null,(select min(ctstartdate) from ctennismatchesstats))
          and m0.ctenddate<=coalesce(null,(select max(ctenddate) from ctennismatchesstats))
          and p1.ctgender='M'
          and p2.ctgender='M'   
          and (p1.ctname='Stefanos, Tsitsipas' or p2.ctname='Stefanos, Tsitsipas')
          order by m0.ctstartdate;*/

/*
  delete from ctennismatchesstats where ctstatsource in('http://www.tennislive.net');
   

    update ctennisplayers
      set ctname='Tatjana, Maria'
    where ctcode='310440-tatjana-maria';

    update ctennisplayers
      set ctname='Daria, Mishina'
    where ctcode='316480-daria-mishina';
*/

/*
select 'update ctennisplayers set ctname='''||split_part(ctname,'-',2)||', '||split_part(ctname,'-',3)||''' where ctcode='''||ctcode||''';' as script
from ctennisplayers
where ctgender='W' and ctname=ctcode;
*/

/*
truncate ctennismatchesstats;
truncate ctennismappingplayers;
truncate ctennismappingtournaments;
truncate ctennisplayers cascade;
truncate ctennistournaments cascade;
*/
/*
with official_source as
     (
      select m0.ctstartdate as startdate,
             m0.ctenddate as enddate,
             replace(p1.ctname,',','')::varchar(200) as player1,
             replace(p2.ctname,',','')::varchar(200) as player2,
             t0.ctname as tournament,
             c0.ctcode as tournament_category,
             s0.ctlabel as surface,
             r0.ctlabel as round,
             m0.ctscore as score,
             fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
             fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
             fn_get_score(m0.ctscore,1,'g') as player_games_won1,
             fn_get_score(m0.ctscore,2,'g') as player_games_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
             m0.ctstatdble1 as double_fault1,
             m0.ctstatdble2 as double_fault2,
             m0.ctstataces1 as aces1,
             m0.ctstataces2 as aces2,
             m0.ctstatsource as source,
             m0.ctcreatedon as createdon,
             m0.ctupdatedon as updatedon
      from ctennismatchesstats as m0
      left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
      left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
      left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
      left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
      left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
      left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
      where m0.ctstatsource in('https://www.atptour.com','https://www.wtatennis.com')
        and m0.ctstartdate>='2010-01-01' --coalesce(_ctstartdate,_min_date)
        and m0.ctenddate<='2020-03-31' --coalesce(_ctenddate,_max_date)
        and p1.ctgender='M' --_ctgender
        and p2.ctgender='M' --_ctgender
        and m0.ctscore not like '%[]%'
     ),
     alternative_source as
     (
      select case when r0.ctlabel like '%Qualifying%' then 
              case when c0.ctcode='grandslam' then (m0.ctstartdate+interval '13 day') else (m0.ctstartdate+interval '2 day') end 
            else m0.ctstartdate 
           end::date as startdate,
             case when r0.ctlabel like '%Qualifying%' then 
                  case when c0.ctcode='grandslam' then (m0.ctenddate+interval '13 day') else  (m0.ctenddate+interval '2 day') end 
                else m0.ctenddate 
             end::date as enddate,
             replace(p1.ctname,',','')::varchar(200) as player1,
             replace(p2.ctname,',','')::varchar(200) as player2,
             t0.ctname as tournament,
             c0.ctcode as tournament_category,
             s0.ctlabel as surface,
             r0.ctlabel as round,
             m0.ctscore as score,
             fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
             fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
             fn_get_score(m0.ctscore,1,'g') as player_games_won1,
             fn_get_score(m0.ctscore,2,'g') as player_games_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
             m0.ctstatdble1 as double_fault1,
             m0.ctstatdble2 as double_fault2,
             m0.ctstataces1 as aces1,
             m0.ctstataces2 as aces2,
             m0.ctstatsource as source,
             m0.ctcreatedon as createdon,
             m0.ctupdatedon as updatedon
      from ctennismatchesstats as m0
      left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
      left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
      left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
      left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
      left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
      left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
      where m0.ctstatsource in('http://www.tennislive.net')
        and m0.ctstartdate>='2010-01-01' --coalesce(_ctstartdate,_min_date)
        and m0.ctenddate<='2020-03-31' --coalesce(_ctenddate,_max_date)
        and p1.ctgender='M' --_ctgender
        and p2.ctgender='M' --_ctgender 
        and m0.ctscore not like '%retired%'
        and m0.ctscore not like '%walk%'    
     )
     select o0.* from official_source as o0
     union all
     select a0.* from alternative_source as a0
     left outer join official_source as o0 
        on ((a0.player1=o0.player1 and a0.player2=o0.player2 and a0.score=o0.score)
        or (a0.player1=o0.player2 and a0.player2=o0.player1 and a0.score=fn_rscore(o0.score)))
       and ((a0.startdate>=o0.startdate and a0.enddate<=o0.enddate)
        or (a0.startdate+interval '1 day'>=o0.startdate and a0.enddate+interval '1 day'<=o0.enddate)
        or (a0.startdate+interval '-1 day'>=o0.startdate and a0.enddate+interval '-1 day'<=o0.enddate))
     where o0.player1 is null 
       and o0.player2 is null
       and o0.score is null
       and a0.tournament not in ('ATP Cup','Laver Cup')
       and a0.tournament not like '%WTA Elite Trophy%'
       and a0.tournament not like '%WTA Finals%'
       and a0.tournament not like '%ATP Finals%';
*/

/*
      select m0.ctstartdate as startdate,
             m0.ctenddate as enddate,
             replace(p1.ctname,',','')::varchar(200) as player1,
             replace(p2.ctname,',','')::varchar(200) as player2,
             t0.ctname as tournament,
             c0.ctcode as tournament_category,
             s0.ctlabel as surface,
             r0.ctlabel as round,
             m0.ctscore as score,
             fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
             fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
             fn_get_score(m0.ctscore,1,'g') as player_games_won1,
             fn_get_score(m0.ctscore,2,'g') as player_games_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
             fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
             m0.ctstatdble1 as double_fault1,
             m0.ctstatdble2 as double_fault2,
             m0.ctstataces1 as aces1,
             m0.ctstataces2 as aces2,
             m0.ctstatsource as source,
             m0.ctcreatedon as createdon,
             m0.ctupdatedon as updatedon
      from ctennismatchesstats as m0
      left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
      left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
      left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
      left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
      left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
      left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
      where m0.ctstatsource in('http://www.tennislive.net')--in('https://www.atptour.com','https://www.wtatennis.com')
        and m0.ctstartdate>='2010-01-01' --coalesce(_ctstartdate,_min_date)
        and m0.ctenddate<='2020-03-31' --coalesce(_ctenddate,_max_date)
        and p1.ctgender='M' --_ctgender
        and p2.ctgender='M' --_ctgender
        and m0.ctscore not like '%[]%'
        and m0.ctscore not like '%retired%'
        and m0.ctscore not like '%walk%'  ;
*/
/*
select m0.ctstartdate as startdate,
       m0.ctenddate as enddate,
       replace(p1.ctname,',','')::varchar(200) as player1,
       replace(p2.ctname,',','')::varchar(200) as player2,
       t0.ctname as tournament,
       c0.ctcode as tournament_category,
       s0.ctlabel as surface,
       r0.ctlabel as round,
       m0.ctscore as score,
       fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
       fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
       fn_get_score(m0.ctscore,1,'g') as player_games_won1,
       fn_get_score(m0.ctscore,2,'g') as player_games_won2,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
       m0.ctstatdble1 as double_fault1,
       m0.ctstatdble2 as double_fault2,
       m0.ctstataces1 as aces1,
       m0.ctstataces2 as aces2,
       m0.ctstatsource as source,
       m0.ctcreatedon as createdon,
       m0.ctupdatedon as updatedon
into temp table official_source
from ctennismatchesstats as m0
left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
where m0.ctstatsource in('https://www.atptour.com','https://www.wtatennis.com')
  and m0.ctstartdate>='2010-01-01' --coalesce(_ctstartdate,_min_date)
  and m0.ctenddate<='2020-03-31' --coalesce(_ctenddate,_max_date)
  and p1.ctgender='M' --_ctgender
  and p2.ctgender='M' --_ctgender
  and m0.ctscore not like '%[]%';


select case when r0.ctlabel like '%Qualifying%' then 
        case when c0.ctcode='grandslam' then (m0.ctstartdate+interval '13 day') else (m0.ctstartdate+interval '2 day') end 
      else m0.ctstartdate 
     end::date as startdate,
       case when r0.ctlabel like '%Qualifying%' then 
            case when c0.ctcode='grandslam' then (m0.ctenddate+interval '13 day') else  (m0.ctenddate+interval '2 day') end 
          else m0.ctenddate 
       end::date as enddate,
       replace(p1.ctname,',','')::varchar(200) as player1,
       replace(p2.ctname,',','')::varchar(200) as player2,
       t0.ctname as tournament,
       c0.ctcode as tournament_category,
       s0.ctlabel as surface,
       r0.ctlabel as round,
       m0.ctscore as score,
       fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
       fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
       fn_get_score(m0.ctscore,1,'g') as player_games_won1,
       fn_get_score(m0.ctscore,2,'g') as player_games_won2,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
       fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
       fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
       fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
       m0.ctstatdble1 as double_fault1,
       m0.ctstatdble2 as double_fault2,
       m0.ctstataces1 as aces1,
       m0.ctstataces2 as aces2,
       m0.ctstatsource as source,
       m0.ctcreatedon as createdon,
       m0.ctupdatedon as updatedon
into temp table alternative_source
from ctennismatchesstats as m0
left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
where m0.ctstatsource in('http://www.tennislive.net')
  and m0.ctstartdate>='2010-01-01' --coalesce(_ctstartdate,_min_date)
  and m0.ctenddate<='2020-03-31' --coalesce(_ctenddate,_max_date)
  and p1.ctgender='M' --_ctgender
  and p2.ctgender='M' --_ctgender 
  and m0.ctscore not like '%retired%'
  and m0.ctscore not like '%walk%';


select o0.* from official_source as o0
union all
select a0.* from alternative_source as a0
left outer join official_source as o0 
  on ((a0.player1=o0.player1 and a0.player2=o0.player2 and a0.score=o0.score)
  or (a0.player1=o0.player2 and a0.player2=o0.player1 and a0.score=fn_rscore(o0.score)))
 and ((a0.startdate>=o0.startdate and a0.enddate<=o0.enddate)
  or (a0.startdate+interval '1 day'>=o0.startdate and a0.enddate+interval '1 day'<=o0.enddate)
  or (a0.startdate+interval '-1 day'>=o0.startdate and a0.enddate+interval '-1 day'<=o0.enddate))
where o0.player1 is null 
 and o0.player2 is null
 and o0.score is null
 and a0.tournament not in ('ATP Cup','Laver Cup')
 and a0.tournament not like '%WTA Elite Trophy%'
 and a0.tournament not like '%WTA Finals%'
 and a0.tournament not like '%ATP Finals%';
 */


-- select sp_feed_ctcleanmstats(_ctstartdate:='2011-12-31',_ctenddate:='2013-12-31');
 
/*
      select
        official_source.startdate,
        official_source.enddate,
        official_source.player1,
        official_source.player2,
        official_source.tournament,
        official_source.tournament_category,
        official_source.surface,
        official_source.round,
        official_source.score,
        official_source.player_sets_won1,
        official_source.player_sets_won2,
        official_source.player_games_won1,
        official_source.player_games_won2,
        official_source.total_serv1,
        official_source.total_serv2,       
        official_source.first_serv1,
        official_source.first_serv2,
        official_source.first_serv_won1,
        official_source.first_serv_won2,
        official_source.second_serv_won1,
        official_source.second_serv_won2,
        official_source.total_break_points1,
        official_source.total_break_points2,
        official_source.break_points_won1,
        official_source.break_points_won2,
        official_source.double_fault1,
        official_source.double_fault2,
        official_source.aces1,
        official_source.aces2,
        official_source.source,
        official_source.createdon,
        current_timestamp
      from
      (
        select m0.ctstartdate as startdate,
               m0.ctenddate as enddate,
               replace(p1.ctname,',','')::varchar(200) as player1,
               replace(p2.ctname,',','')::varchar(200) as player2,
               t0.ctname as tournament,
               c0.ctcode as tournament_category,
               s0.ctlabel as surface,
               r0.ctlabel as round,
               m0.ctscore as score,
               fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
               fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
               fn_get_score(m0.ctscore,1,'g') as player_games_won1,
               fn_get_score(m0.ctscore,2,'g') as player_games_won2,
               fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
               fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
               fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
               fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
               fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
               fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
               fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
               fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
               fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
               fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
               fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
               fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
               m0.ctstatdble1 as double_fault1,
               m0.ctstatdble2 as double_fault2,
               m0.ctstataces1 as aces1,
               m0.ctstataces2 as aces2,
               m0.ctstatsource as source,
               m0.ctcreatedon as createdon
        from ctennismatchesstats as m0
        left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
        left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
        left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
        left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
        left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
        left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
        where m0.ctstatsource in('https://www.atptour.com','https://www.wtatennis.com')
          and m0.ctstartdate>='2010-01-01'
          and m0.ctenddate<='2020-03-31'
          and p1.ctgender='M'
          and p2.ctgender='M'
          and m0.ctscore not like '%[]%'
      ) as official_source
      left outer join ctenniscleanstats as cleanstats
        on  cleanstats.startdate=official_source.startdate
        and cleanstats.enddate=official_source.enddate
        and cleanstats.player1=official_source.player1
        and cleanstats.player2=official_source.player2
        and cleanstats.round=official_source.round
        and cleanstats.score=official_source.score
      where cleanstats.startdate is null
        and cleanstats.enddate is null
        and cleanstats.player1 is null
        and cleanstats.player2 is null
        and cleanstats.round is null
        and cleanstats.score is null;
  
       and cleanstats.source in('https://www.atptour.com','https://www.wtatennis.com');
    select off_table.*,row_number() over(order by off_table.ctname) as row_number
    from
    (
      select p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
      from ctennismatchesstats as m0
      left outer join ctennisplayers  as p1 on p1.ctdid=m0.ctplayer1
      left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p1.ctcode
      where mp0.ctcodeofficial is null
        and p1.ctgender='M'
        and m0.ctstatsource in('https://www.atptour.com') 
      union
      select p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
      from ctennismatchesstats as m0
      left outer join ctennisplayers  as p2 on p2.ctdid=m0.ctplayer2
      left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p2.ctcode
      where mp0.ctcodeofficial is null
        and p2.ctgender='M'
        and m0.ctstatsource in('https://www.atptour.com')
    ) as off_table limit 1 offset 1;



select alt_table.*, row_number() over(order by alt_table.ctcode) as row_number
into temp table tmp_alt_table
from 
(
  select p1.ctcode,lower(replace(p1.ctname,',','')) as ctname
  from ctennismatchesstats as m0
  left outer join ctennisplayers  as p1 on p1.ctdid=m0.ctplayer1
  left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p1.ctcode
  where mp0.ctcodeofficial is null
    and p1.ctgender='M'
    and m0.ctstatsource in('http://www.tennislive.net') 
  union
  select p2.ctcode,lower(replace(p2.ctname,',','')) as ctname
  from ctennismatchesstats as m0
  left outer join ctennisplayers  as p2 on p2.ctdid=m0.ctplayer2
  left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p2.ctcode
  where mp0.ctcodeofficial is null
    and p2.ctgender='M'
    and m0.ctstatsource in('http://www.tennislive.net')
) as alt_table;


select m0.ctstartdate,lower(replace(p1.ctname,',','')) as ctname1,lower(replace(p2.ctname,',','')) as ctname2,t0.ctname as tournament,m0.ctscore,m0.ctstatsource
from ctennismatchesstats as m0
left outer join ctennisplayers  as p1 on p1.ctdid=m0.ctplayer1
left outer join ctennisplayers  as p2 on p2.ctdid=m0.ctplayer2
left outer join ctennismappingplayers as mp0 on mp0.ctcodeofficial=p1.ctcode
left outer join ctennismappingplayers as mp1 on mp1.ctcodeofficial=p2.ctcode
left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
where (mp0.ctcodeofficial is null or mp1.ctcodeofficial is null)
and p1.ctgender='M'
and m0.ctstatsource in('https://www.atptour.com','http://www.tennislive.net') 
and (p1.ctcode in('agustin-suarez-gonzalez-sy25','agustin-gonzalez-ge22') or p2.ctcode in('agustin-suarez-gonzalez-sy25','agustin-gonzalez-ge22'));
and (p1.ctcode in (select ctcode from tmp_alt_table) or p2.ctcode in (select ctcode from tmp_alt_table));


select rd.*
from cttournamentrounds as rd
left outer join ctennismappingrounds as mr1 on mr1.ctcodeofficial=rd.ctcodeofficial
where lower(rd.ctcodeofficial)=lower(_ctround) or lower(mr1.ctcode)=lower(_ctround);
*/
/*
select alternative_source.*
from
(select case when r0.ctlabel like '%Qualifying%' then 
                      case when c0.ctcode='grandslam' then (m0.ctstartdate+interval '13 day') else (m0.ctstartdate+interval '2 day') end 
                    else m0.ctstartdate 
                  end::date as startdate,
                  case when r0.ctlabel like '%Qualifying%' then 
                        case when c0.ctcode='grandslam' then (m0.ctenddate+interval '13 day') else  (m0.ctenddate+interval '2 day') end 
                      else m0.ctenddate 
                  end::date as enddate,
                  replace(p11.ctname,',','')::varchar(200) as player1,
                  replace(p22.ctname,',','')::varchar(200) as player2,
                  t0.ctname as tournament,
                  c0.ctcode as tournament_category,
                  s0.ctlabel as surface,
                  r0.ctlabel as round,
                  m0.ctscore as score,
                  fn_get_score(m0.ctscore,1,'s') as player_sets_won1,
                  fn_get_score(m0.ctscore,2,'s') as player_sets_won2,
                  fn_get_score(m0.ctscore,1,'g') as player_games_won1,
                  fn_get_score(m0.ctscore,2,'g') as player_games_won2,
                  fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',2)) as total_serv1,
                  fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',2)) as total_serv2,
                  fn_into_integer(split_part(replace(replace(m0.ctstat1stserv1,'(',''),')',''),'/',1)) as first_serv1,
                  fn_into_integer(split_part(replace(replace(m0.ctstat1stserv2,'(',''),')',''),'/',1)) as first_serv2,
                  fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon1,'(',''),')',''),'/',1)) as first_serv_won1,
                  fn_into_integer(split_part(replace(replace(m0.ctstat1stservwon2,'(',''),')',''),'/',1)) as first_serv_won2,
                  fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon1,'(',''),')',''),'/',1)) as second_serv_won1,
                  fn_into_integer(split_part(replace(replace(m0.ctstat2ndservwon2,'(',''),')',''),'/',1)) as second_serv_won2,
                  fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',2)) as total_break_points1,
                  fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',2)) as total_break_points2,
                  fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon1,'(',''),')',''),'/',1)) as break_points_won1,
                  fn_into_integer(split_part(replace(replace(m0.ctstatbrkwon2,'(',''),')',''),'/',1)) as break_points_won2,
                  m0.ctstatdble1 as double_fault1,
                  m0.ctstatdble2 as double_fault2,
                  m0.ctstataces1 as aces1,
                  m0.ctstataces2 as aces2,
                  m0.ctstatsource as source,
                  m0.ctcreatedon as createdon
                  from ctennismatchesstats as m0
                  left outer join ctennisplayers as p1 on p1.ctdid=m0.ctplayer1
                  left outer join ctennisplayers as p2 on p2.ctdid=m0.ctplayer2
                  left outer join ctennismappingplayers as mp1 on (mp1.ctcode=p1.ctcode or mp1.ctcodeofficial=p1.ctcode)
                  left outer join ctennismappingplayers as mp2 on (mp2.ctcode=p2.ctcode or mp2.ctcodeofficial=p2.ctcode)
                  left outer join ctennisplayers as p11 on p11.ctcode=mp1.ctcodeofficial
                  left outer join ctennisplayers as p22 on p22.ctcode=mp2.ctcodeofficial  
                  left outer join ctennistournaments as t0 on t0.ctcode=m0.cttournament
                  left outer join cttournamentrounds as r0 on r0.ctcode=m0.ctround
                  left outer join ctennissurfaces as s0 on s0.ctcode=m0.ctsurface
                  left outer join ctennistournamentscategories as c0 on c0.ctdid=t0.ctcategory
                  where m0.ctstatsource in('http://www.tennislive.net')
                  and m0.ctstartdate>='2010-05-16'
                  and m0.ctenddate<='2010-06-20'
                  and p1.ctgender='M'
                  and p2.ctgender='M'
                  and (mp1.ctcode is not null or mp1.ctcodeofficial is not null)
                  and (mp2.ctcode is not null or mp2.ctcodeofficial is not null)
                  and m0.ctscore not like '%retired%'
                  and m0.ctscore not like '%walk%'
                  ) as alternative_source
                  left outer join ctenniscleanstats as cleanstats
                  on  cleanstats.startdate=alternative_source.startdate
                  and cleanstats.enddate=alternative_source.enddate
                  and cleanstats.player1=alternative_source.player1
                  and cleanstats.player2=alternative_source.player2
                  and cleanstats.round=alternative_source.round
                  and cleanstats.score=alternative_source.score
                  where cleanstats.startdate is null
                  and cleanstats.enddate is null
                  and cleanstats.player1 is null
                  and cleanstats.player2 is null
                  and cleanstats.round is null
                  and cleanstats.score is null
                  order by alternative_source.startdate;*/


select alternative_source.startdate,alternative_source.player1,alternative_source.player2,alternative_source.tournament,alternative_source.tournament_category,alternative_source.score,alternative_source.source from sp_get_ctmstats('novak-djokovic-d643','2010-01-01') as alternative_source
left outer join  ctenniscleanstats as cleanstats
    on  cleanstats.startdate=alternative_source.startdate
    and cleanstats.enddate=alternative_source.enddate
    and cleanstats.player1=alternative_source.player1
    and cleanstats.player2=alternative_source.player2
    and cleanstats.round=alternative_source.round
    and cleanstats.score=alternative_source.score
where alternative_source.startdate is null
and alternative_source.enddate is null
and alternative_source.player1 is null
and alternative_source.player2 is null
and alternative_source.round is null
and alternative_source.score is null
and (cleanstats.player1='Novak Djokovic' or cleanstats.player2='Novak Djokovic');
/*
select source,count(*) from sp_get_ctmstats('novak-djokovic-d643','2010-01-01') group by source;
select source,count(*) from ctenniscleanstats where (player1='Novak Djokovic' or player2='Novak Djokovic') group by source;*/