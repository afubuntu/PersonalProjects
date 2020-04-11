import requests
import lxml
import bs4
import abc
import datetime
import logging

#######################################################################################################
#                   Abstract class for dealing with tennis data extraction from web sites             #
#######################################################################################################
class AbstractTennisData(abc.metaclass=abc.ABCMeta):
	__dicoHeaders={'player_profile':['player_name', 'player_birth_date', 'player_country_code', 'player_country_label', 'player_height', 
	                                 'player_weight', 'player_begin_pro','player_style_hand', 'player_rank, player_points', 'player_prize_money'
	                                ],
	               'player_match_stats':['match_date', 'tournament_name', 'tournament_surface','tournament_points', 'tournament_round', 'player1_name', 'player2_name', 
	                                     'match_score', 'match_result', 'stat_player1_first_serv', 'stat_player2_first_serv', 'stat_player1_first_serv_pts', 
	                                     'stat_player2_first_serv_pts', 'stat_player1_2nd_serv_pts', 'stat_player2_2nd_serv_pts', 'stat_player1_brk_pts_won', 
	                                     'stat_player2_brk_pts_won', 'stat_player1_return_pts_won', 'stat_player2_return_pts_won', 'stat_player1_dble_faults',
	                                     'stat_player2_dble_faults', 'stat_player1_aces', 'stat_player2_aces'
	                                    ]
	              }
	__dtFormat={'fr-FR':'%d-%m-%Y','en-US':'%Y-%m-%d'}

	def __init__(self,data_range=[],date_range=[],save_filename=''):
		self.data_range=data_range
		self.date_range=date_range
		self.save_filename=save_filename

	@classmethod
	def getDicoHeaders(cls):
		return cls.__dicoHeaders

	@classmethod
	def getdtFormat(cls):
		return cls.__dtFormat	

	@classmethod
	def formatScore(cls,m_score,sets_sep,game_sep,inverse_score=True):
		formatedscore=''
		sets_list=m_score.split(sets_sep)
		for set_el in sets_list:
			cle=set_el.strip()
			sc1=cle.split('(')[0]
			sc2=cle[len(sc1):]
			if inverse_score:
				if game_sep!='':
					gms=sc1.split(game_sep)
					formatedscore=formatedscore+gms[1]+'-'+gms[0]+sc2+','
				elif game_sep=='':
					formatedscore=formatedscore+sc1[1]+'-'+sc1[0]+sc2+','
			else:
				if game_sep!='':
					gms=sc1.split(game_sep)
					formatedscore=formatedscore+gms[0]+'-'+gms[1]+sc2+','
				elif game_sep=='':
					formatedscore=formatedscore+sc1[0]+'-'+sc1[1]+sc2+','	
		return formatedscore[:len(formatedscore)-1]		

	@classmethod
	def extractTennisScore(cls,raw_content,rmbg,rmed):
		fscore=raw_content.replace(rmbg,'').replace(rmed,'').strip()
		return fscore.replace('<sup>','(').replace('</sup>',')')

	@classmethod
	def resetDicoFromTemplate(cls,dico_to_reset,dico_key):
		for ky in cls.getDicoHeaders()[dico_key]:
			dico_to_reset[ky]=''


	@abc.abstractmethod
	def extractData(self):
		pass


#######################################################################################################
#                  Different implementations of the abstract class AbstractTennisData                 #
#######################################################################################################

# Implementation of AbstractTennisData for Single Men using web site : https://www.atptour.com
class smTennisDatafromOfficialSource(AbstractTennisData):
	def extractData(self):
		root_url='https://www.atptour.com'
		try:
			rq_pge=requests.get(root_url)
		except:
			return

		try:
			bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
			atp_rk_menu=bs_t.find_all('div',{'id':'navigation','class':'navigation'})[0].find_all('div',{'id':'subNav','class':'sub-nav'})[0]
			atp_rk_menu=atp_rk_menu.find_all('div',{'class':'nav-section'})[0].find_all('ul',{'class':'nav-list'})[0].find_all('li')
			atp_rk_url=root_url+atp_rk_menu[1].find_all('a',{'data-ga-label':'Singles'})[0]['href']
		except:
			return

		try:
			rq_pge=requests.get(atp_rk_url)
		except:
			return

		try:
			bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
			atp_rk_menu=bs_t.find_all('div',{'id':'mainContent','class':'main-content'})[0]
			atp_rk_menu=atp_rk_menu.find_all('div',{'id':'filterHolder','class':'content-filters content-filter-full-padding'})[0]
			atp_rk_menu=atp_rk_menu.find_all('div',{'class':'dropdown-layout-wrapper'})[0].find_all('div',{'class':'dropdown-wrapper'})[0]
			atp_rk_menu=atp_rk_menu.find_all('div',{'class':'dropdown-holder-wrapper'})
			filter_date=atp_rk_menu[0]
			filter_ranking=atp_rk_menu[1]
			filter_countries=atp_rk_menu[2]

			filter_date=filter_date.find_all('div',{'class':'dropdown-holder'})[0].find_all('ul',{'class':'dropdown','data-value':'rankDate'})
			filter_date=filter_date[0].find_all('li',{'class':'current'})
			filter_date_str=filter_date[0]['data-value']

			filter_ranking=filter_ranking.find_all('div',{'class':'dropdown-holder'})[0].find_all('ul',{'class':'dropdown','data-value':'rankRange'})
			filter_ranking=filter_ranking[0].find_all('li')
			for li in filter_ranking:
				if li.text.strip().lower()=='all rankings':
					filter_ranking_str=li['data-value']
					break

			filter_countries=filter_countries.find_all('div',{'class':'dropdown-holder'})[0].find_all('ul',{'class':'dropdown','data-value':'countryCode'})
			filter_countries=filter_countries[0].find_all('li')
			dico_countries={}
			for li in filter_countries:
				dico_countries[li['data-value']]=li.text.strip()

			for dk,dv in dico_countries.items():
				if dv.lower()=='all countries':
					filter_countries_str=dk
					break
			if filter_countries_str in dico_countries:
				del dico_countries[filter_countries_str]

			filter_str='?rankDate='+filter_date_str+'&rankRange='+filter_ranking_str+'&countryCode='+filter_countries_str				
			atp_rk_url=atp_rk_url+filter_str
		except:
			return

		try:
			rq_pge=requests.get(atp_rk_url)
		except:
			return

		try:
			bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
			atp_rk_menu=bs_t.find_all('div',{'id':'mainContainer','class':'main-container','data-page':'singles'})
			atp_rk_menu=atp_rk_menu[0].find_all('div',{'id':'mainContent','class':'main-content'})
			atp_rk_menu=atp_rk_menu[0].find_all('div',{'id':'singlesRanking','class':'table-rankings','data-filtered-module':'playerRankDetail'})
			atp_rk_menu=atp_rk_menu[0].find_all('div',{'id':'rankingDetailAjaxContainer','class':'table-rankings-wrapper'})
			atp_players_mega_table=atp_rk_menu[0].find_all('table',{'class':'mega-table'})[0].find_all('tr')

			player_rank=0
			dico_pl_profile={}
			dico_match_stats={}
			for tr in atp_players_mega_table:
				tds=tr.find_all('td',{'class':'rank-cell'})	
				if len(tds)>0:
					player_rank=tds[0].text.strip()
					if player_rank[len(player_rank)-1:]=='T':
						player_rank=player_rank[:len(player_rank)-1]
					player_rank=int(player_rank)
					if player_rank>=self.data_range[0] and player_rank<=self.data_range[1]:
						self.resetDicoFromTemplate(dico_pl_profile,'player_profile')
						dico_pl_profile['player_rank']=player_rank
						dico_pl_profile['player_country_code']=tr.find_all('td',{'class':'country-cell'})[0].find_all('img')[0]['alt']
						dico_pl_profile['player_country_label']=dico_countries[dico_pl_profile['player_country_code']]
						dico_pl_profile['player_points']=tr.find_all('td',{'class':'points-cell'})[0].find_all('a')[0].text.strip().replace(',','')
						tds=tr.find_all('td',{'class':'player-cell'})[0].find_all('a')
						dico_pl_profile['player_name']=tds[0].text.strip()
						atp_rk_url=root_url+tds[0]['href']

						rq_pge=requests.get(atp_rk_url)
						bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')

						atp_rk_menu=bs_t.find_all('div',{'id':'mainContent','class':'main-content'})[0].find_all('div',{'id':'playerProfileHero'})
						atp_rk_menu=atp_rk_menu[0].find_all('div',{'class':'player-profile-hero-overflow'})[0].find_all('div',{'class':'player-profile-hero-table'})
						atp_rk_menu=atp_rk_menu[0].find_all('div',{'class':'inner-wrap'})[0].find_all('table')[0].find_all('tr')
						
						atp_rk_menu1=atp_rk_menu[0].find_all('td')
						td0_birth=atp_rk_menu1[0]
						td1_pro_bg=atp_rk_menu1[1]
						td2_weight=atp_rk_menu1[2]
						td3_height=atp_rk_menu1[3]

						td0_birth=td0_birth.find_all('div',{'class':'wrap'})[0].find_all('div',{'class':'table-big-value'})[0].find_all('span',{'class':'table-birthday'})
						if len(td0_birth)>0:
							dico_pl_profile['player_birth_date']=datetime.datetime.strptime(td0_birth[0].strip().replace('(','').replace(')','').replace('.','-'),self.getdtFormat()['en-US'])

						td1_pro_bg=td1_pro_bg.find_all('div',{'class':'wrap'})[0].find_all('div',{'class':'table-big-value'})[0]
						dico_pl_profile['player_begin_pro']=td1_pro_bg.text.strip()

						td2_weight=td2_weight.find_all('div',{'class':'wrap'})[0].find_all('div',{'class':'table-big-value'})[0]
						weight_lbs=td2_weight.find_all('span',{'class':'table-weight-lbs'})
						weight_kg=td2_weight.find_all('span',{'class':'table-weight-kg-wrapper'})
						weight_label=''
						if len(weight_lbs)>0:
							weight_label=weight_label+weight_lbs[0].text.strip()+'lbs'
						if len(weight_kg)>0:
							if weight_label=='':
								weight_label=weight_label+weight_kg[0].text.strip().repalce('(','').replace(')','')
							else:
								weight_label=weight_label+';'+weight_kg[0].text.strip().repalce('(','').replace(')','')
						dico_pl_profile['player_weight']=weight_label

						td3_height=td3_height.find_all('div',{'class':'wrap'})[0].find_all('div',{'class':'table-big-value'})[0]
						height_ft=td3_height.find_all('span',{'class':'table-height-ft'})
						height_cm=td3_height.find_all('span',{'class':'table-height-cm-wrapper'})
						height_label=''
						if len(height_ft)>0:
							height_label=height_label+height_ft[0].text.strip()
						if len(height_cm)>0:
							if height_label=='':
								height_label=height_label+height_cm[0].text.strip().repalce('(','').replace(')','')
							else:
								height_label=height_label+';'+height_cm[0].text.strip().repalce('(','').replace(')','')
						dico_pl_profile['player_height']=height_label

						atp_rk_menu2=atp_rk_menu[1].find_all('td')
						td2_player_style=atp_rk_menu2[2]
						td2_player_style=td2_player_style.find_all('div',{'class':'wrap'})[0].find_all('div',{'class':'table-value'})[0]
						dico_pl_profile['player_style_hand']=td2_player_style.text.strip()

						atp_rk_menu=bs_t.find_all('div',{'id':'mainContent','class':'main-content'})
						atp_rk_menu=atp_rk_menu[0].find_all('div',{'data-filtered-module':'playerProfileTabs','class':'profile-tabs-wrapper'})
						atp_rk_menu=atp_rk_menu[0].find_all('ul',{'class':'profile-tabs'})[0].find_all('li',{'class':'profile-tab'})

						atp_rk_url=root_url+atp_rk_menu[1].find_all('a')[0]['href']
						rq_pge=requests.get(atp_rk_url)
						bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')

						atp_rk_menu=bs_t.find_all('div',{'id':'playerActivityFilterHolder','class':'content-filters .playerActivityFilterHolder'})
						atp_rk_menu=atp_rk_menu[0].find_all('div',{'class':'dropdown-layout-wrapper'})
						atp_rk_menu=atp_rk_menu[0].find_all('div',{'class':'dropdown-wrapper'})
						atp_rk_menu=atp_rk_menu[0].find_all('div',{'class':'dropdown-holder-wrapper'})
						atp_rk_menu=atp_rk_menu[0].find_all('ul',{'id':'yearDropdown','class':'dropdown','data-value':'year'})
						atp_rk_menu=atp_rk_menu[0].find_all('li')

						atp_dico_years={}
						for lii in atp_rk_menu:
							atp_dico_years[lii['data-value']]=lii['data-value']

						if 'all' in atp_dico_years:
							del atp_dico_years['all']

						for kyr in atp_dico_years:
							yrdate=datetime.datetime.strptime(kyr+'01-01',self.getdtFormat()['en-US'])
							if yrdate>=self.date_range[0] and yrdate<=self.date_range[1]:
								atp_match_url=atp_rk_url+'?year='+kyr+'&matchType=Singles'
								rq_pge=requests.get(atp_match_url)



		except:
			return


# Implementation of AbstractTennisData for Single Men using web site : http://www.tennisendirect.net
class smTennisDatafromTEDSource(AbstractTennisData):
	def extractData(self):
		try:
			rq_pge=requests.get("http://www.tennisendirect.net")
		except:
			# Must write in log file and exit
			return
			
		bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
		atp_menu=bs_t.find_all('div',{'id':'left_column'})

		try:
			atp_menu=atp_menu[0].find_all('div',{'class':'tour_box'})
			for t in atp_menu:
				if t.h2.text.strip()=='CLASSEMENTS DE TENNIS':
					atp_rk=t
					break

			atp_menu=atp_rk.ul.find_all('li',{'class':'menu_main'})
			for t in atp_menu:
				atp_rk=t.find_all('a',{'title':'Classement ATP'})
				if len(atp_rk)>0:
					atp_rk_url=atp_rk[0]['href']
					break

		except:
			return

		try:
			rq_pge=requests.get(atp_rk_url)
		except:
			return

		try:
			bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
			atp_menu=bs_t.find_all('div',{'class':'rank_block'})
			dico_countries={}

			for t in atp_menu:
				if t.h2.span.text.upper()=='CLASSEMENT ATP':
					atp_pl_t=t
				if t.h2.span.text.upper()=='JOUEURS DU PAYS:':
					atp_pl_c=t

			# Extraction of countries list
			atp_menu=atp_pl_c.form.fieldset.select.find_all('option')
			for t in atp_menu:
				dico_countries[t['value']]=t.text.strip()

			atp_menu=atp_pl_t.div.find('table',{'class':'table_pranks'}).find_all('tr')
		except:
			return
			
		# Beginning the main loop for extracting players data
		# First, we'll extract the player info in the dictionary dico_pl_profile
		# Second, we'll extract match stats for a player
		# dico_pl_profile={}
		# dico_match_stats={}

		dico_pl_profile={}
		dico_match_stats={}
		for t in atp_menu:
			valid_link=False
			self.resetDicoFromTemplate(dico_pl_profile,'player_profile')
			try:
				atp_rk=t.find_all('td')
				for td in atp_rk:
					if td['class']=='w20':
						pl_rk=td.text[:len(td.text)-1]
						atp_pl_t=td.find_all('a')
						if len(atp_pl_t)>0:
							atp_rk_url=atp_pl_t[0]['href']
							country_code=atp_pl_t[0].text.split('(')[1]
							country_code=country_code[:len(country_code)-1]
							pl_rk=int(pl_rk)
							valid_link=True
							break
			except:
				return

			if valid_link and pl_rk>=self.data_range[0] and pl_rk<=self.data_range[1]:
				try:
					rq_pge=requests.get(atp_rk_url)
				except:
					return

				try:
					bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
					atp_rk=bs_t.find_all('div',{'class':'player_info'})
					for ti in atp_rk:
						atp_rk1=ti.find_all('div',{'class':'player_stats'})
						if len(atp_rk1)>0:
							break

					# Player info
					profile_t=atp_rk1[0].text.strip().split(' '*7)
					for nm in profile_t:
						nmt=nm.split(':')
						nmt0=nmt[0].strip().lower()
						nmt1=nmt[1].strip().lower()
						if(nmt0=='nom'):
							dico_pl_profile['player_name']=nmt1
						elif(nmt0=='pays'):
							dico_pl_profile['player_country_label']=nmt1
							dico_pl_profile['player_country_code']=country_code
						elif(nmt0=='date de naissance'):
							dico_pl_profile['player_birth_date']=nmt1.split(',')[0].replace('.','-')
						elif(nmt0=='taille'):
							dico_pl_profile['player_height']=nmt1
						elif(nmt0=='poids'):
							dico_pl_profile['player_weight']=nmt1
						elif(nmt0==u'début pro'):
							dico_pl_profile['player_begin_pro']=nmt1
						elif(nmt0=='points'):
							dico_pl_profile['player_points']=nmt1
						elif(nmt0=='primes'):
							dico_pl_profile['player_prize_money']=nmt1
						else:
							nmt00=nmt0.split('classement atp')
							if len(nmt00)>1:
								dico_pl_profile['player_rank']=nmt1
								dico_pl_profile['player_style_hand']=nmt00[0].strip()
				except:
					return

				# Player matches stats
				atp_rk=bs_t.find_all('div',{'class':'player_match_info'})
				atp_rk=atp_rk[0].find_all('table',{'class':'table_stats'})[0].find_all('tr')
				for ti in atp_rk:
					valid_year=False
					try:
						td=ti.find_all('td')
						for tdi in td:
							tda=tdi.find_all('a')
							if len(tda)>0:
								if tda[0]['title'][:5]==u'année':
									stats_year=tda[0].text.strip()
									stats_date=datetime.datetime.strptime('01-01-'+stats_year,self.getdtFormat()['fr-FR'])
									stats_yurl=atp_rk_url+tda[0]['href']
									valid_year=True
									break
					except:
						return

					if valid_year and stats_date>=self.date_range[0] and stats_date<=self.date_range[1]:
						try:
							rq_pge1=requests.get(stats_yurl)
						except:
							return

						bs_t1=bs4.BeautifulSoup(rq_pge1.text,'lxml')
						matches_in_range=bs_t1.find_all('div',{'class':'player_matches'})
						if len(matches_in_range)>1:
							matches_in_range=matches_in_range[1].find_all('table',{'class':'table_pmatches'})
						elif len(matches_in_range)==1:
							matches_in_range=matches_in_range[0].find_all('table',{'class':'table_pmatches'})
						matches_in_range=matches_in_range[0].find_all('tr')
						tourn_name=''
						tourn_surf=''
						for tdi in matches_in_range:
							self.resetDicoFromTemplate(dico_match_stats,'player_match_stats')
							if tdi['class']=='tour_head_pair':
								tourn_name=tdi.find_all('td',{'class':'w200'})[0].find_all('a')[0]['title']
								tourn_surf=tdi.find_all('td',{'class':'w40 surf_1'})[0].text.strip()
							tds=tdi.find_all('td')
							stats_d1=tds[0].text.strip().repalce('.','-')
							stats_d1=datetime.datetime.strptime(stats_d1[:len(stats_d1)-3]+'-'+stats_year,self.getdtFormat()['fr-FR'])
							if stats_d1>=self.date_range[0] and stats_d1<=self.date_range[1]:
								dico_match_stats['match_date']=stats_d1
								dico_match_stats['tournament_name']=tourn_name
								dico_match_stats['tournament_surface']=tourn_surf
								dico_match_stats['tournament_round']=tds[1].text.strip()
								dico_match_stats['match_result']=tds[5].find_all('img')[0]['alt']
								if dico_match_stats['match_result']==u'victoire':
									dico_match_stats['player1_name']=tds[2].text.strip()
									dico_match_stats['player2_name']=tds[3].find_all('a')[0].text.strip()
									dico_match_stats['match_score']=self.extractTennisScore(str(tds[4]),'<td class="w130">','</td>')
								elif dico_match_stats['match_result']==u'défaite':
									dico_match_stats['player1_name']=tds[3].text.strip()
									dico_match_stats['player2_name']=tds[2].find_all('a')[0].text.strip()
									dico_match_stats['match_score']=self.formatScore(self.extractTennisScore(str(tds[4]),'<td class="w130">','</td>'),',','-')

								# Match stats
								try:
									rq_pge2=requests.get(tds[6].find_all('a')[0]['href'])
								except:
									return

								bs_t2=bs4.BeautifulSoup(rq_pge2.text,'lxml')
								tstats_m=bs_t2.find_all('div',{'class':'player_matches'})[0].find_all('table',{'class':'table_stats_match'})[0].find_all('tr')
								if dico_match_stats['match_result']==u'victoire':
									for ti in tstats_m:
										tid=ti.find_all('td')
										if tid[0].text.strip().lower()==u'premier service en pourcentage':
											dico_match_stats['stat_player1_first_serv']=tid[1].text.strip()
											dico_match_stats['stat_player2_first_serv']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 1er service':
											dico_match_stats['stat_player1_first_serv_pts']=tid[1].text.strip()
											dico_match_stats['stat_player2_first_serv_pts']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 2e service':
											dico_match_stats['stat_player1_2nd_serv_pts']=tid[1].text.strip()
											dico_match_stats['stat_player2_2nd_serv_pts']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'balles de break gagnées':
											dico_match_stats['stat_player1_brk_pts_won']=tid[1].text.strip()
											dico_match_stats['stat_player2_brk_pts_won']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur retour':
											dico_match_stats['stat_player1_return_pts_won']=tid[1].text.strip()
											dico_match_stats['stat_player2_return_pts_won']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'double fautes':
											dico_match_stats['stat_player1_dble_faults']=tid[1].text.strip()
											dico_match_stats['stat_player2_dble_faults']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'aces':
											dico_match_stats['stat_player1_aces']=tid[1].text.strip()
											dico_match_stats['stat_player2_aces']=tid[2].text.strip()

								elif dico_match_stats['match_result']==u'défaite':
									for ti in tstats_m:
										tid=ti.find_all('td')
										if tid[0].text.strip().lower()==u'premier service en pourcentage':
											dico_match_stats['stat_player1_first_serv']=tid[2].text.strip()
											dico_match_stats['stat_player2_first_serv']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 1er service':
											dico_match_stats['stat_player1_first_serv_pts']=tid[2].text.strip()
											dico_match_stats['stat_player2_first_serv_pts']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 2e service':
											dico_match_stats['stat_player1_2nd_serv_pts']=tid[2].text.strip()
											dico_match_stats['stat_player2_2nd_serv_pts']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'balles de break gagnées':
											dico_match_stats['stat_player1_brk_pts_won']=tid[2].text.strip()
											dico_match_stats['stat_player2_brk_pts_won']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur retour':
											dico_match_stats['stat_player1_return_pts_won']=tid[2].text.strip()
											dico_match_stats['stat_player2_return_pts_won']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'double fautes':
											dico_match_stats['stat_player1_dble_faults']=tid[2].text.strip()
											dico_match_stats['stat_player2_dble_faults']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'aces':
											dico_match_stats['stat_player1_aces']=tid[2].text.strip()
											dico_match_stats['stat_player2_aces']=tid[1].text.strip()



# Implementation of AbstractTennisData for Single Women using web site : https://www.wtatennis.com
class swTennisDatafromOfficialSource(AbstractTennisData):
	def extractData(self):
		pass


# Implementation of AbstractTennisData for Single Women using web site : http://www.tennisendirect.net
class swTennisDatafromTEDSource(AbstractTennisData):
	def extractData(self):
		try:
			rq_pge=requests.get("http://www.tennisendirect.net")
		except:
			# Must write in log file and exit
			return
			
		bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
		wta_menu=bs_t.find_all('div',{'id':'left_column'})

		try:
			wta_menu=wta_menu[0].find_all('div',{'class':'tour_box'})
			for t in wta_menu:
				if t.h2.text.strip()=='CLASSEMENTS DE TENNIS':
					wta_rk=t
					break

			wta_menu=wta_rk.ul.find_all('li',{'class':'menu_main'})
			for t in wta_menu:
				wta_rk=t.find_all('a',{'title':'Classement WTA'})
				if len(wta_rk)>0:
					wta_rk_url=wta_rk[0]['href']
					break

		except:
			return

		try:
			rq_pge=requests.get(wta_rk_url)
		except:
			return

		try:
			bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
			wta_menu=bs_t.find_all('div',{'class':'rank_block'})
			dico_countries={}

			for t in wta_menu:
				if t.h2.span.text.strip().upper()=='CLASSEMENT WTA':
					wta_pl_t=t
				if t.h2.span.text.strip().upper()=='JOUEURS DU PAYS:':
					wta_pl_c=t

			# Extraction of countries list
			wta_menu=wta_pl_c.form.fieldset.select.find_all('option')
			for t in wta_menu:
				dico_countries[t['value']]=t.text.strip()

			wta_menu=wta_pl_t.div.find('table',{'class':'table_pranks'}).find_all('tr')
		except:
			return
			
		# Beginning the main loop for extracting players data
		# First, we'll extract the player info in the dictionary dico_pl_profile
		# Second, we'll extract match stats for a player
		# dico_pl_profile={}
		# dico_match_stats={}

		dico_pl_profile={}
		dico_match_stats={}
		for t in wta_menu:
			valid_link=False
			self.resetDicoFromTemplate(dico_pl_profile,'player_profile')
			try:
				wta_rk=t.find_all('td')
				for td in wta_rk:
					if td['class']=='w20':
						pl_rk=td.text[:len(td.text)-1]
						wta_pl_t=td.find_all('a')
						if len(wta_pl_t)>0:
							wta_rk_url=wta_pl_t[0]['href']
							country_code=wta_pl_t[0].text.split('(')[1]
							country_code=country_code[:len(country_code)-1]
							pl_rk=int(pl_rk)
							valid_link=True
							break
			except:
				return

			if valid_link and pl_rk>=self.data_range[0] and pl_rk<=self.data_range[1]:
				try:
					rq_pge=requests.get(wta_rk_url)
				except:
					return

				try:
					bs_t=bs4.BeautifulSoup(rq_pge.text,'lxml')
					wta_rk=bs_t.find_all('div',{'class':'player_info'})
					for ti in wta_rk:
						wta_rk1=ti.find_all('div',{'class':'player_stats'})
						if len(wta_rk1)>0:
							break

					# Player info
					profile_t=wta_rk1[0].text.strip().split(' '*7)
					for nm in profile_t:
						nmt=nm.split(':')
						nmt0=nmt[0].strip().lower()
						nmt1=nmt[1].strip().lower()
						if(nmt0=='nom'):
							dico_pl_profile['player_name']=nmt1
						elif(nmt0=='pays'):
							dico_pl_profile['player_country_label']=nmt1
							dico_pl_profile['player_country_code']=country_code
						elif(nmt0=='date de naissance'):
							dico_pl_profile['player_birth_date']=nmt1.split(',')[0].replace('.','-')
						elif(nmt0=='taille'):
							dico_pl_profile['player_height']=nmt1
						elif(nmt0=='poids'):
							dico_pl_profile['player_weight']=nmt1
						elif(nmt0==u'début pro'):
							dico_pl_profile['player_begin_pro']=nmt1
						elif(nmt0=='points'):
							dico_pl_profile['player_points']=nmt1
						elif(nmt0=='primes'):
							dico_pl_profile['player_prize_money']=nmt1
						else:
							nmt00=nmt0.split('classement wta')
							if len(nmt00)>1:
								dico_pl_profile['player_rank']=nmt1
								dico_pl_profile['player_style_hand']=nmt00[0].strip()
				except:
					return

				# Player matches stats
				wta_rk=bs_t.find_all('div',{'class':'player_match_info'})
				wta_rk=wta_rk[0].find_all('table',{'class':'table_stats'})[0].find_all('tr')
				for ti in wta_rk:
					valid_year=False
					try:
						td=ti.find_all('td')
						for tdi in td:
							tda=tdi.find_all('a')
							if len(tda)>0:
								if tda[0]['title'][:5]==u'année':
									stats_year=tda[0].text.strip()
									stats_date=datetime.datetime.strptime('01-01-'+stats_year,self.getdtFormat()['fr-FR'])
									stats_yurl=wta_rk_url+tda[0]['href']
									valid_year=True
									break
					except:
						return
						
					if valid_year and stats_date>=self.date_range[0] and stats_date<=self.date_range[1]:
						try:
							rq_pge1=requests.get(stats_yurl)
						except:
							return

						bs_t1=bs4.BeautifulSoup(rq_pge1.text,'lxml')
						matches_in_range=bs_t1.find_all('div',{'class':'player_matches'})
						if len(matches_in_range)>1:
							matches_in_range=matches_in_range[1].find_all('table',{'class':'table_pmatches'})
						elif len(matches_in_range)==1:
							matches_in_range=matches_in_range[0].find_all('table',{'class':'table_pmatches'})
						matches_in_range=matches_in_range[0].find_all('tr')
						tourn_name=''
						tourn_surf=''
						for tdi in matches_in_range:
							self.resetDicoFromTemplate(dico_match_stats,'player_match_stats')
							if tdi['class']=='tour_head_pair':
								tourn_name=tdi.find_all('td',{'class':'w200'})[0].find_all('a')[0]['title']
								tourn_surf=tdi.find_all('td',{'class':'w40 surf_1'})[0].text.strip()
							tds=tdi.find_all('td')
							stats_d1=tds[0].text.strip().repalce('.','-')
							stats_d1=datetime.datetime.strptime(stats_d1[:len(stats_d1)-3]+'-'+stats_year,self.getdtFormat()['fr-FR'])
							if stats_d1>=self.date_range[0] and stats_d1<=self.date_range[1]:
								dico_match_stats['match_date']=stats_d1
								dico_match_stats['tournament_name']=tourn_name
								dico_match_stats['tournament_surface']=tourn_surf
								dico_match_stats['tournament_round']=tds[1].text.strip()
								dico_match_stats['match_result']=tds[5].find_all('img')[0]['alt']
								if dico_match_stats['match_result']==u'victoire':
									dico_match_stats['player1_name']=tds[2].text.strip()
									dico_match_stats['player2_name']=tds[3].find_all('a')[0].text.strip()
									dico_match_stats['match_score']=self.extractTennisScore(str(tds[4]),'<td class="w130">','</td>')
								elif dico_match_stats['match_result']==u'défaite':
									dico_match_stats['player1_name']=tds[3].text.strip()
									dico_match_stats['player2_name']=tds[2].find_all('a')[0].text.strip()
									dico_match_stats['match_score']=self.formatScore(self.extractTennisScore(str(tds[4]),'<td class="w130">','</td>'),',','-')

								# Match stats
								try:
									rq_pge2=requests.get(tds[6].find_all('a')[0]['href'])
								except:
									return

								bs_t2=bs4.BeautifulSoup(rq_pge2.text,'lxml')
								tstats_m=bs_t2.find_all('div',{'class':'player_matches'})[0].find_all('table',{'class':'table_stats_match'})[0].find_all('tr')
								if dico_match_stats['match_result']==u'victoire':
									for ti in tstats_m:
										tid=ti.find_all('td')
										if tid[0].text.strip().lower()==u'premier service en pourcentage':
											dico_match_stats['stat_player1_first_serv']=tid[1].text.strip()
											dico_match_stats['stat_player2_first_serv']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 1er service':
											dico_match_stats['stat_player1_first_serv_pts']=tid[1].text.strip()
											dico_match_stats['stat_player2_first_serv_pts']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 2e service':
											dico_match_stats['stat_player1_2nd_serv_pts']=tid[1].text.strip()
											dico_match_stats['stat_player2_2nd_serv_pts']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'balles de break gagnées':
											dico_match_stats['stat_player1_brk_pts_won']=tid[1].text.strip()
											dico_match_stats['stat_player2_brk_pts_won']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur retour':
											dico_match_stats['stat_player1_return_pts_won']=tid[1].text.strip()
											dico_match_stats['stat_player2_return_pts_won']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'double fautes':
											dico_match_stats['stat_player1_dble_faults']=tid[1].text.strip()
											dico_match_stats['stat_player2_dble_faults']=tid[2].text.strip()
										elif tid[0].text.strip().lower()==u'aces':
											dico_match_stats['stat_player1_aces']=tid[1].text.strip()
											dico_match_stats['stat_player2_aces']=tid[2].text.strip()

								elif dico_match_stats['match_result']==u'défaite':
									for ti in tstats_m:
										tid=ti.find_all('td')
										if tid[0].text.strip().lower()==u'premier service en pourcentage':
											dico_match_stats['stat_player1_first_serv']=tid[2].text.strip()
											dico_match_stats['stat_player2_first_serv']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 1er service':
											dico_match_stats['stat_player1_first_serv_pts']=tid[2].text.strip()
											dico_match_stats['stat_player2_first_serv_pts']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur 2e service':
											dico_match_stats['stat_player1_2nd_serv_pts']=tid[2].text.strip()
											dico_match_stats['stat_player2_2nd_serv_pts']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'balles de break gagnées':
											dico_match_stats['stat_player1_brk_pts_won']=tid[2].text.strip()
											dico_match_stats['stat_player2_brk_pts_won']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'points gagnés sur retour':
											dico_match_stats['stat_player1_return_pts_won']=tid[2].text.strip()
											dico_match_stats['stat_player2_return_pts_won']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'double fautes':
											dico_match_stats['stat_player1_dble_faults']=tid[2].text.strip()
											dico_match_stats['stat_player2_dble_faults']=tid[1].text.strip()
										elif tid[0].text.strip().lower()==u'aces':
											dico_match_stats['stat_player1_aces']=tid[2].text.strip()
											dico_match_stats['stat_player2_aces']=tid[1].text.strip()


#######################################################################################################
#                  Factory class for creating the right tennisData object                             #
#######################################################################################################
class tennisData:
	__dicoSources={"https://www.atptour.com":"smofficial",
		           "http://www.tennisendirect.net":"smted",
		           "https://www.wtatennis.com":"swofficial",
		           "http://www.tennisendirect.net":"swted"
		          }

	def __init__(self,tsource,data_range=[],date_range=[],save_filename=''):
		self.__tsource=tsource
		self.__data_range=data_range
		self.__date_range=date_range
		self.__save_filename=save_filename
		self.__ctennisData=self.__createTennisData()

	def setTsource(self,src_name=''):
		self.__tsource=src_name
		self.__ctennisData=self.__createTennisData()

	def getTsource(self):
		return self.__tsource

	def setDaterange(self,dte_range=[]):
		self.__date_range=dte_range

	def getDaterange(self):
		return self.__date_range

	def setDatarange(self,dt_range=[]):
		self.__data_range=dt_range

	def getDatarange(self):
		return self.__data_range

	def setDestfilename(self,dest_fname=''):
		self.__save_filename=dest_fname

	def getDestfilename(self):
		return self.__save_filename

	def extractData(self):
		self.__ctennisData.extractData()

	def __createTennisData(self):
		src_code=tennisData.__dicoSources[self.__tsource]
		if src_code=='smofficial':
			return smTennisDatafromOfficialSource(self.__data_range,self.__date_range,self.__save_filename)
		elif src_code=='smted':
			return smTennisDatafromTEDSource(self.__data_range,self.__date_range,self.__save_filename)
		elif src_code=='swofficial':
			return swTennisDatafromOfficialSource(self.__data_range,self.__date_range,self.__save_filename)
		elif src_code=='swted':
			return swTennisDatafromTEDSource(self.__data_range,self.__date_range,self.__save_filename)


	@classmethod	
	def getSources(cls):
		return cls.__dicoSources

	@classmethod
	def removeSource(cls,src_name=''):
		if len(src_name)>0:
			try:
				del cls.__dicoSources[src_name]
			except KeyError:
				pass

	@classmethod
	def addSource(cls,new_source=[]):
		if new_source is not None:
			if len(new_source)>1:
				cls.__dicoSources[new_source[0]]=new_source[1]