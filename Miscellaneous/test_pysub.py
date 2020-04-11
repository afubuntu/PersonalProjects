import requests
import lxml
import bs4
import logging
import datetime



def extractSinglePlayerInfo(_ct_url):
	try:
		_raw_page=requests.get(_ct_url)
		print(f'[{datetime.datetime.now()}][Single Men Data from TennisLive] The page :{_ct_url} has been successfully received.')
	except:
		print(f'[{datetime.datetime.now()}][Single Men Data from TennisLive] The page :{_ct_url} failed to be received.')

		return None

	try:
		_bs=bs4.BeautifulSoup(_raw_page.text,'lxml')
		_bs_result=_bs.find_all('div',{'class':'player_stats'})
		
		_player_stats_text=_bs_result[0].text.strip()
		_info_arr=_player_stats_text.split(' '*12)

		_player_info_dico={}

		for i in _info_arr:
			if i.find('Name:')>=0:
				_player_info_dico['player_name']=i[len('Name: '):]

			elif i.find('Country:')>=0:
				_player_info_dico['player_country_label']=i[len('Country: '):]

			elif i.find('Birthdate:')>=0:
				_current_date=datetime.date.today()
				_pbirthdate=[]

				_tmp_var=i[len('Birthdate: '):i.find(' years')].split(', ')
				_page=int(_tmp_var[1])
				_tmp_var=_tmp_var[0].split('.')

				for ii in _tmp_var:
					_pbirthdate.append(int(ii))				

				_tmp_pbirthdate=datetime.date(_current_date.year,_pbirthdate[1],_pbirthdate[0])

				if _tmp_pbirthdate>_current_date:
					_page+=1

				_player_info_dico['player_birth_date']=f'{_current_date.year-_page}-{_tmp_var[1]}-{_tmp_var[0]}'

			elif i.find('Height:')>=0:
				_player_info_dico['player_height']=i[len('Height: '):]

			elif i.find('Weight:')>=0:
				_player_info_dico['player_weight']=i[len('Weight: '):]

			elif i.find('Profi since:')>=0:
				_player_info_dico['player_begin_pro']=i[len('Profi since: '):]

			elif i.find('ATP ranking:')>=0:
				ll1=i.find('ATP ranking:')
				_player_info_dico['player_rank']=i[ll1+len('ATP ranking: '):]
				_player_info_dico['player_style_hand']=i[:ll1-1] if ll1>0 else ''

		_player_info_dico['player_base_link']=_ct_url
		_player_info_dico['player_code']=_ct_url[len('http://')+len(_ct_url[len('http://'):].split('/')[0]+'/atp/'):-1]

		return _player_info_dico
		
		print(f'[{datetime.datetime.now()}][Single Men Data from TennisLive] The following player info :{str(_player_info_dico)} has been successfully extracted.')
	except:
		print(f'[{datetime.datetime.now()}][Single Men Data from TennisLive] Failed to extract the player info for the following link :{_ct_url}')

		return None	


if __name__=='__main__':
	_ct_url='http://www.tennislive.net/atp/norbert-gombos/'
	dic_info=extractSinglePlayerInfo(_ct_url)

	print('Extracted info :')
	for i in dic_info:
		print(f'{i} : {dic_info[i]}')