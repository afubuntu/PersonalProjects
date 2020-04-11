import requests
import lxml
import bs4
import logging

def formatScore(m_score,sets_sep,games_sep,inver_game=True):
	sets_list=m_score.split(sets_sep)
	formatedScore=''
	for le in sets_list:
		cle=le.strip()
		sc1=cle.split('(')[0]
		sc2=cle[len(sc1):]
		if inver_game:
			if games_sep!='':
				gms=sc1.split(games_sep)
				formatedScore=formatedScore+gms[1]+'-'+gms[0]+sc2+','
			elif games_sep=='':
				formatedScore=formatedScore+sc1[1]+'-'+sc1[0]+sc2+','
		else:
			if games_sep!='':
				gms=sc1.split(games_sep)
				formatedScore=formatedScore+gms[0]+'-'+gms[1]+sc2+','
			elif games_sep=='':
				formatedScore=formatedScore+sc1[0]+'-'+sc1[1]+sc2+','
	return formatedScore[:len(formatedScore)-1]

def modifyDico(dico_to_m,new_key,new_value):
	dico_to_m[new_key]=new_value

def extractTennisScore(raw_content,rmbg,rmed):
	fscore=raw_content.replace(rmbg,'').replace(rmed,'')
	return fscore.replace('<sup>','(').replace('</sup>',')')

if __name__=='__main__':
	m_s=input('Please enter a tennis score : ')
	print('The reverse formated score is : ',formatScore(m_s,' ', ''))
	print('The non reverse formated score is : ',formatScore(m_s,' ','',False))
	rq=requests.get('http://www.tennisendirect.net/atp/match/sam-querrey-VS-novak-djokovic/wimbledon-london-2016/')
	bst=bs4.BeautifulSoup(rq.text,'lxml')
	#print('Display the xml : ')
	#print(bst.prettify())
	atprk=bst.find_all('div',{'class':'player_matches'})[0].find_all('table',{'class':'table_pmatches'})[0].find_all('tr')
	tds=atprk[0].find_all('td')
	td4=str(tds[4])
	print('Raw content : ',str(tds[4].find('span',{'id':'score'}))[len('<span id="score">'):-len('</span>')].replace('<sup>','(').replace('</sup>',')').replace(',',''))
	td4=extractTennisScore(td4,'<td class="w130"><span id="score">','</span> <span id="p_game"></span></td>')#td4.replace('<td class="w130"><span id="score">','').replace('</span> <span id="p_game"></span></td>','').strip()
	print('Extracted score : ',td4)
	print('*********** Lets test the function modifyDico ******************')
	someDico={'nom':'tanoh','prenom':'alain fabrice'}
	print('Before modification : ',someDico)
	modifyDico(someDico,'age',18)
	print('After modification : ',someDico)
	print('Dico items : ',someDico.items())
	for dk,dv in someDico.items():
		print('Key : %s ; Value : %s'%(dk,dv))
	print('Dico keys : ',someDico.keys())
	print('Dico values : ',someDico.values())