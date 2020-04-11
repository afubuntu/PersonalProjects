/**
 * LICENCE[[
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1/CeCILL 2.O
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is kelis.fr code.
 *
 * The Initial Developer of the Original Code is 
 * nicolas.boyer@kelis.fr
 *
 * Portions created by the Initial Developer are Copyright (C) 2013-2014
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * samuel.monsarrat@kelis.fr
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either of the GNU General Public License Version 2 or later (the "GPL"),
 * or the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * or the CeCILL Licence Version 2.0 (http://www.cecill.info/licences.en.html),
 * in which case the provisions of the GPL, the LGPL or the CeCILL are applicable
 * instead of those above. If you wish to allow use of your version of this file
 * only under the terms of either the GPL, the LGPL or the CeCILL, and not to allow
 * others to use your version of this file under the terms of the MPL, indicate
 * your decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL, the LGPL or the CeCILL. If you do not
 * delete the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL, the LGPL or the CeCILL.
 * ]]LICENCE
 */
 
/* Media manager */
var scMediaMgr = {
	fSkinRelPath : scServices.scLoad.getRootUrl() + "/skin",
	fMediaPath : "",
	fIsLocal : window.location.protocol == "file:",
	fNavie9 : parseFloat(scCoLib.userAgent.substring(scCoLib.userAgent.indexOf("msie")+5)) < 10,
	fListeners : {mediaLoaded:[],mediaEnded:[],mediaUpdate:[]},

	fStrings : ["Montrer les sous-titres","Cacher les sous-titres",
	/*02*/      "Choisir la langue","Off",
	/*04*/      "Clic droit, enregistrer sous... pour télécharger","Lecture",
	/*06*/      "Désactiver le son","Désactiver le son",
	/*08*/      "Transcription","Voir la transcription",
	/*10*/      "Alternative vidéo","Voir l\'alternative vidéo (LSF,LPC)",
	/*12*/      "Audiodescription","Voir l\'audiodescription",
	/*14*/      "Vidéo","Voir le média d\'origine",
	/*16*/      "Sous-titres","Voir les sous-titres",
	/*18*/      "Pause","Lecture",
	/*20*/      "Activer le son","Désactiver le son",
	/*22*/      "Cacher la transcription","Voir la transcription",
	/*24*/      "Voir la vidéo en plein écran","Revenir au mode normal (appuyer sur Échap)",
	/*26*/      "Pour quitter le plein écran, appuyer sur Échap...","",
	/*28*/      "Barre de navigation","Barre de volume",
	/*30*/      "Télécharger la transcription"],
		
	init : function(pMediaPath,pOpt){
		try{
			//scCoLib.util.log("scMediaMgr.init");
			if (typeof pMediaPath != "undefined") this.fMediaPath = pMediaPath;
			scOnLoads.push(this);
		} catch(e){scCoLib.util.log("ERROR - scMediaMgr.init : "+e)}
	},

	onLoad : function() {
		//scCoLib.util.log("scMediaMgr.onLoad");
		try{
			var vMedias = scPaLib.findNodes(this.fMediaPath);
			if(!vMedias) return;
			for (var i = 0; i < vMedias.length; i++) {
				vMedia = {};
				vMedia.fSrcTabs = [];
				vMedia.fParent = vMedias[i];
				vMedia.fIsTranscript = vMedia.fParent.getAttribute('data-alt-istranscript') || "no";
				vMedia.fTranscript = vMedia.fParent.getAttribute('data-alt-transcript') || "no";
				vMedia.fTranscriptInfosDoc = vMedia.fParent.getAttribute('data-alt-transcriptinfosdoc');
				vMedia.fSrc = vMedia.fParent.getAttribute('data-src');
				vMedia.fWidth = vMedia.fParent.getAttribute('data-width');
				vMedia.fHeight = vMedia.fParent.getAttribute('data-height');
				vMedia.fOtherEncoding = vMedia.fParent.getAttribute('data-alt-otherencoding') || "no";
				vMedia.fAltVideo = vMedia.fParent.getAttribute('data-alt-altvideo') || "no";
				vMedia.fAudioDesc = vMedia.fParent.getAttribute('data-alt-audiodesc') || "no";
				var vAudioDescType = vMedia.fParent.getAttribute('data-alt-audiodesctype') || "no";
				vMedia.fAudioDescType = vAudioDescType.substring(vAudioDescType.lastIndexOf("_")+1);

				this.fType = vMedia.fType = vMedia.fParent.getAttribute('data-type');
				this.fIsAltBtn = false;
				vMedia.fToolsBtnCnt = 0;
				vMedia.fSubtitles = vMedia.fParent.getAttribute('data-subtitles') || "no";
				this.createMedia(vMedia);
			}
		} catch(e){scCoLib.util.log("ERROR - scMediaMgr.onLoad : "+e);}
	},

	addListener: function(pKey, pFunc) {
		if (!this.fListeners[pKey]) return scCoLib.util.log("scMediaMgr.addListener ERROR : "+pKey+" is not a valid listener");
		this.fListeners[pKey].push(pFunc);
	},

	createMedia : function(pMedia, pType, pStart) {
		
		// Création du média
		pMedia.fContainer = document.createElement(pMedia.fType);
		pMedia.fParent.appendChild(pMedia.fContainer);
		
		var vSrc = pStart!=undefined?pMedia.fSrcTabs[scCoLib.toInt(pStart)]:pMedia.fSrc;
		var vSources = pMedia.fOtherEncoding!="no"?[vSrc,pMedia.fOtherEncoding]:[vSrc];
		for (var i = 0; i < vSources.length; i++) {
			var vSourceElt = document.createElement("source");
			pMedia.fContainer.appendChild(vSourceElt);
			vSourceElt.src = vSources[i];
		}
		if(pMedia.fType=='video') {
			if(pMedia.fWidth) pMedia.fContainer.width = pMedia.fWidth;
			if(pMedia.fHeight) pMedia.fContainer.height = pMedia.fHeight;
		}

		// Fallback Flash si erreur
		pMedia.flashFallback = scPaLib.findNode("chi:object|div.infoVideo|div.infoAudio",pMedia.fParent);
		pMedia.fContainer.childNodes[vSources.length-1].onerror = function(){scMediaMgr.xCreateFlashFallback(pMedia);};
		if(pMedia.flashFallback) pMedia.fParent.removeChild(pMedia.flashFallback);

		// Création du lecteur et des boutons par défaut du lecteur
		pMedia.fPlayerElt = scDynUiMgr.addElement("div",pMedia.fParent,"player_bk "+pMedia.fType+"_bk");

		pMedia.fPlayBtn = this.xAddBtn(pMedia.fPlayerElt,"play_btn",this.fStrings[19],this.fStrings[19]);
		pMedia.fPlayBtn.media = pMedia;
		pMedia.fPlayBtn.onclick = this.sPlayPause;
		var vSeeProgress = scDynUiMgr.addElement("div",pMedia.fPlayerElt,"seek_progress");
		pMedia.fLoadProgress = scDynUiMgr.addElement("div",vSeeProgress,"load_progress");
		pMedia.fSeekValue = scDynUiMgr.addElement("div",vSeeProgress,"seek_value");
		// Ne pas créer l'input range si IE9 ou inférieur car non supporté
		if(!this.fNavie9) {
			var vSeekBtn = pMedia.fSeekBtn = scDynUiMgr.addElement("input",vSeeProgress,"seek_btn");
			vSeekBtn.media = pMedia;
			vSeekBtn.title = this.fStrings[28];
			vSeekBtn.type = "range";
			vSeekBtn.value = 0;
			vSeekBtn.max = "";
			vSeekBtn.onchange = function(){pMedia.fContainer.currentTime = pMedia.fSeekBtn.value;};
			vSeekBtn.setAttribute("aria-valuemin", 0);
			vSeekBtn.setAttribute("aria-valuenow", 0);

		}
		var vTime = scDynUiMgr.addElement("div",pMedia.fPlayerElt,"time_bk");
		pMedia.fMuteBtn = this.xAddBtn(pMedia.fPlayerElt,"mute_btn",this.fStrings[6],this.fStrings[6]);
		pMedia.fMuteBtn.media = pMedia;
		pMedia.fMuteBtn.onclick = this.sMute;
		var vVolumeBk = scDynUiMgr.addElement("div",pMedia.fPlayerElt,"volume_bk");
		pMedia.fVolumeValue = scDynUiMgr.addElement("div",vVolumeBk,"volume_value");
		// Ne pas créer l'input range si IE9 ou inférieur car non supporté
		if(!this.fNavie9) {
			var vVolumeBtn = pMedia.fVolumeBtn = scDynUiMgr.addElement("input",vVolumeBk,"volume_btn");
			vVolumeBtn.media = pMedia;
			vVolumeBtn.title = this.fStrings[29];
			vVolumeBtn.type = "range";
			vVolumeBtn.step = 0.1;
			vVolumeBtn.max = vVolumeBtn.value = 1;
			vVolumeBtn.onchange = this.sSetVolume;
			vVolumeBtn.setAttribute("aria-valuemin", 0);
			vVolumeBtn.setAttribute("aria-valuemax", 100);
			vVolumeBtn.setAttribute("aria-valuenow", 100);
			vVolumeBtn.setAttribute("aria-valuetext", "100%");
		}
		pMedia.fDefaultVidsBtns = [];
		pMedia.fVidsBtns = [];

		// Init subtitles
		if(pStart!=undefined || pMedia.fType=='video') this.initSubTitles(pMedia);

		// Crée les boutons de vidéos/audios alternatives si elles existent
		if(pMedia.fIsTranscript !='no') {
			var vTitle = pMedia.fTranscript=='text'?this.fStrings[30]:this.fStrings[30]+" ("+pMedia.fTranscript.substring(pMedia.fTranscript.lastIndexOf('/')+1)+pMedia.fTranscriptInfosDoc+")",
				vTranscriptBtn = this.xAddBtn(pMedia.fPlayerElt,"transcript_btn",this.fStrings[8],vTitle);
			vTranscriptBtn.fElt = pMedia.fTranscript=='text'?scPaLib.findNode("nsi:",pMedia.fParent):pMedia.fTranscript;
			if(pMedia.fTranscript == 'text') this.xToggleTranscript(vTranscriptBtn,true);
			else vTranscriptBtn.fTranscriptIsPdf = true;
			if (pMedia.fType=='audio') vTranscriptBtn.onclick = this.sToggleTranscript;
			else {
				vTranscriptBtn.fClick = this.sToggleTranscript;
				pMedia.fDefaultVidsBtns.push(vTranscriptBtn);
				this.fIsAltBtn = true;
			}
			pMedia.fToolsBtnCnt += 1;
		}
		var vAltVidsBtns = scDynUiMgr.addElement("span",pMedia.fPlayerElt,"altVids_bk");
		if(pMedia.fAltVideo != 'no') {
			var vAltVideoBtn = this.xAddBtn(vAltVidsBtns,"altvideo_btn",this.fStrings[10],this.fStrings[11]);
			vAltVideoBtn.media = pMedia;
			vAltVideoBtn.src = pMedia.fAltVideo;
			vAltVideoBtn.typeElt = "video";
			pMedia.fVidsBtns.push(vAltVideoBtn);
			this.fIsAltBtn = true;
			pMedia.fToolsBtnCnt += 1;
		} 
		if(pMedia.fAudioDesc != 'no') {
			var vAudiodesc = this.xAddBtn(vAltVidsBtns,"audiodesc_btn",this.fStrings[12],this.fStrings[13]);
			vAudiodesc.media = pMedia;
			vAudiodesc.src = pMedia.fAudioDesc;
			vAudiodesc.typeElt = pMedia.fAudioDescType=="mp4"||pMedia.fAudioDescType=="webm"?"video":"audio";
			pMedia.fVidsBtns.push(vAudiodesc);
			this.fIsAltBtn = true;
			pMedia.fToolsBtnCnt += 1;
		}
		if(this.fIsAltBtn == true) {
			var vDefaultMedia = this.xAddBtn(vAltVidsBtns,"defaultvideo_btn",this.fStrings[14],this.fStrings[15]);
			vDefaultMedia.media = pMedia;
			vDefaultMedia.src = pMedia.fSrc;
			vDefaultMedia.defaultSrc = true;
			vDefaultMedia.typeElt = this.fType;
			pMedia.fVidsBtns.push(vDefaultMedia);
			pMedia.fToolsBtnCnt += 1;

			// Init des onclick des boutons du player
			this.xInitClickBtns(pStart!=undefined?pMedia.fVidsBtns[scCoLib.toInt(pStart)]:vDefaultMedia);
		}

		// Listeners
		pMedia.fContainer.addEventListener('loadedmetadata', function (){
			if(pMedia.fType=='video') {
				// Création du bouton de fullscreen
				if (document.fullscreenEnabled || document.webkitFullscreenEnabled || document.mozFullScreenEnabled || document.msFullscreenEnabled){
					var vFullScreenBtn = scMediaMgr.xAddBtn(pMedia.fPlayerElt,"fullScreen_btn",scMediaMgr.fStrings[24],scMediaMgr.fStrings[24]);
					vFullScreenBtn.video = pMedia.fContainer;
					vFullScreenBtn.isFullScreen = false;
					vFullScreenBtn.onclick = scMediaMgr.sSetFullScreen;
					pMedia.fToolsBtnCnt += 1;
					document.addEventListener('fullscreenchange', function (){scMediaMgr.xFullScreenToggle(this)}, false);
					document.addEventListener('webkitfullscreenchange', function (){scMediaMgr.xFullScreenToggle(this)}, false);
					document.addEventListener('mozfullscreenchange', function (){scMediaMgr.xFullScreenToggle(this)}, false);
					document.addEventListener('msfullscreenchange', function (){scMediaMgr.xFullScreenToggle(this)}, false);
				}
			}
			pMedia.fPlayerElt.className += " is_"+pMedia.fToolsBtnCnt+"_toolsBtn";
			vTime.innerHTML = "<span class='curTime_bk'>"+scMediaMgr.xFormatTime(this.currentTime) + "</span><span class='sepTime_bk'> / </span><span class='totalTime_bk'>"+scMediaMgr.xFormatTime(this.duration)+"</span>";
			if(!scMediaMgr.fNavie9) {
				pMedia.fSeekBtn.max = scCoLib.toInt(this.duration);
				pMedia.fSeekBtn.setAttribute("aria-valuemax", scCoLib.toInt(this.duration));
			}
			scMediaMgr.xNotifyListener("mediaLoaded", this);
		}, false);
		pMedia.fContainer.addEventListener('timeupdate', function (){
			// TODO : Fonction à peut-être liée avec l'update flash
			if(!scMediaMgr.fNavie9) {
				pMedia.fSeekBtn.value = parseInt(this.currentTime, 10);
				pMedia.fSeekBtn.setAttribute("aria-valuenow", scCoLib.toInt(this.currentTime));
				pMedia.fSeekBtn.setAttribute("aria-valuetext", scMediaMgr.xFormatTime(this.currentTime));
			}
			pMedia.fSeekValue.style.width = (this.currentTime/this.duration)*100+"%";
			pMedia.fLoadProgress.style.width=(this.buffered.end(0)/this.duration)*100+"%";
			vTime.innerHTML = "<span class='curTime_bk'>"+scMediaMgr.xFormatTime(this.currentTime) + "</span><span class='sepTime_bk'> / </span><span class='totalTime_bk'>"+scMediaMgr.xFormatTime(this.duration)+"</span>";
			// Lance le traitement des sous-titres
			scMediaMgr.xUpdateSub(pMedia);
			scMediaMgr.xNotifyListener("mediaUpdate", this);
		}, false);
		pMedia.fContainer.addEventListener('volumechange', function (){
			if(!scMediaMgr.fNavie9){
				pMedia.fVolumeBtn.value = this.volume;
				pMedia.fVolumeBtn.setAttribute("aria-valuenow", this.volume*100);
				pMedia.fVolumeBtn.setAttribute("aria-valuetext", this.volume*100 + "%");
				pMedia.fContainer.addEventListener('ended', function (){scMediaMgr.xStop(pMedia);}, false);
			}
			pMedia.fVolumeValue.style.width = this.volume*100+"%";
		}, false);	
		pMedia.fContainer.addEventListener('ended', function (){
			scMediaMgr.xNotifyListener("mediaEnded", this);
		}, false);
	},

	initSubTitles : function(pMedia) {
		try{
			if(pMedia.fSubtitles == 'no' || this.fIsLocal) return;
			this.fIsAltBtn = true;
			pMedia.fToolsBtnCnt += 1;

			// Traitement des sous titres et création de l'objet
			var vSubTemp = this.xDeserialiseObjJs(pMedia.fSubtitles).subtitles,
				isVideo = false;
			pMedia.fSubs = {};
			pMedia.fSubs.fSubtitles = [];

			for (var i = 0; i < vSubTemp.length; i++) {
				var vSubs = vSubTemp[i];
				pMedia.fSubs.fSubtitles[i] = {};
				pMedia.fSubs.fSubtitles[i].lang = vSubs.lang;
				pMedia.fSubs.fSubtitles[i].file = vSubs.url;
				var vExtension = vSubs.url.substring(vSubs.url.lastIndexOf(".")+1);
				pMedia.fSubs.fSubtitles[i].video = vExtension!="srt"&&vExtension!="vtt"?true:false;
				if(vExtension!="srt"&&vExtension!="vtt") isVideo = vSubs.url;
			}

			// Création des éléments
			pMedia.fSubs.fSubBox = scDynUiMgr.addElement("div",pMedia.fParent,"subBox subDisplay_off");
			pMedia.fSubs.fSubBoxCo = scDynUiMgr.addElement("div",pMedia.fSubs.fSubBox,"subBox_co subDisplay_off");
			pMedia.fSubs.fSubHolder = scDynUiMgr.addElement("div",pMedia.fSubs.fSubBoxCo,"subtitles");
			pMedia.fSubs.fSubHolder.title = this.fStrings[16];
			pMedia.fSubs.fSubHolder.setAttribute("aria-live", "polite");
			pMedia.fSubs.fSubHolder.setAttribute("aria-relevant", "text");

			// Déclaration de la langue par défaut : la première rentrée
			pMedia.fSubs.fLang = pMedia.fSubs.fSubtitles[0].lang;

			// Création du lien sur le bouton permettant le choix des sous-titres
			var vBtnDisplayLangSub = this.xAddBtn(pMedia.fPlayerElt,"subtitles_btn",this.fStrings[16],this.fStrings[17]);
			if(pMedia.fSubs.fSubtitles.length == 1 && isVideo) {
				vBtnDisplayLangSub.typeElt = "video";
				vBtnDisplayLangSub.media = pMedia;
				vBtnDisplayLangSub.src = isVideo;
				pMedia.fVidsBtns.push(vBtnDisplayLangSub);
			}
			else {
				vBtnDisplayLangSub.fClick = this.sToggleSubs;
				pMedia.fDefaultVidsBtns.push(vBtnDisplayLangSub);
			}
			vBtnDisplayLangSub.fSubs = pMedia.fSubs;

			// Création de la liste des langues et changement du title de vBtnDisplayLangSub s'il y a plus d'un sous-titre
			pMedia.fSubs.fBtnsLang = [];
			if(pMedia.fSubs.fSubtitles.length > 1) {
				vBtnDisplayLangSub.title = this.fStrings[2];
				pMedia.fSubs.fBtnsLangList = scDynUiMgr.addElement("ul",pMedia.fPlayerElt,"btnsLangList subDisplay_off");
				var vSubLi = scDynUiMgr.addElement("li",pMedia.fSubs.fBtnsLangList);
				var vSubBtn = this.xAddLnk(vSubLi, "btnSub_choice subSelect_on", this.fStrings[3], this.fStrings[1]);
				pMedia.fSubs.fBtnsLang.push(vSubBtn);
				vSubBtn.fSubs = pMedia.fSubs;
				vSubBtn.onclick = this.sHideSubs;

			} else vBtnDisplayLangSub.title = this.fStrings[0];

			for (var i = 0; i < pMedia.fSubs.fSubtitles.length; i++) {
				var vSubtitles = pMedia.fSubs.fSubtitles[i];
				if(!vSubtitles.video) {
					vSubtitles.showing = false;
					vSubtitles.active = 0;
					// On ajoute les sous-titres
					this.xSubsAdd(vSubtitles.file,i,pMedia.fSubs.fSubtitles);
				}
				// Création de la liste des langues
				if(pMedia.fSubs.fSubtitles.length > 1) {
					var vSubLi = scDynUiMgr.addElement("li",pMedia.fSubs.fBtnsLangList);
					var vSubBtn = this.xAddLnk(vSubLi, "btnSub_choice", vSubtitles.lang, vSubtitles.lang);
					pMedia.fSubs.fBtnsLang.push(vSubBtn);
					vSubBtn.fSubs = pMedia.fSubs;
					vSubBtn.fSub = vSubtitles;
					if(!vSubtitles.video) vSubBtn.onclick = this.sToggleSubLang;
					else {
						vSubBtn.typeElt = "video";
						vSubBtn.src = vSubtitles.file;
						vSubBtn.media = pMedia
						pMedia.fVidsBtns.push(vSubBtn);
					}
				}
			}
		} catch(e){scCoLib.util.log("ERROR - scMediaMgr.initSubTitles : "+e);}
	},

	stop : function(pBtn) {
		if (!pBtn) return;
		this.xStop(pBtn.media);
	},

	playPause : function(pBtn, pForcedPause) {
		if (!pBtn) return;
		if(!this.fNavie9) pBtn.media.fSeekBtn.max = pBtn.media.fContainer.duration;
		if(!pBtn.media.fContainer.paused || pForcedPause) {
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[19]+"' src='"+this.fSkinRelPath+"/img/player/play_btn.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/play_btnOVER.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/play_btn.png'");


//			pBtn.innerHTML = "<span class='capt'>"+this.fStrings[19]+"</span>";
			pBtn.title = this.fStrings[19];
			pBtn.className = pBtn.className.replace("pause_btn", "play_btn");
			pBtn.media.fContainer.paused = true;
			if(pBtn.media.isFlash) pBtn.media.fContainer.SetVariable("method:pause", "");				
			else pBtn.media.fContainer.pause();
			this.xSwitchClass(pBtn.media.fParent,"is_on","is_off",true);
		} else {
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[18]+"' src='"+this.fSkinRelPath+"/img/player/pause_btn.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/pause_btnOVER.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/pause_btn.png'");

//			pBtn.innerHTML = "<span class='capt'>"+this.fStrings[18]+"</span>";
			pBtn.title = this.fStrings[18];
			pBtn.className = pBtn.className.replace("play_btn", "pause_btn");
			pBtn.media.fContainer.paused = false;
			if(pBtn.media.isFlash) pBtn.media.fContainer.SetVariable("method:play", "");
			else pBtn.media.fContainer.play();
			this.xSwitchClass(pBtn.media.fParent,"is_off","is_on",true);
		}
	},

	mute : function(pBtn) {
		if (!pBtn) return;
		if(pBtn.media.muted == true) {
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[21]+"' src='"+this.fSkinRelPath+"/img/player/mute_btn.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/mute_btnOVER.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/mute_btn.png'");
			
//			pBtn.innerHTML = "<span class='capt'>"+this.fStrings[21]+"</span>";
			pBtn.title = this.fStrings[21];
			pBtn.className = pBtn.className.replace("muted_btn", "mute_btn");
			if(!pBtn.media.isFlash) pBtn.media.fContainer.volume = 1;
			else pBtn.media.fContainer.SetVariable("method:setVolume", 100);
			pBtn.media.muted = false;			
		} else {
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[20]+"' src='"+this.fSkinRelPath+"/img/player/muted_btn.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/muted_btnOVER.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/muted_btn.png'");

//			pBtn.innerHTML = "<span class='capt'>"+this.fStrings[20]+"</span>";
			pBtn.title = this.fStrings[20];
			pBtn.className = pBtn.className.replace("mute_btn", "muted_btn");
			if(!pBtn.media.isFlash) pBtn.media.fContainer.volume = 0;
			else pBtn.media.fContainer.SetVariable("method:setVolume", 0);
			pBtn.media.muted = true;			
		}
	},

	/* ===  ============================================================ */

	sPlayPause : function() {
		try{
			scMediaMgr.playPause(this);
		} catch(e){}
		return false;
	},

	sSetVolume : function() {
		try{
			scMediaMgr.xSetVolume(this);
		} catch(e){}
		return false;
	},

	sMute : function() {
		try{
			scMediaMgr.mute(this);
		} catch(e){}
		return false;
	},

	sToggleTranscript : function() {
		try{
			scMediaMgr.xToggleTranscript(this);
		} catch(e){}
		return false;
	},

	// Suppression de l'insertion de la vidéo dans le src, récréation à chaque fois pour gérer les différentes sources - A ENLEVER quand trouver pourquoi la recréation de media peut causer des ralentissements
	/*sLoadVideo : function(){
		try{
			scMediaMgr.xLoadVideo(this);
		} catch(e){}
		return false;
	},*/

	sToggleSubs : function() {
		// Choix des langues si plusieurs langues ou toggle on/off sous-titres
		try{
			if(this.fSubs.fSubtitles.length > 1) scMediaMgr.xSubLangSelect(this);
			else scMediaMgr.xToggleSubsOnOff(this);
		} catch(e){}
		return false;
	},

	sHideSubs : function() {
		// Cache les sous-titres et ferme le menu des sous titres
		try{
			scMediaMgr.xHideSubs(this);
			scMediaMgr.xSubLangSelect(this);
		} catch(e){}
		return false;
	},

	sToggleSubLang : function() {
		// Choix des langues et affichage du bon sous-titres
		try{
			scMediaMgr.xShowSubs(this);
			if(this.fSub.showing) scMediaMgr.xShowSub(this.fSubs.fSubHolder, this.fSub[this.fSub.lang][this.fSub.active].text, true);
			this.fSubs.fLang = this.fSub.lang;
			scMediaMgr.xSubLangSelect(this);
		} catch(e){}
		return false;
	},

	sSetFullScreen : function() {
		try{
			scMediaMgr.xSetFullScreen(this);
		} catch(e){}
		return false;
	},


	/* ===  ============================================================ */

	xCreateFlashFallback : function(pMedia){
		// Remplacement de l'objet et définition des attributs
		pMedia.fParent.replaceChild(pMedia.flashFallback,pMedia.fContainer);
		pMedia.fContainer = pMedia.flashFallback;
		var vFullScreenBtn = scMediaMgr.xAddBtn(pMedia.fPlayerElt,"fullScreen_btn",scMediaMgr.fStrings[24],scMediaMgr.fStrings[24]);
		vFullScreenBtn.isFlash = pMedia.isFlash = true;
		pMedia.fPlayerElt.style.display = "none";
		pMedia.flashFallback.style.visibility = "visible";
		pMedia.fContainer.paused = true;
		vFullScreenBtn.video = pMedia.fContainer;
	},

	xSetVolume : function(pBtn) {
		if (!pBtn) return;
		if(!pBtn.media.isFlash) pBtn.media.fContainer.volume = pBtn.value;
		// Todo : A tester dans lexique
		else pBtn.media.fContainer.SetVariable("method:setVolume", pBtn.value*100);
		if(pBtn.media.fContainer.volume != 0) {
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[20]+"' src='"+this.fSkinRelPath+"/img/player/mute_btn.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/mute_btnOVER.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/mute_btn.png'");

//			pBtn.media.fMuteBtn.innerHTML = "<span class='capt'>"+this.fStrings[20]+"</span>";
			pBtn.media.fMuteBtn.title = this.fStrings[20];
			pBtn.media.fMuteBtn.className = pBtn.media.fMuteBtn.className.replace("muted_btn", "mute_btn");
		} else {
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[21]+"' src='"+this.fSkinRelPath+"/img/player/muted_btn.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/muted_btnOVER.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/muted_btn.png'");

//			pBtn.media.fMuteBtn.innerHTML = "<span class='capt'>"+this.fStrings[21]+"</span>";
			pBtn.media.fMuteBtn.title = this.fStrings[21];
			pBtn.media.fMuteBtn.className = pBtn.media.fMuteBtn.className.replace("mute_btn", "muted_btn");
		}
	},

	xToggleTranscript : function(pBtn,pVis) {
		if(!pBtn) return;
		if(pBtn.fTranscriptIsPdf) {
			window.open(pBtn.fElt,"_blank");
			return;
		}
		var vVis = pBtn.fElt.vis?pBtn.fElt.vis:pVis;

		var vClasse=pBtn.className.indexOf(' ')==-1?pBtn.className:pBtn.className.substr(0,pBtn.className.indexOf(' '));

		if(!vVis){
			scMediaMgr.xSwitchClass(pBtn.fElt,"display_off","display_on",true);
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[22]+"' src='"+this.fSkinRelPath+"/img/player/"+vClasse+"ON.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+"OVERON.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+"ON.png'");
			
//			scMediaMgr.xSwitchClass(pBtn,"act_off","act_on",true);
			pBtn.title = this.fStrings[22];
			pBtn.fElt.vis = true;
		} else {
			scMediaMgr.xSwitchClass(pBtn.fElt,"display_on","display_off",true);
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[23]+"' src='"+this.fSkinRelPath+"/img/player/"+vClasse+".png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+"OVER.png'");
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+".png'");

//			scMediaMgr.xSwitchClass(pBtn,"act_on","act_off",true);
			pBtn.title = this.fStrings[23];
			pBtn.fElt.vis = false;
		}
	},

	xStop : function(pMedia) {
		if (!pMedia) return;
		scMediaMgr.playPause(pMedia.fPlayBtn,true);
		if(!pMedia.isFlash) pMedia.fContainer.currentTime = 0;
		else pMedia.fContainer.SetVariable("method:setPosition", 0)
	},

	xFullScreenToggle : function() {
		scCoLib.util.log("scMediaMgr.xFullScreenToggle");
		var vFullscreenElement = document.fullscreenElement || document.webkitFullscreenElement || document.mozFullScreenElement || document.msFullscreenElement;
		if (vFullscreenElement){
			this.fCurrentFullScreenElement = vFullscreenElement;
			this.fCurrentFullScreenElement.setAttribute("controls", true);
		} else if (this.fCurrentFullScreenElement){
			this.fCurrentFullScreenElement.removeAttribute("controls");
			this.fCurrentFullScreenElement = null;
		}
	},

	xSetFullScreen : function(pBtn) {
		if (pBtn.video.requestFullscreen){
			pBtn.video.requestFullscreen();
		} else if (pBtn.video.msRequestFullscreen) {
			pBtn.video.msRequestFullscreen();
		} else if (pBtn.video.mozRequestFullScreen) {
			pBtn.video.mozRequestFullScreen();
		} else if (pBtn.video.webkitRequestFullscreen) {
			pBtn.video.webkitRequestFullscreen();
		}
	},
	xNotifyListener: function(pKey, pParam) {
		try{
			for (var i=0; i<this.fListeners[pKey].length; i++){
				this.fListeners[pKey][i](pParam);
			}
		} catch(e){
			scCoLib.util.log("scMediaMgr.xNotifyListener("+pKey+") - ERROR : "+e);
		}
	},

	/*xLoadVideo : function(pBtn){
		if(!pBtn || !pBtn.media) return;
		// A commenter pour remettre le currentTime sur la nouvelle vidéo
		this.xStop(pBtn.media);

		var vContainer = pBtn.media.fContainer;
		// A décommenter pour remettre le currentTime sur la nouvelle vidéo
		// var vCurrentTime = vContainer.currentTime;

		if(pBtn.media.isFlash) vContainer.SetVariable("method:setUrl", pBtn.src);
		else vContainer.src = pBtn.src;
		scMediaMgr.xInitClickBtns(pBtn);		

		if(!pBtn.media.isFlash) vContainer.load();
		scMediaMgr.playPause(pBtn.media.fPlayBtn);
		// A décommenter pour remettre le currentTime sur la nouvelle vidéo
		// vContainer.addEventListener('loadedmetadata', function() {
		// 	this.currentTime = vCurrentTime;
		// }, false);
	},*/

	xInitClickBtns : function(pBtn){
		if(!pBtn) return;
		for (var i = 0; i < pBtn.media.fVidsBtns.length; i++) {
			// Init click pour les boutons sous titre et transcript affichés seulement lorsqu'on est sur le bouton défaut
			for (var j = 0; j < pBtn.media.fDefaultVidsBtns.length; j++) {
				var vDefaultVidsBtn = pBtn.media.fDefaultVidsBtns[j];
				if(pBtn.defaultSrc){
					vDefaultVidsBtn.onclick = vDefaultVidsBtn.fClick;
					vDefaultVidsBtn.disabled = false;
					scMediaMgr.xSwitchClass(vDefaultVidsBtn,"video_act_on","video_act_off",true);
				} else {
					vDefaultVidsBtn.onclick = function(){return false;};
					vDefaultVidsBtn.disabled = true;
					scMediaMgr.xSwitchClass(vDefaultVidsBtn,"video_act_off","video_act_on",true);
					// Gestion spécifique pour les sous-titres (quand srt ou vtt)
					if(vDefaultVidsBtn.fSubs) {
						if(vDefaultVidsBtn.fSubs.fSubtitles.length > 1) {
							scMediaMgr.xHideSubs(vDefaultVidsBtn);
							vDefaultVidsBtn.fSubs.subLangState = true;
							scMediaMgr.xSubLangSelect(vDefaultVidsBtn);
						} else scMediaMgr.xToggleSubsOnOff(vDefaultVidsBtn,true)
					}
				}
			}
			// Init click pour le bouton défaut at les boutons alternatifs
			var vAltVidsBtn = pBtn.media.fVidsBtns[i],
				vSrcTabs = pBtn.media.fSrcTabs;
			if(!vSrcTabs.length || vSrcTabs.length!=i+1) pBtn.media.fSrcTabs[i] = vAltVidsBtn.src;
			vAltVidsBtn.pos = i;
			if(pBtn == vAltVidsBtn) {
				vAltVidsBtn.onclick = function(){return false;};
				vAltVidsBtn.disabled = true;
				scMediaMgr.xSwitchClass(vAltVidsBtn,"video_act_off","video_act_on",true);
			} else {
				//if(vAltVidsBtn.typeElt && pBtn.media.fType != vAltVidsBtn.typeElt) {
					vAltVidsBtn.onclick = function(){
						var vMedia = this.media;
						scMediaMgr.xStop(vMedia);
						vMedia.fType = this.typeElt;
						while (vMedia.fParent.hasChildNodes()) {
						    vMedia.fParent.removeChild(vMedia.fParent.lastChild);
						}
						//vMedia.fParent.innerHTML = "";
						scMediaMgr.createMedia(vMedia, this.typeElt, this.pos);
						scMediaMgr.playPause(vMedia.fPlayBtn);
					}
				//} else vAltVidsBtn.onclick = this.sLoadVideo;
				vAltVidsBtn.disabled = false;
				scMediaMgr.xSwitchClass(vAltVidsBtn,"video_act_on","video_act_off",true);				
			}
		};
	},

	xToggleSubsOnOff : function(pBtn,pVis) {
		// Toggle sous-titres on/off
		if(!pBtn) return;
		var vSubVis = pBtn.subvis?pBtn.subvis:pVis;
		if (vSubVis){
			scMediaMgr.xHideSubs(pBtn);
			pBtn.title = scMediaMgr.fStrings[0];
			pBtn.subvis = false;
		} else {
			scMediaMgr.xShowSubs(pBtn);
			pBtn.title = scMediaMgr.fStrings[1];
			pBtn.subvis = true;
		}
	},

	xShowSubs : function(pBtn) {
		// Montre les sous-titres
		if(!pBtn) return;
		this.xSwitchClass(pBtn.fSubs.fSubBox, "subDisplay_off", "subDisplay_on", true);
		this.xSwitchClass(pBtn, "btnSubDisplay_off", "btnSubDisplay_on", true);
		
		if(pBtn.tagName.toLowerCase()!="a") {
			var vClasse=pBtn.className.indexOf(' ')==-1?pBtn.className:pBtn.className.substr(0,pBtn.className.indexOf(' '));
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[1]+"' src='"+this.fSkinRelPath+"/img/player/"+vClasse+"ON.png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+"OVERON.png'");	
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+"ON.png'");
		}
			
	},

	xHideSubs : function(pBtn) {
		// Cache les sous-titres
		if(!pBtn) return;
		this.xSwitchClass(pBtn.fSubs.fSubBox, "subDisplay_on", "subDisplay_off", true);
		this.xSwitchClass(pBtn, "btnSubDisplay_on", "btnSubDisplay_off", true);
		if(pBtn.tagName.toLowerCase()!="a") {
			var vClasse=pBtn.className.indexOf(' ')==-1?pBtn.className:pBtn.className.substr(0,pBtn.className.indexOf(' '));
			pBtn.innerHTML = "<img class='btnImg' alt='"+this.fStrings[0]+"' src='"+this.fSkinRelPath+"/img/player/"+vClasse+".png'/>";
			pBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+"OVER.png'");	
			pBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/"+vClasse+".png'");
		}

		// On repasse la liste de choix active sur le off
		for (var i = 0; i < pBtn.fSubs.fBtnsLang.length; i++) {	
			var vBtn = pBtn.fSubs.fBtnsLang[i];	
			if(i == 0) this.xSwitchClass(vBtn, "subSelect_off", "subSelect_on", true);
			else this.xSwitchClass(vBtn, "subSelect_on", "subSelect_off", true);
		}
	},

	xSubLangSelect : function(pBtn) {
		// Ouverture du menu des sous-titres
		if(!pBtn) return;
		if (!pBtn.fSubs.subLangState) {
			this.xSwitchClass(pBtn.fSubs.fBtnsLangList, "subDisplay_off", "subDisplay_on", true);
			pBtn.fSubs.subLangState = true;
		} else {
			this.xSwitchClass(pBtn.fSubs.fBtnsLangList, "subDisplay_on", "subDisplay_off", true);
			if (pBtn.fSubs.fBtnsLang.indexOf(pBtn) != -1) {
				for (var i = 0; i < pBtn.fSubs.fBtnsLang.length; i++) {
					var vBtn = pBtn.fSubs.fBtnsLang[i];		
					if(pBtn == vBtn) this.xSwitchClass(pBtn, "subSelect_off", "subSelect_on", true);
					else this.xSwitchClass(vBtn, "subSelect_on", "subSelect_off", true);
				}
			}
			pBtn.fSubs.subLangState = false;
		}
	},

	xShowSub : function(pElt, pText, pIsActive) {
		// Insertion des sous-titre s'ils sont actifs
		if(pIsActive) pElt.innerHTML = pText;
		else pElt.innerHTML = "";
	},

	xUpdateSub : function(pMedia) {
		// Affichage des sous-titres en fonction de la postion et du temps
		if(pMedia.fSubtitles == 'no') return;
		for(var i in pMedia.fSubs.fSubtitles){
			var vSubsObj = pMedia.fSubs.fSubtitles[i];
			if(!vSubsObj[vSubsObj.lang]) return 0;
			var vRecordedTime = vSubsObj[vSubsObj.lang].curTime,
				vCurrentTime = pMedia.fContainer.currentTime;
			if(vCurrentTime === vRecordedTime) continue;
			else {
				var j = vSubsObj.active;
				vSubsObj[vSubsObj.lang].curTime = vCurrentTime;
				if(vRecordedTime < vCurrentTime) {
					if((vSubsObj[vSubsObj.lang][j].end > vCurrentTime && vSubsObj.showing)) continue;

					for(var dd = j; dd < vSubsObj[vSubsObj.lang].length; dd++ )
						if(!scMediaMgr.xSubtitleLoop(vSubsObj,vCurrentTime,dd,pMedia)) break;

					for(var dd = 0; dd < j; dd++)
						if(!scMediaMgr.xSubtitleLoop(vSubsObj,vCurrentTime,dd,pMedia)) break;

				} else if( vRecordedTime > vCurrentTime )
					for(j = j; j >= 0; j-- )
						if(!scMediaMgr.xSubtitleLoop(vSubsObj,vCurrentTime,dd,pMedia)) break;
			}
		}
	},

	xSubtitleLoop : function(pSubsObj, pCurrentTime, pKey, pMedia){
		// Loop sur le tableau des sous-titres pour affichage
		var vSrtTmp = pSubsObj[pSubsObj.lang][pKey];

		// Réinititalisation si pas de vSrtTmp
		if(!vSrtTmp) {
			for(var i in pMedia.fSubs.fSubtitles) pMedia.fSubs.fSubtitles[i].showing = false;
			this.xSwitchClass(pMedia.fSubs.fSubBoxCo, "subDisplay_on", "subDisplay_off", true);
			return;
		}

		var vTimeStart = vSrtTmp.start,
			vTimeEnd = vSrtTmp.end;
			
		if(pCurrentTime > vTimeStart && pCurrentTime < vTimeEnd) {	
			if(pSubsObj.active !== pKey || pSubsObj.showing === false) {	
				pSubsObj.active = pKey;
				if(pSubsObj[pMedia.fSubs.fLang]) this.xShowSub(pMedia.fSubs.fSubHolder, vSrtTmp.text, true); //on
				pSubsObj.showing = true;
				this.xSwitchClass(pMedia.fSubs.fSubBoxCo, "subDisplay_off", "subDisplay_on", true);
			}
			return false;
		} else {
			if(pSubsObj.active === pKey) {
				pSubsObj.showing = false;
				if(pSubsObj[pMedia.fSubs.fLang]) this.xShowSub(pMedia.fSubs.fSubHolder, vSrtTmp.text, false); //off
				this.xSwitchClass(pMedia.fSubs.fSubBoxCo, "subDisplay_on", "subDisplay_off", true);
			}
		}
		return true;
	},

	xSubsAdd : function(pFile, pKey, pSubs) {
		// Chargement des sous-titres
		var vReq = null;
		if ("XMLHttpRequest" in window && (!this.fIsLocal || !("ActiveXObject" in window))) vReq = new XMLHttpRequest();
		else if ("ActiveXObject" in window) vReq = new ActiveXObject("Microsoft.XMLHTTP");
		vReq.open("GET", pFile, true);	
		vReq.onreadystatechange = function () {
			if(vReq.readyState != 4) return;
			if(vReq.status != 0 && vReq.status != 200 && vReq.status != 304) {
				alert("ERROR : unable de retreive "+pFile+": " + vReq.status);
				return;
			}
			pSubs[pKey][pSubs[pKey].lang] = scMediaMgr.xSubsParser(vReq.responseText);
		}
		vReq.send();
	},

	//Parser from http://bubbles.childnodes.com/
	xSubsParser : function(obj) {
		try{
			// Parsage des sous-titres dans un objet
			var fileLines = obj.split('\n'),
				len = fileLines.length - 1,
				ret = [],
				old_int = 0,
				j = 0,
				tmp,
				c,
				str="";
			
			for(var i = 0; i < len; i++) {
				var string = fileLines[i].replace(/^\s+|\s+$/g, "");
				if(!isNaN(string) &&  parseInt(fileLines[i]) === (old_int + 1)) {
					++j;

					old_int = parseInt(fileLines[i]);
					ret[ j ] = [];
				
					tmp = [];
					tmpEnd = [];
					tmp = fileLines[ ++i ].split("-->");

					tmpEnd = tmp[1].split(" ");
					tmp[1] = tmpEnd[1].replace(".",",");
					tmp[0] = tmp[0].replace(".",",");
				
					ret[j]["start"]	= this.xToSecs(tmp[0]);
					ret[j]["end"]	= this.xToSecs(tmp[1]);
					ret[j]["text"]	= "";
					ret[j]["cue"]	= tmpEnd[2]?tmpEnd[2]:"";
				
					c = 0;
					while(fileLines[i + c+1] && fileLines[i + ++c].replace(/^\s+|\s+$/g, "" ) !== "")
						ret[j]["text"] += fileLines[i + c].replace(/\n\r|\r\n|\n|\r/g, "<br />");
				}
			}
			
			//printing the array
			tmp = ret.length;
			str = [];
			for(var i = 1;i < tmp;i++) {
				str[ i - 1 ] = {
					start:	ret[i]["start"],
					end:	ret[i]["end"],
					text: 	ret[i]["text"],
					cue: 	ret[i]["cue"]
				};
			}		
			return str;
		} catch(e){scCoLib.util.log("ERROR - scMediaMgr.xSubsParser : "+e);}
	},

	/* === Utilities ============================================================ */
	/** scMediaMgr.xFormatTime : Format time. */
	xFormatTime : function(pTime) {
		pTime = Number(pTime);
		var h = Math.floor(pTime / 3600);
		var m = Math.floor(pTime % 3600 / 60);
		var s = Math.floor(pTime % 3600 % 60);
		return ((h > 0 ? h + ":" : "") + (m > 0 ? (h > 0 && m < 10 ? "0" : "") + m + ":" : "0:") + (s < 10 ? "0" : "") + s);
	},
	/** scMediaMgr.xToSecs. */
	xToSecs : function(pTime) {
		var vSec = 0.0, vTimeTab = [];
		if (pTime) {
			vTimeTab = pTime.split(':');		
			for (var i = 0; i < vTimeTab.length; i++) vSec = vSec * 60 + parseFloat(vTimeTab[i].replace(',','.'));
		}
		return vSec;
	},

	/** scMediaMgr.xDeserialiseObjJs. */
	xDeserialiseObjJs : function(pStr){
		if(!pStr) return {};
		var vVal;
		eval("vVal="+pStr);
		return vVal;
	},

	/** scMediaMgr.xAddBtn : Add a HTML button to a parent node. */
	xAddBtn : function(pParent, pClassName, pCapt, pTitle, pNxtSib) {
		var vBtn = pParent.ownerDocument.createElement("button");
		vBtn.className = pClassName;
		vBtn.fName = pClassName;
		if (pTitle) vBtn.setAttribute("title", pTitle.replace("&apos;","'"));
		if (pCapt) vBtn.innerHTML = '<img class="btnImg" src="'+this.fSkinRelPath+"/img/player/" + pClassName + '.png"/>';
		vBtn.firstChild.setAttribute("alt", pTitle);
		vBtn.setAttribute("onmouseover", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/" + pClassName + "OVER.png'");
		vBtn.setAttribute("onmouseout", "this.firstChild.src='"+this.fSkinRelPath+"/img/player/" + pClassName + ".png'");
		if (pNxtSib) pParent.insertBefore(vBtn,pNxtSib);
		else pParent.appendChild(vBtn);
		return vBtn;
	},

	/** scMediaMgr.xAddLnk : Add a tag A to a parent node. */
	xAddLnk : function(pParent, pClassName, pCapt, pTitle, pNxtSib) {
		var vBtn = pParent.ownerDocument.createElement("a");
		vBtn.className = pClassName;
		vBtn.fName = pClassName;
		vBtn.href = "#";
		vBtn.target = "_self";
		if (pTitle) vBtn.setAttribute("title", pTitle);
		if (pCapt) vBtn.innerHTML = '<span class="capt">' + pCapt + '</span>';
		if (pNxtSib) pParent.insertBefore(vBtn,pNxtSib);
		else pParent.appendChild(vBtn);
		return vBtn;
	},

	/** scMediaMgr.xSwitchClass - replace a class name. */
	xSwitchClass : function(pNode, pClassOld, pClassNew, pAddIfAbsent, pMatchExact) {
		var vAddIfAbsent = typeof pAddIfAbsent == "undefined" ? false : pAddIfAbsent;
		var vMatchExact = typeof pMatchExact == "undefined" ? true : pMatchExact;
		var vClassName = pNode.className;
		var vReg = new RegExp("\\b"+pClassNew+"\\b");
		if (vMatchExact && vClassName.match(vReg)) return;
		var vClassFound = false;
		if (pClassOld && pClassOld != "") {
			if (vClassName.indexOf(pClassOld)==-1){
				if (!vAddIfAbsent) return;
				else if (pClassNew && pClassNew != '') pNode.className = vClassName + " " + pClassNew;
			} else {
				var vCurrentClasses = vClassName.split(' ');
				var vNewClasses = new Array();
				for (var i = 0, n = vCurrentClasses.length; i < n; i++) {
					var vCurrentClass = vCurrentClasses[i];
					if (vMatchExact && vCurrentClass != pClassOld || !vMatchExact && vCurrentClass.indexOf(pClassOld) != 0) {
						vNewClasses.push(vCurrentClasses[i]);
					} else {
						if (pClassNew && pClassNew != '') vNewClasses.push(pClassNew);
						vClassFound = true;
					}
				}
				pNode.className = vNewClasses.join(' ');
			}
		}
		return vClassFound;
	}
}