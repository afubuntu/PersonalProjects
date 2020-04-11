/* === MathJax manager =============================================== */
var mathjaxMgr = {

	init : function(){
		var vScript = document.createElement("script");
		vScript.type = "text/javascript";
		vScript.src  = scServices.scLoad.getRootUrl()+"/wdgt/mathjax/MathJax.js?locale=fr";

		var vConfig = 'MathJax.Hub.Config({';
		vConfig +=    'jax: ["input/TeX","output/SVG"],';
		vConfig +=    'extensions: ["tex2jax.js","MathMenu.js","MathZoom.js"],';
		vConfig +=    'imageFont: null,';
		vConfig +=    'webFont: "TeX",';
		vConfig +=    'MathMenu: {showLocale: false, showRenderer: false},';
		vConfig +=    'TeX: {extensions:["AMSmath.js","AMSsymbols.js","noErrors.js","noUndefined.js","mhchem.js"]},';
		vConfig +=    'menuSettings: { zoom: "Hover", zscale: "200%"},';
		vConfig +=    'MathEvents: { hover: 400 }';
		vConfig +=    '});';

		if (window.opera) {vScript.innerHTML = vConfig}
		else {vScript.text = vConfig}

		document.getElementsByTagName("head")[0].appendChild(vScript);
	}
}


