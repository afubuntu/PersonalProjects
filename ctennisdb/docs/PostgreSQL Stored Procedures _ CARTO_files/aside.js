/* Build dynamic aside menu for articles */
var $sidebar = $(".Aside-inner"), $window = $(window), offset = $sidebar.offset(), topPadding = 80, $asideindex = $('#Index-aside');
var sections = $('.container').find('h2')
var footer = document.querySelector("footer");
var aside = document.getElementById("Index-aside");
var asideElement = aside.parentElement;
var subnavbar = document.querySelector(".js-subNavbar--fixed");

var getCurrentSection = function(){
  var sect = sections.get();
  var height = window.innerHeight;
  if (sect.length){
    var currentElement = sect[0];
    for(var i = 1; i<sect.length; i++){
      if (sect[i].getBoundingClientRect().top < 100){
        currentElement = sect[i];
      } else{
        break;
      }
    }
  }
  return currentElement;
}

var selectCurrentSection = function() {
  var currentSection = getCurrentSection();
  var sideElement = getSideElement(currentSection);
  var prevSelected = document.querySelector(".Index-listItem.is-LinkDecorated");
  if (prevSelected && (sideElement != prevSelected))
    prevSelected.classList.remove("is-LinkDecorated");
  if (sideElement)
    sideElement.classList.add("is-LinkDecorated");
}

var getSideElement = function(e) {
  if (e && e.id)
  {
    var currentSectionId = e.id;
    var sideElement = document.querySelector(".Index-list a[href^='#"+currentSectionId+"']");
    if (sideElement)
      sideElement = sideElement.parentElement;
  }
  return sideElement;
}

if (sections.length) {
  $(sections).each( function(){
    $('.Index-list').append("<li class='Index-listItem'><a href='"+'#'+$(this).attr('id')+"' class='is-txtBaseGrey'>"+$(this).text()+'</a></li>');
  });
} else {
  $('#Index-aside').remove();
}
selectCurrentSection();

$(document).on('scroll', function(){
  var subnavbarHeight = subnavbar? subnavbar.getBoundingClientRect().height: 0;
  if (asideElement.getBoundingClientRect().bottom < aside.getBoundingClientRect().height + subnavbarHeight){ // Page reached the footer
    aside.style.top = asideElement.getBoundingClientRect().bottom - aside.getBoundingClientRect().height + "px";
  } else if(asideElement.getBoundingClientRect().top <= subnavbarHeight){ //Page in the middle
    aside.style.top = subnavbar ? subnavbar.getBoundingClientRect().height + "px" : "";
    aside.classList.add("is-fixed");
    aside.style.width = asideElement.getBoundingClientRect().width + "px";
  } else { //Page at the begining (top)
    aside.classList.remove("is-fixed");
  }
  selectCurrentSection();
});