document.addEventListener("DOMContentLoaded", function() {

  var articleShow = document.querySelectorAll('.js-articlesLabel');

  for (var i = 0; i < articleShow.length; i++) {
    articleShow[i].addEventListener('click', function (event) {

      this.classList.toggle('is-active');

      if (this.classList.contains('is-active')) {
        this.querySelector("span").textContent = "See less"
      } else {
        this.querySelector("span").textContent = "See more"
      }

      var active = this.parentNode.querySelector(".landing-list");
      active.classList.toggle('is-disabled');

    }, false);
  }
});

var Hangar = window.HangarAlpha.Components;

function initNavbar(){
  var navbar = new Hangar.Navbar();
  var header = document.getElementsByClassName("js-Header")[0];
  if (!header)
    header = document.getElementsByClassName("Header")[0]
  this.navbarFixed = new Hangar.NavbarFixed({
      el: document.getElementsByClassName("js-subNavbar--fixed"),
      $header: header,
      after: true
  });
}

function initDropdowns(){
  dropdowns = document.getElementsByClassName("js-Dropdown");
  for (var i = 0; i < dropdowns.length; i++){
      new Hangar.Dropdown({
          el: dropdowns[i]
      })
  }
}

function offsetAnchor() {
  var width = (window.innerWidth > 0) ? window.innerWidth : screen.width;
  if(location.hash.length !== 0 && width > 800) {
    window.scrollTo(window.scrollX, window.scrollY - 65);
  }
}

window.addEventListener("hashchange", offsetAnchor);

initNavbar();
initDropdowns();
offsetAnchor();

window.onload = function () {
  offsetAnchor();
}