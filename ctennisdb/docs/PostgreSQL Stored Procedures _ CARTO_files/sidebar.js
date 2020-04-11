//Guides sidebar
function isGuide(){
    var guideElement = document.getElementsByClassName("docs-guide");
    return (guideElement.length > 0);
}

function isArticle(){
    var guideElement = document.getElementsByClassName("article");
    return (guideElement.length > 0);
}

function isSupport(){
    var supportElement = document.getElementsByClassName("docs-support");
    return (supportElement.length > 0);
}

function isReference(){
    var referenceElement = document.getElementsByClassName("docs-reference");
    return (referenceElement.length > 0);
}

function isOverview(){
    var overviewElement = document.getElementsByClassName("docs-overview");
    return (overviewElement.length > 0);
}

function isReferenceImported(){
    var referenceElement = document.getElementsByClassName("content-reference-imported");
    return (referenceElement.length > 0);
}

function getSidebar(){
    var sidebar = document.getElementsByClassName("js-sidebar");
    return sidebar[0];
}

function getSidebarInner(){
    var sidebar = document.getElementsByClassName("js-sidebar-inner");
    return sidebar[0];
}

function getSelectedElement(){
    var selectedElement = getSidebar().getElementsByClassName("js-selectedElement");
    return selectedElement[0];
}

function createLinkElement(classes, linkURL, text){
    var element = document.createElement("A");
    element.className = classes;
    element.href = linkURL;
    element.title = text;
    var textNode = document.createTextNode(text);
    element.appendChild(textNode);
    return element;
}

function createListElement(child){
    var element = document.createElement("LI");
    element.classList.add("one-line--ellipsis")
    element.appendChild(child);
    return element;
}

function generateSubtitles(tag){
    var subtitles = document.getElementsByTagName(tag);
    var ulSubtitles = document.createElement("UL");
    for(i=0; i < subtitles.length; i++){
        var link = createLinkElement("text is-small is-txtBaseGrey u-lspace--16 js-listElement", "#" + subtitles[i].id, subtitles[i].textContent);
        var listElement = createListElement(link);
        ulSubtitles.appendChild(listElement);
    }
    return ulSubtitles;
}

function addSubelementsSideBar(){
    var subtitlesList = generateSubtitles("h3");
    var list = getSelectedElement().parentElement;
    list.appendChild(subtitlesList);
}

function removeClassFromElements(elements, className){
    for(i = 0; i < elements.length; i++){
        elements[i].classList.remove(className);
    }
}

function currentSection(selectors){
    var subtitles = getSubtitles(selectors);
    var elem = null;
    for(i = 0; i < subtitles.length; i++){
        elem = subtitles[i];
        if(subtitles[i].getBoundingClientRect().top > 200){
            if(i > 0)
                elem = subtitles[i-1];
            else
                elem = subtitles[0];
            break;
        }
    }
    return elem;
}

function selectElement(selectors){
    var section = currentSection(selectors);
    if(!section)
        return null;
    var elemToBold = document.querySelectorAll("a[href='#" + section.id + "']");
    if(!elemToBold[0])
        return null;
    if(!elemToBold[0].classList.contains("is-semibold-selected")){
        elements = document.getElementsByClassName("js-listElement");
        removeClassFromElements(elements, "is-semibold-selected");
    }
    elemToBold[0].classList.add("is-semibold-selected");
}

function getSubtitles(selectors) {
    var subtitles = [];
    for(i = 0; i < selectors.length; i++){
        subtitles.push(getArrayFromSelector(selectors[i]))
    }
    return [].concat.apply([], subtitles)
}

function getArrayFromSelector(selector) {
     //get the NodeList and transform it into an array
     return Array.prototype.slice.call(document.querySelectorAll(selector));
}

function syncronizeSidebarScroll(element) {
    if (element) {
      var topPos = element.offsetTop - 200 ;
      getSidebarInner().scrollTop = topPos;
    }
}

// Open collapsed block when hash or when clicking sidebar item
function openReferenceBlock(hash) {
    var selectedBlock = document.getElementById(hash.replace('#',''));
        elements = document.getElementsByClassName("wrap-block");
        removeClassFromElements(elements, "is-active");
        selectedBlock.closest('.wrap-block').classList.add("is-active")
}

//Fixed Sidebar
if (isGuide() || isSupport() || isReference()){
    window.addEventListener('scroll', function(e) {
        var sidebar = getSidebar();
        if (isReference() && !isReferenceImported()) {
            selectElement([".js-imported-content h2", "h4"]);
        } else {
            selectElement(["h3"]);
        }

        if(sidebar) {
            var paddingTop = parseFloat(window.getComputedStyle(sidebar).getPropertyValue('padding-top'))
            if(sidebar.getBoundingClientRect().top < -paddingTop) {
                sidebar.firstElementChild.classList.add("fixed-top");
                // sidebar.firstElementChild.style.top = "0px";
                if(sidebar.getBoundingClientRect().bottom < window.innerHeight)
                    sidebar.firstElementChild.style.top = (sidebar.getBoundingClientRect().bottom - window.innerHeight) + "px";
            }
            else
                sidebar.firstElementChild.classList.remove("fixed-top");
        }
    });
}


// Syncronize sidebar with scrolling for reference pages
if (isReference() && !isReferenceImported()) {
    window.addEventListener('scroll', function() {
        var currentElement = currentSection(["h2"]);
        var elementSidebar = document.querySelectorAll("a[href='#" + currentElement.id + "']");
        syncronizeSidebarScroll(elementSidebar[0])
    });

    //Open collapsed block when clicking sidebar item
    var listElements = document.getElementsByClassName('js-listElement');
        for(var i =0; i < listElements.length; i++) {
            listElements[i].onclick = function() {
                openReferenceBlock(this.getAttribute('href'));
        };
    }
    //Open collapsed block when hash
    if (window.location.hash && ($(window.location.hash).offset() != undefined)) {
        openReferenceBlock(window.location.hash);
    }


}

//Add tip classes
function getStrongElementsByText(text){
    var strong = document.getElementsByTagName("strong");
    var tipStrong = [];
    for(i = 0; i < strong.length; i++){
        if(strong[i].textContent == text){
            tipStrong.push(strong[i]);
        }
    }
    return tipStrong;
}

function addTipClass(strongElement, classToAdd){
    strongElement.parentElement.classList.add(classToAdd);
    strongElement.parentElement.removeChild(strongElement);
}

function addClassByText(textToFind, classToAdd){
    var tips = getStrongElementsByText(textToFind);
    for(i = 0; i < tips.length; i++){
        addTipClass(tips[i], classToAdd);
    }
}

if(isGuide() || isSupport() || isOverview() || isArticle()){
    addClassByText("Tip:", "Content-tip");
    addClassByText("Note:", "Content-note");
    addClassByText("Warning:", "Content-warning");
    addClassByText("Warning:", "Content-alert");
    if(!isOverview()) {
        addSubelementsSideBar();
    }
}

//Examples selected item

function putActiveElement(){
    var selectedElement = document.querySelectorAll(".js-exampletab[href='" + window.location.hash + "']");
    if(selectedElement.length > 0)
        selectedElement[0].classList.add("is-semibold");
}

window.addEventListener("hashchange", function(e){
    var elements = document.getElementsByClassName("js-exampletab");
    removeClassFromElements(elements, "is-semibold");
    putActiveElement()
});

putActiveElement()

//Fixes tables

function addDivChildsWrapper(element){
    var kids = element.innerHTML;
    var wrapper = '<div class="u-horizontal-scroll">' + kids + "</div>";
    element.innerHTML = wrapper;
}

function wrapElementInDiv(element){
    var parent = element.parentElement;
    var newDiv = document.createElement("div");
    newDiv.classList.add("u-horizontal-scroll");
    var clone = element.cloneNode(true);
    newDiv.appendChild(clone);
    parent.insertBefore(newDiv, element);
    element.remove();
}

var isFirefox = navigator.userAgent.toLowerCase().indexOf('firefox') > -1;

if(isFirefox && isGuide()){
    var td = document.getElementsByTagName("td");
    for(i=0; i<td.length; i++){
        if(td[i].getElementsByTagName("code").length > 0)
            addDivChildsWrapper(td[i]);
    }
}

if(isGuide()){
    var tables = document.getElementsByTagName("table");
    for(i=0; i<tables.length; i++){
        wrapElementInDiv(tables[i]);
    }
}
