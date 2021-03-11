window.onload = function() {
  // Get application name
  let pathname = window.location.pathname.replace('/','').replace('/','');
  let swaggerlink = document.querySelector('.smartdata_swaggerlink');
  let href = swaggerlink.getAttribute('href');
  swaggerlink.setAttribute('href',href + '?selectapi=' + pathname);
  
  let erimgElem = document.querySelector('.smartdata_er');
  erimgElem.src = erimgElem.src + '/' + pathname + '.png';
};