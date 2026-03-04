window.dataLayer = window.dataLayer || [];
window.gtag = window.gtag || function () {
  window.dataLayer.push(arguments);
};

(function loadGtag() {
  var script = document.createElement('script');
  script.async = true;
  script.src = 'https://www.googletagmanager.com/gtag/js?id=G-YDY73GL0WF';
  document.head.appendChild(script);
})();

window.gtag('js', new Date());
window.gtag('config', 'G-YDY73GL0WF');
