// Backend connection settings
var SWAC_config = {
    lang: 'de',
    notifyDuration: 5000,
    remoteTimeout: 50000,
    debugmode: false,
    debug: 'all',
    datasources: [
        {
            url: "[fromName]"
        },
        {
            url: "../[fromName]"
        }
    ],
    progressive: {
        active: false,
        cachetimeout: 30, // Timeout in days after that a reload should be done or unused pages leave the cache
        precache: [
            // List files here that should be available offline for the user after first visit
            '/css/global.css',
            '/sites/index.html',
            '/css/index.css',
            // basic content (design pictures)
            '/content/logo.png',
            // default data
            '/manifest.json',
            '/configuration.js',
            '/data/routes.js'
        ],
        // List components here that should be precached
        components: [
            'Navigation'
        ]
    },
    onlinereactions: []
};

/**
 * Links for footer navigation
 */
var footerlinks = [];
footerlinks[0] = {id: 1, rfrom: "*", rto: "datenschutz.html", name: "Datenschutzerklärung"};
footerlinks[1] = {id: 2, rfrom: "*", rto: "impressum.html", name: "Impressum"};
footerlinks[2] = {id: 3, rfrom: "*", rto: "http://git01-ifm-min.ad.fh-bielefeld.de/Forschung/smartecosystem/smartdata/-/wikis/home", name: "Über SmartData"};
