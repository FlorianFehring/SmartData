// Backend connection settings
SWAC_config.datasources = [
    {
        url: "/SmartData/smartdata/[iface]/[fromName]",
        interfaces: {
            get: ['GET','records'],
            list: ['GET','records'],
            defs: ['GET','collection'],
            create: ['POST','records'],
            update: ['PUT','records'],
            delete: ['DELETE','records']
        }
    },
    {
        url: "/SmartData/smartdata/[fromName]"
    }
];

// Options for progressive webapp
SWAC_config.progressive = {
    active: false,
    cachetimeout: 30, // Timeout in days after that a reload should be done or unused pages leave the cache
    precache: [
        // List files here that should be available offline for the user after first visit
        SWAC_config.app_root + '/css/global.css',
        SWAC_config.app_root + '/sites/index.html',
        SWAC_config.app_root + '/css/index.css',
        // basic content (design pictures)
        SWAC_config.app_root + '/content/logo.png',
        // default data
        SWAC_config.app_root + '/manifest.json',
        SWAC_config.app_root + '/configuration.js',
        SWAC_config.app_root + '/data/routes.js'
    ],
    // List components here that should be precached
    components: [
        'Navigation'
    ]
};

// OnlineReactions
SWAC_config.onlinereactions = [];
//SWAC_config.onlinereactions[0] = {
//    path: SWAC_config.swac_root + '/swac/components/Upload/UploadOnReact.js',
//    config: {}
//};

SWAC_config.algosources = [];

// Connection timeout in miliseconds
SWAC_config.remoteTimeout = 50000;

/* Language for notifications from SWAC */
SWAC_config.lang = 'de';

/* Frontend behaivior settings */
// Time nodifications should be displayed in miliseconds
SWAC_config.notifyDuration = 5000;

/* Debugging mode for output of SWAC NOTICE and SWAC WARNING messages */
SWAC_config.debugmode = false;

/**
 * Links for footer navigation
 */
var footerlinks = [];
footerlinks[0] = {id: 1, rfrom: "*", rto: "datenschutz.html", name: "Datenschutzerklärung"};
footerlinks[1] = {id: 2, rfrom: "*", rto: "impressum.html", name: "Impressum"};
footerlinks[2] = {id: 3, rfrom: "*", rto: "http://git01-ifm-min.ad.fh-bielefeld.de/Forschung/smartecosystem/smartdata/-/wikis/home", name: "Über SmartData"};