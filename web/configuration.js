/* 
 * This is the main configuration file for SWAC documentation.
 * 
 * You should only change values in these file for your setup. No need to
 * modify other files.
 */

/**
 * Configuration for SWAC
 */

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

// Backend connection settings
SWAC_config.datasources = [];
SWAC_config.datasources[0] = SWAC_config.app_root + "/smartdata/[fromName]";

SWAC_config.interfaces = {};
SWAC_config.interfaces.get = 'get';
SWAC_config.interfaces.list = 'list';
//SWAC_config.interfaces.create = '';
SWAC_config.interfaces.update = 'update';
SWAC_config.interfaces.delete = 'delete';
SWAC_config.interfaces.definition = 'definition';
SWAC_config.apicheckup = false;

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
 * SWAC core components can be deactivated if they are not needed.
 * 
 */
// SWAC core components can be deactivated if they are not needed.
SWAC_config.coreComponents = [
    SWAC_config.swac_root + "/swac/Msg.js",
    SWAC_config.swac_root + "/swac/swac.css",
    //TODO move to algorithm as dependency (only load when need)
    SWAC_config.swac_root + "/swac/libs/luxon.min.js",
    //TODO check if this is no longer neccessery
    SWAC_config.swac_root + "/swac/libs/moment-with-locales.min.js",
    SWAC_config.swac_root + "/swac/connectors/remote.js",
    // TODO check if this every time load is neccessery
    SWAC_config.swac_root + "/swac/algorithms/DatatypeReflection.js",
    SWAC_config.swac_root + "/swac/View.js",
    SWAC_config.swac_root + "/swac/Binding.js",
    SWAC_config.swac_root + "/swac/WatchableSet.js",
    SWAC_config.swac_root + "/swac/BindPoint.js",
    SWAC_config.swac_root + "/swac/model.js",
    SWAC_config.swac_root + "/swac/storage.js",
    SWAC_config.swac_root + "/swac/Component.js",
    SWAC_config.swac_root + "/swac/ComponentHandler.js",
    SWAC_config.swac_root + "/swac/ComponentPlugin.js",
    SWAC_config.swac_root + "/swac/ComponentPluginHandler.js",
    //TODO check implementation
    //SWAC_config.swac_root + "/swac/Reactions.js"
    SWAC_config.swac_root + "/swac/OnlineReactions.js",
    SWAC_config.swac_root + "/swac/OnlineReaction.js",
    SWAC_config.swac_root + "/swac/language.js",
    SWAC_config.swac_root + "/swac/langs/de.js",
    //TODO check if uikit dependencies are neccessery (allready loaded by html)
    //SWAC_config.swac_root + "/swac/libs/uikit/js/uikit.min.js",
    //SWAC_config.swac_root + "/swac/libs/uikit/css/uikit.min.css",
    //SWAC_config.swac_root + "/swac/libs/uikit/js/uikit-icons.min.js"
];

/**
 * Links for footer navigation
 */
var footerlinks = [];
footerlinks[0] = {id: 1, rfrom: "*", rto: "datenschutz.html", name: "Datenschutzerklärung"};
footerlinks[1] = {id: 2, rfrom: "*", rto: "impressum.html", name: "Impressum"};
footerlinks[2] = {id: 3, rfrom: "*", rto: "haftung.html", name: "Haftungsausschluss"};
footerlinks[3] = {id: 4, rfrom: "*", rto: "http://git01-ifm-min.ad.fh-bielefeld.de/scl/2015_03_SCL_SmartMonitoring_Frontend/wikis/home", name: "Über SmartMonitoring"};