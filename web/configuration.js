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
SWAC_config.progressive = {};
SWAC_config.progressive.active = false;
SWAC_config.progressive.cachetimeout = 30; // Timeout in days after that a reload should be done or unused pages leave the cache
SWAC_config.progressive.precache = [];
// List files here that should be available offline for the user after first visit
SWAC_config.progressive.precache[0] = SWAC_config.app_root + '/css/global.css';
SWAC_config.progressive.precache[1] = SWAC_config.app_root + '/sites/index.html';
SWAC_config.progressive.precache[2] = SWAC_config.app_root + '/css/index.css';
// basic content (design pictures)
SWAC_config.progressive.precache[3] = SWAC_config.app_root + '/content/header.jpg';
SWAC_config.progressive.precache[4] = SWAC_config.app_root + '/content/logo.jpg';
// default data
SWAC_config.progressive.precache[5] = SWAC_config.app_root + '/manifest.json';
SWAC_config.progressive.precache[6] = SWAC_config.app_root + '/configuration.js';
SWAC_config.progressive.precache[7] = SWAC_config.app_root + '/data/routes.js';

// Used components
SWAC_config.progressive.components = [];
SWAC_config.progressive.components[0] = 'Navigation';

// OnlineReactions
SWAC_config.onlinereactions = [];
//SWAC_config.onlinereactions[0] = {
//    path: SWAC_config.swac_root + '/swac/components/Upload/UploadOnReact.js',
//    config: {}
//};

// Backend connection settings
SWAC_config.datasources = [];
/*SWAC_config.datasources[0] = "/SWAC/data/[fromName]";
SWAC_config.datasources[1] = "/SmartMonitoringBackend/[fromName]";*/
SWAC_config.datasources[0] = "/SmartData/smartdata/[fromName]";

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
SWAC_config.debugmode = true;

/* Hint mode gives you usefull tipps for useing swac */
SWAC_config.hintmode = true;

/**
 * SWAC core components can be deactivated if they are not needed.
 * 
 */
SWAC_config.coreComponents = [];
SWAC_config.coreComponents[0] = SWAC_config.swac_root + "/swac/debug.js";
SWAC_config.coreComponents[1] = SWAC_config.swac_root + "/libs/uikit/js/uikit.min.js";
SWAC_config.coreComponents[2] = SWAC_config.swac_root + "/libs/uikit/css/uikit.min.css";
SWAC_config.coreComponents[3] = SWAC_config.swac_root + "/libs/uikit/js/uikit-icons.min.js";
SWAC_config.coreComponents[4] = SWAC_config.swac_root + "/swac/libs/moment-with-locales.min.js";
SWAC_config.coreComponents[42] = SWAC_config.swac_root + "/swac/libs/luxon.min.js";
SWAC_config.coreComponents[5] = SWAC_config.swac_root + "/swac/connectors/remote.js";
SWAC_config.coreComponents[6] = SWAC_config.swac_root + "/swac/algorithms/DatatypeReflection.js";
SWAC_config.coreComponents[7] = SWAC_config.swac_root + "/swac/View.js";
SWAC_config.coreComponents[8] = SWAC_config.swac_root + "/swac/Binding.js";
SWAC_config.coreComponents[9] = SWAC_config.swac_root + "/swac/WatchableSet.js";
SWAC_config.coreComponents[10] = SWAC_config.swac_root + "/swac/BindPoint.js";
SWAC_config.coreComponents[11] = SWAC_config.swac_root + "/swac/model.js";
SWAC_config.coreComponents[12] = SWAC_config.swac_root + "/swac/storage.js";
SWAC_config.coreComponents[13] = SWAC_config.swac_root + "/swac/Component.js";
SWAC_config.coreComponents[14] = SWAC_config.swac_root + "/swac/ComponentHandler.js";
SWAC_config.coreComponents[15] = SWAC_config.swac_root + "/swac/ComponentPlugin.js";
SWAC_config.coreComponents[16] = SWAC_config.swac_root + "/swac/ComponentPluginHandler.js";
//SWAC.coreComponents[14] = SWAC_config.swac_root + "/swac/Reactions.js";
SWAC_config.coreComponents[18] = SWAC_config.swac_root + "/swac/OnlineReactions.js";
SWAC_config.coreComponents[19] = SWAC_config.swac_root + "/swac/OnlineReaction.js";
SWAC_config.coreComponents[20] = SWAC_config.swac_root + "/swac/language.js";
SWAC_config.coreComponents[21] = SWAC_config.swac_root + "/swac/langs/de.js";
SWAC_config.coreComponents[22] = SWAC_config.swac_root + "/swac/swac.css";


/**
 * Options for swac_user component
 * Used on every page
 */
var user_options = {
    mode: 'form',
    loginurl: '../data/user/exampleuserdata.json',
    afterLoginLoc: '../sites/user_example1.html',
    afterLogoutLoc: '../sites/user.html'
};
user_options.loggedinRedirects = new Map();
user_options.loggedinRedirects.set('user_example3.html','../sites/user_example2.html');

/**
 * Links for footer navigation
 */
var footerlinks = [];
footerlinks[0] = {id: 1, rfrom: "*", rto: "datenschutz.html", name: "Datenschutzerklärung"};
footerlinks[1] = {id: 2, rfrom: "*", rto: "impressum.html", name: "Impressum"};
footerlinks[2] = {id: 3, rfrom: "*", rto: "haftung.html", name: "Haftungsausschluss"};
footerlinks[3] = {id: 4, rfrom: "*", rto: "http://git01-ifm-min.ad.fh-bielefeld.de/scl/2015_03_SCL_SmartMonitoring_Frontend/wikis/home", name: "Über SmartMonitoring"};