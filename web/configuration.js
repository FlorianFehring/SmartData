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
        active: false
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
