window['select_target_options'] = {
    showWhenNoData: true,
    onChange: function (evt) {
        // You are inside the component here
        let inputs = this.getInputs();
        let source;
        for (let curInput of inputs) {
            source = curInput.name;
        }
        window['adm_presets_options'].targetSource = {
            url: '/' + source + '/smartdata/[iface]/[fromName]?storage=smartmonitoring',
            interfaces: {
                get: ['GET', 'records'],
                list: ['GET', 'records'],
                defs: ['GET', 'collection'],
                cdefs: ['POST', 'collection'],
                create: ['POST', 'records'],
                update: ['PUT', 'records'],
                delete: ['DELETE', 'records']
            }
        };

        // Reload presets component
        let presetsElem = document.querySelector('#adm_presets');
        presetsElem.swac_comp.reload();
    }
};
window['adm_presets_options'] = {
    showWhenNoData: true
};

