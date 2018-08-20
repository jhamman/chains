localrules: config_mizuroute


def mizuroute_init_state(wcs):
    if wcs.scen in ['hist', 'obs_hist']:
        state = NULL_STATE
    else:
        kwargs = dict(wcs)
        kwargs['scen'] = 'hist'
        kwargs['date'] = '{:4d}-12-31-82800'.format(
            config['SCEN_YEARS'][wcs.scen]['start'])
        state = MIZUROUTE_STATE.format(**kwargs)
    return state


rule config_mizuroute:
    input:
        route_forcings,
        template = mizuroute_template,
        in_state = mizuroute_init_state
    output:
        config = MIZUROUTE_CONFIG
    run:

        options = dict(config['ROUTING']['mizuroute'])

        # The commented out lines will come from a config file
        # options['ancil_dir'] = 'TODO'
        options['input_dir'] = 'TODO'
        options['output_dir'] = 'TODO'
        options['startyear'] = 'TODO'
        options['endyear'] = 'TODO'
        # options['fname_ntopOld'] = 'TODO'
        # options['fname_ntopNew'] = 'TODO'
        options['fname_qsim'] = 'TODO'
        options['fname_output'] = 'TODO'
        options['fname_state_in'] = in_state
        options['fname_state_out'] = 'TODO'
        # options['param_nml'] = 'TODO'

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(**options, **wildcards))

rule run_mizutroute:
    input:
        route_forcings,
        mizuroute_init_state,
        mizuroute_exe = route_executable
        config = MIZUROUTE_CONFIG
    output:
        ROUTE_OUTPUT
        state = MIZUROUTE_STATE
    log:
        NOW.strftime(ROUTE_LOG)
    run:
        # run mizuroute
        shell("{input.mizuroute_exe} {input.config} > {log} 2>&1")
