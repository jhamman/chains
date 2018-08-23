
# def mizuroute_init_state(wcs):
#     if wcs.scen in ['hist', 'obs_hist']:
#         state = NULL_STATE
#     else:
#         kwargs = dict(wcs)
#         kwargs['scen'] = 'hist'
#         kwargs['date'] = '{:4d}-12-31-82800'.format(
#             config['SCEN_YEARS'][wcs.scen]['start'])
#         state = MIZUROUTE_STATE.format(**kwargs)
#     return state


def routing_template(wcs):
    return config['ROUTING']['mizuroute']['template']


# route_forcings,
# template = mizuroute_template,
# in_state = mizuroute_init_state
rule config_mizuroute:
    output:
        config = MIZUROUTE_CONFIG
    input:
        template = routing_template
    run:
        options = dict(config['ROUTING']['mizuroute'])

        instate = 'place_holder_in_state'

        # The commented out lines will come from a config file
        # options['ancil_dir'] = 'TODO'
        options['input_dir'] = HYDRO_OUTPUT.format(**wildcards)
        options['output_dir'] = ROUTE_OUT_DIR.format(**wildcards)
        options['startyear'] = config['SCEN_YEARS'][wildcards.scen]['start']
        options['endyear'] = config['SCEN_YEARS'][wildcards.scen]['stop']
        # options['fname_ntopOld'] = 'TODO'
        # options['fname_ntopNew'] = 'TODO'
        options['fname_qsim'] = 'TODO'
        options['fname_output'] = 'TODO'
        options['fname_state_in'] = 'TODO'
        options['fname_state_out'] = 'TODO'
        # options['param_nml'] = 'TODO'

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(**options, **wildcards))

# rule run_mizutroute:
#     input:
#         route_forcings,
#         mizuroute_init_state,
#         mizuroute_exe = route_executable,
#         config = MIZUROUTE_CONFIG
#     output:
#         ROUTE_OUTPUT,
#         state = MIZUROUTE_STATE
#     log:
#         NOW.strftime(ROUTE_LOG)
#     run:
#         # run mizuroute
#         shell("{input.mizuroute_exe} {input.config} > {log} 2>&1")
