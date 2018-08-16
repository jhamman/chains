
localrules: config_metsim


def metsim_state(wcs):
    year = int(wcs.year)
    scen = wcs.scen
    if 'rcp' in scen and year == 2006:
        scen = 'hist'
    if wcs.dsm == 'bcsd' and scen == 'hist' and year == 2006:
        scen = 'rcp45'

    state = DOWNSCALING_DATA.format(dsm=wcs.dsm, gcm=wcs.gcm, scen=scen,
                                    year=year - 1)

    return state


def maybe_make_cfg_list(obj):
    if isinstance(obj, str) or not hasattr(obj, '__iter__'):
        return obj
    elif len(obj) == 1:
        return obj[0]
    return '%s' % ', '.join(obj)


rule run_metsim:
    input:
        readme = README,
        config = DISAGG_CONFIG,
        forcing = DOWNSCALING_DATA,
        state = metsim_state
    output: DISAGG_OUTPUT
    log: NOW.strftime(DISAGG_LOG)
    threads: 36
    # conda: "envs/metsim.yaml"
    # benchmark: BENCHMARK
    # TODO: remove exe path
    shell: "/glade/u/home/jhamman/anaconda/envs/storylines/bin/ms -n {threads} {input.config} > {log} 2>&1"

rule config_metsim:
    input:
        README,
        config['DISAGG']['metsim']['template'],
        forcing = DOWNSCALING_DATA,
        state = metsim_state
    output:
        config = DISAGG_CONFIG
    run:
        log_to_readme('Configuring MetSim for %s' % str(wildcards), input[0])

        data_dir = DISAGG_DIR.format(**wildcards)
        out_state = METSIM_STATE.format(**wildcards)

        prefix = DISAGG_PREFIX.format(**wildcards)

        time_step = int(wildcards.disagg_ts)
        out_vars = config['DISAGG']['metsim'][time_step]['out_vars']
        if wildcards.dsm in config['DOWNSCALING']:
            in_vars = config['DOWNSCALING'][wildcards.dsm]['variables']
        else:
            in_vars = config['OBS_FORCING'][wildcards.dsm]['variables']

        forcing = maybe_make_cfg_list(input.forcing)
        input_state = maybe_make_cfg_list(input.state)
        domain = config['domain']

        with open(input[1], 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(forcing=forcing,
                                    metsim_dir=data_dir,
                                    prefix=prefix,
                                    input_state=input_state,
                                    output_state=out_state,
                                    out_vars=out_vars,
                                    domain=domain,
                                    time_step=time_step,
                                    **wildcards, **in_vars))
