import os
import shutil


localrules: config_vic


def vic_init_state(wcs):
    if wcs.scen in ['hist', 'obs_hist']:
        state = NULL_STATE
    else:
        kwargs = dict(wcs)
        kwargs['scen'] = 'hist'
        kwargs['date'] = '{:4d}-12-31-82800'.format(
            config['SCEN_YEARS'][wcs.scen]['start'])
        state = VIC_STATE.format(**kwargs)
    return state


rule rename_hydro_forcings_for_vic:
    input: DISAGG_OUTPUT
    output: temp(VIC_FORCING)
    shell:
        "module load nco && time ncks --no_tmp_fl --cnk_dmn time,1 --cnk_dmn lat,224 --cnk_dmn lon,464 {input} {output}"


rule config_vic:
    input:
        hydro_forcings,
        template = hydro_template,
        in_state = vic_init_state
    output:
        config = HYDRO_CONFIG.replace('{model_id}', 'vic')
    run:
        options = dict(config['HYDROLOGY'][wildcards.model])
        options['out_prefix'] = VIC_OUT_PREFIX.format(**wildcards)
        options['startyear'] = config['SCEN_YEARS'][wildcards.scen]['start']
        options['startmonth'] = '01'
        options['startday'] = '01'
        options['endyear'] = config['SCEN_YEARS'][wildcards.scen]['stop']
        options['endmonth'] = '12'
        options['endday'] = '31'
        options['calendar'] = 'standard'  # TODO: read from metsim output
        options['domain'] = config['domain']

        options['out_state'] = VIC_STATE_PREFIX.format(**wildcards)
        options['stateyear'] = options['endyear']
        options['statemonth'] = options['endmonth']
        options['stateday'] = options['endday']
        options['forcings'] = VIC_FORCING.format(
            year='',
            disagg_ts=config['HYDROLOGY'][wildcards.model]['force_timestep'],
            **wildcards)[:-3]

        options['result_dir'] = HYDRO_OUT_DIR.format(**wildcards)

        if wildcards.scen in ['hist', 'obs_hist']:
            options['init_state'] = ''
        else:
            kwargs = dict(wildcards)
            kwargs['scen'] = 'hist'
            options['init_state'] = 'INIT_STATE ' + input.in_state.format(
                **kwargs)

        with open(input.template, 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(**options, **wildcards))


rule run_vic:
    input:
        hydro_forcings,
        vic_init_state,
        vic_exe = hydro_executable,
        config = HYDRO_CONFIG.replace('{model_id}', 'vic')
    output:
        HYDRO_OUTPUT.replace('{model_id}', 'vic').replace(
            '{outstep}', 'daily'),
        HYDRO_OUTPUT.replace('{model_id}', 'vic').replace(
            '{outstep}', 'monthly'),
        state = VIC_STATE
    threads: 36
    # benchmark: BENCHMARK
    # log:
    #     NOW.strftime(HYDRO_LOG.replace('{model_id}', 'vic'))
    run:
        # run VIC
        # TODO: the -n 36 should go away at some point
        shell("module load intel impi netcdf && mpirun -n {threads} {input.vic_exe} -g {input.config}")

        # rename output files
        for freq, end in [('daily', '.daily.{:4d}-01-01.nc'),
                          ('monthly', '.monthly.{:4d}-01.nc')]:
            dirname = HYDRO_OUT_DIR.format(**wildcards)
            suffix = end.format(config['SCEN_YEARS'][wildcards.scen]['start'])
            infile = os.path.join(dirname, VIC_OUT_PREFIX.format(
                model_id='vic', **wildcards) + suffix)
            outfile = HYDRO_OUTPUT.format(model_id='vic', outstep=freq,
                                          **wildcards)
            # rename
            # print('%s-->%s' % (infile, outfile), flush=True)
            shutil.move(infile, outfile)

        # rename output state (just dropping the date-string)
        try:
            infile = glob.glob(VIC_STATE_PREFIX.format(**wildcards) + '*.nc')[0]
        except IndexError:
            raise IndexError(VIC_STATE_PREFIX.format(**wildcards) + '*.nc',
                             'failed to return a valid state file')
        outfile = output.state
        # print('%s-->%s' % (infile, outfile), flush=True)
        shutil.move(infile, outfile)
