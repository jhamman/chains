import glob
import os
from tools.utilities import make_case_readme


configfile: "/glade/u/home/jhamman/projects/storylines/storylines_workflow/config.yml"


case_dirs = ['configs', 'disagg_data', 'hydro_data', 'routing_data', 'logs']


def get_year_range(years):
    return list(range(years['start'], years['stop'] + 1))


def inverse_lookup(d, key):
    for k, v in d.items():
        if v == key:
            return k
    raise KeyError(key)


def maybe_make_yaml_list(obj):
    if isinstance(obj, str) or not hasattr(obj, '__iter__'):
        return obj
    return '[%s]' % ', '.join('"{0}"'.format(l) for l in obj)

def vic_forcings(wcs):
    return [os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}', 'disagg_data',
                         '{disagg_method}.force_{gcm}_{scen}_{dsm}_{year}0101-{year}1231.nc').format(
                year=year, gcm=wcs.gcm, scen=wcs.scen, dsm=wcs.dsm, disagg_method='metsim')
            for year in get_year_range(config['SCEN_YEARS'][wcs.scen])]


def metsim_forcings(wcs):
    gcm = inverse_lookup(config['DOWNSCALING'][wcs.dsm]['gcms'], wcs.gcm)

    if wcs.dsm == 'bcsd' and wcs.scen == 'hist':
        scen = 'rcp45'
    else:
        scen = wcs.scen

    files = glob.glob(config['DOWNSCALING'][wcs.dsm]['data'].format(
        gcm=gcm, scen=scen, year=wcs.year))
    return files

def metsim_state(wcs):
    gcm = inverse_lookup(config['DOWNSCALING'][wcs.dsm]['gcms'], wcs.gcm)
    year = int(wcs.year)
    scen = wcs.scen
    if year == 2006:
        year -= 1
        scen = 'hist'
    if wcs.dsm == 'bcsd' and scen == 'hist':
        scen = 'rcp45'

    pattern = config['DOWNSCALING'][wcs.dsm]['data'].format(
        gcm=gcm, scen=scen, year=year)
    files = glob.glob(pattern)
    return files

# Workflow bookends
onsuccess:
    print('Workflow finished, no error')

onerror:
    print('An error occurred')

onstart:
    print('starting now')


# Rules
# -----------------------------------------------------------------------------
localrules: readme, setup_casedirs, hydrology_models, config_vic, disagg_methods, config_metsim

# readme / logs
rule readme:
    input:
        expand(os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                            'readme.md'),
               gcm=config['GCMS'], scen=config['SCENARIOS'],
               dsm=config['DOWNSCALING_METHODS'])

rule setup_casedirs:
    output: os.path.join(os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}', 'readme.md'))
    run:
        case_dir = os.path.dirname(output[0])
        for d in case_dirs:
            os.makedirs(os.path.join(case_dir, d), exist_ok=True)

        make_case_readme(wildcards,
                         os.path.join(case_dir, 'readme.md'),
                         disagg_methods=config['DISAGG_METHODS'],
                         hydro_methods=config['HYDRO_METHODS'],
                         routing_methods=config['ROUTING_METHODS'])


# Hydrologic Models
rule hydrology_models:
    input:
        expand(os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                            'hydro_data',  '{model}_{disagg_method}_hist_{outstep}.nc'),
              gcm=config['GCMS'], scen=config['SCENARIOS'],
              disagg_method=config['DISAGG_METHODS'],
              dsm=config['DOWNSCALING_METHODS'], model=config['HYDRO_METHODS'],
              outstep=['daily', 'monthly'])


rule config_vic:
    output:
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'configs', 'vic.global_param.{gcm}.{scen}.{dsm}.txt')
    shell:
        "touch {output}"


rule run_vic:
    input:
        vic_forcings,
        config = os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                              'configs',
                              'vic.global_param.{gcm}.{scen}.{dsm}.txt')

    output:
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'hydro_data',  'vic_{disagg_method}_hist_daily.nc'),
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'hydro_data',  'vic_{disagg_method}_hist_monthly.nc')
    shell:
        "touch {output}"


rule run_prms:
    output:
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'hydro_data',  'prms_{disagg_method}_hist_{outstep}.nc')
    shell:
        "touch {output}"


rule run_fuse:
    output:
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'hydro_data',  'fuse_{disagg_method}_hist_{outstep}.nc')
    shell:
        "touch {output}"


rule run_summa:
    output:
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'hydro_data',  'summa_{disagg_method}_hist_{outstep}.nc')
    shell:
        "touch {output}"


# Disaggregation methods
rule disagg_methods:
    input:
        [expand(os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}', 'disagg_data',
                             '{disagg_method}.force_{gcm}_{scen}_{dsm}_{year}0101-{year}1231.nc'),
                gcm=config['GCMS'], scen=scen,
                dsm=config['DOWNSCALING_METHODS'],
                disagg_method=config['DISAGG_METHODS'],
                year=get_year_range(config['SCEN_YEARS'][scen]))
         for scen in config['SCENARIOS']]


rule run_metsim:
    input:
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'configs', 'metsim_{gcm}_{scen}_{dsm}_{year}.cfg'),
        forcing = metsim_forcings,
        state = metsim_state
    output:
        os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                     'disagg_data',  'metsim.force_{gcm}_{scen}_{dsm}_{year}0101-{year}1231.nc')
    shell:
        "echo {input}; touch {output}"


# "ms -n 10 {input.config}"


rule config_metsim:
    input:
        config['DISAGG']['metsim']['subdaily']['template'],
        forcing = metsim_forcings,
        state = metsim_state
    output:
        config = os.path.join(config['workdir'], '{gcm}', '{scen}', '{dsm}',
                              'configs', 'metsim_{gcm}_{scen}_{dsm}_{year}.cfg')
    run:
        data_dir = os.path.dirname(output.config).replace('configs', 'disagg_data')
        out_state = os.path.join(data_dir, 'metsim.state_{gcm}_{scen}_{dsm}_{year}1231.nc'.format(**wildcards))

        prefix = 'metsim.force_{gcm}_{scen}_{dsm}.'.format(**wildcards)

        with open(input[0], 'r') as f:
            template = f.read()
        with open(output.config, 'w') as f:
            f.write(template.format(forcing=maybe_make_yaml_list(input.forcing),
                                    metsim_dir=data_dir,
                                    prefix=prefix,
                                    input_state=maybe_make_yaml_list(input.state),
                                    output_state=out_state,
                                    out_vars=config['DISAGG']['metsim']['subdaily']['out_vars'],
                                    **wildcards))
    # shell:
    #     "touch {output}"
