from storylines.tools.gard_utils import read_config

POST_PROCESS_TEMPLATE = '/glade/scratch/jhamman/GARD_downscaling_EGU_20180402/post_processed/gard_output.{setname}.NCAR_WRF_50km.{gcm}.{scen}.{date_range}.dm.nc'
QM_TEMPLATE = '/glade/scratch/jhamman/GARD_downscaling_EGU_20180402/post_processed/gard_output.{setname}.NCAR_WRF_50km.{gcm}.{scen}.{date_range}.dm.qm.nc'


scen_range = {'hist': '19510101-20051231',
              'rcp45': '20060101-20991231', 'rcp85': '20060101-20991231'}
sets = ['pass_through', 'pure_regression_0',
        'analog_regression_2', 'analog_regression_3']
gcms = ['access13', 'canesm', 'cesm', 'gfdl',
        'giss', 'miroc5', 'mri', 'noresm']


rule config_gard:
    output: GARD_CONFIG
    run:

        import f90nml



        nml = dict(
            parameters=dict(
                output_file=GARD_OUTFILE.format(**wildcards),
                start_date=start_date,
                end_date=end_date,
                start_train=start_train,
                end_train=end_train,
                start_transform=start_transform,
                end_transform=end_transform,
                start_post=start_post,
                end_post=end_post,
                pure_regression=pure_regression,
                pure_analog=pure_analog,
                analog_regression=analog_regression,
                pass_through=pass_through,
                pass_through_var=pass_through_var,
                n_analogs=n_analogs,
                sample_analog=sample_analog,
                logistic_from_analog_exceedance=logistic_from_analog_exceedance,
                logistic_threshold=logistic_threshold,
                weight_analogs=weight_analogs,
                post_correction_transform=post_correction_transform,
                debug=debug,
                interactive=interactive),
            training_parameters=dict(
                name=training_name,
                interpolation_method=interpolation_method,
                nvars=len(train_vars),
                data_type="GCM",
                lat_name="XLAT",
                lon_name="XLONG",
                time_name="XTIME",
                nfiles=30,
                selected_level=-1,
                normalization_method=1,
                input_transformations=(0, 0, 3),
                var_names=train_vars,
                file_list="filelists/erai_30yr.txt",
                calendar="gregorian",
                calendar_start_year=1900,
                timezone_offset=0),
            prediction_parameters=dict(
                name=prediction_name,
                interpolation_method=interpolation_method,
                nvars=len(predict_vars),
                data_type="GCM",
                lat_name="XLAT",
                lon_name="XLONG",
                time_name="XTIME",
                nfiles=96,
                input_transformations=3,
                transformations=1,
                normalization_method=2,
                var_names="PREC_TOT",
                file_list="filelists/canesm_full.txt",
                calendar="gregorian",
                calendar_start_year=1900,
                timezone_offset=0),
            obs_parameters=dict(
                name=obs_name,
                nvars=len(obs_vars),
                nfiles=1,
                data_type="obs",
                lat_name=obs_lat_name,
                lon_name=obs_lon_name,
                time_name=obs_time_name,
                input_transformations=obs_input_transforms,
                var_names="pcp",
                file_list="filelists/ens_obs_files.txt",
                calendar="gregorian",
                calendar_start_year=1900,
                mask_variable=1,
                mask_value=-999)
        )

        f90nml.write(nml, output[0])


rule post_process_gard_output:
    output:
        POST_PROCESS_TEMPLATE.replace(
            '{scen}.{date_range}', 'hist.19510101-20051231'),
        POST_PROCESS_TEMPLATE.replace(
            '{scen}.{date_range}', 'rcp45.20060101-20991231'),
        POST_PROCESS_TEMPLATE.replace(
            '{scen}.{date_range}', 'rcp85.20060101-20991231')
    threads: 36
    run:

        from storylines.tools.post_process_gard_output import run
        from dask.distributed import Client

        gard_config = read_config(GARD_CONFIG)
        # if threads > 1:
        #     client = Client(n_workers=threads, processes=False)

        run(GARD_CONFIG, gcms=[wildcards.gcm], sets=[wildcards.setname],
            return_processed=False)

rule quantile_map_gard:
    output:
        QM_TEMPLATE
    input:
        data_file = POST_PROCESS_TEMPLATE,
        ref_file = POST_PROCESS_TEMPLATE.replace(
            '{scen}.{date_range}', 'hist.19510101-20051231'),
        obs_files = [
            '/glade/scratch/jhamman/GARD_inputs/newman_ensemble/conus_ens_00%d.nc' % i for i in range(1, 6)]
    run:
        from storylines.tools.quantile_mapping import run

        run(input.data_file, input.ref_file, input.obs_files,
            'gard', ['pcp', 't_mean', 't_range'])


rule all_qm:
    input:
        [expand(QM_TEMPLATE,
                setname=sets, gcm=gcms, scen=scen, date_range=date_range)
         for scen, date_range in scen_range.items()]


rule all:
    input:
        [expand(POST_PROCESS_TEMPLATE,
                setname=sets, gcm=gcms, scen=scen, date_range=date_range)
         for scen, date_range in scen_range.items()]
