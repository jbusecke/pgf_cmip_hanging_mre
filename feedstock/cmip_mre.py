from dataclasses import dataclass

import apache_beam as beam
import xarray as xr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    Indexed,
    T,
)

@dataclass
class Preprocessor(beam.PTransform):
    """
    Preprocessor for xarray datasets.
    Set all data_variables except for `variable_id` attrs to coord
    Add additional information

    """

    @staticmethod
    def _keep_only_variable_id(item: Indexed[T]) -> Indexed[T]:
        """
        Many netcdfs contain variables other than the one specified in the `variable_id` facet.
        Set them all to coords
        """
        index, ds = item
        print(f'Preprocessing before {ds =}')
        new_coords_vars = [var for var in ds.data_vars if var != ds.attrs['variable_id']]
        ds = ds.set_coords(new_coords_vars)
        print(f'Preprocessing after {ds =}')
        return index, ds

    @staticmethod
    def _sanitize_attrs(item: Indexed[T]) -> Indexed[T]:
        """Removes non-ascii characters from attributes see https://github.com/pangeo-forge/pangeo-forge-recipes/issues/586"""
        index, ds = item
        for att, att_value in ds.attrs.items():
            if isinstance(att_value, str):
                new_value = att_value.encode('utf-8', 'ignore').decode()
                if new_value != att_value:
                    print(
                        f'Sanitized datasets attributes field {att}: \n {att_value} \n ----> \n {new_value}'
                    )
                    ds.attrs[att] = new_value
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (
            pcoll
            | 'Fix coordinates' >> beam.Map(self._keep_only_variable_id)
            | 'Sanitize Attrs' >> beam.Map(self._sanitize_attrs)
        )

iid = 'CMIP6.CMIP.CMCC.CMCC-ESM2.historical.r1i1p1f1.3hr.pr.gn.v20210114'

urls = [
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_185001010130-185412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_185501010130-185912312230.nc',
]
pattern = pattern_from_file_sequence(urls, concat_dim='time')

urls_full = [
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_185001010130-185412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_185501010130-185912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_186001010130-186412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_186501010130-186912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_187001010130-187412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_187501010130-187912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_188001010130-188412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_188501010130-188912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_189001010130-189412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_189501010130-189912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_190001010130-190412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_190501010130-190912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_191001010130-191412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_191501010130-191912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_192001010130-192412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_192501010130-192912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_193001010130-193412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_193501010130-193912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_194001010130-194412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_194501010130-194912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_195001010130-195412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_195501010130-195912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_196001010130-196412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_196501010130-196912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_197001010130-197412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_197501010130-197912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_198001010130-198412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_198501010130-198912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_199001010130-199412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_199501010130-199912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_200001010130-200412312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_200501010130-200912312230.nc',
    'https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/CMCC/CMCC-ESM2/historical/r1i1p1f1/3hr/pr/gn/v20210114/pr_3hr_CMCC-ESM2_historical_r1i1p1f1_gn_201001010130-201412312230.nc',
]

pattern_full = pattern_from_file_sequence(urls_full, concat_dim='time')
# full example with only time chunking

time_only = (
    f'Creating {iid}' >> beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True})
    | Preprocessor()
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'time': 300, 'lon': 288, 'bnds': 2, 'lat': 192}
    )
)

lon_only = (
    f'Creating {iid}' >> beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True})
    | Preprocessor()
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'lon': 10, 'time':29200, 'bnds': 2, 'lat': 192}
    )
)

lon_only_full = (
    f'Creating {iid}' >> beam.Create(pattern_full.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True, copy_to_local=True})
    | Preprocessor()
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'lon': 10, 'time':29200, 'bnds': 2, 'lat': 192}
    )
)

time_only_load = (
    f'Creating {iid}' >> beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True}, load=True)
    | Preprocessor()
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'time': 300, 'lon': 288, 'bnds': 2, 'lat': 192}
    )
)

lon_only_load = (
    f'Creating {iid}' >> beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True}, load=True)
    | Preprocessor()
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'lon': 10, 'time':29200, 'bnds': 2, 'lat': 192}
    )
)
