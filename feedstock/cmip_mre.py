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
