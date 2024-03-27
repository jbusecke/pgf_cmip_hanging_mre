from dataclasses import dataclass

import apache_beam as beam
import xarray as xr
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
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
    # do not specify file type to accomodate both ncdf3 and ncdf4
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True})
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'time': 300, 'lon': 288, 'bnds': 2, 'lat': 192}
    )
)

lon_only = (
    f'Creating {iid}' >> beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    # do not specify file type to accomodate both ncdf3 and ncdf4
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True})
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'lon': 10, 'time':29200, 'bnds': 2, 'lat': 192}
    )
)

time_only_load = (
    f'Creating {iid}' >> beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    # do not specify file type to accomodate both ncdf3 and ncdf4
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True}, load=True)
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'time': 300, 'lon': 288, 'bnds': 2, 'lat': 192}
    )
)

lon_only_load = (
    f'Creating {iid}' >> beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    # do not specify file type to accomodate both ncdf3 and ncdf4
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True}, load=True)
    | StoreToZarr(
        store_name=f'{iid}.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'lon': 10, 'time':29200, 'bnds': 2, 'lat': 192}
    )
)
