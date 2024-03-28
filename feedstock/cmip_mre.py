import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr
)

urls = [
    "gs://cmip6/pgf-debugging/hanging_bug/file_a.nc",
    "gs://cmip6/pgf-debugging/hanging_bug/file_b.nc"
]

pattern = pattern_from_file_sequence(urls, concat_dim='time')
fail = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    # do not specify file type to accomodate both ncdf3 and ncdf4
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True})
    | StoreToZarr(
        store_name=f'simple_test.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'x': 1, 'time':4},
    )
)

success = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    # do not specify file type to accomodate both ncdf3 and ncdf4
    | OpenWithXarray(xarray_open_kwargs={'use_cftime': True})
    | StoreToZarr(
        store_name=f'simple_test.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'x': 2, 'time':1},
    )
)
