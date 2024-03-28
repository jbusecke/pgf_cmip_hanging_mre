import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr
)

urls = [
    "gs://cmip6/pgf-debugging/hanging_bug/file_a_huge.nc",
    "gs://cmip6/pgf-debugging/hanging_bug/file_b_huge.nc"
]

pattern = pattern_from_file_sequence(urls, concat_dim='time')
fail = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray()
    | StoreToZarr(
        store_name=f'simple_test.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'x': 10, 'y':200, 'time':30000},
    )
)

success = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray()
    | StoreToZarr(
        store_name=f'simple_test.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'x': 100, 'y':200, 'time':300},
    )
)
