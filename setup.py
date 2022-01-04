from setuptools import setup

setup(
    name="digital-land-brownfield-land",
    entry_points={"digital_land": ["brownfield_land_harmonise=pipeline.plugins:harmoniser_plugin"]},
)
