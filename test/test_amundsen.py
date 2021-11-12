import yaml
from databuilder import Scoped
from pyhocon import ConfigFactory

from dbcat.amundsen import CatalogExtractor


def test_simple_extract(load_source):
    catalog, conf, source = load_source
    catalog_config = ConfigFactory.from_dict(yaml.safe_load(conf)["catalog"])
    config = ConfigFactory.from_dict(
        {
            f"tokern.catalog.{CatalogExtractor.CATALOG_CONFIG}": catalog_config,
            "tokern.catalog.source_names": [source.name],
        }
    )

    extractor = CatalogExtractor()
    extractor.init(Scoped.get_scoped_conf(config, extractor.get_scope()))

    num_tables = 0
    while extractor.extract():
        num_tables += 1
    assert num_tables == 3
