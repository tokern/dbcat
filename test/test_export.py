import yaml
from databuilder import Scoped
from datahub.ingestion.api.common import PipelineContext
from pyhocon import ConfigFactory

from dbcat.amundsen import CatalogExtractor
from dbcat.datahub import CatalogSource


def test_simple_amundsen_extract(load_source):
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


def test_simple_datahub_extract(load_source):
    catalog, conf, source = load_source
    datahub_source = CatalogSource.create(
        yaml.safe_load(conf)["catalog"], PipelineContext(run_id="test_extract")
    )
    datahub_source.config.source_names = [source.name]

    wu_list = list(datahub_source.get_workunits())
    assert len(wu_list) == 3
