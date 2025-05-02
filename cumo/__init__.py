#from cumo.yaml_validation import get_definition
from dagster_factory_pipelines import DagsterPipeline, registry
from cumo.modules.cumulocity import module
from cumo.modules.myrequests import module
from cumo.modules.arcgis import module
from cumo.modules.general import module
from cumo.modules.check import module
from dagster import get_dagster_logger
from cumo.resources import resources
logger = get_dagster_logger()

# for some reason in docker it inits for two times
registry.JOB_REPO.clear()
registry.ASSET_REPO.clear()
registry.SCHEDULES_REPO.clear()
registry.TRIGGER_REPO.clear()
registry.CHECK_REPO.clear()

logger.info("STARTING DAGSTER PIPELINE")
pipeline = DagsterPipeline()
pipeline.module_file = "modules.yaml"

logger.info("RUNNING DAGSTER PIPELINE BUILDER")
pipeline.run()

logger.info(registry.JOB_REPO)
logger.info(registry.ASSET_REPO)
for asset in registry.ASSET_REPO:
    logger.info(asset)
    
defs = pipeline.get_definition()
