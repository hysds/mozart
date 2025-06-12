from future import standard_library
standard_library.install_aliases()

from hysds_commons.queue_utils import get_all_queues

from mozart import app, mozart_es


def get_queue_names(_id):
    """
    List the queues available for job-running
    Note: does not return celery internal queues
    @param _id - identity of job
    @return: list of queues
    """

    queues = set(get_all_queues(app.config["RABBITMQ_ADMIN_API"]))  # Non-celery queues set
    protected = set(app.config["PROTECTED_QUEUES"])
    visible = queues - protected  # Visible generic queues

    spec = {}
    try:
        spec = mozart_es.get_by_id(index="job_specs", id=_id, ignore=404)
        if spec['found'] is True:
            spec = spec['_source']
    except Exception as e:
        app.logger.warn(f"Failed to get job-spec: {_id} proceeding without it. {type(e)}:{e}")

    # adding backwards compatibility to queues
    required = set(spec.get("required-queues", spec.get("required_queues", [])))
    recommended = set(spec.get("recommended-queues", spec.get("recommended_queues", [])))
    queue_config = {
        "queues": sorted(visible | required | recommended),
        "recommended": sorted(required | recommended)
    }
    return queue_config
