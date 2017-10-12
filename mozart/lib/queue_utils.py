import os, json, requests, traceback

from hysds_commons.queue_utils import get_all_queues
from hysds_commons.job_spec_utils import get_job_spec

from mozart import app


def get_queue_names(ident):
    '''
    List the queues available for job-running
    Note: does not return celery internal queues
    @param ident - identity of job
    @return: list of queues
    '''

    #Non-celery queues set
    queues = set(get_all_queues(app.config["RABBITMQ_ADMIN_URL"]))
    #app.logger.info("queues: %s" % queues)
    protected = set(app.config["PROTECTED_QUEUES"])
    #app.logger.info("protected: %s" % protected)
    #Visible generic queues
    visible = queues - protected
    #app.logger.info("visible: %s" % visible)
    #app.logger.info("ident: %s" % ident)
    spec = {}
    try:
        spec = get_job_spec(app.config['ES_URL'], ident)
    except Exception as e:
        app.logger.warn("Failed to get job-spec: {0} proceeding without it. {1}:{2}".format(ident,type(e),e))
    #app.logger.info("spec: %s" % spec)
    #adding backwards compatibility to queues
    required = set(spec.get("required-queues", spec.get("required_queues",[])))
    recommended = set(spec.get("recommended-queues",spec.get("recommended_queues",[])))
    queue_config = { 
        "queues": sorted(visible | required | recommended),
        "recommended": sorted(required | recommended)
    }
    #app.logger.info("queue_config: %s" % json.dumps(queue_config, indent=2))
    return queue_config
