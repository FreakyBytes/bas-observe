"""
BAS Observe

contains misc function to setup/configure AMQP message pipelines
this is only to be invoked by the Config class
"""
from . import config
import pika


def declare_amqp_pipeline(conf: config, channel: pika.channel.Channel, durable: bool=True) -> None:
    """Declare AMQP Pipeline

    This function declares all necessary exchanges and queues based on conf.project_name
    aka. does the plumbing
    """

    # agents to collector
    channel.exchange_declare(exchange=conf.name_exchange_agents, type='fanout')
    queue_agents = channel.queue_declare(queue=conf.name_queue_agents, durable=durable)

    channel.queue_bind(exchange=conf.name_exchange_agents, queue=queue_agents.method.queue)

    # collector to analysers
    channel.exchange_declare(exchange=conf.name_exchange_analyser, type='fanout')
    queue_analyser_addr = channel.queue_declare(queue=conf.name_queue_analyser_addr, durable=durable)
    queue_analyser_entropy = channel.queue_declare(queue=conf.name_queue_analyser_entropy, durable=durable)
    queue_analyser_lof = channel.queue_declare(queue=conf.name_queue_analyser_lof, durable=durable)

    channel.queue_bind(exchange=conf.name_exchange_analyser, queue=queue_analyser_addr.method.queue)
    channel.queue_bind(exchange=conf.name_exchange_analyser, queue=queue_analyser_entropy.method.queue)
    channel.queue_bind(exchange=conf.name_exchange_analyser, queue=queue_analyser_lof).method.queue

    # analysers to collector for metrics
    channel.exchange_declare(exchange=conf.name_exchange_metric, type='fanout')
    queue_metric = channel.queue_declare(queue=conf.name_queue_metric, durable=durable)

    channel.queue_bind(exchange=conf.name_exchange_metric, queue=queue_metric.method.queue)
