import asyncio
import os
import random
from typing import List

import dotenv

from core.base_model import ProcessorStateDirection, ProcessorState, Processor, ProcessorProvider
from core.base_processor import StatePropagationProviderDistributor, StatePropagationProviderRouterStateSyncStore, \
    StatePropagationProviderRouterStateRouter
from core.messaging.base_message_consumer_processor import BaseMessageConsumerProcessor
from core.messaging.base_message_router import Router
from core.messaging.nats_message_provider import NATSMessageProvider
from core.processor_state import State
from db.processor_state_db_storage import PostgresDatabaseStorage
from logger import logging
from processor_state_coalescer import StateCoalescerProcessor

dotenv.load_dotenv()

logging.info('starting up pulsar consumer for state coalescer.')

# database related
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")

# Message Routing File (
#   The responsibility of this state sync store is to take inputs and
#   store them into a consistent state storage class. After, the intent is
#   to automatically route the newly synced data to the next state processing unit
#   route them to the appropriate destination, as defined by the
#   route selector
# )
ROUTING_FILE = os.environ.get("ROUTING_FILE", '.routing.yaml')
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# state storage specifically to handle this processor state (stateless obj)
storage = PostgresDatabaseStorage(
    database_url=DATABASE_URL,
    incremental=True
)

# routing the persistence of individual state entries to the state sync store topic
message_provider = NATSMessageProvider()
router = Router(
    provider=message_provider,
    yaml_file=ROUTING_FILE
)

# find the monitor route for telemetry updates
monitor_route = router.find_route("processor/monitor")
state_sync_route = router.find_route("processor/state/sync")
state_router_route = router.find_route('processor/state/router')
state_coalescer_route_subscriber = router.find_route_by_subject("processor.transform.coalescer")

# state_router_route = router.find_router("processor/monitor")
state_propagation_provider = StatePropagationProviderDistributor(
    propagators=[
        StatePropagationProviderRouterStateSyncStore(route=state_sync_route),
        StatePropagationProviderRouterStateRouter(route=state_router_route, storage=storage)
    ]
)


class MessagingConsumerCoalescer(BaseMessageConsumerProcessor):

    async def fetch_input_output_states(self, processor_id: str):
        # fetch the processors to forward the state query to, state must be an input of the state id
        output_states = self.storage.fetch_processor_state_route(
            processor_id=processor_id,
            direction=ProcessorStateDirection.OUTPUT
        )

        if not output_states:
            raise BrokenPipeError(f'no output state found for processor id: {processor_id}')

        # identify all the input states we need to fuse together and keep track of
        input_states = self.storage.fetch_processor_state_route(
            processor_id=processor_id,
            direction=ProcessorStateDirection.INPUT
        )

        if not input_states:
            raise BrokenPipeError(f'no input states found to fuse for processor id: {processor_id}')

        return input_states, output_states

    def filter_and_load_secondary_state_id(self, primary_state_id, input_states: List[ProcessorState]):
        # filter out the current query state entry input_state and focus only on the other input states
        secondary_processor_states = [
            input_state for input_state in input_states
            if input_state.state_id != primary_state_id
        ]

        # TODO this needs to be resolved
        if not secondary_processor_states or len(secondary_processor_states) > 1:
            raise ValueError(OverflowError(
                f'unsupported number of input states, was expecting '
                f'only a single input state state to be merged with current '
                f'input_state_id query state entry'
            ))

        # load the secondary state and return
        secondary_processor_state = secondary_processor_states[0]
        return self.storage.load_state(state_id=secondary_processor_state.state_id)

    def create_processor(self,
                         processor: Processor,
                         provider: ProcessorProvider,
                         output_processor_state: ProcessorState,
                         output_state: State):

        processor = StateCoalescerProcessor(
            state_machine_storage=storage,
            output_state=output_state,
            provider=provider,
            processor=processor,
            output_processor_state=output_processor_state,
            monitor_route=self.monitor_route,
            state_propagation_provider=state_propagation_provider
        )

        return processor

    async def execute(self, message: dict):
        if message['type'] != 'query_state':
            raise ValueError(f'unsupported message type: {type}')

        if 'route_id' not in message:
            raise ValueError(f'route id is not defined on message {message}')

        # fetch the route that was invoked (aka the processor state route ~ input state <func> output state)
        route_id = message['route_id']
        processor_state_route = storage.fetch_processor_state_route(route_id=route_id)
        if processor_state_route and len(processor_state_route) != 1:
            raise ValueError(f'invalid processor state route found, expected 1 route, got {processor_state_route}')
        processor_state_route = processor_state_route[0]

        primary_state_id = processor_state_route.state_id   # the prime is defined by the query state entry(s) state id

        # fetch processor and provider information
        processor = self.storage.fetch_processor(processor_id=processor_state_route.processor_id)
        provider = self.storage.fetch_processor_provider(processor.provider_id)

        # fetch the input and output states
        input_states, output_states = await self.fetch_input_output_states(processor.id)
        secondary_state = self.filter_and_load_secondary_state_id(primary_state_id, input_states)

        # fetch query state input entries
        query_states = message['query_state']
        logging.info(f'starting processing of {len(output_states)} states on processor id {processor.id} with provider {provider.id}')

        # iterate all output states
        for output_processor_state in output_states:

            # load the output state and relevant state instruction
            output_state = self.storage.load_state(
                state_id=output_processor_state.state_id,
                load_data=False
            )

            logging.info(f'creating processor provider {processor.id} with: '
                         f'output state id {output_processor_state.state_id} with '
                         f'current index: {output_processor_state.current_index}, '
                         f'maximum processed index: {output_processor_state.maximum_index}, '
                         f'count: {output_processor_state.count}')

            # create (or fetch cached state) processor handling this state output
            # TODO amalgamate BUT state input is different?
            coalescer = StateCoalescerProcessor(
                output_state=output_state,
                secondary_input_state=secondary_state,
                state_machine_storage=self.storage,
                provider=provider,
                processor=processor,
                output_processor_state=output_processor_state,
                state_propagation_provider=state_propagation_provider,
                monitor_route=monitor_route,
            )

            # iterate each query state entry and forward it to the processor
            if isinstance(query_states, dict):
                logging.debug(f'submitting single query state entry count: solo, '
                              f'with processor_id: {processor.id}, '
                              f'provider_id: {provider.id}')

                await coalescer.execute(input_query_state=query_states)
            elif isinstance(query_states, list):
                logging.debug(f'submitting batch query state entries count: {len(query_states)}, '
                              f'with processor_id: {processor.id}, '
                              f'provider_id: {provider.id}')

                # iterate each individual entry and submit
                # TODO modify to submit as a batch?? although this consumer should be handling 1 request
                for query_state_entry in query_states:
                    await coalescer.execute(input_query_state=query_state_entry)
            else:
                raise NotImplemented('unsupported query state entry, it must be a Dict or a List[Dict] where Dict is a '
                                     'key value pair of values, defining a single row and a column per key entry')


if __name__ == '__main__':
    consumer = MessagingConsumerCoalescer(
        storage=storage,
        route=state_coalescer_route_subscriber,
        monitor_route=monitor_route
    )

    consumer.setup_shutdown_signal()
    consumer_no = random.randint(0, 10)
    asyncio.get_event_loop().run_until_complete(consumer.start_consumer())
