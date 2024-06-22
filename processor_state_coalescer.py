import json
import dotenv
from core.base_processor import BaseProcessor
from core.processor_state import State, extract_values_from_query_state_by_key_definition
from logger import log

dotenv.load_dotenv()
logging = log.getLogger(__name__)


class StateCoalescerProcessor(BaseProcessor):

    def __init__(self,
                 output_state: State,
                 secondary_input_state: State,
                 primary_input_state: State = None,
                 **kwargs):
        super().__init__(output_state=output_state, **kwargs)
        self.primary_input_state = primary_input_state
        self.secondary_input_state = secondary_input_state

    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        def merge_dicts(primary, secondary):
            return {
                k:
                    secondary.get(k, primary.get(k)) if primary.get(k) is None
                    else primary.get(k)
                for k in
                    set(primary) | set(secondary)
            }

        secondary_count = self.secondary_input_state.count

        # if the inheritance is defined, the use it, otherwise inherit everything from the primary state
        if self.config.query_state_inheritance:
            primary_query_state = extract_values_from_query_state_by_key_definition(
                key_definitions=self.config.query_state_inheritance,
                query_state=input_query_state)
        else:
            primary_query_state = input_query_state

        output_query_states = []
        for secondary_index in range(secondary_count):
            secondary_query_state = self.secondary_input_state.build_query_state_from_row_data(secondary_index)

            # if the inheritance is defined, then use it, otherwise inherit everything from the secondary state
            if self.config.query_state_inheritance:
                secondary_query_state = extract_values_from_query_state_by_key_definition(
                    key_definitions=self.config.query_state_inheritance,
                    query_state=secondary_query_state
                )

            # join the primary and secondary states into a single state entry
            joined_query_state = merge_dicts(primary_query_state, secondary_query_state)
            output_query_states.append(self.output_state.apply_query_state(query_state=joined_query_state))

        # apply the newly created state
        self.apply_states(output_query_states)

    def apply_states(self, query_states: [dict]):
        route_message = {
            "route_id": self.output_processor_state.id,
            "type": "query_state_list",
            "query_state_list": query_states
        }

        self.sync_store_route.send_message(json.dumps(route_message))