
"""
update_stream.py
Batch updater for Kafka data queues (social, IoT, satellite) into Streamlit session state buffers.
"""

# Utilities
from typing import Tuple, List, Dict, Any
import queue


def update_all_data_batch(
    social_queue: queue.Queue,
    iot_queue: queue.Queue,
    sat_queue: queue.Queue,
    state: Dict[str, List[Dict[str, Any]]]
) -> Tuple[int, int, int]:
    """
    Update state buffers from Kafka queues with batch limits.
    Returns tuple of processed counts (social, iot, sat).
    """

    social_count = _process_queue_to_state(
        queue=social_queue,
        state=state,
        state_key="social_messages",
        batch_size=50,
        keep_last=20
    )

    iot_count = _process_queue_to_state(
        queue=iot_queue,
        state=state,
        state_key="iot_data",
        batch_size=10,
        keep_last=10
    )

    sat_count = _process_queue_to_state(
        queue=sat_queue,
        state=state,
        state_key="sat_data",
        batch_size=10,
        keep_last=10
    )

    return social_count, iot_count, sat_count


def _process_queue_to_state(
    queue: queue.Queue,
    state: Dict[str, List[Dict[str, Any]]],
    state_key: str,
    batch_size: int,
    keep_last: int
) -> int:
    """
    Helper function to process a queue and store into a given state dict.
    """
    batch = []
    processed = 0

    while not queue.empty() and processed < batch_size:
        try:
            batch.append(queue.get_nowait())
            processed += 1
        except queue.Empty:
            break

    if batch:
        state[state_key].extend(batch)
        if len(state[state_key]) > keep_last + 5:
            state[state_key] = state[state_key][-keep_last:]

    return processed


