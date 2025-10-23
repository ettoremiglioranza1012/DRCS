
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
    Pulls data in batches from the Kafka queues (social, IoT, satellite),
    updates the corresponding session state buffers, and returns the number
    of new items processed from each queue.

    Each queue is processed with a defined batch size and the resulting
    data is appended to the corresponding list in the Streamlit session state.
    The oldest items are removed to retain only the most recent entries.

    Returns:
        Tuple of integers indicating how many items were processed from:
        (social queue, IoT queue, satellite queue)
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
    Internal helper that reads up to `batch_size` items from the given queue
    and appends them to the session state list identified by `state_key`.

    Ensures that the session state retains only the latest `keep_last` items
    (plus a small buffer) by trimming old entries if necessary.

    Returns:
        The number of items successfully read from the queue.
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


