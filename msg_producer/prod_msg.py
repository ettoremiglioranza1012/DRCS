
# Utilities
from Utils.stream_msg_utils import stream_micro_msg

def stream_example():
    macroarea_i = 1
    microarea_i = 40
    stream_micro_msg(macroarea_i=macroarea_i, microarea_i=microarea_i)


if __name__ == "__main__":
    stream_example()

