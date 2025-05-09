
# Utilities
from Utils.stream_sens_utils import stream_micro_sens


def stream_example():
    macroarea_i = 5
    microarea_i = 40
    stream_micro_sens(macroarea_i=macroarea_i, microarea_i=microarea_i)


if __name__ == "__main__":
    stream_example()