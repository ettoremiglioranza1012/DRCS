
# Utilites
from imgproducer_m5.Utils.stream_img_utils import stream_macro_imgs


def stream_example():
    macroarea_i = 5
    microarea_i = 40
    stream_macro_imgs(macroarea_i=macroarea_i, microarea_i=microarea_i)


if __name__ == "__main__":
    stream_example()


