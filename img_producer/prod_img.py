
# Utilites
from src.stream_img import stream_macro_imgs


def stream_example():
    macroarea_i = 1
    microarea_i = 10
    stream_macro_imgs(macroarea_i=macroarea_i, microarea_i=microarea_i)


if __name__ == "__main__":
    stream_example()


