
# Utilites
from Utils.stream_img_utils import stream_macro_imgs


def stream_example():
    # Macroarea number, currently from 1 to 5
    i = 5
    stream_macro_imgs(macroarea_i=i)


if __name__ == "__main__":
    stream_example()

# SATELLITE MISSING TIMESTAMP! UPDATE MUST BE DONE!

# CURRENTLY EXTRACTING ONE CASUAL MICRO FROM MACRO FOR DEBUG AND EXPERIMENNTS
# FUTURE: EVERY SCRIPTS STREAM_IMG WILL BE NAME 'stream_img_An-Mmm' AND PARAMETER 'macroarea_i' and 'microarea_i' WILL BE FEED TO SCRIPTS.
# CHANGING IN BBOX FETCHING MUST BE DONE
# CHANGING IN THE LOGS MUST BE DONE AND FUNCTIONS COMMENTS MUST BE CHECKED (OUTERLEVEL) 
# DESIGN: ONE SCRIPTS IN CONTAINER STREAMING FOR MICROAREA