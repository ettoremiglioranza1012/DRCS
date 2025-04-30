# Important

On the evening of 30 april i made some important changes to the code: i realised that in the same for loop i was doing things like fetching the image, but at the same time processing the macroareas, saving the data to dict, recreating the macros for sanity check and now i was going to add db saves to each iteration. I therefore split the programme into two in the image fecthing, modification and compression and sending to kafka + programme 2 simply the part up to the database.  Since I'm a pirate and forgot to pull before making changes, now it's a mess to update the repo so I made another one just in case.

Translated with DeepL.com (free version)