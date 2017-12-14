# Re/framing the international: Have Love, Will Travel

Kunstenpunt (internationally known as Flanders Arts Institute) investigates how Belgian live music acts operate internationally.
To gather the data, we developed a process to aggregate concert information from online gig finder platforms, i.e. Songkick, setlist.fm, Bands in Town en Facebook (events).
The process incorporates human-assisted cleaning algorithms and duplicate recognition.
In addition, we keep a channel open to report concert info manually, and there has been an effort to do "one off" imports of podiuminfo.nl, festivalinfo.nl, and the now defunct Arts Flanders agenda (not yet implemented).

## Aggregation flow

![Automatic flow](flow.png?raw=true "Automatic flow of concert info aggregation")

Information about artists, including their identity on Songkick, Bands in Town, Setlist.fm and Facebook, is collected via the open database musicbrainz.org.
Based on the identify information for the gig finder platforms in Musicbrainz, we can use the APIs of the gig finder platforms to gather the concert data.


## Data cleaning

The gathered data needs to be cleaned:

- within platform
 - we have to consider artists with the same name whose concerts are reported as coming from the same artist

- across platforms
 - we have to deduplicate concerts that are announced on several platforms
 - we have to harmonize venue names and place names

We are powerless against faulty reported data.


## Further information

### April 2017
- https://blog.kunsten.be/belgische-bands-in-het-buitenland-da8bc7d6e104

### Juni 2017
- https://blog.kunsten.be/grabbing-the-data-concerts-abroad-52144547e1e1
- https://blog.kunsten.be/4-things-you-didnt-know-about-flemish-musicians-on-tour-b9a579ce82b4

### Juli 2017
- https://blog.kunsten.be/bored-in-the-usa-4f5afbc4b1b9
- https://blog.kunsten.be/internationaal-doorbreken-for-dummies-deel-1-volgens-het-boekje-5eec522853cc
- https://blog.kunsten.be/internationaal-doorbreken-for-dummies-deel-2-leren-van-de-ondergrond-121155b888e9

### Augustus 2017
- https://blog.kunsten.be/beren-gieren-ook-buiten-de-landsgrenzen-ed85dd23dc7c
- https://blog.kunsten.be/whos-playing-where-be7e9da5c11f

### September 2017
- https://blog.kunsten.be/concerten-op-online-platformen-songkick-bandsintown-facebook-en-setlist-fm-8dd58caf6971

### Oktober 2017
- https://blog.kunsten.be/building-a-visual-tool-to-explore-where-belgian-artists-perform-concerts-abroad-25b876b605c9
- https://blog.kunsten.be/touring-abroad-10aba1c7f133

### November 2017
- https://blog.kunsten.be/touring-abroad-e35647c5318f
- https://blog.kunsten.be/beste-buren-c65976c096f0
- https://medium.com/@Simon_at_FlandersArtsInstitute/touring-abroad-31ee0729a60d

### December 2017
- https://blog.kunsten.be/cultuurindex-2017-praatje-belgische-artiesten-in-het-buitenland-a1ea394e42dd