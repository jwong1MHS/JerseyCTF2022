# osint

- [x] [dns-joke (100)](#dns-joke)
- [x] [photo-op-spot (250)](#photo-op-spot)
- [x] [rarity (350)](#rarity)

## dns-joke

### *Description*

- A system administrator hasn't smiled in days. Legend has it, there is a DNS joke hidden somewhere in [www.jerseyctf.com](http://www.jerseyctf.com/). Can you help us find it to make our system administrator laugh?

<details>
    <summary>View Hint</summary>
    How are IP addresses pointed towards domain names?
</details>

### *Writeup*

Use DNS lookup tool on https://www.jerseyctf.com/.

Flag: `jctf{DNS_J0k3s_t@k3_24_hrs}`

## photo-op-shot

### *Description*

- In three words tell me where I stood when I grabbed this picture.
- Solution format: jctf{yourthreewords} - no special characters

<details>
    <summary>View Hint</summary>
    GPS coordinates aren't the only method of specifying a location.
</details>

[photo-op-shot.JPG](https://www.jerseyctf.site/files/6ff5e1564d4137f5d48463aedb8e5632/photo-op-spot.JPG?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6NDB9.YlR90A.K0YFsNi4Ihmwgz69ZbnR1DWQs-E)

### *Writeup*

Radio tower is called “Transforest” after doing some Google image searching.

We want to specify the location with just three words, which we can use https://what3words.com and give the address “130 Minor Ave N”.

Flag: `jctf{solofadesbrief}`

## rarity

### *Description*

- With three belonging to a respective company, there is only a two-digit number amount of this entity left in the world. There is one near this picture... how close can you get to it?
- The flag format is the coordinates in decimal degrees notation, for example: `jctf{-65.913734,-10.814140}`
- Get the coordinates **at the gate**

<details>
    <summary>View Hint</summary>
    Aren't sub sandwiches great?
</details>
<details>
    <summary>View Hint</summary>
    <a href="https://en.wikipedia.org/wiki/Hindenburg_disaster">https://en.wikipedia.org/wiki/Hindenburg_disaster</a>
</details>

[picture.png](https://www.jerseyctf.site/files/4133210b82b0c48fc842bee2a7ab7fee/picture.png?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6NDF9.YlSBiA.AIUYELs0qMedI9HnhxsrEjieKoM)

### *Writeup*

The entity is a Blimp, more specifically a Goodyear blimp. The gate must near the Wingfoot Lake base in Akron, OH given the 330 area code in the bottom right.

The address is 1012 Goodyear Park.

Flag: `jctf{41.0198897,-81.3621548}`