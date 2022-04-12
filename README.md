# **Table of Contents**
| Categories              | Completed | Progress                                                     | Points     |
| ----------------------- | --------- | ------------------------------------------------------------ | ---------- |
| [crypto](#crypto)       | 5/9       | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/56) | 1175/2725  |
| [bin](#bin)             | 2/8       | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/25) | 600/3250   |
| [osint](#osint)         | 3/6       | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/50) | 700/1750   |
| [forensics](#forensics) | 5/7       | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/71) | 1400/1950  |
| [misc](#misc)           | 7/8       | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/88) | 1350/1750  |
| [web](#web)             | 5/7       | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/71) | 1150/1700  |
| [sponker](#sponker)     | 8/13      | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/62) | 160/260    |
| **Total**               | 35/58     | ![](https://us-central1-progress-markdown.cloudfunctions.net/progress/60) | 6535/13385 |

# **crypto**
- [salad (50)](#salad)
- [new-algorithm (75)](#new-algorithm)
- [would-you-wordle (250)](#would-you-wordle)
- [hidden-in-plain-sight (350)](#hidden-in-plain-sight)
- [file-zip-cracker (450)](#file-zip-cracker)

## **salad**

### ***Description***
- Roman generals really knew how to make salad!
- <span style="color: #e83e8c;">`atkw{plddp_jrcru_uivjjzex}`</span>


### ***Writeup***
Caesar cipher shift of 17.

Flag: `jctf{yummy_salad_dressing}`

## **new-algorithm**

### ***Description***

- On the first day of the job, a new cryptography intern is insisting to upper management that he developed a new encryption algorithm for the company to use for sensitive emails and should get a raise. This seems too good to be true... are you able to prove the intern wrong by decrypting it?
- Here's an example of an encrypted email message using the intern's algorithm: <span style="color: #e83e8c;">`amN0Znt0UllfQUVTX0lOc1QzQGR9`</span>

<details>
    <summary>View Hint</summary>
    What are some differences between encryption, encoding, and hashing?
</details>

### ***Writeup***

Base64 decode.

Flag: `jctf{tRY_AES_INsT3@d}`

## **would-you-wordle**

### ***Description***

- Someone left this secret text string and unfinished Wordle. Can you put them together to get the flag?
- pUpPHg3KfB15MG2KGtQQMDEECPOF8oa3VA==

<details>
    <summary>View Hint</summary>
    Ron's Code
</details>

[Wordle-Words.jpg](https://www.jerseyctf.site/files/9e4f94397863b790448bdf8eeddb771b/Wordle-Words.jpg?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MjR9.YlRzBw.2Zq5pK-TK91ofW5wbFrS9Oi5azo)

### ***Writeup***

Wordle is “thorn”. You can use any 5 letter finder like the one here: https://wordfinderx.com/wordle/.

Then use a RC4 decryption (aka Ron's cipher) with the key "thorn" to get the flag, like the oner here: https://www.howtocreate.co.uk/emails/test_RC4B64_js.htm.

Flag: `jctf{CryptoIsTheKeyToFun}`

## **hidden-in-plain-sight**

### ***Description***

- A file contains the flag but it is encrypted. Normally this would be impossible to crack, but you have the encryption algorithm source code in front of you. Try to shift through it and see the vulnerabilities that can get that flag decrypted!

<details>
    <summary>View Hint</summary>
    The file looks a little longer than you would expect.
</details>

[encrypted.pco](https://www.jerseyctf.site/files/4adc86865b2d62c34926ba5bb015c091/encrypted.pco?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MjJ9.YlRyaA.bifKmUpWQOC-6gxQxHf8pGuqw_A)

[encryption.py](https://www.jerseyctf.site/files/7dcacf39976161066a24293b9fd35913/encryption.py?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MjN9.YlRyaA.nKTTT-djO20EyX5uxoLL_54yF74)

### ***Writeup***

Reverse the AES encryption.

```python
from base64 import b64decode
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import unpad

key = b64decode("QE1jUWZUalduWnI0dTd4IUElRCpHLUphTmRSZ1VrWHA=".encode('utf-8'))
iv = b64decode("zTnn5Yv9aHjVIhHX2BetXQ==".encode('utf-8'))
ciphered_data = b'nbXg75/acDR47Zgtho29ZVnHqFb7Ikca2SNCWj9SNNe1M+J22JxBrg94feT3anuIx2dQusjf1HJ4fRamU2xGUmHL/Sctgx0ZOsSbIyuksblsjNPmajhzTpljIY0ztR/f6LH5Iq6XJ3MjpTnp4wNg4ODQXfgjyc+UPfk91le4/zIFyAMISCskjw1OYGAOHoS5'
cipher = AES.new(key, AES.MODE_CBC, iv=iv)
original_data = cipher.decrypt(b64decode(ciphered_data))
print(original_data)
```

Flag: `jctf{CryptoIsTheKeyToFun}`

## **file-zip-cracker**

### ***Description***

- We have a secret file that is password protected. However, we have obtained a wordlist of actors that is part of the password. The password is the combination of one of the names on the list with a year.
- **Format**: "Actor_NameYYYY" **Example**: "Henry_Cavill1964"
- Fix the script to brute force the password.

[actorList.txt](https://www.jerseyctf.site/files/a375d21bafaf6d09c841e5a52d18dcc2/actorList.txt?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MjV9.YlR0Cg.hqcogEop_PKiTQAlL_-34ek53O0)

[FileZipCracker_Challenge_Version.py](https://www.jerseyctf.site/files/25785216f98fbf26c525f6045099b2a6/FileZipCracker_Challenge_Version.py?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MjZ9.YlR0Cg.J7clsEBT1gfSnnPJbYyT0ZKg7lg)

[secret_folder.zip](https://www.jerseyctf.site/files/29d964bd6ef2680db85a65d701945900/secret_folder.zip?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6Mjd9.YlR0Cg.W9lzEoUc_iASDC_hzwm3uwr7F-A)

### ***Writeup***

Brute force using a python script. Caesar cipher to decode the key. Run `file` and change file type to .gif.

Flag: `jctf{ew8WhHuhmv}`

# **bin**

- [misdirection (250)](#misdirection)
- [going-over (350)](#going-over)

## **misdirection**

### ***Description***

Where'd the flag go?

<details>
    <summary>View Hint</summary>
    There are many ways to solve this challenge, some of which are much easier than others.
</details>

[misdirection](https://www.jerseyctf.site/files/3eadc1906cef0acc4030cb7a9afa3ea2/misdirection?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzF9.YlR4EQ.XKV_izbjw9xvfblun3io4A9Ry0w)

### ***Writeup***

![img](https://lh6.googleusercontent.com/dejQOwGP9W-J6-ZTmAl8cnInzoFDXmskqJ9CHTcNX1HSitcU1_8pHPsvGiYvSjeKEtwGBc69aSShyad7kU1pzJo7yCgiKAO5WGEiRZNuPs5dBYaI88C6mZJHkY5_hjNWPWFGPK5k)

Examine program in ghidra and notice it only contains one function. Buf1 is our target. Set a breakpoint at write in GDB and examine the value of the register containing buf1 (%rsi).

![img](https://lh4.googleusercontent.com/OOM36OUFV1U7TH2-PQ4pT-B5sQ5Te2Bz0CnUaFiD21yxQHCfcFS8wr7y2bjHLMGNIu29ef6hE5tA4i-2u0-sDGW00BVieN8lEmNqIHKglypKRbdkpaPtmOiSPnvTHGvTq4s46u_6)

![img](https://lh4.googleusercontent.com/bMTd_phCMUFuRqfpsKOma88qTiJqv_2XASGVs6KIakpLQCRaMyJVfOARRXAquyt3WW-w526J_pZ3EZlhb0SVRThKdi4W-YUw4Vgg5FZ67PidbAOlSqmvXqMTqsCTezj-cG0oI9kQ)

![img](https://lh6.googleusercontent.com/NfoB-sj63uQk5m6_5F3dB0fjPMvo8rlodFREnwAjm9xHUKLeKBRhrcoJBvK9dtnJcunvEUScSdX5xi9m2cJPBxl-tLiUx3i4uVI0ARptJlsBpcdb3SnOOGE2MJwxIXvztXceOrUS)

Flag: `jctf{l00k5_1ik3_u_f0Und_m3_018a09d6}`

## **going-over**

### ***Description***

- My friends said they were going on a trip but I think they ran into some trouble...
- They sent me these two files before we lost contact (**src.c** and **going-over**)

<span style="color: #e83e8c;">nc 0.cloud.chals.io 10197</span>

<details>
    <summary>View Hint</summary>
    If only there were a way to find the exact location of the ledge... like if the ledge had an address or something
</details>

[going-over](https://www.jerseyctf.site/files/294adfee89ff8062305c35796b90b97e/going-over?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzN9.YlR40Q.f-2IiuCiVZ-JOVxpKQ-BVa4GUsU)

[src.c](https://www.jerseyctf.site/files/a55cf2daca28047e9932cf984c7d6fae/src.c?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzR9.YlR40Q.vCGi5mXxagmbVfsN-Y7k9LbTNFc)

### ***Writeup***

![img](https://lh6.googleusercontent.com/m6MWpDpnY8FiKuoCRR7m0svxvvWKnquvaQfza4-lIV42QTz8KNAo7aga-YUF81eEfVvbtiFLQX4k5MZqb69q1UvC-kTzCymeNo-wub1288uzlpnoYgI3IhzbOtuduSDlSNnQY3jd)

# **osint**

- [dns-joke (100)](#dns-joke)
- [photo-op-spot (250)](#photo-op-spot)
- [rarity (350)](#rarity)

## **dns-joke**

### ***Description***

- A system administrator hasn't smiled in days. Legend has it, there is a DNS joke hidden somewhere in [www.jerseyctf.com](http://www.jerseyctf.com/). Can you help us find it to make our system administrator laugh?

<details>
    <summary>View Hint</summary>
    How are IP addresses pointed towards domain names?
</details>

### ***Writeup***

Use DNS lookup tool on https://www.jerseyctf.com/.

Flag: `jctf{DNS_J0k3s_t@k3_24_hrs}`

## **photo-op-shot**

### ***Description***

- In three words tell me where I stood when I grabbed this picture.
- Solution format: jctf{yourthreewords} - no special characters

<details>
    <summary>View Hint</summary>
    GPS coordinates aren't the only method of specifying a location.
</details>

[photo-op-shot.JPG](https://www.jerseyctf.site/files/6ff5e1564d4137f5d48463aedb8e5632/photo-op-spot.JPG?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6NDB9.YlR90A.K0YFsNi4Ihmwgz69ZbnR1DWQs-E)

### ***Writeup***

Radio tower is called “Transforest” after doing some Google image searching.

We want to specify the location with just three words, which we can use https://what3words.com and give the address “130 Minor Ave N”.

Flag: `jctf{solofadesbrief}`

## **rarity**

### ***Description***

- With three belonging to a respective company, there is only a two-digit number amount of this entity left in the world. There is one near this picture... how close can you get to it?
- The flag format is the coordinates in decimal degrees notation, for example: <span style="color: #e83e8c;">`jctf{-65.913734,-10.814140}`</span>
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

### ***Writeup***

The entity is a Blimp, more specifically a Goodyear blimp. The gate must near the Wingfoot Lake base in Akron, OH given the 330 area code in the bottom right.

The address is 1012 Goodyear Park.

Flag: `jctf{41.0198897,-81.3621548}`

# **forensics**

- [speedy-at-midi (150)](#speedy-at-midi)
- [data-backup (250)](#data-backup)
- [recent-memory (250)](#recent-memory)
- [scavenger-hunt (350)](#scavenger-hunt)
- [corrupted-file (400)](#corrupted-file)

## **speedy-at-midi**

### ***Description***

- Your partner-in-crime gets a hold of a MIDI file, <span style="color: #e83e8c;">`riff.mid`</span>, which intelligence officials claim to contain confidential information. He has tried opening it in VLC Media Player, but it sounds just like the piano riff in <span style="color: #e83e8c;">`riff.mp3`</span>. Can you find the right tool to extract the hidden data?

<details>
    <summary>View Hint</summary>
    You wouldn't have the audacity to try using a MIDI editor, would you?
</details>

[riff.mid](https://www.jerseyctf.site/files/531056bf3e98adcbda54f68023e70622/riff.mid?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MTh9.YlSYQw.eFW82LKkVc_JmmuGGQn26vtAB-g)

[riff.mp3](https://www.jerseyctf.site/files/965ce2c09022315db0136ea155068369/riff.mp3?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MTl9.YlSYQw.BxS2VCX1G1Z66IoAEuBZs1Z1RCE)

### ***Writeup***

![img](https://lh4.googleusercontent.com/wLAEp2VN2C_XtRQIwjy5iuK5qnlExrZAy22gKd7oxotvxTEsIkOU3G43EeaPa6iVD8PG6z_iw1vaZfbiWvSgP1LZBmcc2QyuFboAS2haAqI4DZ389RqCsVEbbzBnc2SjfnWYHtms)

Flag: `jctf{kicking_it_since_1983}`

## **data-backup**

### ***Description***

- The backup of our data was somehow corrupted. Recover the data and be rewarded with a flag.

<details>
    <summary>View Hint</summary>
    Try a tool a surgeon might use.
</details>

[data-backup](https://www.jerseyctf.site/files/6953c4156b0de04c42e0a32c7e56b2f1/data-backup?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzB9.YlSZ6w.oQ-_d4EhvMDDvB0DyZLDa2PJYGA)

### ***Writeup***

Run `bulk_extractor -f jctf -o output <filename>` and open the file that it outputted.

Flag: `jctf{fun_w17h_m461c_by735}`

## **recent-memory**

### ***Description***

- Use the memory image in the Google drive link below. An attacker left behind some evidence in the network connections. Follow the attacker's tracks to find the flag.
- https://drive.google.com/drive/folders/1ubSx3pwHOSZ9oCShHBPToVdHjTev7hXL
- Backup link if the above link is not working: https://drive.google.com/drive/folders/192ELa6W5OZyeWi3DlRd-_TndzN2p_Xz8?usp=sharing

<details>
    <summary>View Hint</summary>
    Try connecting to the attacker's system.
</details>

### ***Writeup***

`strings recent-memory.mem | grep -i jctf`

Flag: `jctf{f0ll0w_7h3_7r41l}`

## **Scavenger-hunt**

### ***Description***

- My friend told me he hid a flag for me on this server! Server: 0.cloud.chals.io SSH port: 24052
- **Username**: *jersey*
- **Password**: *securepassword*

<details>
    <summary>View Hint</summary>
    <ul>
        <li>If only there were a way to see all folders... even hidden ones</li>
        <li>I wonder where passwords are typically stored on ssh servers</li>
    </ul>
</details>

### ***Writeup***

Hint implies to look for the password files. Here a suspicious package file was located. Go to this package’s installation using `dpkg -L <packagename>` and access its manual page to find the key.

Flag: `jctf{f0ll0w_7h3_7r41l}`