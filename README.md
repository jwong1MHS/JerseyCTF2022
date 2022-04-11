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
- <span style="color: palevioletred;">`atkw{plddp_jrcru_uivjjzex}`</span>


### ***Writeup***
Caesar cipher shift of 17.

Flag: `jctf{yummy_salad_dressing}`

## **new-algorithm**

### ***Description***

- On the first day of the job, a new cryptography intern is insisting to upper management that he developed a new encryption algorithm for the company to use for sensitive emails and should get a raise. This seems too good to be true... are you able to prove the intern wrong by decrypting it?
- Here's an example of an encrypted email message using the intern's algorithm: <span style="color: palevioletred;">`amN0Znt0UllfQUVTX0lOc1QzQGR9`</span>

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

Brute force using a python script

Caesar cipher to decode the key

Run `file` and change file type to .gif

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

Examine program in ghidra and notice it only contains one function. Buf1 is our target. Set a breakpoint at write in GDB and examine the value of the register containing buf1 (%rsi)

![img](https://lh4.googleusercontent.com/OOM36OUFV1U7TH2-PQ4pT-B5sQ5Te2Bz0CnUaFiD21yxQHCfcFS8wr7y2bjHLMGNIu29ef6hE5tA4i-2u0-sDGW00BVieN8lEmNqIHKglypKRbdkpaPtmOiSPnvTHGvTq4s46u_6)

![img](https://lh4.googleusercontent.com/bMTd_phCMUFuRqfpsKOma88qTiJqv_2XASGVs6KIakpLQCRaMyJVfOARRXAquyt3WW-w526J_pZ3EZlhb0SVRThKdi4W-YUw4Vgg5FZ67PidbAOlSqmvXqMTqsCTezj-cG0oI9kQ)

![img](https://lh6.googleusercontent.com/NfoB-sj63uQk5m6_5F3dB0fjPMvo8rlodFREnwAjm9xHUKLeKBRhrcoJBvK9dtnJcunvEUScSdX5xi9m2cJPBxl-tLiUx3i4uVI0ARptJlsBpcdb3SnOOGE2MJwxIXvztXceOrUS)

## **going-over**

### ***Description***

- My friends said they were going on a trip but I think they ran into some trouble...
- They sent me these two files before we lost contact (**src.c** and **going-over**)

<span style="color: palevioletred;">nc 0.cloud.chals.io 10197</span>

<details>
    <summary>View Hint</summary>
    There are many ways to solve this challenge, some of which are much easier than others.
</details>

[misdirection](https://www.jerseyctf.site/files/3eadc1906cef0acc4030cb7a9afa3ea2/misdirection?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzF9.YlR4EQ.XKV_izbjw9xvfblun3io4A9Ry0w)

### ***Writeup***

![img](https://lh6.googleusercontent.com/dejQOwGP9W-J6-ZTmAl8cnInzoFDXmskqJ9CHTcNX1HSitcU1_8pHPsvGiYvSjeKEtwGBc69aSShyad7kU1pzJo7yCgiKAO5WGEiRZNuPs5dBYaI88C6mZJHkY5_hjNWPWFGPK5k)

