# bin

- [x] [misdirection (250)](#misdirection)
- [x] [going-over (350)](#going-over)

## misdirection

### *Description*

Where'd the flag go?

<details>
    <summary>View Hint</summary>
    There are many ways to solve this challenge, some of which are much easier than others.
</details>
[misdirection](https://www.jerseyctf.site/files/3eadc1906cef0acc4030cb7a9afa3ea2/misdirection?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzF9.YlR4EQ.XKV_izbjw9xvfblun3io4A9Ry0w)

### *Writeup*

![img](https://lh6.googleusercontent.com/dejQOwGP9W-J6-ZTmAl8cnInzoFDXmskqJ9CHTcNX1HSitcU1_8pHPsvGiYvSjeKEtwGBc69aSShyad7kU1pzJo7yCgiKAO5WGEiRZNuPs5dBYaI88C6mZJHkY5_hjNWPWFGPK5k)

Examine program in ghidra and notice it only contains one function. Buf1 is our target. Set a breakpoint at write in GDB and examine the value of the register containing buf1 (%rsi).

![img](https://lh4.googleusercontent.com/OOM36OUFV1U7TH2-PQ4pT-B5sQ5Te2Bz0CnUaFiD21yxQHCfcFS8wr7y2bjHLMGNIu29ef6hE5tA4i-2u0-sDGW00BVieN8lEmNqIHKglypKRbdkpaPtmOiSPnvTHGvTq4s46u_6)

![img](https://lh4.googleusercontent.com/bMTd_phCMUFuRqfpsKOma88qTiJqv_2XASGVs6KIakpLQCRaMyJVfOARRXAquyt3WW-w526J_pZ3EZlhb0SVRThKdi4W-YUw4Vgg5FZ67PidbAOlSqmvXqMTqsCTezj-cG0oI9kQ)

![img](https://lh6.googleusercontent.com/NfoB-sj63uQk5m6_5F3dB0fjPMvo8rlodFREnwAjm9xHUKLeKBRhrcoJBvK9dtnJcunvEUScSdX5xi9m2cJPBxl-tLiUx3i4uVI0ARptJlsBpcdb3SnOOGE2MJwxIXvztXceOrUS)

Flag: `jctf{l00k5_1ik3_u_f0Und_m3_018a09d6}`

## going-over

### *Description*

- My friends said they were going on a trip but I think they ran into some trouble...
- They sent me these two files before we lost contact (**src.c** and **going-over**)

<span style="color: #e83e8c;">nc 0.cloud.chals.io 10197</span>

<details>
    <summary>View Hint</summary>
    If only there were a way to find the exact location of the ledge... like if the ledge had an address or something
</details>
[going-over](https://www.jerseyctf.site/files/294adfee89ff8062305c35796b90b97e/going-over?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzN9.YlR40Q.f-2IiuCiVZ-JOVxpKQ-BVa4GUsU)

[src.c](https://www.jerseyctf.site/files/a55cf2daca28047e9932cf984c7d6fae/src.c?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6MzR9.YlR40Q.vCGi5mXxagmbVfsN-Y7k9LbTNFc)

### *Writeup*

![img](https://lh6.googleusercontent.com/m6MWpDpnY8FiKuoCRR7m0svxvvWKnquvaQfza4-lIV42QTz8KNAo7aga-YUF81eEfVvbtiFLQX4k5MZqb69q1UvC-kTzCymeNo-wub1288uzlpnoYgI3IhzbOtuduSDlSNnQY3jd)