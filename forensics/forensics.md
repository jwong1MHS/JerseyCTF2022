# **forensics**

- [x] [speedy-at-midi (150)](#speedy-at-midi)
- [x] [data-backup (250)](#data-backup)
- [x] [recent-memory (250)](#recent-memory)
- [x] [scavenger-hunt (350)](#scavenger-hunt)
- [x] [corrupted-file (400)](#corrupted-file)

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