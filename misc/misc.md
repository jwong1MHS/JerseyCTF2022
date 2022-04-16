# **misc**

- [x] [firewall-rules (100)](#firewall-rules)
- [x] [snort-log (100)](#snort-log)
- [x] [we-will (150)](#we-will)
- [x] [filtered-feeders (150)](#filtered-feeders)
- [x] [bank-clients (250)](#bank-clients)
- [x] [dnsmasq-ip-extract (300)](#dnsmasq-ip-extract)
- [x] [check-the-shadows (300)](#check-the-shadows)

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