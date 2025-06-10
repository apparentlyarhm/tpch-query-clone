## Reference Output - Scale Factor of 1.

Source: [tpch-dbgen answers/q5.out](https://github.com/electrum/tpch-dbgen/blob/master/answers/q5.out)

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 55,502,041.17 |
| VIETNAM   | 55,295,087.00 |
| CHINA     | 53,724,494.26 |
| INDIA     | 52,035,512.00 |
| JAPAN     | 45,410,175.70 |

## NOTE: AVG RUNTIME IS OF THE QUERY ONLY. I didnt parallelise the data reading process.

## Manual runs

### 4 Threads - SF = 1

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 55,502,041.17 |
| VIETNAM   | 55,295,087.00 |
| CHINA     | 53,724,494.26 |
| INDIA     | 52,035,512.00 |
| JAPAN     | 45,410,175.70 |

**AVG Runtime:** 1941 ms

### 1 Thread - SF = 1

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 55,502,041.17 |
| VIETNAM   | 55,295,087.00 |
| CHINA     | 53,724,494.26 |
| INDIA     | 52,035,512.00 |
| JAPAN     | 45,410,175.70 |

**AVG Runtime:** 3991 ms

### 4 Threads - SF = 2

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 115,979,499.65|
| CHINA     | 109,568,736.22|
| INDIA     | 106,258,458.17|
| JAPAN     | 104,738,341.03|
| VIETNAM   | 98,052,109.13 |

**AVG Runtime:** 3883 ms

### 1 Thread - SF = 2

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 115,979,499.65|
| CHINA     | 109,568,736.22|
| INDIA     | 106,258,458.17|
| JAPAN     | 104,738,341.03|
| VIETNAM   | 98,052,109.13 |

**AVG Runtime:** 8191 ms

---

# OLD RUNS - WRONG IMPLEMENTATIONS

`less data`

### 1 Thread

| n_name    | revenue    |
|-----------|------------|
| INDIA     | 171,682.84 |
| CHINA     | 50,100.36  |
| INDONESIA | 38,141.55  |
| VIETNAM   | 35,530.31  |
| JAPAN     | 28,676.63  |

**AVG Runtime:** 107 ms



### 4 Threads

| n_name    | revenue    |
|-----------|------------|
| INDIA     | 171,682.84 |
| CHINA     | 50,100.36  |
| INDONESIA | 38,141.55  |
| VIETNAM   | 35,530.31  |
| JAPAN     | 28,676.63  |

**AVG Runtime:** 96 ms


## Updated run - SF = 2

`full data but flawed implementation`

### 4 Threads

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 548,006,304.77|
| CHINA     | 538,243,372.74|
| JAPAN     | 525,068,963.42|
| INDIA     | 524,577,848.91|
| VIETNAM   | 524,377,067.47|

**AVG Runtime:** 4033 ms

### 1 Thread

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 548,006,304.77|
| CHINA     | 538,243,372.74|
| JAPAN     | 525,068,963.42|
| INDIA     | 524,577,848.91|
| VIETNAM   | 524,377,067.47|

**AVG Runtime:** 8687 ms


## Comment history

1. [first impl] Yeah this will make a difference if the entire 1.4gbs worth of records were processed-- my CPU is fairly powerful

2. [second impl] Since we load the entire data now, we see much closer results. On second note, this implementation was wrong.

3. [third fix] I think its correct now. The SF = 1 output matches.