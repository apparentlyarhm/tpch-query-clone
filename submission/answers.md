## Reference Output

Source: [tpch-dbgen answers/q5.out](https://github.com/electrum/tpch-dbgen/blob/master/answers/q5.out)

| n_name    | revenue        |
|-----------|---------------|
| INDONESIA | 55,502,041.17 |
| VIETNAM   | 55,295,087.00 |
| CHINA     | 53,724,494.26 |
| INDIA     | 52,035,512.00 |
| JAPAN     | 45,410,175.70 |

---

## Manual Run Results

### 1 Thread

| n_name    | revenue    |
|-----------|------------|
| INDIA     | 171,682.84 |
| CHINA     | 50,100.36  |
| INDONESIA | 38,141.55  |
| VIETNAM   | 35,530.31  |
| JAPAN     | 28,676.63  |

**AVG Runtime:** 107 ms

---

### 4 Threads

| n_name    | revenue    |
|-----------|------------|
| INDIA     | 171,682.84 |
| CHINA     | 50,100.36  |
| INDONESIA | 38,141.55  |
| VIETNAM   | 35,530.31  |
| JAPAN     | 28,676.63  |

**AVG Runtime:** 96 ms


## Updated runs 

### 4 Threads

n_name                    |revenue
INDONESIA                 |548006304.77
CHINA                     |538243372.74
JAPAN                     |525068963.42
INDIA                     |524577848.91
VIETNAM                   |524377067.47

**AVG Runtime:** 4033 ms

### 1 Thread

n_name                    |revenue
INDONESIA                 |548006304.77
CHINA                     |538243372.74
JAPAN                     |525068963.42
INDIA                     |524577848.91
VIETNAM                   |524377067.47

**AVG Runtime:** 8687 ms


## Comments

[first impl] Yeah this will make a difference if the entire 1.4gbs worth of records were processed-- my CPU is fairly powerful

[second impl] Since we load the entire data now, we see much closer results. 