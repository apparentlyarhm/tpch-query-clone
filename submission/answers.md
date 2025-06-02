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

## Comments

Yeah this will make a difference if the entire 1.4gbs worth of records were processed-- my CPU is fairly powerful