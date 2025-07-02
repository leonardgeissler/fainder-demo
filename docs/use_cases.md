# Use Cases

DQL queries and results for different use cases using the Kaggle dataset COLlection.

## Lung Cancer

1. `KW('"lung cancer"')`
   → approx. **100** results

2. `KW('"lung cancer"') AND COL(name("age"; 0))`
   → approx. **10** results

3. `KW('"lung cancer"') AND COL(name("age"; 4))`
   → approx. **70** results

4. `KW('"lung cancer"') AND COL(name("age"; 4) AND PP(0.7; le; 50))`

   → **with exact:** 18 results

   → **Runtime:**
      - big config: **2 sec**
      - middle config: **2.45 sec**
      - small config: **2.54 sec**
      - ultra small config: **3.9 sec**
      - bad_3 config: **3 sec**
      - bad_1_config: **5 sec**

   → **with full_recall:**
      - big config: **18** results
      - middle config: **18** results
      - small config: **18** results
      - ultra small config **21** results
      - bad_3 config: **63** results
      - bad_1_config: **71** results

   → **with full_precision:**
      - big config: **0** results
      - middle config: **0** results
      - small config: **0** results
      - ultra small config: **0** results
      - bad_3 config: **0** results
      - bad_1_config: **0** results

## Cardiovascular

1. `KW('Cardiovascular')`
   → approx. **200** results

2. `KW('Cardiovascular') AND COL(name("age"; 3))`
   → approx. **100** results

3. `KW('Cardiovascular') AND COL(name("age"; 3) AND PP(0.75; le; 45))`

   → **with exact:** 11 results

   → **Runtime:**
      - big config: **1.7 sec**
      - middle config: **2.4 sec**
      - small config: **2.45 sec**
      - ultra small config: **3.1 sec**
      - bad_3 config: **2.9 sec**
      - bad_1_config: **5 sec**

   → **with full_recall:**
      - big config: **11** results
      - middle config: **11** results
      - small config: **12** results
      - ultra small config: **18** results
      - bad_3 config: **42** results
      - bad_1_config: **88** results

   → **with full_precision:**
      - big config: **10** results
      - middle config: **10** results
      - small config: **10** results
      - ultra small config: **9** results
      - bad_3 config: **10** results
      - bad_1_config: **5** results

## Cars

1. `KW('"Car"')`
   → approx. **2,300** results

2. `KW('"Car"') AND COL(name("price"; 0))`
   → approx. **460** results

3. `KW('"Car"') AND COL(name("price"; 0) AND PP(1.0; le; 9000))`

   → **with exact:** 29 results

   → **Runtime:**
      - big config: **1 sec**
      - middle config: **1.3 sec**
      - small config: **1.3 sec**
      - ultra small config: **1.7 sec**
      - bad_3 config: **1.8 sec**
      - bad_1_config: **2.9 sec**

   → **with full_recall:**
      - big config: **33** results
      - middle config: **42** results
      - small config: **33** results
      - ultra small config: **68** results
      - bad_3 config: **69** results
      - bad_1_config: **355** results

   → **with full_precision:**
      - big config: **14** results
      - middle config: **14** results
      - small config: **14** results
      - ultra small config: **14** results
      - bad_3 config: **14** results
      - bad_1_config: **13** results

## Loans

1. `KW('"loan"')`
   → approx. **1100** results

2. `KW('"loan"') AND COL(name("income";0))`
   → approx. **45** results

3. `KW('"loan"') AND COL(name("income";0) AND PP(0.4;le;15000))`

   → **with exact:**  28 results

   → **Runtime:**
      - big config: **1.8 sec**
      - middle config: **1.9 sec**
      - small config: **1.9 sec**
      - ultra small config: **2 sec**
      - bad_3 config: **2.3 sec**
      - bad_1_config: **2.89 sec**

   → **with full_recall:**
      - big config: **28** results
      - middle config: **28** results
      - small config: **28** results
      - ultra small config: **32** results
      - bad_3 config: **39** results
      - bad_1_config: **40** results

   → **with full_precision:**
      - big config: **13** results
      - middle config: **13** results
      - small config: **13** results
      - ultra small config: **13** results
      - bad_3 config: **13** results
      - bad_1_config: **8** results

## Air Quality

1. `KW('"air quality"')`
   → approx. **480** results

2. `KW('"air quality"') AND COL(name("no2";0))`
   → approx. **31** results

3. `KW('"air quality"') AND COL(name("no2";0) AND PP(0.2; ge; 100))`

   → **with exact:**  4 results

   → **Runtime:**
      - big config: **1.9 sec**
      - middle config: **2.0 sec**
      - small config: **2.1 sec**
      - ultra small config: **2.6 sec**
      - bad_3 config: **2.6 sec**
      - bad_1_config: **4.4 sec**

   → **with full_recall:**
      - big config: **5** results
      - middle config: **6** results
      - small config: **7** results
      - ultra small config: **7** results
      - bad_3 config: **8** results
      - bad_1_config: **11** results

   → **with full_precision:**
      - big config: **2** results
      - middle config: **2** results
      - small config: **2** results
      - ultra small config: **0** results
      - bad_3 config: **1** results
      - bad_1_config: **0** results

## Student Performance

1. `KW('"student performance"')`
   → approx. **260** results

2. `KW('"student performance"') AND COL(name('"math score"';2))`
   → approx. **50** results

3. `KW('"student performance"') AND COL(name('"math score"';2) AND PP(0.25; lt; 50))`

   → **with exact:** 5 results

   → **Runtime:**
      - big config: **2.8 sec**
      - middle config: **2.9 sec**
      - small config: **2.9 sec**
      - ultra small config: **3.4 sec**
      - bad_3 config: **3.7 sec**
      - bad_1_config: **5 sec**

   → **with full_recall:**
      - big config: **46** results
      - middle config: **46** results
      - small config: **46** results
      - ultra small config: **46** results
      - bad_3 config: **49** results
      - bad_1_config: **50** results

   → **with full_precision:**
      - big config: **1** results
      - middle config: **1** results
      - small config: **1** results
      - ultra small config: **1** results
      - bad_3 config: **0** results
      - bad_1_config: **0** results
