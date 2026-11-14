# hadoop-s: Hotel Reservations (Zestaw 0)

Kompletne rozwiązanie zadania **Hadoop MapReduce** (Streaming & Classic) + **Apache Hive** + **Apache Airflow** w środowisku Docker.

## 📋 Spis Treści
* [Architektura Rozwiązania](#-architektura-rozwiązania)
* [Struktura Projektu](#-struktura-projektu)
* [Uruchomienie w Dockerze](#-uruchomienie-w-dockerze)
* [Wyniki Przetwarzania](#-wyniki-przetwarzania)
* [Szczegółowe Wyjaśnienie Skryptu Hive HQL](#-szczegółowe-wyjaśnienie-skryptu-hive-hql-hotel_rankinghql)
* [Dokumentacja i Samouczki](#-dokumentacja-i-samouczki)

---

## 🏗 Architektura Rozwiązania

1. **MapReduce (3):**
   * Agreguje rezerwacje hotelowe (`datasource1`: 10 000 wierszy) według `(hotel_id, year)`.
   * Oblicza: `reservation_count`, `avg_price` oraz `cancelled_count`.
   * Zwraca dokładnie **120 rekordów** (20 hoteli $\times$ 6 lat).
2. **Apache Hive (6):**
   * Łączy wyniki MapReduce ze słownikiem hoteli (`datasource4`: 20 hoteli).
   * Ogranicza do rezerwacji z roku 2024.
   * Oblicza współczynnik obłożenia $\frac{\text{rezerwacje 2024}}{\text{liczba pokoi}}$ oraz wyznacza pozycję za pomocą funkcji analitycznej `RANK() OVER`.
   * Zapisuje wynik do HDFS w formacie **JSON SerDe** (`org.apache.hive.hcatalog.data.JsonSerDe`).

---

## 📂 Struktura Projektu

```
hadoop-s/
├── data/
│   ├── datasource1/          # 10 plików CSV (reservations00..09.csv) - 10 000 wierszy
│   └── datasource4/          # Słownik hoteli (hotels.csv) - 20 hoteli
├── mapreduce/
│   ├── mapper.py             # Hadoop Streaming Mapper
│   ├── combiner.py           # Hadoop Streaming Combiner
│   ├── reducer.py            # Hadoop Streaming Reducer
│   ├── pom.xml               # Maven POM dla Classic Java
│   └── src/main/java/hotels/ # Kody Classic: Driver, Mapper, Combiner, Reducer
├── hive/
│   └── hotel_ranking.hql     # Zapytanie Hive HQL (EXTERNAL TABLE, SerDe JSON, RANK)
├── scripts/
│   ├── setup_hdfs.sh         # Skrypt tworzenia katalogów i wgrywania danych do HDFS
│   ├── run_mr.sh             # Skrypt uruchamiania MapReduce (streaming / classic)
│   ├── run_hive.sh           # Skrypt uruchamiania Hive przez Beeline
│   └── run_all.sh            # End-to-end potok
├── airflow/dags/             # Airflow DAG (hotel_reservations_dag.py)
├── output/                   # Wygenerowane pliki wynikowe (mr_results, hive_results)
├── Dockerfile                # Obraz środowiska Hadoop 3.3.6 + Hive 3.1.3
├── docker-compose.yml        # Konfiguracja kontenera
├── visualization.html        # Interaktywna platforma i dashboard w przeglądarce
├── interactive_tutorial.html # Interaktywny samouczek (Streaming vs Classic, Quiz)
├── TUTORIAL_OGOLNY_HADOOP_HIVE_DOCKER.md # Podręcznik architektury i usług Docker
├── PELNY_TUTORIAL_I_KOMENDY.md # Podręcznik kodu linijka po linijce i baza komend
├── RECENZJA_TUTORIAL.md      # Przewodnik recenzenta i oficjalne kryteria oceny
└── SCIAGA_KOMEND.md          # Ściąga najważniejszych komend CLI
```

---

## 🚀 Uruchomienie w Dockerze

```bash
# 1. Zbudowanie i uruchomienie kontenera:
docker-compose up -d

# 2. Wejście do kontenera:
docker exec -it hadoop-hotels bash

# 3. Uruchomienie pełnego potoku:
bash /opt/scripts/run_all.sh streaming
```

### Szybki test lokalny (bez klastra):
```bash
cat data/datasource1/*.csv | python3 mapreduce/mapper.py | sort | python3 mapreduce/combiner.py | sort | python3 mapreduce/reducer.py | head -15
```

---

## 📊 Wyniki Przetwarzania (Top 5 w 2024 r.)

| Pozycja | Nazwa Hotelu | Rezerwacje 2024 | Średnia cena | Anulowane (%) | Pokoje | Wskaźnik (rez./pok.) |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: |
| **🥇 1** | **Wiza Kutch and Treutel Hotel** | 100 | 966.91 zł | 33.00% | 88 | **1.1364** |
| **🥈 2** | **Feeney-Thiel Hotel** | 106 | 1003.93 zł | 43.40% | 95 | **1.1158** |
| **🥉 3** | **Dicki Group Hotel** | 106 | 966.34 zł | 31.13% | 98 | **1.0816** |
| **4** | **Jacobs Brown and Grant Hotel** | 111 | 1087.96 zł | 31.53% | 111 | **1.0000** |
| **5** | **Mann Inc Hotel** | 99 | 952.76 zł | 34.34% | 100 | **0.9900** |

---

## 🐝 Szczegółowe Wyjaśnienie Skryptu Hive HQL (`hotel_ranking.hql`)

Poniżej znajduje się pełna analiza skryptu HiveQL (`hive/hotel_ranking.hql`) w odniesieniu do oficjalnych kryteriów oceny (H01 – H15).

```sql
-- =============================================================
-- KROK 1: Idempotentność i czyszczenie struktur (Kryterium H03)
-- =============================================================
DROP TABLE IF EXISTS mr_results;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS hotel_ranking;
```
* **Wyjaśnienie:** Każde `CREATE TABLE` musi być poprzedzone `DROP TABLE IF EXISTS`. Dzięki temu skrypt można uruchamiać wielokrotnie bez błędu *Table already exists*.

---

```sql
-- =============================================================
-- KROK 2: Definicja tabeli dla wyników MapReduce (Kryteria H01, H02, H04)
-- =============================================================
CREATE EXTERNAL TABLE mr_results (
    hotel_id          STRING,
    year              STRING,
    reservation_count BIGINT,
    avg_price         DOUBLE,
    cancelled_count   BIGINT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir3}';
```
* **`EXTERNAL TABLE` (Kryterium H02):** Kluczowy element architektury Big Data. Tabela jedynie wskazuje pliki w HDFS (`LOCATION`). Hive nie przenosi danych (brak `LOAD DATA INPATH`, które niszczy pliki źródłowe).
* **`FIELDS TERMINATED BY '\t'`:** Wynik MapReduce jest rozdzielany tabulatorami, więc Hive poprawnie mapuje kolumny.
* **`${hivevar:input_dir3}` (Kryterium H01):** Ścieżka nie jest wpisana na stałe (*hardcoded*), lecz przekazywana dynamicznie z Beeline przez parametr `--hivevar`.
* **Trwałość źródeł (Kryterium H04):** Tabela ta nie jest usuwana na końcu skryptu – recenzent może ją odpytać po zakończeniu zadania w celu debugowania.

---

```sql
-- =============================================================
-- KROK 3: Definicja tabeli dla słownika hoteli (Kryteria H01, H02)
-- =============================================================
CREATE EXTERNAL TABLE hotels (
    hotel_id  STRING,
    name      STRING,
    region    STRING,
    category  STRING,
    rooms     INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:input_dir4}'
TBLPROPERTIES ('skip.header.line.count'='1');
```
* **`FIELDS TERMINATED BY ','`:** Słownik hoteli to plik CSV, więc separatorem pól jest przecinek.
* **`TBLPROPERTIES ('skip.header.line.count'='1')`:** Automatycznie instruuje Hive, aby pominąć pierwszy wiersz nagłówka CSV (`hotel_id,name,region...`), zapobiegając błędom konwersji typów (np. próbie wczytania słowa "rooms" do kolumny typu `INT`).

---

```sql
-- =============================================================
-- KROK 4: Tabela wynikowa z mechanizmem SerDe JSON (Kryterium H15)
-- =============================================================
CREATE EXTERNAL TABLE hotel_ranking (
    hotel_name          STRING,
    total_reservations  BIGINT,
    avg_price           DOUBLE,
    cancelled_ratio     DOUBLE,
    rooms               INT,
    rank                INT
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:output_dir6}';
```
* **`ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'` (Kryterium H15):** Zamiast ręcznego sklejania ciągów znaków funkcją `CONCAT`, używamy wbudowanej klasy serializatora Hive. Każdy rekord zapisywany poleceniem `INSERT` automatycznie staje się poprawnym, zwalidowanym obiektem JSON w HDFS.

---

```sql
-- =============================================================
-- KROK 5: Złożone zapytanie jednoetapowe (Kryteria H05, H06, H07, H08, H09)
-- =============================================================
INSERT OVERWRITE TABLE hotel_ranking
SELECT
    -- Usunięcie ewentualnych cudzysłowów z nazwy hotelu
    REGEXP_REPLACE(TRIM(h.name), '^\"|\"$', '')                                         AS hotel_name,
    mr.reservation_count                                                                 AS total_reservations,
    mr.avg_price                                                                         AS avg_price,
    -- Obliczenie udziału anulowanych rezerwacji (wartość od 0.0 do 1.0)
    ROUND(CAST(mr.cancelled_count AS DOUBLE) / CAST(mr.reservation_count AS DOUBLE), 6) AS cancelled_ratio,
    h.rooms                                                                              AS rooms,
    -- Funkcja analityczna RANK() obsługująca remisy (Kryterium H09)
    RANK() OVER (
        ORDER BY (CAST(mr.reservation_count AS DOUBLE) / CAST(h.rooms AS DOUBLE)) DESC
    )                                                                                    AS rank
FROM mr_results mr
JOIN hotels h 
    ON mr.hotel_id = h.hotel_id
WHERE mr.year = '2024';
```
* **Brak zbędnych tabel pośrednich (Kryterium H05):** Całe przetwarzanie (złączenie, filtrowanie, wyliczenie metryk i ranking) odbywa się w **jednym zapytaniu**, co przekłada się na pojedynczy, optymalny przebieg na klastrze YARN.
* **`JOIN hotels h ON mr.hotel_id = h.hotel_id` (Kryterium H06):** Poprawne złączenie wewnętrzne po unikalnym identyfikatorze hotelu.
* **`WHERE mr.year = '2024'` (Kryterium H07):** Filtrowanie danych wyłącznie do roku 2024, zgodnie z treścią zadania.
* **`cancelled_ratio` (Kryterium H08):** Obliczony ze wzoru $\frac{\text{cancelled\_count}}{\text{reservation\_count}}$ rzutowanego na `DOUBLE`, z zaokrągleniem do 6 miejsc po przecinku (`ROUND(..., 6)`).
* **`RANK() OVER (ORDER BY (rezerwacje / pokoje) DESC)` (Kryterium H09):**
  * Użycie funkcji `RANK()` zamiast `ROW_NUMBER()` gwarantuje, że przy remisach hotele otrzymują tę samą pozycję, a następna jest pomijana (np. 1, 2, 3, 4, 4, 6).

---

### Jak uruchomić to zapytanie przez Beeline? (Kryterium H14)

```bash
beeline -u "jdbc:hive2://localhost:10000/default" -n root \
    --hivevar input_dir3="/user/root/output/mr_results" \
    --hivevar input_dir4="/user/root/input/hotels" \
    --hivevar output_dir6="/user/root/output/hive_results" \
    -f /opt/hive/hotel_ranking.hql
```

---

## 📚 Dokumentacja i Samouczki

* [TUTORIAL_OGOLNY_HADOOP_HIVE_DOCKER.md](TUTORIAL_OGOLNY_HADOOP_HIVE_DOCKER.md) – **Kompleksowy podręcznik architektoniczny:** Jak działa Hadoop Streaming, model MapReduce, Apache Hive, mechanizm SerDe oraz usługi kontenera Docker (HDFS NameNode/DataNode, YARN, HiveServer2, Metastore)
* [PELNY_TUTORIAL_I_KOMENDY.md](PELNY_TUTORIAL_I_KOMENDY.md) – Analiza kodu linijka po linijce, różnice Streaming vs Classic, komendy Beeline & Streaming
* [RECENZJA_TUTORIAL.md](RECENZJA_TUTORIAL.md) – Jak oceniać projekty wg oficjalnych 35 kryteriów i unikać pułapek
* [SCIAGA_KOMEND.md](SCIAGA_KOMEND.md) – Gotowe komendy do HDFS, MapReduce, Hive i Dockera
* [interactive_tutorial.html](interactive_tutorial.html) – Interaktywna aplikacja webowa
