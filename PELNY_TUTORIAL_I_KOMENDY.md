# 📚 Kompendium Wiedzy: Hadoop Streaming, Classic & Apache Hive (Zestaw 0)

Kompleksowy podręcznik techniczny wyjaśniający **architekturę, kod linijka po linijce, różnice Streaming vs Classic** oraz **pełną bazę komend** do Hadoop Streaming i Apache Hive / Beeline.

---

## 📑 Spis Treści
1. [Logika Zadania i Architektura Przepływu](#1-logika-zadania-i-architektura-przepływu)
2. [Analiza Kodu MapReduce Linijka po Linijce](#2-analiza-kodu-mapreduce-linijka-po-linijce)
   - [Hadoop Streaming: mapper.py](#a-hadoop-streaming-mapperpy)
   - [Hadoop Streaming: combiner.py](#b-hadoop-streaming-combinerpy)
   - [Hadoop Streaming: reducer.py](#c-hadoop-streaming-reducerpy)
   - [Hadoop Classic (Java): Driver, Mapper, Combiner, Reducer](#d-hadoop-classic-java)
3. [Analiza Kodu Apache Hive Linijka po Linijce](#3-analiza-kodu-apache-hive-linijka-po-linijce)
   - [Definicja Tabel Zewnętrznych (EXTERNAL TABLE)](#a-tabele-źródłowe-external-table)
   - [Serializacja JSON SerDe](#b-serializacja-json-serde-kryterium-h15)
   - [Zapytanie Główne: JOIN + RANK() OVER](#c-zapytanie-główne-join--rank-over)
4. [Hadoop Streaming vs Classic – Głębokie Porównanie](#4-hadoop-streaming-vs-classic--głębokie-porównanie)
5. [Kompendium Komend Hadoop Streaming](#5-kompendium-komend-hadoop-streaming)
6. [Kompendium Komend Apache Hive & Beeline](#6-kompendium-komend-apache-hive--beeline)
7. [Weryfikacja i Debugowanie Wyników](#7-weryfikacja-i-debugowanie-wyników)

---

## 1. Logika Zadania i Architektura Przepływu

```
[datasource1: 10x reservations.csv] ⟶ (Mapper) ⟶ [hotel_id, year \t price,is_canc]
                                       ↓
                                (Shuffle & Sort)
                                       ↓
                                (Reducer) ⟶ [HDFS: mr_results (120 wierszy)]
                                                    ↓
[datasource4: hotels.csv] ────────────────────⟶ (Hive HQL)
                                                    ↓
                                            [HDFS: hive_results (20 hoteli w JSON SerDe)]
```

### Cele Etapów:
1. **MapReduce (3):**
   * Grupowanie po `(hotel_id, year)` wyciągniętym z `check_in`.
   * Obliczenie: `reservation_count` (liczba rezerwacji), `avg_price` (średnia cena), `cancelled_count` (liczba anulowanych o statusie `Cancelled`).
   * Zwraca dokładnie **120 rekordów** (20 hoteli $\times$ 6 lat).
2. **Hive (5) $\rightarrow$ (6):**
   * Złączenie wyniku MapReduce ze słownikiem hoteli (`datasource4`) po `hotel_id`.
   * Ograniczenie do rezerwacji z roku `2024`.
   * Obliczenie współczynnika anulowania: $\text{cancelled\_ratio} = \frac{\text{cancelled\_count}}{\text{total\_reservations}}$.
   * Obliczenie pozycji w rankingu za pomocą funkcji okienkowej `RANK() OVER (ORDER BY (rezerwacje / pokoje) DESC)`.
   * Zapis każdego wiersza do HDFS w formacie **JSON** za pomocą mechanizmu **JsonSerDe**.

---

## 2. Analiza Kodu MapReduce Linijka po Linijce

### A. Hadoop Streaming: `mapper.py`

```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    # Pominięcie pustych linii oraz wiersza nagłówkowego CSV (Kryterium M01)
    if not line or line.startswith("reservation_id"):
        continue

    parts = line.split(",")
    # Walidacja liczby kolumn - ochrona przed uszkodzonymi wierszami
    if len(parts) < 7:
        continue

    try:
        hotel_id = parts[1].strip()
        # Wyciągnięcie roku z formatu yyyy-MM-dd'T'HH:mm (pierwsze 4 znaki)
        year = parts[3].strip()[:4]
        int(year)  # walidacja czy rok jest liczbą

        price = float(parts[5].strip())
        # Flaga anulowania: 1 jeśli Cancelled, w przeciwnym razie 0
        is_cancelled = 1 if parts[6].strip() == "Cancelled" else 0

        # EMISJA: Klucz i wartość ROZDZIELONE TABULATOREM (\t)
        # Klucz złożony: hotel_id \t year
        # Wartość: cena,flaga_anulacji (Kryterium M02 & M03)
        print(f"{hotel_id}\t{year}\t{price},{is_cancelled}")
    except (ValueError, IndexError):
        # Błędne rekordy są bezpiecznie pomijane bez awarii zadania (Kryterium M05)
        continue
```

#### Dlaczego tak?
* **Brak `records.append()`:** Pętla czyta strumieniowo ze `sys.stdin` linijka po linijce.
* **Emisja składowych:** Mapper emituje cenę jednostkową, a **NIE średnią**. Pozwala to na poprawne działanie Combinera.

---

### B. Hadoop Streaming: `combiner.py`

```python
#!/usr/bin/env python3
import sys

current_key = None
count = 0
sum_price = 0.0
cancelled = 0

def emit(key, count, sum_price, cancelled):
    hotel_id, year = key.split("\t")
    # FORMAT WYJŚCIA IDENTYCZNY Z MAPPEREM (Kryterium M07):
    # klucz \t count,sum_price,cancelled_count
    print(f"{hotel_id}\t{year}\t{count},{round(sum_price, 2)},{cancelled}")

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 3:
        continue

    key = f"{parts[0]}\t{parts[1]}"
    val_parts = parts[2].split(",")

    try:
        # Obsługa wejścia zarówno z Mappera (2 pola), jak i z innego Combinera (3 pola)
        if len(val_parts) == 2:
            p = float(val_parts[0])
            c = int(val_parts[1])
            k_count = 1
        elif len(val_parts) == 3:
            k_count = int(val_parts[0])
            p = float(val_parts[1])
            c = int(val_parts[2])
        else:
            continue
    except (ValueError, IndexError):
        continue

    # Akumulacja na posortowanym strumieniu
    if current_key and current_key != key:
        emit(current_key, count, sum_price, cancelled)
        count = 0
        sum_price = 0.0
        cancelled = 0

    current_key = key
    count += k_count
    sum_price += p
    cancelled += c

if current_key:
    emit(current_key, count, sum_price, cancelled)
```

#### Kluczowa zasada Combinera:
Combiner **nie wylicza średniej**. Gdyby wyliczył średnią cząstkową, końcowa średnia w Reducerze byłaby matematycznie błędna! Combiner łączy sumy i liczności.

---

### C. Hadoop Streaming: `reducer.py`

```python
#!/usr/bin/env python3
import sys

current_key = None
count = 0
sum_price = 0.0
cancelled = 0

def emit(key, count, sum_price, cancelled):
    hotel_id, year = key.split("\t")
    # Średnia wyliczana DOPIERO na koniec całej grupy (Kryterium M09)
    avg_price = round(sum_price / count, 2) if count > 0 else 0.0
    # Czysty format wyjściowy zgodny ze specyfikacją (Kryterium M10)
    print(f"{hotel_id}\t{year}\t{count}\t{avg_price}\t{cancelled}")

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) < 3:
        continue

    key = f"{parts[0]}\t{parts[1]}"
    val_parts = parts[2].split(",")

    try:
        if len(val_parts) == 2:
            p = float(val_parts[0])
            c = int(val_parts[1])
            k_count = 1
        else:
            k_count = int(val_parts[0])
            p = float(val_parts[1])
            c = int(val_parts[2])
    except (ValueError, IndexError):
        continue

    if current_key and current_key != key:
        emit(current_key, count, sum_price, cancelled)
        count, sum_price, cancelled = 0, 0.0, 0

    current_key = key
    count += k_count
    sum_price += p
    cancelled += c

if current_key:
    emit(current_key, count, sum_price, cancelled)
```

---

### D. Hadoop Classic (Java)

W wariancie Classic (Java API) zamiast strumieni Unix używamy natywnych klas Hadoop:

1. **`ReservationMapper.java`:**
   * Dziedziczy po `Mapper<LongWritable, Text, Text, Text>`.
   * Przekazuje obiekty w pamięci JVM przez `context.write(new Text(hotel_id + "\t" + year), new Text(price + "," + isCancelled))`.
2. **`ReservationCombiner.java`:**
   * Dziedziczy po `Reducer<Text, Text, Text, Text>`.
   * Wyjście: `context.write(key, new Text(count + "," + sumPrice + "," + cancelled))`.
3. **`ReservationReducer.java`:**
   * Sumuje składowe i używa `BigDecimal.divide(..., 2, RoundingMode.HALF_UP)`.
   * Wyjście: `context.write(new Text(hotel_id + "\t" + year), new Text(count + "\t" + avg + "\t" + cancelled))`.
4. **`ReservationDriver.java`:**
   * Konfiguruje obiekt `Job`:
     ```java
     job.setJarByClass(ReservationDriver.class);
     job.setMapperClass(ReservationMapper.class);
     job.setCombinerClass(ReservationCombiner.class);
     job.setReducerClass(ReservationReducer.class);
     ```

---

## 3. Analiza Kodu Apache Hive Linijka po Linijce

Skrypt: `hive/hotel_ranking.hql`

### A. Tabele Źródłowe (EXTERNAL TABLE)

```sql
-- Powtarzalność: DROP IF EXISTS (Kryterium H03)
DROP TABLE IF EXISTS mr_results;
DROP TABLE IF EXISTS hotels;
DROP TABLE IF EXISTS hotel_ranking;

-- Tabela dla wyników MapReduce (Kryterium H01, H02)
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

-- Tabela dla słownika hoteli (Kryterium H01, H02)
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

* **Dlaczego `EXTERNAL TABLE`:** Pliki w HDFS nie są kopiowane ani przenoszone. Po zakończeniu skryptu dane źródłowe pozostają nienaruszone (Kryterium H04).
* **Zmienne `${hivevar:...}`:** Ścieżki są sparametryzowane i przekazywane z zewnątrz (Kryterium H01).

---

### B. Serializacja JSON SerDe (Kryterium H15)

```sql
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

* Użycie **`JsonSerDe`** powoduje, że Hive automatycznie zapisuje każdy wiersz tabeli jako czysty obiekt JSON (np. `{"hotel_name":"...", "rank":1}`). Zabronione jest ręczne klejenie tekstu przez `CONCAT`!

---

### C. Zapytanie Główne: JOIN + RANK() OVER

```sql
INSERT OVERWRITE TABLE hotel_ranking
SELECT
    -- Usunięcie ewentualnych cudzysłowów z nazwy hotelu
    REGEXP_REPLACE(TRIM(h.name), '^\"|\"$', '')                                         AS hotel_name,
    mr.reservation_count                                                                 AS total_reservations,
    mr.avg_price                                                                         AS avg_price,
    -- Współczynnik anulowania (od 0.0 do 1.0) z dokładnością do 6 miejsc
    ROUND(CAST(mr.cancelled_count AS DOUBLE) / CAST(mr.reservation_count AS DOUBLE), 6) AS cancelled_ratio,
    h.rooms                                                                              AS rooms,
    -- Funkcja okienkowa RANK(): remisy otrzymują tę samą pozycję (Kryterium H09)
    RANK() OVER (
        ORDER BY (CAST(mr.reservation_count AS DOUBLE) / CAST(h.rooms AS DOUBLE)) DESC
    )                                                                                    AS rank
FROM mr_results mr
JOIN hotels h 
    ON mr.hotel_id = h.hotel_id
WHERE mr.year = '2024';
```

* **Brak tabel pośrednich (Kryterium H05):** Całe przetwarzanie odbywa się w jednym złożonym zapytaniu (jeden przebieg MapReduce/Tez).
* **`RANK() OVER`:** Sortuje malejąco po $\frac{\text{rezerwacje}}{\text{pokoje}}$. Przy remisie oba hotele dostają ten sam numer, a kolejna pozycja jest pomijana (np. 1, 2, 3, 4, 4, 6).

---

## 4. Hadoop Streaming vs Classic – Głębokie Porównanie

| Cecha | 🐍 Hadoop Streaming | ☕ Hadoop Classic (Java) |
| :--- | :--- | :--- |
| **Model procesów** | Hadoop uruchamia procesy podrzędne interpretera (`python3`). | Wszystko działa wewnątrz wątków wirtualnej maszyny Java (JVM). |
| **Mechanizm IPC** | Łącza nienazwane Unix (`pipe`). Przekazywanie bajt po bajcie. | Bezpośrednie wywołania metod w pamięci RAM. |
| **Format danych** | Tekst rozdzielany `\t` (tabulator) i `\n` (nowa linia). | Obiekty binarne implementujące interfejs `Writable`. |
| **Zarządzanie pamięcią** | Niezależne procesy systemu operacyjnego. | Zarządzane przez Garbage Collector maszyny JVM. |
| **Elastyczność** | Dowolny język (Python, Bash, Perl, Node, C++). | Tylko języki działające na JVM (Java, Scala, Kotlin). |
| **Szybkość kompilacji** | **Zero czasu:** Zmieniasz skrypt i odpalasz od razu. | Wymaga cyklu budowania: `mvn clean package`. |
| **Overhead wydajności** | ~10-25% narzutu na serializację tekstową i IPC. | Maksymalna wydajność, brak narzutu na IPC. |

---

## 5. Kompendium Komend Hadoop Streaming

### 1. Podstawowe uruchomienie Streaming
```bash
mapred streaming \
    -files /opt/mapreduce/mapper.py,/opt/mapreduce/combiner.py,/opt/mapreduce/reducer.py \
    -input /user/root/input/reservations \
    -output /user/root/output/mr_results \
    -mapper "python3 mapper.py" \
    -combiner "python3 combiner.py" \
    -reducer "python3 reducer.py"
```

### 2. Zaawansowane flagi Streaming
* **`-files plik1,plik2`** – dystrybuuje pliki do katalogu roboczego każdego węzła na klastrze.
* **`-D mapreduce.job.reduces=2`** – wymusza liczbę reducerów (np. 2 partycje wyjściowe `part-00000`, `part-00001`).
* **`-D stream.num.map.output.key.fields=2`** – określa, ile początkowych pól tworzy klucz (domyślnie 1).
* **`-cmdenv VAR=value`** – przekazuje zmienną środowiskową do skryptu mappera/reducera.

### 3. Testowanie lokalne w terminalu (w 1 linijce, bez klastra!)
```bash
# Bash:
cat data/datasource1/*.csv | python3 mapreduce/mapper.py | sort | python3 mapreduce/combiner.py | sort | python3 mapreduce/reducer.py | head -20

# PowerShell:
Get-Content data\datasource1\*.csv | python mapreduce\mapper.py | Sort-Object | python mapreduce\combiner.py | Sort-Object | python mapreduce\reducer.py | Select-Object -First 20
```

---

## 6. Kompendium Komend Apache Hive & Beeline

### 1. Uruchomienie skryptu HQL przez Beeline (zalecany standard)
```bash
beeline -u "jdbc:hive2://localhost:10000/default" -n root \
    --hivevar input_dir3="/user/root/output/mr_results" \
    --hivevar input_dir4="/user/root/input/hotels" \
    --hivevar output_dir6="/user/root/output/hive_results" \
    -f /opt/hive/hotel_ranking.hql
```

### 2. Tryb wbudowany Beeline (gdy HiveServer2 nie nasłuchuje na porcie 10000)
```bash
beeline -u "jdbc:hive2://" \
    --hivevar input_dir3="/user/root/output/mr_results" \
    --hivevar input_dir4="/user/root/input/hotels" \
    --hivevar output_dir6="/user/root/output/hive_results" \
    -f /opt/hive/hotel_ranking.hql
```

### 3. Szybkie zapytanie inline (flaga `-e`)
```bash
beeline -u "jdbc:hive2://localhost:10000/default" -n root -e "SHOW TABLES; SELECT * FROM hotel_ranking LIMIT 5;"
```

### 4. Sprawdzenie definicji i parametrów tabeli
```sql
-- W konsoli Hive / Beeline:
DESCRIBE FORMATTED hotel_ranking;
SHOW CREATE TABLE hotel_ranking;
```

---

## 7. Weryfikacja i Debugowanie Wyników

### 1. Sprawdzenie liczby rekordów MapReduce (dokładnie 120 wierszy):
```bash
hdfs dfs -cat /user/root/output/mr_results/part-* | wc -l
```

### 2. Podgląd wyniku MapReduce:
```bash
hdfs dfs -cat /user/root/output/mr_results/part-* | head -10
```

### 3. Weryfikacja liczby rekordów Hive (dokładnie 20 wierszy):
```bash
hdfs dfs -cat /user/root/output/hive_results/* | wc -l
```

### 4. Sprawdzenie poprawności JSON (narzędziem `jq`):
```bash
hdfs dfs -cat /user/root/output/hive_results/* | jq . | head -25
```

### 5. Format rekordu wyjściowego SerDe JSON:
```json
{"hotel_name":"Wiza Kutch and Treutel Hotel","total_reservations":100,"avg_price":966.91,"cancelled_ratio":0.33,"rooms":88,"rank":1}
```

---

## 8. Pliki w Projekcie

* 🌐 **Interaktywny tutorial i wizualizacja w przeglądarce:** [interactive_tutorial.html](file:///c:/Users/macwo/Desktop/hadoop-0/interactive_tutorial.html)
* 📋 **Ściąga komend i uruchamianie Docker:** [SCIAGA_KOMEND.md](file:///c:/Users/macwo/Desktop/hadoop-0/SCIAGA_KOMEND.md)
* 🎓 **Poradnik recenzenta i kryteria oceny:** [RECENZJA_TUTORIAL.md](file:///c:/Users/macwo/Desktop/hadoop-0/RECENZJA_TUTORIAL.md)
* 🐍 **Kody MapReduce:** [mapper.py](file:///c:/Users/macwo/Desktop/hadoop-0/mapreduce/mapper.py), [combiner.py](file:///c:/Users/macwo/Desktop/hadoop-0/mapreduce/combiner.py), [reducer.py](file:///c:/Users/macwo/Desktop/hadoop-0/mapreduce/reducer.py)
* 🐝 **Skrypt Hive:** [hotel_ranking.hql](file:///c:/Users/macwo/Desktop/hadoop-0/hive/hotel_ranking.hql)
* 📁 **Czyste dane wejściowe:** [data/datasource1/](file:///c:/Users/macwo/Desktop/hadoop-0/data/datasource1) oraz [data/datasource4/](file:///c:/Users/macwo/Desktop/hadoop-0/data/datasource4)
