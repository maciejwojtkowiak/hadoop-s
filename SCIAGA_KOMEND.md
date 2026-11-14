# 🏨 Hotel Reservations (Zestaw 0) – Ściąga Komend

Kompletna ściąga komend do uruchomienia i testowania projektu **Hadoop MapReduce (Classic & Streaming)** + **Apache Hive** w środowisku Docker.

---

## 📋 Spis Treści
0. [Spodziewane Wyniki Wzorcowe (Benchmark)](#0-spodziewane-wyniki-wzorcowe-benchmark)
1. [Poradnik Recenzenta i Kryteria Oceny](#1-poradnik-recenzenta-i-kryteria-oceny)
2. [Docker – Start i Zarządzanie](#2-docker--start-i-zarządzanie)
2. [HDFS – Wgrywanie i Sprawdzanie Danych](#2-hdfs--wgrywanie-i-sprawdzanie-danych)
3. [MapReduce Streaming (Python + Combiner)](#3-mapreduce-streaming-python--combiner)
4. [MapReduce Classic (Java JAR)](#4-mapreduce-classic-java-jar)
5. [Apache Hive (Beeline + SerDe JSON)](#5-apache-hive-beeline--serde-json)
6. [Skrypt All-in-One](#6-skrypt-all-in-one)
7. [Apache Airflow (Opcjonalnie)](#7-apache-airflow-opcjonalnie)
8. [Szybki Test Lokalny (bez Hadoop)](#8-szybki-test-lokalny-bez-hadoop)

---

## 0. Spodziewane Wyniki Wzorcowe (Benchmark)

Zgodnie ze specyfikacją kursu dla pełnego zbioru Zestawu 0:

### Etap MapReduce (120 rekordów):
* Kolumny: `hotel_id`, `year`, `reservation_count`, `avg_price`, `cancelled_count`
* Przykładowy rekord:
  ```
  HOTEL0001    2021    79    1009.86    26
  ```

### Etap Hive (20 rekordów – wyłącznie rezerwacje z roku 2024):
* Kolumny: `hotel_name`, `total_reservations`, `avg_price`, `cancelled_ratio`, `rooms`, `rank`
* Zwycięzca rankingu (pozycja 1):
  ```
  Farrell Inc Hotel    488    1010.7    0.360656    50    1
  ```
* Format wyjściowy w HDFS (zgodnie z kryterium H15): **JSON SerDe**
  ```json
  {"hotel_name":"Farrell Inc Hotel","total_reservations":488,"avg_price":1010.7,"cancelled_ratio":0.360656,"rooms":50,"rank":1}
  ```

---

## 1. Poradnik Recenzenta i Kryteria Oceny

* 📚 **Pełny Podręcznik Kodu i Komend (Nowość):** [PELNY_TUTORIAL_I_KOMENDY.md](file:///c:/Users/macwo/Desktop/hadoop-0/PELNY_TUTORIAL_I_KOMENDY.md)  
  *(Szczegółowa analiza kodu linijka po linijce, różnice Streaming vs Classic, komendy Beeline i Streaming, weryfikacja wyników)*
* 🌐 **Interaktywna Platforma Edukacyjna (w przeglądarce):** [interactive_tutorial.html](file:///c:/Users/macwo/Desktop/hadoop-0/interactive_tutorial.html)  
  *(Zawiera: porównanie Streaming vs Classic side-by-side, architekturę, wyniki 120/20, laboratorium błędów i konsolę komend z kopiowaniem)*
* 📖 **Poradnik recenzenta Markdown:** [RECENZJA_TUTORIAL.md](file:///c:/Users/macwo/Desktop/hadoop-0/RECENZJA_TUTORIAL.md)

---

## 2. Docker – Start i Zarządzanie

### Budowa i uruchomienie kontenera:
```bash
# 1. Zbuduj obraz (z pobranym Hadoop i Hive)
docker-compose build

# 2. Uruchom kontener w tle
docker-compose up -d

# 3. Sprawdź czy kontener działa
docker ps

# 4. Wejdź do powłoki bash wewnątrz kontenera
docker exec -it hadoop-hotels bash

# 5. Zatrzymanie kontenera
docker-compose down
```

---

## 2. HDFS – Wgrywanie i Sprawdzanie Danych

Wewnątrz kontenera (`docker exec -it hadoop-hotels bash`):

```bash
# Uruchomienie gotowego skryptu przygotowania HDFS:
bash /opt/scripts/setup_hdfs.sh

# Lub ręcznie krok po kroku:
hdfs dfs -mkdir -p /user/root/input/reservations
hdfs dfs -mkdir -p /user/root/input/hotels

# Wgranie plików CSV do HDFS:
hdfs dfs -put -f /opt/data/reservations.csv /user/root/input/reservations/
hdfs dfs -put -f /opt/data/hotels.csv /user/root/input/hotels/

# Podgląd wgranych danych:
hdfs dfs -ls -R /user/root/input/
hdfs dfs -cat /user/root/input/reservations/reservations.csv | head -5
```

---

## 3. MapReduce Streaming (Python + Combiner)

Kryteria: **M01-M12** (obsługa błędnych rekordów, pomijanie nagłówka, Combiner cząstkowy, parametry `input_dir1`, `output_dir3`).

```bash
# Uruchomienie przez skrypt (zalecane):
bash /opt/scripts/run_mr.sh /user/root/input/reservations /user/root/output/mr_results streaming

# Ręczne bezpośrednie polecenie Hadoop Streaming:
hdfs dfs -rm -r -f /user/root/output/mr_results 2>/dev/null || true

mapred streaming \
    -files /opt/mapreduce/mapper.py,/opt/mapreduce/combiner.py,/opt/mapreduce/reducer.py \
    -input /user/root/input/reservations \
    -output /user/root/output/mr_results \
    -mapper "python3 mapper.py" \
    -combiner "python3 combiner.py" \
    -reducer "python3 reducer.py"

# Podgląd wyniku pośredniego (3):
hdfs dfs -cat /user/root/output/mr_results/part-*
```

Format wyniku pośredniego (tab-separated):
```
hotel_id    year    reservation_count    avg_price    cancelled_count
HOTEL0001   2024    3                    580.17       1
HOTEL0005   2024    4                    299.06       1
```

---

## 4. MapReduce Classic (Java JAR)

Kompilacja i uruchomienie wariantu Java:

```bash
# 1. Kompilacja JAR (jeśli Maven jest zainstalowany w kontenerze)
cd /opt/mapreduce && mvn clean package -DskipTests

# 2. Uruchomienie skryptem:
bash /opt/scripts/run_mr.sh /user/root/input/reservations /user/root/output/mr_results classic

# Lub bezpośrednio przez hadoop jar:
hdfs dfs -rm -r -f /user/root/output/mr_results 2>/dev/null || true

hadoop jar /opt/mapreduce/target/hotel-reservations-1.0.jar \
    hotels.ReservationDriver \
    /user/root/input/reservations \
    /user/root/output/mr_results
```

---

## 5. Apache Hive (Beeline + SerDe JSON)

Kryteria: **H01-H15** (zewnętrzne tabele, parametry `input_dir3`, `input_dir4`, `output_dir6`, `beeline`, `RANK() OVER`, wyjście w `org.apache.hive.hcatalog.data.JsonSerDe`).

```bash
# Uruchomienie przez skrypt (zalecane):
bash /opt/scripts/run_hive.sh \
    /user/root/output/mr_results \
    /user/root/input/hotels \
    /user/root/output/hive_results

# Bezpośrednie uruchomienie przez Beeline:
hdfs dfs -rm -r -f /user/root/output/hive_results 2>/dev/null || true

beeline -u "jdbc:hive2://localhost:10000/default" -n root \
    --hivevar input_dir3="/user/root/output/mr_results" \
    --hivevar input_dir4="/user/root/input/hotels" \
    --hivevar output_dir6="/user/root/output/hive_results" \
    -f /opt/hive_scripts/hotel_ranking.hql

# Sprawdzenie wyniku końcowego (6) w HDFS:
hdfs dfs -cat /user/root/output/hive_results/*
```

Przykładowy rekord w formacie JSON (wygenerowany przez SerDe):
```json
{"hotel_name":"Riverside Retreat","total_reservations":4,"avg_price":299.06,"cancelled_ratio":0.25,"rooms":60,"rank":1}
```

---

## 6. Skrypt All-in-One

Jedno polecenie wykonujące cały przepływ end-to-end:

```bash
# Domyślnie Streaming:
bash /opt/scripts/run_all.sh streaming

# Lub wariant Classic:
bash /opt/scripts/run_all.sh classic
```

---

## 7. Apache Airflow (Opcjonalnie)

Plik DAG: `/opt/airflow/dags/hotel_reservations_dag.py`
Zawiera operatory:
- `clean_output_mr_dir`, `clean_output_dir`
- `pick_classic_or_streaming` (wybór ścieżki)
- `mapreduce_classic` / `hadoop_streaming`
- `hive` (wywołanie beeline)
- `get_output` (wypisanie JSON do logów)

```bash
# Test pojedynczego taska w Airflow CLI:
airflow tasks test hotel_reservations_pipeline hadoop_streaming 2024-01-01
airflow tasks test hotel_reservations_pipeline hive 2024-01-01
```

---

## 8. Szybki Test Lokalny (bez Hadoop)

Możesz przetestować logikę transformacji MapReduce na dowolnym systemie z Pythonem:

```bash
# Test potoku Mapper -> Combiner -> Reducer w PowerShell / Bash:
cat data/reservations.csv | python mapreduce/mapper.py | sort | python mapreduce/combiner.py | sort | python mapreduce/reducer.py
```
