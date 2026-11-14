# 🎓 Poradnik: Jak Przeprowadzić Profesjonalną Recenzję Projektu Hadoop Streaming & Hive

Niniejszy przewodnik powstał na bazie oficjalnych kryteriów oceny projektów Big Data (35 kryteriów binarnych, warunki konieczne, punkty za wydajność i poprawność). 

---

## 📑 Spis Treści
1. [Złota Zasada Oceny – 3 Warunki Konieczne](#1-złota-zasada-oceny--3-warunki-konieczne)
2. [Część I: Recenzja Kodu MapReduce (Hadoop Streaming)](#2-część-i-recenzja-kodu-mapreduce-hadoop-streaming)
3. [Część II: Recenzja Skryptu Apache Hive](#3-część-ii-recenzja-skryptu-apache-hive)
4. [Część III: Recenzja Apache Airflow](#4-część-iii-recenzja-apache-airflow)
5. [Niezbędnik Komend – Jak Szybko Weryfikować Projekty](#5-niezbędnik-komend--jak-szybko-weryfikować-projekty)
6. [Checklista Recenzenta (Szybka Ściąga)](#6-checklista-recenzenta-szybka-ściąga)

---

## 1. Złota Zasada Oceny – 3 Warunki Konieczne

Zanim zaczniesz liczyć punkty szczegółowe, sprawdź **warunki konieczne**. Niespełnienie któregokolwiek z nich oznacza **0 punktów** za dany moduł:

| Moduł | Warunek Konieczny | Na co zwrócić uwagę? |
| :--- | :--- | :--- |
| **MapReduce** | Zgodność wariantu i realna implementacja | Czy zadeklarowano `Streaming` i są pliki `mapper.py`, `reducer.py` (lub `Classic` z JAR-em)? Czy kod rozwiązuje zadany zestaw, a nie jest przypadkowym szablonem WordCount? |
| **Hive** | Zgodność zestawu i realna implementacja | Czy plik `.hql` dotyczy właściwego problemu, łączy właściwe tabele i nie ma fundamentalnych błędów uniemożliwiających uruchomienie? |
| **Airflow** | Realna implementacja DAG-a | Czy DAG został dostosowany do orkiestracji tego projektu (a nie pusty plik szablonu)? |

---

## 2. Część I: Recenzja Kodu MapReduce (Hadoop Streaming)

### A. Mapper (`mapper.py`)
* **Kryterium M01 (Parsowanie & Nagłówek):**
  * ❌ **Częsty błąd:** Brak pominięcia wiersza nagłówkowego pliku CSV. Wtedy tekst `price` trafia do `float()` i mapper wywala błąd lub traktuje go jako dane.
  * ❌ **Częsty błąd:** Sztywne dzielenie po przecinku rekordu, w którym pole tekstowe zawiera przecinek (warto sprawdzić moduł `csv` lub ostrożne dzielenie).
  * ✅ **Poprawne podejście:**
    ```python
    if line.startswith("reservation_id"):
        continue  # pominięcie nagłówka
    ```
* **Kryterium M02 (Klucz pośredni):**
  * Klucz musi zawierać wszystkie pola grupowania (np. `hotel_id` ORAZ `year`).
  * Klucz i wartość muszą być rozdzielone **tabulatorem** (`\t`).
* **Kryterium M03 (Wartości do agregacji wielopoziomowej):**
  * ❌ **Kardynalny błąd:** Wyliczanie średniej w Mapperze! Średnia nie jest funkcją dystrybutywną: $\text{avg}(a, b) \neq \text{avg}(\text{avg}(a), \text{avg}(b))$.
  * ✅ **Poprawne podejście:** Mapper emituje **składowe**: sumę (lub pojedynczą cenę) oraz liczność (1), aby Combiner mógł je prawidłowo scalić.
* **Kryterium M05 (Wydajność & Przetwarzanie Strumieniowe):**
  * ❌ **Kardynalny błąd wydajnościowy:** Wczytywanie rekordów do listy lub słownika przed emisją:
    ```python
    # ŹLE! (łamanie przetwarzania strumieniowego)
    records = []
    for line in sys.stdin:
        records.append(line)
    ```
  * ✅ **Poprawne podejście:** Przetwarzanie linijka po linijce, `try...except` na konwersjach typów (`ValueError`, `IndexError`) i natychmiastowy `print()`.

---

### B. Combiner (`combiner.py`)
* **Kryterium M06 & M07:**
  * Combiner jest uruchamiany między Mapperem a Reducerem w węźle mapującym.
  * ❌ **Kardynalny błąd:** Zmiana formatu wyjściowego. **Format wyjściowy Combinera MUSI być identyczny z formatem wyjściowym Mappera**, ponieważ Combiner może zostać wywołany 0, 1 lub wielokrotnie!
  * Combiner musi przetwarzać dane strumieniowo (dane na wejściu są już posortowane wg klucza w ramach map taska).

---

### C. Reducer (`reducer.py`)
* **Kryterium M08 & M09 (Obliczenia):**
  * Reducer sumuje wszystkie składowe dla danego klucza, a dopiero **na koniec grupy** oblicza średnią:
    $$\text{avg\_price} = \frac{\text{sum\_price}}{\text{count}}$$
* **Kryterium M10 (Czystość strumienia):**
  * ❌ **Częsty błąd:** Pozostawienie komunikatów debugujących: `print("Przetwarzam klucz:", key)`. Każdy `print()` w stdout staje się rekordem w HDFS i zepsuje ładowanie do Hive! Wszelkie logi diagnostyczne muszą iść na `sys.stderr.write(...)`.
  * Reducer musi emitować dokładnie te kolumny, których wymaga specyfikacja, bez nagłówków.

---

### D. Skrypt Uruchamiający (`run_mr.sh`)
* **Kryterium M11:** Przyjmuje parametry ścieżek `input_dir1` i `output_dir3`, a nie ma ich wpisanych na stałe.
* **Kryterium M12:** Przed startem usuwa katalog wyjściowy w HDFS:
  ```bash
  hdfs dfs -rm -r -f "$output_dir3" 2>/dev/null || true
  ```
  Dzięki temu skrypt można uruchamiać wielokrotnie bez błędu `Output directory already exists`.

---

## 3. Część II: Recenzja Skryptu Apache Hive

### A. Definicja Struktur (H01 - H04)
* **Kryterium H02 (Brak LOAD DATA):**
  * ❌ **Częsty błąd:** Użycie `LOAD DATA INPATH ...`. Komenda ta fizycznie przenosi pliki z HDFS i niszczy katalog źródłowy!
  * ✅ **Poprawne podejście:**
    ```sql
    CREATE EXTERNAL TABLE ... STORED AS TEXTFILE LOCATION '${hivevar:input_dir3}';
    ```
* **Kryterium H03 (Powtarzalność):**
  * Każde `CREATE TABLE` / `CREATE VIEW` musi być poprzedzone przez `DROP TABLE IF EXISTS`.
* **Kryterium H04 (Trwałość tabel źródłowych):**
  * Na końcu skryptu **NIE WOLNO** usuwać tabel źródłowych (`DROP TABLE mr_results`). Mają pozostać dostępne do debugowania i weryfikacji.

---

### B. Wydajność Przetwarzania (H05)
* ❌ **Kardynalny błąd:** Tworzenie serii tabel pośrednich:
  ```sql
  -- ŹLE! Zbędne tabele pośrednie
  CREATE TABLE krok1 AS SELECT ... FROM zrodlo;
  CREATE TABLE krok2 AS SELECT ... FROM krok1;
  CREATE TABLE wynik AS SELECT ... FROM krok2;
  ```
* ✅ **Poprawne podejście:** Jedno złożone zapytanie (np. z `JOIN`, podzapytaniem lub `WITH ... AS CTE`) wykonane w jednym przebiegu.

---

### C. Poprawność Logiki (H06 - H10)
* **Kryterium H06 (Złączenie):** `JOIN` wykonany na właściwym kluczu (`hotel_id`).
* **Kryterium H07 (Filtrowanie i Agregacje):** Warunek `WHERE mr.year = '2024'` oraz wyliczenie `cancelled_ratio = cancelled_count / total_reservations` (musi mieścić się w $[0.0, 1.0]$).
* **Kryterium H09 (Funkcja analityczna RANK):**
  * Wymagane jest użycie `RANK() OVER (ORDER BY (rezerwacje / pokoje) DESC)`.
  * Zgodnie z zasadą `RANK()`: remisy otrzymują tę samą pozycję, a kolejna jest pomijana (np. `1, 2, 2, 4`). Użycie `ROW_NUMBER()` przy remisach byłoby błędem!

---

### D. Format Końcowy JSON SerDe (H14 - H15)
* **Kryterium H14:** Skrypt uruchamiany przez `beeline`, a nie przestarzałe polecenie `hive`.
* **Kryterium H15 (SerDe JSON):**
  * ❌ **Kardynalny błąd:** Sklejanie tekstu za pomocą `CONCAT('{"hotel_name":"', name, ...)`!
  * ✅ **Poprawne podejście:** Tabela wynikowa zdefiniowana z oficjalnym SerDe:
    ```sql
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    ```

---

## 4. Część III: Recenzja Apache Airflow

* **W01:** Domyślna wartość parametru `classic_or_streaming` odpowiada faktycznie zaimplementowanemu wariantowi.
* **W02 & W03:** BashOperatory przekazują parametry przez Jinja: `{{ params.input_dir }}`, `{{ params.output_mr_dir }}`, `{{ params.output_dir }}` zamiast sztywnych ścieżek.
* **W04:** Zachowana nienaruszona struktura DAG-a (operator wyboru gałęzi `pick_classic_or_streaming` oraz zadania czyszczące).
* **W05:** Brak zbędnych zadań (np. pobieranie plików, curl, unzip wewnątrz DAG-a – dane z założenia są już w HDFS).
* **W08:** Zadanie `get_output` wyświetla w logu Airflow czytelne rekordy JSON.

---

## 5. Niezbędnik Komend – Jak Szybko Weryfikować Projekty

Oto zestaw poleceń, które każdy recenzent powinien mieć pod ręką:

### 1. Test lokalny potoku strumieniowego (w 3 sekundy, bez klastra):
```bash
# Weryfikuje: parsing, nagłówek, format pośredni, brak crashy na błędnych danych
cat data/datasource1/*.csv | python3 mapreduce/mapper.py | sort | python3 mapreduce/combiner.py | sort | python3 mapreduce/reducer.py | head -20
```

### 2. Sprawdzenie liczby rekordów MapReduce:
```bash
# Dla Zestawu 0 powinno być dokładnie 120 wierszy:
cat data/datasource1/*.csv | python3 mapreduce/mapper.py | sort | python3 mapreduce/reducer.py | wc -l
```

### 3. Weryfikacja wyniku w HDFS:
```bash
# Podgląd pierwszych rekordów z HDFS:
hdfs dfs -cat /user/root/output/mr_results/part-* | head -10

# Sprawdzenie czy wynik Hive to poprawny JSON (walidacja przez jq):
hdfs dfs -cat /user/root/output/hive_results/* | jq . | head -25

# Zliczenie wierszy w wyniku Hive (powinno być 20 hoteli):
hdfs dfs -cat /user/root/output/hive_results/* | wc -l
```

### 4. Uruchomienie skryptu Hive przez Beeline:
```bash
beeline -u "jdbc:hive2://localhost:10000/default" -n root \
  --hivevar input_dir3="/user/root/output/mr_results" \
  --hivevar input_dir4="/user/root/input/hotels" \
  --hivevar output_dir6="/user/root/output/hive_results" \
  -f hive/hotel_ranking.hql
```

---

## 6. Checklista Recenzenta (Szybka Ściąga)

Podczas oceny sprawdzaj po kolei:
- [ ] Czy kod odpala się od razu, czy wymaga ręcznych poprawek ścieżek?
- [ ] Czy w mapperze/reducerze są kolekcje gromadzące wszystkie wiersze (`list.append`, `dict`)? *(Jeśli tak $\rightarrow$ błąd strumieniowości)*
- [ ] Czy Combiner zachowuje dokładnie ten sam format wyjścia co Mapper?
- [ ] Czy `run_mr.sh` usuwa katalog wyjściowy przed startem zadania?
- [ ] Czy tabele Hive są tworzone jako `EXTERNAL TABLE ... LOCATION`? *(Czy brak `LOAD DATA`?)*
- [ ] Czy przed każdym `CREATE TABLE` jest `DROP TABLE IF EXISTS`?
- [ ] Czy tabele źródłowe pozostają w Hive po zakończeniu skryptu?
- [ ] Czy wynik Hive jest generowany przez `JsonSerDe`?
- [ ] Czy `cancelled_ratio` mieści się w przedziale $[0.0, 1.0]$?
- [ ] Czy remisy w rankingu mają ten sam numer i funkcja to `RANK()`, a nie `ROW_NUMBER()`?
