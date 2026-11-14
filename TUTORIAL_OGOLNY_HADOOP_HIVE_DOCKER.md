# 📖 Podręcznik Architektury: Hadoop Streaming, Apache Hive i Ekosystem Docker

Kompleksowy tutorial wprowadzający w świat przetwarzania wielkich zbiorów danych (Big Data). Wyjaśnia od podstaw zasady działania **Hadoop Streaming**, **Apache Hive**, a także architekturę **usług działających w kontenerze Docker**.

---

## 📑 Spis Treści
1. [Wprowadzenie do Świata Big Data i Ekosystemu Hadoop](#1-wprowadzenie-do-świata-big-data-i-ekosystemu-hadoop)
2. [Hadoop Streaming – Jak Działa Przetwarzanie Rozproszone?](#2-hadoop-streaming--jak-działa-przetwarzanie-rozproszone)
   - [Model MapReduce (Krok po Kroku)](#a-model-mapreduce-krok-po-kroku)
   - [Architektura Pipes w Hadoop Streaming](#b-architektura-pipes-w-hadoop-streaming)
   - [Rola Mappera, Combinera i Reducera](#c-rola-mappera-combinera-i-reducera)
3. [Apache Hive – SQL na Klastrze Big Data](#3-apache-hive--sql-na-klastrze-big-data)
   - [Jak Hive tłumaczy SQL na zadania klastra?](#a-jak-hive-tłumaczy-sql-na-zadania-klastra)
   - [Architektura: HiveServer2, Beeline i Metastore](#b-architektura-hiveserver2-beeline-i-metastore)
   - [Zarządzane (Managed) vs Zewnętrzne (External) Tabele](#c-zarządzane-managed-vs-zewnętrzne-external-tabele)
   - [Magia mechanizmu SerDe (Serializer / Deserializer)](#d-magia-mechanizmu-serde-serializer--deserializer)
   - [Funkcje Okienkowe i Analityczne (np. RANK)](#e-funkcje-okienkowe-i-analityczne-np-rank)
4. [Architektura Usług w Kontenerze Docker](#4-architektura-usług-w-kontenerze-docker)
   - [Przegląd Uruchomionych Usług i Portów](#a-przegląd-uruchomionych-usług-i-portów)
   - [HDFS: NameNode vs DataNode](#b-hdfs-namenode-vs-datanode)
   - [YARN: ResourceManager vs NodeManager](#c-yarn-resourcemanager-vs-nodemanager)
   - [HiveServer2 i Hive Metastore](#d-hiveserver2-i-hive-metastore)
   - [Cykl Życia Zapytania w Kontenerze](#e-cykl-życia-zapytania-w-kontenerze)
5. [Podsumowanie Najważniejszych Zasad i Dobrych Praktyk](#5-podsumowanie-najważniejszych-zasad-i-dobrych-praktyk)

---

## 1. Wprowadzenie do Świata Big Data i Ekosystemu Hadoop

Tradycyjne bazy danych (np. PostgreSQL, MySQL) działają na pojedynczych maszynach (skalowanie pionowe – *scale-up*). Gdy danych jest zbyt dużo, maszyna przestaje mieścić je w pamięci RAM lub na dysku.

**Apache Hadoop** rozwiązuje ten problem poprzez **skalowanie poziome (*scale-out*)**:
* Zamiast jednego superkomputera łączy dziesiątki, setki lub tysiące zwykłych serwerów (*commodity hardware*).
* **Zasada Data Locality (Przesyłaj kod do danych, a nie dane do kodu):** Zamiast pobierać terabajty danych przez sieć do procesora, program (kod) wysyłany jest bezpośrednio na serwer, na którego dysku leżą odpowiednie bloki danych.

Ekosystem Hadoop opiera się na 3 filarach:
1. **HDFS (Hadoop Distributed File System):** Rozproszony system plików dzielący pliki na bloki (domyślnie 128 MB) i replikujący je na wielu węzłach.
2. **YARN (Yet Another Resource Negotiator):** System operacyjny klastra zarządzający pamięcią RAM i rdzeniami CPU.
3. **MapReduce:** Model programistyczny przetwarzania równoległego.

---

## 2. Hadoop Streaming – Jak Działa Przetwarzanie Rozproszone?

### A. Model MapReduce (Krok po Kroku)

Przetwarzanie MapReduce dzieli się na fazy:
```
[Plik Wejściowy w HDFS]
        ↓
    (Splits) ⟶ Dzielenie pliku na porcje dla równoległych workerów
        ↓
     [MAP]   ⟶ Filtrowanie, ekstrakcja pól, emisja par (Klucz, Wartość)
        ↓
   [COMBINE] ⟶ (Opcjonalnie) Lokalna wstępna agregacja na węźle mapującym
        ↓
   [SHUFFLE] ⟶ Sortowanie i przesyłanie przez sieć: wszystkie rekordy 
               o tym samym kluczu trafiają do tego samego Reducera!
        ↓
    [REDUCE] ⟶ Ostateczna agregacja (suma, średnia, liczność) per klucz
        ↓
[Plik Wynikowy w HDFS (part-xxxxx)]
```

---

### B. Architektura Pipes w Hadoop Streaming

Klasyczny Hadoop wymaga pisania kodu w Javie. **Hadoop Streaming** to moduł narzędziowy, który pozwala pisać Mappery i Reducery w **dowolnym języku programowania** (Python, Bash, Ruby, Perl, Node.js, C++).

#### Jak to działa pod maską?
1. Hadoop uruchamia Twój skrypt jako **zewnętrzny proces systemowy** (np. `python3 mapper.py`).
2. Hadoop podaje dane wejściowe linijka po linijce na standardowe wejście procesu (**`sys.stdin`**).
3. Twój skrypt wykonuje logikę i wypisuje wyniki na standardowe wyjście (**`sys.stdout`** / `print()`).
4. **Format komunikacji:** Klucz i wartość muszą być oddzielone znakiem **tabulacji** (`\t`), a rekordy znakiem nowej linii (`\n`):
   ```
   KLUCZ\tWARTOŚĆ\n
   ```

```
[Hadoop TaskTracker / NodeManager]
       │
       │ (wiersz CSV przez STDIN)
       ▼
 ┌───────────────┐
 │ python3       │  Twój kod: for line in sys.stdin:
 │ mapper.py     │  Twój kod: print(f"{klucz}\t{wartosc}")
 └───────────────┘
       │
       │ (klucz \t wartosc przez STDOUT)
       ▼
[Hadoop zbiera dane, sortuje i przekazuje do Shuffle/Reducer]
```

---

### C. Rola Mappera, Combinera i Reducera

#### 1. Mapper (`mapper.py`)
* **Zadanie:** Przeczytać surowy rekord, zweryfikować poprawność, pominąć nagłówek i wyemitować parę: **Klucz grupowania** oraz **Wartości składowe**.
* **Złota zasada:** Mapper przetwarza dane **strumieniowo** (nigdy nie zapisuje całego pliku do pamięci RAM!).

#### 2. Combiner (`combiner.py`) – Mini-Reducer w węźle Mapera
* **Zadanie:** Zredukować ilość danych przesyłanych przez sieć w fazie Shuffle. Jeśli na jednym węźle powstało 10 000 wpisów dla `HOTEL0001`, Combiner łączy je lokalnie w 1 wpis.
* **Kluczowa zasada:** Combiner **nie może zmieniać formatu danych** (musi emitować dokładnie taki sam format jak Mapper) i **nie może wyliczać średniej**, ponieważ średnia cząstkowa zniekształca wynik końcowy!

#### 3. Reducer (`reducer.py`)
* **Zadanie:** Otrzymuje dane **posortowane według klucza**. Sumuje wszystkie wartości dla bieżącego klucza, a w momencie pojawienia się nowego klucza wylicza miary ostateczne (np. średnią $\frac{\text{suma}}{\text{liczba}}$) i emituje wynik do HDFS.

---

## 3. Apache Hive – SQL na Klastrze Big Data

Pisanie zadań MapReduce w czystym Pythonie lub Javie dla złożonych zapytań analitycznych bywa czasochłonne. **Apache Hive** powstał w firmie Facebook, aby umożliwić analitykom odpytywanie gigantycznych zbiorów w HDFS za pomocą dobrze znanego języka **SQL (HiveQL / HQL)**.

---

### A. Jak Hive tłumaczy SQL na zadania klastra?

Gdy wykonujesz zapytanie SQL w Hive:
```sql
SELECT hotel_id, count(*) FROM mr_results GROUP BY hotel_id;
```
1. **Parser & Compiler:** Hive sprawdza składnię SQL i pobiera definicje kolumn z katalogu Metastore.
2. **Optimizer:** Tworzy zoptymalizowany plan wykonania (Directed Acyclic Graph – DAG).
3. **Execution Engine:** Tłumaczy zapytanie SQL na **jedno lub więcej fizycznych zadań MapReduce** (lub Apache Tez), które uruchamiają się na klastrze YARN!

---

### B. Architektura: HiveServer2, Beeline i Metastore

```
[Klient: Beeline CLI / DBeaver]
            │ (Połączenie JDBC / Thrift na porcie 10000)
            ▼
     [HiveServer2]
      ├── 1. Pyta o strukturę tabel ⟶ [Hive Metastore (Baza Derby/MySQL)]
      └── 2. Kompiluje do MapReduce ⟶ [Klaster Hadoop YARN / HDFS]
```

* **HiveServer2 (HS2):** Serwer wielowątkowy, który przyjmuje zapytania SQL od wielu użytkowników jednocześnie przez protokół Thrift/JDBC.
* **Beeline:** Nowoczesny, bezpieczny klient wiersza poleceń łączący się z HiveServer2.
* **Hive Metastore:** Baza relacyjna (w naszym kontenerze Apache Derby), która przechowuje **metadane**: jakie istnieją tabele, jakie mają kolumny, jakie typy danych oraz **gdzie fizycznie leżą ich pliki w HDFS**.

---

### C. Zarządzane (Managed) vs Zewnętrzne (External) Tabele

W Hive istnieją dwa typy tabel:

| Cecha | Tabela Zarządzana (`MANAGED TABLE`) | Tabela Zewnętrzna (`EXTERNAL TABLE`) |
| :--- | :--- | :--- |
| **Definicja** | `CREATE TABLE nazwa (...)` | `CREATE EXTERNAL TABLE nazwa (...) LOCATION '...'` |
| **Własność danych** | Hive jest wyłącznym właścicielem danych w `/user/hive/warehouse/`. | Dane należą do zewnętrznego procesu (np. MapReduce). |
| **Instrukcja `DROP TABLE`** | **Usuwa metadane ORAZ kasuje fizyczne pliki z HDFS!** | **Usuwa TYLKO wpis w katalogu tabel! Pliki w HDFS pozostają nienaruszone.** |
| **Zastosowanie** | Tabele tymczasowe. | **Złoty standard Big Data:** Bezpieczny dostęp do danych bez ryzyka ich przypadkowego skasowania. |

> ⚠️ **Kryterium oceny H02:** W projekcie bezwzględnie używamy `EXTERNAL TABLE ... LOCATION`, aby wskazać pliki tam, gdzie leżą, bez ich kopiowania!

---

### D. Magia mechanizmu SerDe (Serializer / Deserializer)

Hive nie narzuca własnego formatu binarnego na pliki. Zamiast tego używa interfejsu **SerDe**:
* **Deserializer:** Odpowiada za odczyt surowego bajtu z pliku i zmapowanie go na wiersz z kolumnami SQL.
* **Serializer:** Odpowiada za wzięcie wiersza SQL i zapisanie go w żądanym formacie pliku.

Przykłady SerDe:
1. **Delimited (CSV / TSV):** Dzieli pola po przecinku lub tabulatorze.
2. **`JsonSerDe` (`org.apache.hive.hcatalog.data.JsonSerDe`):**
   * Przy zapisie (`INSERT INTO tabela`) zamienia rekord SQL w czysty, poprawny obiekt JSON:
     ```json
     {"hotel_name":"Wiza Kutch and Treutel Hotel","total_reservations":100,"avg_price":966.91,"cancelled_ratio":0.33,"rooms":88,"rank":1}
     ```
   * *Spełnia kryterium oceny H15 bez uciekania się do sztucznego sklejania napisów funkcją CONCAT!*

---

### E. Funkcje Okienkowe i Analityczne (np. RANK)

W przeciwieństwie do zwykłego `GROUP BY`, które zwija wiele wierszy w jeden, **funkcje okienkowe (`OVER (...)`)** wykonują obliczenia analityczne, zachowując indywidualne wiersze.

#### Jak działa `RANK() OVER (ORDER BY obłożenie DESC)`?
* Oblicza iloraz $\frac{\text{rezerwacje}}{\text{pokoje}}$ dla każdego hotelu.
* Sortuje hotele malejąco.
* Jeśli dwa hotele mają identyczny wynik (remis), **dostają tę samą pozycję w rankingu**, a następna pozycja zostaje pominięta:
  * Hotel A: 1.1364 $\rightarrow$ **Pozycja 1**
  * Hotel B: 1.1158 $\rightarrow$ **Pozycja 2**
  * Hotel C: 1.0000 $\rightarrow$ **Pozycja 3 (remis)**
  * Hotel D: 1.0000 $\rightarrow$ **Pozycja 3 (remis)**
  * Hotel E: 0.9900 $\rightarrow$ **Pozycja 5** *(pozycja 4 została pominięta!)*

---

## 4. Architektura Usług w Kontenerze Docker

W projekcie stworzyliśmy pełnowartościowy, jednouzłowy klaster Hadoop + Hive wewnątrz kontenera Docker (`hadoop-hotels`).

### A. Przegląd Uruchomionych Usług i Portów

```
┌────────────────────────────────────────────────────────────────────────┐
│                        KONTENER: hadoop-hotels                         │
│                                                                        │
│   ┌──────────────────┐    ┌──────────────────┐    ┌────────────────┐   │
│   │  HDFS NameNode   │    │  HDFS DataNode   │    │ YARN Res.Mgr   │   │
│   │  (Port 9870)     │    │  (Port 9864)     │    │ (Port 8088)    │   │
│   └──────────────────┘    └──────────────────┘    └────────────────┘   │
│                                                                        │
│   ┌──────────────────┐    ┌──────────────────┐    ┌────────────────┐   │
│   │  YARN NodeMgr    │    │   HiveServer2    │    │ Hive Metastore │   │
│   │  (Kontenery MR)  │    │ (Port 10000/10002│    │ (Baza Derby)   │   │
│   └──────────────────┘    └──────────────────┘    └────────────────┘   │
└────────────────────────────────────────────────────────────────────────┘
```

| Usługa / Proces | Port zewnętrzny | Do czego służy? | Jak sprawdzić w przeglądarce? |
| :--- | :---: | :--- | :--- |
| **HDFS NameNode** | **9870** | Główny zarządca plików HDFS. Wie, gdzie leżą bloki danych. | [http://localhost:9870](http://localhost:9870) |
| **YARN ResourceManager** | **8088** | Zarządca zasobów klastra. Pokazuje status uruchomionych zadań MapReduce. | [http://localhost:8088](http://localhost:8088) |
| **HDFS DataNode** | **9864** | Węzeł przechowujący fizyczne bloki danych. | [http://localhost:9864](http://localhost:9864) |
| **HiveServer2 Web UI** | **10002** | Panel podglądu aktywnych sesji i zapytań SQL w Hive. | [http://localhost:10002](http://localhost:10002) |
| **HiveServer2 JDBC** | **10000** | Port komunikacyjny dla Beeline, DBeaver, aplikacji Java/Python. | `jdbc:hive2://localhost:10000/default` |

---

### B. HDFS: NameNode vs DataNode

* **NameNode (Mózg systemu plików):**
  * Nie przechowuje zawartości plików!
  * Trzyma w pamięci RAM całe drzewo katalogów, uprawnienia oraz mapowanie: *„plik `/user/root/input/reservations/reservations00.csv` składa się z bloku `blk_1001`, który leży na DataNode 1”*.
* **DataNode (Magazyn danych):**
  * Fizycznie zapisuje bloki na lokalnym dysku serwera.
  * Regularnie wysyła meldunki (*Heartbeats* i *Block Reports*) do NameNode'a, potwierdzając swoją gotowość.

---

### C. YARN: ResourceManager vs NodeManager

YARN to system operacyjny klastra:
* **ResourceManager (Dyspozytor):** Decyduje, które zadanie otrzymuje ile pamięci RAM i rdzeni procesora.
* **NodeManager (Kierownik na maszynie):** Uruchamia fizyczne kontenery zadań (np. proces Pythona dla Mappera lub Javy dla Reducera) i monitoruje zużycie zasobów.

---

### D. HiveServer2 i Hive Metastore

* **Hive Metastore:** Relacyjna baza danych (w kontenerze Apache Derby). Zapisuje definicję tabeli `hotel_ranking` (kolumny, typy `STRING`, `BIGINT`, `DOUBLE`, klasę SerDe).
* **HiveServer2:** Działa jako usługa w tle. Przyjmuje zapytania z klienta Beeline, odpytuje Metastore o strukturę tabeli, generuje plan zapytania i przekazuje go do wykonania przez YARN.

---

### E. Cykl Życia Zapytania w Kontenerze (End-to-End)

Gdy uruchamiasz skrypt `run_all.sh`:
1. **`setup_hdfs.sh`:** Klient HDFS wysyła pliki CSV do NameNode'a, który przydziela bloki i zapisuje je w DataNode.
2. **`run_mr.sh`:**
   * Skrypt wysyła polecenie `mapred streaming` do YARN ResourceManagera.
   * ResourceManager przydziela kontenery na NodeManagerze.
   * NodeManager uruchamia procesy `python3 mapper.py`, czyta bloki z DataNode, wykonuje lokalny `combiner.py`.
   * Faza Shuffle przesyła posegregowane dane do `reducer.py`.
   * Wynik 120 wierszy zostaje zapisany w katalogu `/user/root/output/mr_results`.
3. **`run_hive.sh`:**
   * Klient Beeline łączy się przez port 10000 do HiveServer2.
   * HiveServer2 czyta metadane z Metastore, tworzy tabele zewnętrzne nad `/user/root/output/mr_results` oraz `/user/root/input/hotels`.
   * HiveServer2 wykonuje `JOIN`, filtruje rok 2024, wylicza `RANK()`.
   * Mechanizm `JsonSerDe` zapisuje wynikowe 20 obiektów JSON do `/user/root/output/hive_results`.

---

## 5. Podsumowanie Najważniejszych Zasad i Dobrych Praktyk

1. **Strumieniowość to podstawa:** W Hadoop nigdy nie wczytuj całego zbioru do pamięci RAM (`sys.stdin.readlines()` lub `records.append()` to błąd).
2. **Combiner musi być łączny i przemienny:** Nie wyliczaj średniej w Combinerze ani Mapperze – emituj sumę kwot i liczność!
3. **Tabele zewnętrzne chronią dane:** W Hive zawsze stosuj `CREATE EXTERNAL TABLE ... LOCATION` zamiast niszczącego dane `LOAD DATA`.
4. **Czystość strumienia stdout:** W skryptach Hadoop Streaming na standardowe wyjście (`print()`) mogą trafiać **tylko i wyłącznie dane wynikowe**. Wszelkie logi i komunikaty diagnostyczne kieruj do `sys.stderr`.
5. **JSON przez SerDe:** Zgodnie z najlepszymi praktykami inżynierii danych, format JSON generujemy za pomocą oficjalnej biblioteki serializacji (`JsonSerDe`), a nie ręcznego klejenia ciągów znaków.
