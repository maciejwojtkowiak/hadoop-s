# hadoop-s: Hotel Reservations (Zestaw 0)

Kompletne rozwiązanie zadania **Hadoop MapReduce** (Streaming & Classic) + **Apache Hive** + **Apache Airflow** w środowisku Docker.

## 📋 Spis Treści
* [Architektura Rozwiązania](#-architektura-rozwiązania)
* [Struktura Projektu](#-struktura-projektu)
* [Uruchomienie w Dockerze](#-uruchomienie-w-dockerze)
* [Wyniki Przetwarzania](#-wyniki-przetwarzania)
* [Dokumentacja i Samouczki](#-dokumentacja-i-samouczki)

---

## 🏗 Architektura Rozwiązania

1. **MapReduce (3):**
   * Agreguje rezerwacje hotelowe (`datasource1`: 10 000 wierszy) według `(hotel_id, year)`.
   * Oblicza: `reservation_count`, `avg_price` oraz `cancelled_count`.
   * Zwraca dokładnie **120 rekordów**.
2. **Apache Hive (6):**
   * Łączy wyniki MapReduce ze słownikiem hoteli (`datasource4`: 20 hoteli).
   * Ogranicza do rezerwacji z roku 2024.
   * Oblicza współczynnik obłożenia $\frac{\text{rezerwacje 2024}}{\text{liczba pokoi}}$ oraz wyznacza pozycję za pomocą `RANK() OVER`.
   * Zapisuje wynik do HDFS w formacie **JSON SerDe** (`org.apache.hive.hcatalog.data.JsonSerDe`).

---

## 📂 Struktura Projektu

```
hadoop-s/
├── data/
│   ├── datasource1/          # 10 plików CSV (reservations00..09.csv)
│   └── datasource4/          # Słownik hoteli (hotels.csv)
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

## 📚 Dokumentacja i Samouczki

* [TUTORIAL_OGOLNY_HADOOP_HIVE_DOCKER.md](TUTORIAL_OGOLNY_HADOOP_HIVE_DOCKER.md) – **Kompleksowy podręcznik architektoniczny:** Jak działa Hadoop Streaming, model MapReduce, Apache Hive, mechanizm SerDe oraz usługi kontenera Docker (HDFS NameNode/DataNode, YARN, HiveServer2, Metastore)
* [PELNY_TUTORIAL_I_KOMENDY.md](PELNY_TUTORIAL_I_KOMENDY.md) – Analiza kodu linijka po linijce, różnice Streaming vs Classic, komendy Beeline & Streaming
* [RECENZJA_TUTORIAL.md](RECENZJA_TUTORIAL.md) – Jak oceniać projekty wg oficjalnych 35 kryteriów i unikać pułapek
* [SCIAGA_KOMEND.md](SCIAGA_KOMEND.md) – Gotowe komendy do HDFS, MapReduce, Hive i Dockera
* [interactive_tutorial.html](interactive_tutorial.html) – Interaktywna aplikacja webowa
