# Fundament Sparka

Projekt edukacyjny do nauki Apache Spark w języku Scala. Zawiera przykłady i ćwiczenia z podstawowych operacji na RDD, DataFrame, ładowania danych, łączeń, operacji na JSON oraz UDF (User Defined Functions).

## Spis treści

- [Wymagania](#wymagania)
- [Struktura projektu](#struktura-projektu)
- [Uruchomienie](#uruchomienie)
- [Testy](#testy)
- [Zależności](#zależności)
- [Dane](#dane)
- [Licencja](#licencja)

## Wymagania

- **Scala** 2.13.16  
- **sbt** (Scala Build Tool)  
- **Apache Spark** 3.5.0 (dołączony jako zależność)

## Struktura projektu

```
fundament-sparka/
├── src/main/scala/
│   ├── App.scala              # Punkt wejścia – uruchamianie wybranych modułów
│   ├── module1/               # Wprowadzenie – DataFrame, podstawowe dane
│   │   ├── People.scala
│   │   └── Exercise.scala
│   ├── module2/               # Operacje na RDD, ładowanie danych
│   │   ├── BasicOperation.scala
│   │   ├── LoadData.scala
│   │   └── Exercise.scala
│   ├── module3/               # Unie, joiny, operacje na JSON
│   │   ├── Unions.scala
│   │   ├── Joining.scala
│   │   ├── OtherOperations.scala
│   │   ├── Json.scala
│   │   ├── People.scala
│   │   └── Exercise.scala
│   └── module4/               # UDF (User Defined Functions)
│       ├── UdfsOperations.scala
│       ├── Exercise.scala
│       └── udfs/
│           ├── InitialsUDF.scala
│           ├── InterestCapitalizationUDF.scala
│           ├── InterestCapitalizationWithUDF.scala
│           └── CountWords.scala
├── data/                      # Zbiory danych (CSV m.in. Netflix, pizza, covid)
└── build.sbt
```

## Uruchomienie

1. **Sklonuj repozytorium** (jeśli jeszcze tego nie zrobiłeś):
   ```bash
   git clone <url-repozytorium>
   cd fundament-sparka
   ```

2. **Uruchom aplikację** (domyślnie wykonywane jest ćwiczenie z modułu 4):
   ```bash
   sbt run
   ```

3. **Uruchamianie konkretnych modułów**  
   W pliku `src/main/scala/App.scala` odkomentuj wybrane wywołania, np.:
   - `People.run(spark)` – moduł 1  
   - `BasicOperation.example1(spark)` – moduł 2  
   - `Joining.example1(spark)` – moduł 3  
   - `udfsOperations.initials(spark)` – moduł 4  

   Następnie uruchom ponownie: `sbt run`.

## Testy

```bash
sbt test
```

Projekt używa **MUnit** do testów jednostkowych.

## Zależności

- `org.apache.spark:spark-core:3.5.0`  
- `org.apache.spark:spark-sql:3.5.0`  
- `org.scalameta:munit:0.7.29` (testy)

## Dane

W katalogu `data/` znajdują się pliki CSV wykorzystywane w przykładach, m.in.:

- `netflix_titles.csv` – tytuły Netflix (używane w modułach 3 i 4)  
- `pizza_data.csv`, `pizza_data_half.csv`, `pizza_data_half2.csv`  
- `financial.csv`, `money_saving.csv`  
- `covid19_tweets.csv`, `GRAMMYs_tweets.csv`  

Upewnij się, że uruchamiasz aplikację z katalogu głównego projektu, aby ścieżki do plików w `data/` były poprawne.

## Licencja

Projekt edukacyjny – szczegóły licencji w repozytorium.
