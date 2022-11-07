Zbiór danych, którym się zajmowałam to tb.csv (numer 2). 

Plik tb.csv znajduje się w folderze o nazwie ../original_data/. 
Dane zawarte w pliku obejmują informacje od 1996 do 2008 roku i dotyczą przypadków gruźlicy w różnych grupach pacjentów, w zależności od wieku, państwa, rodzaju choroby.
Zadaniem było "oczyszczenie danych" i uporządkowanie ich zgodnie z zasadami "tidy data" i protokołem TIER.

Ponieważ tabela koduje jednocześnie kilka informacji w kolumnach to w pierwszej kolejności zajęłam się zamianą kolumny na wiersze wykorzystując funkcję melt, oraz pozbyciem się pustych miejsc. Nowa kolumna zawierająca zakodowane wartości nazwana została key.
Następnie podzieliłam ją na 3 kolejne każda przyporządkowana do innej informacji - new, type, oraz sex_and_age. Ostatnia zawiera dane o płci pacjenta oraz przedziale wiekowym w jakim się znajduje.
Ponownie rozdzieliłam ją otrzymując dwie osobne - płeć i wiek, gdzie w przypadku tego drugiego wykorzystane zostało mapowanie do określonych przedziałów aby nadać czytelności.
Następnie nowo otrzymane kolumny zostały dodane do tabeli, niepotrzebne usunięte, a wartości posortowane w kolejności: państwo, rok, płeć i wiek.

Ostateczna tabela zawiera następujące kolumny:
1) Pierwsza kolumna - skrót nazwy państwa - "country"
2) Druga kolumna - rok - "year"
3) Trzecia kolumna - typ choroby - "type"
4) Czwarta kolumna - płeć - "sex"
5) Piąta kolumna - zakres wieku - "age"
5) Szósta kolumna - ilość przypadków - "cases"

Na końcu mojej pracy zapisałam przefiltrowany DataFrame do pliku o nazwie tb-tidy.csv.