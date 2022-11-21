Analizowanym przeze mnie zbiorem danym jest 2_LUBELKIE.csv.

1. Dane w pierwotnej postaci znajdują się w folderze "orginal_data" o nazwie 2_LUBELSKIE.csv.

2. Wczytanie i czyszczenie danych odbywa się w pliku data_prcessing.ipynb z wykorzystaniem biblioteki pandas.

3. Po przeanalizowaniu zbioru zauważone zostały braki dotyczące kolumn "Wiek kupującego" oraz "Płeć kupującego". W drugiej z nich występowały w 2 wierszach, w pierwszej aż w 50, zatem usunięcie ich spowodowałoby utratę wielu innych potrzebnych informacji na temat innych kategorii. Puste komórki zostały zastąpione wartością np.NaN 

4. Oczyszczony plik gotowy do analizy został zapisany w folderze "analysis_data" pod nazwą analysis_data.csv.

5. W następnym kroku dla każdej kategorii wykonano stosowne wykresy - histogram lub wykres słupkowy, które dodatkowo zapisałam w formie pliku png w folderze diagrams znajdującym się w folderze documents.