# Parallel-Inverted-Index-using-Map-Reduce

Tema 1 - Calculul paralel al unui index inversat
         folosind paradigma Map-Reduce

In aceasta tema, am implementat un proces de tip Map-Reduce
utilizand programarea paralela in C. Scopul este de a analiza
continutul unor fisiere text si de a organiza informatiile in
functie de prima litera a cuvintelor, dar si de frecventa
cuvintelor in fisiere.

Map
- Mapperii sunt thread-uri paralele care citesc fisierele de
intrare sincronizat, folosind un mutex pentru a prelua
urmatorul fisier disponibil. Fiecare mapper proceseaza
fisierul, normalizeaza cuvintele si creeaza o lista partiala,
asociind fiecarui cuvant o lista de ID-uri ale fisierelor
in care apare. Rezultatele fiecarui mapper sunt stocate
in structuri separate, urmand sa fie accesibile reducerilor.

Reduce
- Reducerii sunt thread-uri paralele care proceseaza
datele procesate de mapperi pe baza literelor alfabetului.
Acestia preiau fiecare litera pe rand dintr-o coada alfabetica
sincronizata. Coada alfabetica permite distribuirea literelor
intre reduceri, fiecare procesand litere diferite simultan.
Reducerii acceseaza structurile MapperResults sincronizat, folosind
mutex pentru a evita conflictele.
Pentru litera preluata, fiecare reducer identifica cuvintele
care incep cu litera respectiva din listele mapperilor
si le sorteaza conform criteriilor, eliminand duplicatele.
Rezultatele finale sunt scrise in fisiere separate, cate unul
pentru fiecare litera.

    Cu ajutorul paralelizarii reducem timpul de procesare prin
utilizarea eficienta a resurselor.
