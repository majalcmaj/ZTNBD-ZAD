Środowisko deweloperskie
------------------------

Do uruchomienia projektu lokalnie wymagany jest [Docker](https://www.docker.com) oraz bash. W przypadku Windowsa zalecane jest wykorzystywanie Git Bash, które instalowane jest wraz z instalacją gita. Środowisko budujemy i uruchamiamy poleceniem:

```bash
bin/jupyer.sh
```

**Uwaga:** Jeśli korzystamy z Docker Toolbox (Windows), a kod projektu nie jest zlokalizowany na dysku C, musimy ręcznie podmontować dysk na którym znajduje się projekt. Instrukcję możemy znależć na [GitHubGist](https://gist.github.com/first087/2214c81114f190271d26c3e88da36104).

Po zbudowaniu projektu (pierwsze budowanie projektu może zająć trochę więcej czasu) i uruchomieniu jupytera powinniśmy w konsoli zobaczyć komunikat:

```bash
[I 19:59:59.302 LabApp] The Jupyter Notebook is running at:
[I 19:59:59.302 LabApp] http://[all ip addresses on your system]:8888/?token=c271c08e3da12801d99c2287ab7cef1f9020adcc43af6c29
[I 19:59:59.302 LabApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 19:59:59.303 LabApp]

    Copy/paste this URL into your browser when you connect for the first time,
    to login with a token:
        http://localhost:8888/?token=c271c08e3da12801d99c2287ab7cef1f9020adcc43af6c29
```

Skopiuj link i wklej go do przeglądarki, w otwartym notebooku otwórz plik `work/main.ipynb`, a następnie z menu wybierz `Cell -> Run All`. Prawdopodobnie skrypt zakończy się niepowodzeniem co będzie spowodowane brakiem danych wejściowych, wszystko zależy od wykorzystanego modułu wczytującego dane. Informacja o danych wejściowych dla domyślnych ustawień znajduje się w dalszej części instrukcji.

**Uwaga:** Dla Docker Toolbox (Windows) należy podmienić `localhost` na adres ip serwera. Można go znaleźc zaraz po uruchomieniu `Docker Quickstart Terminal`:

```bash
Setting Docker configuration on the remote daemon...



                        ##         .
                  ## ## ##        ==
               ## ## ## ## ##    ===
           /"""""""""""""""""\___/ ===
      ~~~ {~~ ~~~~ ~~~ ~~~~ ~~~ ~ /  ===- ~~~
           \______ o           __/
             \    \         __/
              \____\_______/

docker is configured to use the default machine with IP 192.168.99.100
For help getting started, check out the docs at https://docs.docker.com
``` 

**Dane wejściowe:** Domyślnie dane wejściowe należy pobrać z Google Drive i wypakować do głównego folderu. Plik `mapping.csv` powinien się znajdować w folderze `kascysko.blogspot.com/mapping.csv` (informacja z dnia 13/12/2017).