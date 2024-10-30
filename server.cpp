#include "header.h"

using namespace std;
mutex mtx;
void newClient(int fd, TableNode* tables, configjson schema){
    cout << "Клиент подключен (id = " << fd << ')' << endl;
    // Отправляем сообщение о подключении
    const char *connect_msg = "connected successfully\n";
    ssize_t nsent = send(fd, connect_msg, strlen(connect_msg), 0);
    if (nsent == -1) {
        cerr << "Ошибка при отправке сообщения о подключении" << endl;
        //thrcount--;
        close(fd);
        return;
    }

    while (true) {
        char buf[1024];
        ssize_t nread = recv(fd, buf, sizeof(buf) - 1, 0); // Чтение данных из сокета
        if (nread == -1) {
            cerr << "Ошибка при чтении запроса" << endl;
            break;
        }
        if (nread == 0) {
            cout << "Клиент отключился" << endl;
            break;
        }

        buf[nread] = '\0';
        int i = 0;
        string request;
        while(true){
            if (buf[i] != '\0'){
                request += buf[i];
                i++;
            }
            else {
                break;
            }
        }

        if (strlen(buf) == 0) { 
            cout << "Закрытие соединения..." << endl;
            break;
        }

        cout << "Клиент (id = " << fd << ')' << ": " << request << endl;
        mtx.lock();
        string res = doCommand(request, tables, schema);
        mtx.unlock();
        const char *response; 
        if (!res.empty()){
            response = res.c_str();
        }
        else {
            response = "Done\0";
        }

        // Отправляем ответ клиенту
        nsent = send(fd, response, strlen(response), 0);
        if (nsent == -1) {
            cerr << "Ошибка при отправке ответа" << endl;
            break;
        }
    }
    //thrcount--;
    close(fd);
}


int main() {
    string config;
    readFile(config);
    configjson schema;
    TableNode* tables = ParseJson(config, schema);
    cout << schema.name << ' ' << schema.tuples_limit << endl;

    int server = socket(AF_INET, SOCK_STREAM, 0); // Получение дескриптора сокета
    if (server == -1) {
        cerr << "Ошибка при создании сокета" << endl;
        return 1;
    }

    struct sockaddr_in adr = {0};
    adr.sin_family = AF_INET;
    adr.sin_port = htons(7432);
    int res = bind(server, (struct sockaddr *)&adr, sizeof adr); // Связь дескриптора с адресом
    if (res == -1) {
        cerr << "Ошибка при связывании сокета" << endl;
        close(server);
        return 1;
    }

    res = listen(server, 1); // Перевод в режим прослушивания
    if (res == -1) {
        cerr << "Ошибка при прослушивании соединений" << endl;
        close(server);
        return 1;
    }

    socklen_t adrlen = sizeof adr;

    do {
        int fd = accept(server, (struct sockaddr *)&adr, &adrlen); // Принятие клиентского соединения
        cout << "fd = " << fd << endl;
        if (fd == -1) {
            cerr << "Ошибка принятия соединения" << endl;
            continue;
        }

        thread client(newClient, fd, tables, schema);
        //cout << "+thread" << endl;
        client.detach(); // Отсоединяем поток, чтобы он работал независим
    } while(true);
    
    close(server);
    cout << "Сервер закрыт" << endl;
    return 0;
}