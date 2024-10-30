#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

using namespace std;
int main() {

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        cerr << "Ошибка создания сокета" << endl;
        return 1;
    }

    struct sockaddr_in adr = {0};
    adr.sin_family = AF_INET;
    adr.sin_port = htons(7432);

    int res = inet_pton(AF_INET, "127.0.0.1", &adr.sin_addr);
    if (res == 0) {
        cerr << "inet_pton failed: Неверный формат IP-адреса" << endl;
        close(fd);
        return 1;
    }
    if (res == -1) {
        cerr << "Ошибка преобразования IP-адреса" << endl;
        close(fd);
        return 1;
    }

    res = connect(fd, (struct sockaddr *)&adr, sizeof adr);
    if (res == -1) {
        cerr << "Ошибка подключения" << endl;
        close(fd);
        return 1;
    } else {
        cout << "Подключено" << endl;
    }

    // Принимаем информацию от сервера о подключении
    char buf[1024];
    ssize_t nread = recv(fd, buf, sizeof(buf) - 1, 0);
    if (nread == -1) {
        cerr << "Ошибка получения сообщения от сервера" << endl;
        close(fd);
        return 1;
    }
    buf[nread] = '\0';
    cout << "Сервер: " << buf << endl;

    // Цикл для отправки запросов серверу
    while (true) {
        string request;
        cout << "command>> ";
        getline(cin, request);
        
        if (request.empty()) {
            break;
        }

        // Отправляем запрос серверу
        ssize_t nsent = send(fd, request.c_str(), request.size(), 0);
        if (nsent == -1) {
            cerr << "Ошибка отправки запроса" << endl;
            close(fd);
            return 1;
        }

        // Принимаем ответ от сервера
        nread = recv(fd, buf, sizeof(buf) - 1, 0);
        if (nread == -1) {
            cerr << "Ошибка чтения ответа" << endl;
            close(fd);
            return 1;
        }
        buf[nread] = '\0';
        cout << "Сервер: " << buf << endl;
    }

    close(fd);
    return 0;
}