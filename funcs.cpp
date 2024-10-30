#include "header.h"
#include "rapidcsv.h"
// Чтение файла
void readFile(string &config) {
    string line;
    ifstream in("schema.json");
    if (in.is_open()) {
        while (getline(in, line)) {
            config += line;
            config += '\n';
        }
    }
}
// Парсер файла
TableNode* ParseJson(const string& config, configjson& schema){
    cout << "Создать новую схему - 1, оставить нынешнюю - любой символ >> ";
    string decision;
    getline(cin, decision);
    int inext;
    int dotcounter = 0;
    for (int i = 0; i < config.size(); i++) {
        if (config[i] == ':') {
            dotcounter++;
            if (dotcounter == 1) {
                i += 3;
                while (config[i] != '"') {
                    schema.name += config[i];
                    i++;
                }
            }
            if (dotcounter == 2) {
                string tupleslimit;
                i += 2;
                while (config[i] != ',') {
                    tupleslimit += config[i];
                    i++;
                }
                schema.tuples_limit = stoi(tupleslimit);
            }
            if (dotcounter > 2) {
                while (config[i] != '{') { i++; }
                inext = i++;
                break;
            } else {
                continue;
            }
        }
    }
    filesystem::create_directory(schema.name); // Создали директорию со схемой
    filesystem::current_path(schema.name);

    TableNode* tablesHead = nullptr;
    TableNode* tablesTail = nullptr;

    for (int i = inext; i < config.size(); i++) {
        if (config[i] == '"') {
            i++;
            string temptablename;
            while (config[i] != '"') {
                temptablename += config[i];
                i++;
            }
            i += 3;
            // Создаем новую таблицу
            TableNode* newTableNode = new TableNode{temptablename, nullptr, nullptr};
            filesystem::create_directory(temptablename); // Создали директорию с таблицей
            //cout << "sozdal table" << endl;
            filesystem::current_path(temptablename);
            // Добавляем таблицу в связный список таблиц
            if (tablesHead == nullptr) {
                tablesHead = newTableNode;
                tablesTail = newTableNode;
            } else {
                tablesTail->next = newTableNode;
                tablesTail = newTableNode;
            }
            i++;
            string special_col = temptablename + "_pk";
            Node* Col_pk = new Node{special_col, nullptr};
            newTableNode->columns = Col_pk;
            if (decision == "1"){ // Если созд новую
                ofstream file("1.csv");
                file << special_col << ',';
                while (config[i] != ']') {
                if (config[i] == '"') {
                    i++;
                    string tempcolname;
                    while (config[i] != '"') {
                        tempcolname += config[i];
                        i++;
                    }
                    // Создаем новую колонку и добавляем ее в список колонок таблицы
                    Node* newColumnNode = new Node{tempcolname, nullptr};
                    if (config[i + 1] != ']') {
                        file << tempcolname << ",";
                    } else {
                        file << tempcolname;
                    }
                    if (newTableNode->columns == nullptr) {
                        newTableNode->columns = newColumnNode;
                    } else {
                        Node* lastColumn = newTableNode->columns;
                        while (lastColumn->next != nullptr) {
                            lastColumn = lastColumn->next;
                        }
                        lastColumn->next = newColumnNode;
                    }
                }
                i++;
            }
            file << "\n";
            file.close();
            string pk = special_col + "_sequence.txt";
            ofstream filepk(pk);
            filepk << '0';
            filepk.close();
            filesystem::current_path("..");
            } else { // Если оставляем нынешнюю
                while (config[i] != ']') {
                    if (config[i] == '"') {
                        i++;
                        string tempcolname;
                        while (config[i] != '"') {
                            tempcolname += config[i];
                            i++;
                        }
                        // Создаем новую колонку и добавляем ее в список колонок таблицы
                        Node* newColumnNode = new Node{tempcolname, nullptr};
                        if (newTableNode->columns == nullptr) {
                            newTableNode->columns = newColumnNode;
                        } else {
                            Node* lastColumn = newTableNode->columns;
                            while (lastColumn->next != nullptr) {
                                lastColumn = lastColumn->next;
                            }
                            lastColumn->next = newColumnNode;
                        }
                    }
                    i++;
                }
                filesystem::current_path("..");
            }
        }
    }
    return tablesHead;
}

string doCommand(string command, TableNode* tables, configjson& schema){
    cout << "==SLEEP==" << endl;
    this_thread::sleep_for(chrono::milliseconds(5000));
    cout << "==WOKE UP==" << endl;
    CommandType commandType = getCommandType(command);
    string result = "Wrong command!";
        switch (commandType) {
            case CommandType::INSERT:
                result = Insert(command, tables, schema);
                break;
            case CommandType::DELETE:
                result = Delete(command, tables, schema);
                break;
            case CommandType::SELECT:
                result = Select(command, tables, schema);
                break;
            case CommandType::UNKNOWN:
            default:
                return "Unknown command!";
        }
    return result;
}
// Проверка, существует ли таблица
bool tableExists(TableNode* tables, const string& tableName, configjson schema) {
    string filePath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tableName;
    TableNode* currentTable = tables;
    while (currentTable != nullptr) {
        if (currentTable->tabl == tableName) {
            filesystem::current_path(filePath);
            return true;
            
        }
        currentTable = currentTable->next;
    }
    return false;
}
// Проверка, существует ли колонка
bool columnExists(TableNode* tables, const string& columnName, const string& tableName) {
    TableNode* currentTable = tables;
    while (currentTable != nullptr) {
        if (currentTable->tabl == tableName) {
            Node* currentColumn = currentTable->columns;
            while (currentColumn != nullptr) {
                if (currentColumn->col == columnName) {
                    return true;
                }
                currentColumn = currentColumn->next;
            }
        }
        currentTable = currentTable->next;  // Переход к следующей таблице
    }
    return false;
}

// Функция, определяющая команду
CommandType getCommandType(const string& command) {
    istringstream iss(command);
    string word;
    iss >> word;

    if (word == "INSERT") {
        return CommandType::INSERT;
    } else if (word == "DELETE") {
        return CommandType::DELETE;
    } else if (word == "SELECT") {
        return CommandType::SELECT;
    } else if (word == "EXIT") {
        return CommandType::EXIT;
    } else {
        return CommandType::UNKNOWN;
    }
}
// Копировать названия колонок в другой .csv
string copyFirstRow(const string& ColCsvPath) {
    ifstream csvoldFile(ColCsvPath);
    string line;
    if (csvoldFile.is_open()) {
        getline(csvoldFile, line);
    }
    csvoldFile.close();
    return line;
}
// Изменить первичный ключ
int pkChanger(string filePath){
    ifstream pkReader(filePath);
    string number;
    getline(pkReader, number);
    pkReader.close();
    ofstream pkWriter(filePath);
    pkWriter << to_string(stoi(number)+1);
    pkWriter.close(); 
    return stoi(number)+1;    
}

string Insert(const string& command, TableNode* tables, const configjson &schema) {
    istringstream iss(command); 
    string word;
    iss >> word;
    string temptablename;
    int count = 1;
    while (iss >> word) { 
        count++;
        if (word != "INTO" && count == 2){
            return "Wrong command!";
        }
        if (count == 3) {
            if (!tableExists(tables, word, schema)){
                return "No table named '" + word + "'";
            } else {
                temptablename = word;
            }
        }
        if (word != "VALUES" && count == 4){
            return "Wrong command!";
        }
        if (word[0] != '(' && word[word.size()-1] != ')' && count == 5){
            return "Wrong command!";
        }
        if (count == 5){
            string pkPath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + temptablename + "/" + temptablename + "_pk_sequence.txt";
            int fnum = 1;
            int tabpk = pkChanger(pkPath);
            string values = to_string(tabpk) + ',';
            for (int i = 0; i < word.size(); i++){
                if (word[i] == '\''){
                    i++;
                    string value;
                    while (word[i] != '\''){
                        value += word[i];
                        i++;
                    }
                    if(word[i+1] == ')'){
                        values += value + "\n";
                    }
                    else{
                        values += value + ",";
                    }
                }
            }
            while (true) {
                string filePath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + temptablename + "/" + to_string(fnum) + ".csv";
                // Проверка существования файла
                ifstream testFile(filePath);
                if (!testFile.is_open()) {
                    break; 
                }
                testFile.close();
                rapidcsv::Document doc(filePath);
                if (doc.GetRowCount() < schema.tuples_limit) {
                    break; 
                }
                fnum++;
            }
            
            string filePath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + temptablename + "/" + to_string(fnum) + ".csv";
            //cout << filePath << endl;
            ofstream file(filePath, ios::app);
            ifstream test(filePath); 
            string teststr;
            getline(test, teststr);
            if (teststr.size() <= 1){
                string cols = copyFirstRow("/home/andrey/Documents/coding/project4/" + schema.name + "/" + temptablename + "/1.csv");
                file << cols << endl;
                test.close();
            }
            file << values;
            file.close();
        }
    }
    return "";
}
// Подсчет файлов
int countCSVFiles(const string& directoryPath) {
    int count = 0;
    for (const auto& entry : filesystem::directory_iterator(directoryPath)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            count++;
        }
    }
    return count;
}

string Delete(const string& command, TableNode* tables, const configjson &schema) {
    istringstream iss(command); 
    string word;
    iss >> word;
    string temptablename;
    string tempcolname = "";
    int count = 1;
    while (iss >> word) { 
        count++;
        if (word != "FROM" && count == 2){
            return "Wrong command!";
        }
        if ((count == 3) && (!tableExists(tables, word, schema))){
            return "No table named '" + word + "'";
        }
        if (word != "WHERE" && count == 4){
            return "Wrong command!";
        }
        if (count == 5){
            bool foundDot = false;
            for (size_t i = 0; i < word.size(); i++) {
                if (word[i] == '.') {
                    foundDot = true;
                    continue;
                }
                if (!foundDot) {
                    temptablename += word[i];
                } else {
                    tempcolname += word[i];
                }
            }
            if (!columnExists(tables, tempcolname, temptablename)){
                return "No column named '" + tempcolname + "'";
            }
        }
        if (word != "=" && count == 6){
            return "Wrong command!";
        }
        if (word[0] != '\'' && word[word.size()-1] != '\'' && count == 7){
            return "Wrong command!";
        }
        if (count == 7){
            const string directoryPath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + temptablename;
            int csvnum = countCSVFiles(directoryPath);
            string value;
            for (size_t i = 1; i < word.size()-1; i++){
                value += word[i];
            }
            for(size_t fnum = 1; fnum <= csvnum; fnum++){
                string filePath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + temptablename + "/" + to_string(fnum) + ".csv";
                rapidcsv::Document doc(filePath);
                size_t columnIndex = doc.GetColumnIdx(tempcolname);
                for (size_t i = doc.GetRowCount(); i > 0; --i) {
                    if (doc.GetCell<string>(columnIndex, i - 1) == value) {
                        doc.RemoveRow(i - 1);
                    }
                }
                // Сохраняем изменения в тот же файл
                doc.Save(filePath);
            }
        }
    }
    return "";
}
// Удаление запятых
string DelComa(string word){
    if (word[word.size()-1] != ','){
        return word;
    }
    string newword;
    for (size_t i = 0; i < word.size(); i++){
        if (word[i] != ','){
            newword+=word[i];
        }
    }
    return newword;
}
// Отделение таблицы от колонки
string DelTab(string word) {
    string newword;
    bool foundDot = false;
    for (size_t i = 0; i < word.size(); i++) {
        if (word[i] == '.') {
            foundDot = true;
            continue;
        }
        if (foundDot) {
            newword += word[i];
        }
    }
    return newword;
}
// Отделение колонки от таблицы
string DelCol(string word) {
    string newword;
    for (size_t i = 0; i < word.size(); i++) {
        if (word[i] != '.') {
            newword += word[i];
        }
        else{break;}
    }
    return newword;
}
// Отладочная функция для вывода первичных ключей
void Printer(Sel* head) {
    Sel* currentNode = head;
    while (currentNode != nullptr) {
        cout << "TAB: " << currentNode->tab << ", COL: " << currentNode->col << endl;
        cout << "PK: ";
        Nodeint* pkNode = currentNode->id;
        while (pkNode != nullptr) {
            cout << pkNode->pk << " ";
            pkNode = pkNode->next;
        }
        cout << endl;
        currentNode = currentNode->next;
    }
}
// Содержится ли первичный ключ 
bool Contains(Nodeint* head, int pk) {
    Nodeint* current = head;
    while (current != nullptr) {
        if (current->pk == pk) {
            return true;
        }
        current = current->next;
    }
    return false;
}
// Печать SELECT FROM
string Selectselect(Sel* SelHead, const configjson &schema){
    string result;
    string directoryPath1 = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + SelHead->tab;
    string directoryPath2 = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + SelHead->next->tab;
    int csvnum1 = countCSVFiles(directoryPath1);
    int csvnum2 = countCSVFiles(directoryPath2);
        for (size_t fnum = 1; fnum <= csvnum1; fnum++) {
            string filePath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + SelHead->tab + "/" + to_string(fnum) + ".csv";
            rapidcsv::Document doc1(filePath);            
            size_t col_idx = doc1.GetColumnIdx(SelHead->col);
            //cout << "colid = " << col_idx << endl;
            for (size_t i = 0; i < doc1.GetRowCount(); ++i) {
                int pk_value = stoi(doc1.GetCell<string>(0, i));
                //cout << pk_value << " ";
                if (Contains(SelHead->id, pk_value)) {
                    string name_value = doc1.GetCell<string>(col_idx, i);
                    //cout << name_value << " ";
                    // Проход по второй таблице
                    Sel* nextSel = SelHead->next;
                    if (nextSel != nullptr) {
                        for (size_t fnum2 = 1; fnum2 <= csvnum2; fnum2++) {
                            string filePath2 = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + nextSel->tab + "/" + to_string(fnum2) + ".csv";
                            rapidcsv::Document doc2(filePath2);
                            size_t col_idx2 = doc2.GetColumnIdx(nextSel->col);

                            for (size_t j = 0; j < doc2.GetRowCount(); ++j) {
                                int pk_value2 = stoi(doc2.GetCell<string>(0, j));
                                //cout << pk_value << " ";
                                if (Contains(nextSel->id, pk_value2)) {
                                    string name_value2 = doc2.GetCell<string>(col_idx2, j);
                                    result+= name_value + " " + name_value2 + '\n';
                                }
                            }
                        }
                    }
                }
            }
        }
    return result;
}
// Логическое OR 
void AddNode(Nodeint*& head, int pk) {
    // Проверка, существует ли уже такой элемент
    Nodeint* current = head;
    while (current != nullptr) {
        if (current->pk == pk) {
            return; // Элемент уже существует, ничего не делаем
        }
        current = current->next;
    }

    // Если элемент не существует, добавляем его
    Nodeint* newNode = new Nodeint{pk, nullptr};
    if (head == nullptr) {
        head = newNode;
    } else {
        current = head;
        while (current->next != nullptr) {
            current = current->next;
        }
        current->next = newNode;
    }
}
// Логический AND
void ClearNodeintList(Nodeint*& head, int*& array, int& arrsize, int& actualArrsize) {
    int newarrsize = 0;
    bool* isMatched = new bool[actualArrsize](); // Массив для отслеживания совпадений
    for (int i = 0; i < actualArrsize; ++i) {
        Nodeint* current = head;
        bool matched = false;
        while (current != nullptr) {
            if (array[i] == current->pk) {
                matched = true;
                break;
            }
            current = current->next;
        }
        if (matched) {
            array[newarrsize++] = array[i];
            isMatched[i] = true; // Отмечаем элемент как совпавший
        }
    }
    delete[] isMatched; 
    actualArrsize = newarrsize;
    // Чистим список
    while (head != nullptr) {
        Nodeint* next = head->next;
        delete head;
        head = next;
    }
    // Перезаписываем узлы
    int arrayIndex = 0;
    while (arrayIndex < actualArrsize) {
        AddNode(head, array[arrayIndex]);
        arrayIndex++;
    }

    delete[] array;
    array = nullptr;
}
// Равенство строк
Sel* Strequals(Sel* SelHead, string tabl1, string tabl2, string col1, string col2, string op, configjson& schema){
    string directoryPath1 = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tabl1;
    string directoryPath2 = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tabl2;
    int csvnum1 = countCSVFiles(directoryPath1);
    int csvnum2 = countCSVFiles(directoryPath2);
    int N1, N2 = 30;
    int *arr1 = new int[N1];
    int *arr2 = new int[N2];
    int arrsize1 = 0;
    int arrsize2 = 0;
    for (size_t fnum = 1; fnum <= csvnum1; fnum++) {
        string filePath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tabl1 + "/" + to_string(fnum) + ".csv";
        rapidcsv::Document doc1(filePath);
        size_t col_idx = doc1.GetColumnIdx(col1);
        for (size_t i = 0; i < doc1.GetRowCount(); ++i) {
            string name_value = doc1.GetCell<string>(col_idx, i);
            //cout << name_value << " ";
            int pk_value = stoi(doc1.GetCell<string>(0, i));
            //cout << pk_value << endl;
                for (size_t fnum2 = 1; fnum2 <= csvnum2; fnum2++) {
                    string filePath2 = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tabl2 + "/" + to_string(fnum2) + ".csv";
                    rapidcsv::Document doc2(filePath2);
                    size_t col_idx2 = doc2.GetColumnIdx(col2);
                    for (size_t j = 0; j < doc2.GetRowCount(); ++j) {
                        string name_value2 = doc2.GetCell<string>(col_idx2, j);
                        //cout << name_value2 << " ";
                        int pk_value2 = stoi(doc2.GetCell<string>(0, j));
                        //cout << pk_value2 << endl;
                        if (name_value == name_value2){
                            //cout << "NASHEL! = "<< pk_value << " " << pk_value2 << endl;
                            if (op == "OR") {
                                AddNode(SelHead->id, pk_value);
                            }
                            if (op == "AND") {
                                arr1[arrsize1] = pk_value;
                                arrsize1++;
                                cout << arrsize1 << " ";
                            }
                            // Добавляем pk_value2 в список Nodeint второго узла Sel
                            if (SelHead->next != nullptr) {
                                if (op == "OR") {
                                    AddNode(SelHead->next->id, pk_value2);
                                }
                                if (op == "AND") {
                                    arr2[arrsize2] = pk_value2;
                                    arrsize2++;
                                    cout << arrsize2 << " ";
                                }
                            }
                        }
                    }
                }
        }
    }
    if (op == "AND"){
        ClearNodeintList(SelHead->id, arr1, N1, arrsize1);
        ClearNodeintList(SelHead->next->id, arr2, N2, arrsize2);
    }
    return SelHead;
}
// Равенство колонок
Sel* Valequals(Sel* SelHead, string tabl1, string col1, string value, string op, configjson& schema) {
    Sel* currentNode = SelHead;
    while (currentNode != nullptr) {
        if (currentNode->tab == tabl1) {
            break; 
        }
        currentNode = currentNode->next;
    }
    string directoryPath1 = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tabl1;
    int csvnum1 = countCSVFiles(directoryPath1);
    int N = 15;
    int *arr = new int[N];
    int arrsize = 0;
    for (size_t fnum = 1; fnum <= csvnum1; fnum++) {
        string filePath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tabl1 + "/" + to_string(fnum) + ".csv";
        rapidcsv::Document doc1(filePath);
        size_t col_idx = doc1.GetColumnIdx(col1);
        for (size_t i = 0; i < doc1.GetRowCount(); ++i) {
            string name_value = doc1.GetCell<string>(col_idx, i);
            int pk_value = stoi(doc1.GetCell<string>(0, i));
            if (name_value == value) {
                if (op == "OR") {
                    AddNode(currentNode->id, pk_value);
                }
                if (op == "AND") {
                    arr[arrsize] = pk_value;
                    arrsize++;
                }
            }
        }
    }
    if (op == "AND"){
        ClearNodeintList(currentNode->id, arr, N, arrsize);
    }
    return SelHead;
}
// Парсер условия
Sel* parseCondition(Sel* SelHead, string cond, string op, configjson& schema){
    string bef;
    string aft;
    bool foundEqual = false;
    for (char ch : cond) {
        if (ch == '=') {
            foundEqual = true;
            continue;
        }
        if (!foundEqual) {
            bef += ch;
        } else {
            aft += ch;
        }
    }

    string tabl1 = DelCol(bef);
    string col1 = DelTab(bef);
    string value;
    string tabl2;
    string col2;
    if (aft[0] == '\'' && aft[aft.size()-1] == '\''){
        for (size_t i = 1; i < aft.size()-1; i++){
            value+=aft[i];
        }
        SelHead = Valequals(SelHead, tabl1, col1, value, op, schema);
    } else{
        tabl2 = DelCol(aft);
        col2 = DelTab(aft);
        if (tabl1 == tabl2){
            cout << "Same tables!" << endl;
            return SelHead = nullptr;
        }
        Sel* currentNode = SelHead;
            if (currentNode->tab != tabl1) {
                string temp = tabl1;
                string tempcol = col1;
                tabl1 = tabl2;
                col1 = col2;
                tabl2 = temp;
                col2 = tempcol;
                cout << tabl1 << tabl2 << col1 << col2 << endl;
            }
        SelHead = Strequals(SelHead, tabl1, tabl2, col1, col2, op, schema);
    }
    return SelHead;
}

string Select(string& command, TableNode* tables, configjson &schema) {
    string result;
    istringstream iss(command); 
    string word;
    iss >> word; // select skipped

    Sel* selectedColumnsHead = nullptr;
    Sel* selectedColumnsTail = nullptr;

    // Обработка части SELECT
    while (iss >> word) {
        word = DelComa(word);
        if (word == "FROM"){
            break;
        }
        string colword = DelTab(word);
        string tabword = DelCol(word);
        Sel* newSelNode = new Sel{tabword, colword, nullptr, nullptr};
        Nodeint* pklistHead = nullptr;
        Nodeint* pklistTail = nullptr;

        const string directoryPath = "/home/andrey/Documents/coding/project4/" + schema.name + "/" + tabword;
        int csvnum = countCSVFiles(directoryPath);

        for (size_t fnum = 1; fnum <= csvnum; fnum++) {
            string filePath = directoryPath + "/" + to_string(fnum) + ".csv";
            rapidcsv::Document doc(filePath);
            size_t rowCount = doc.GetRowCount();

            for (size_t i = 0; i < rowCount; ++i) {
                string value = doc.GetCell<string>(0, i);
                Nodeint* newNode = new Nodeint{stoi(value), nullptr};

                if (pklistHead == nullptr) {
                    pklistHead = newNode;
                    pklistTail = newNode;
                } else {
                    pklistTail->next = newNode;
                    pklistTail = newNode;
                }
            }
        }
        
        newSelNode->id = pklistHead; // Привязываем список первичных ключей к узлу Sel
        
        if (selectedColumnsHead == nullptr) {
            selectedColumnsHead = newSelNode;
            selectedColumnsTail = newSelNode;
        } else {
            selectedColumnsTail->next = newSelNode;
            selectedColumnsTail = newSelNode;
        }
    }
    // Обработка части FROM
    if (word != "FROM") {
        result += "Ошибка: отсутствует FROM\n";
        return result;
    }

    Sel* currentNode = selectedColumnsHead;
    while (iss >> word) {
        if (word == "WHERE"){
            break;
        }
        word = DelComa(word);
        Sel* tempNode = currentNode;
        while (tempNode != nullptr) {
            if (tempNode->tab != word) {
                tempNode = tempNode->next;
            } else {
                break;
            }
        }

        if (tempNode == nullptr) {
            result += "Таблицы не совпадают\n";
            return result;
        }
    }
    cout << "остановился на " << word << endl;
    if (word == "WHERE"){
        string cond;
        string op = "AND";
        while (iss >> word) {
            if (word == "AND" || word == "OR"){
                selectedColumnsHead = parseCondition(selectedColumnsHead, cond, op, schema);
                op = word;
                cond = "";
            }
            else {
                cond += word;
            }
        }
        selectedColumnsHead = parseCondition(selectedColumnsHead, cond, op, schema);
    }
    //Printer(selectedColumnsHead);
    result += Selectselect(selectedColumnsHead, schema);
    return result;
}


