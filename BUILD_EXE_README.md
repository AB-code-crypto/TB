# Сборка Windows .exe

## Что собираем

Основная сборка для клиента:

```powershell
.\build_exe_client.ps1
```

Результат:

```text
dist\TBankRobot\TBankRobot.exe
```

Клиенту передаём zip-архив всей папки:

```text
dist\TBankRobot
```

Не передавать клиенту свою локальную базу:

```text
data\tbank_robot.sqlite3
```

Не передавать клиенту свой `.env`.

## Отладочная сборка

Если exe не стартует или молча закрывается:

```powershell
.\build_exe_debug.ps1
```

Запустить:

```powershell
.\dist\TBankRobotDebug\TBankRobotDebug.exe
```

Эта сборка открывает консоль и показывает traceback.

## Где лежит база у клиента

В собранном exe база создаётся рядом с exe:

```text
dist\TBankRobot\data\tbank_robot.sqlite3
```

Это сделано специально: база не должна жить во временной папке PyInstaller.
