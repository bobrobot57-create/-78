# Деплой VoiceLab License Server на Railway

**Webhook-режим:** сервер спит, просыпается только когда:
- пользователь пишет боту
- exe проверяет/активирует лицензию

---

## Шаг 1. Репозиторий на GitHub

1. Создай **новый репозиторий** на GitHub (пустой, без README).
2. Зайди в папку `deploy` на своём компе:
   ```
   cd c:\Users\1\Desktop\voicer2\deploy
   ```
3. Инициализируй Git и запушь:
   ```bash
   git init
   git add .
   git commit -m "VoiceLab License Server"
   git branch -M main
   git remote add origin https://github.com/ТВОЙ_ЮЗЕР/ИМЯ_РЕПО.git
   git push -u origin main
   ```

---

## Шаг 2. Railway — проект

1. Зайди на **https://railway.app**
2. Войди через **GitHub**
3. Нажми **«New Project»**
4. Выбери **«Deploy from GitHub repo»**
5. Выбери свой репозиторий (где лежит папка deploy)
6. **Важно:** если в репо только папка `deploy`, всё ок. Если нет — в настройках сервиса укажи **Root Directory: `deploy`**

---

## Шаг 3. Переменные окружения

1. В Railway открой свой сервис (карточка)
2. Вкладка **«Variables»**
3. Нажми **«New Variable»** или **«+ Add»**
4. Добавь по одной:

| Имя | Значение |
|-----|----------|
| `ADMIN_BOT_TOKEN` | Токен админ-бота от @BotFather |
| `CLIENT_BOT_TOKEN` | Токен клиентского бота |
| `ADMIN_USER_IDS` | Твой Telegram ID (у @userinfobot) |
| `API_SECRET` | Любая строка для защиты API |
| `WEBHOOK_BASE_URL` | Пока оставь пустым — заполнишь после |

---

## Шаг 4. Домен и WEBHOOK_BASE_URL

1. Включи **Settings** → **Networking**
2. Нажми **«Generate Domain»**
3. Появится URL вида `https://voicer-xxx.up.railway.app`
4. **Скопируй его**
5. Вернись в **Variables** и добавь/измени:
   - `WEBHOOK_BASE_URL` = `https://voicer-xxx.up.railway.app` (без слэша в конце)

6. Railway автоматически пересоберёт проект после изменения переменных

---

## Шаг 5. Клиенты (exe)

В `voicer_config.json` рядом с exe:

```json
{
  "license_url": "https://ТВОЙ-ПРОЕКТ.up.railway.app/check",
  "api_secret": "тот_же_что_API_SECRET"
}
```

---

## Что проверить

- **Health:** открой в браузере `https://твой-проект.up.railway.app/health` — должен быть `{"status":"ok"}`
- **Бот:** напиши в админ-боте `/start` — он должен ответить
- **Логи:** в Railway → Deployments → выбери последний деплой → View Logs
