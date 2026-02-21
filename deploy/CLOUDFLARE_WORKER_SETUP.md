# Cloudflare Worker — обход блокировок для РФ

**Зачем:** Railway блокируют из РФ. Cloudflare доступен. Но Cloudflare Proxy (оранжевое облако) режет запросы от exe как ботов → 403.

**Решение:** Worker выполняется **до** Bot Fight и WAF. Запросы проходят без проверок.

**Цепочка:** РФ юзер → voicer-api.ru (Cloudflare) → **Worker** → Railway. Всё работает.

---

## Шаг 1. Создать Worker

1. Cloudflare Dashboard → **Workers & Pages** (слева в меню)
2. **Создать** → **Создать Worker**
3. Имя: `voicer-proxy`
4. Удали весь код в редакторе, вставь код ниже
5. **Сохранить и развернуть** (Save and Deploy)

## Шаг 2. Код Worker

**ВАЖНО:** Замени `78-production.up.railway.app` на свой URL Railway, если другой!

```javascript
const RAILWAY = 'https://78-production.up.railway.app';

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const target = RAILWAY + url.pathname + url.search;
    const headers = new Headers(request.headers);
    headers.set('Host', new URL(RAILWAY).host);
    return fetch(target, {
      method: request.method,
      headers,
      body: request.method !== 'GET' && request.method !== 'HEAD' ? request.body : undefined,
    });
  },
};
```

## Шаг 3. Привязать Worker к домену (маршрут)

1. В Worker нажми **Настройки** (Settings) или **Триггеры** (Triggers)
2. **Маршруты** (Routes) → **Добавить маршрут**
3. **Маршрут**: `voicer-api.ru/*` (или `*voicer-api.ru/*`)
4. **Зона**: voicer-api.ru
5. Сохранить

## Шаг 4. DNS — прокси ВКЛЮЧЁН

- Запись `voicer-api.ru` → CNAME → `78-production.up.railway.app`
- **Прокси: ВКЛЮЧЁН** (оранжевое облако)

Без прокси из РФ не работает. С Worker — прокси включён, но Worker обходит блокировки.

## Проверка

1. В браузере: `https://voicer-api.ru/health` → `{"status":"ok"}`
2. Exe: ввести код, активировать — должно пройти.
