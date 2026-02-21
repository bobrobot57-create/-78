// Cloudflare Worker — прокси на Railway
// Маршрут: voicer-api.ru/* -> 78-production.up.railway.app/*

const RAILWAY_ORIGIN = 'https://78-production.up.railway.app';

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const targetUrl = RAILWAY_ORIGIN + url.pathname + url.search;

    // Не пересылаем Host — Railway должен получить свой хост
    const headers = new Headers(request.headers);
    headers.delete('Host');
    headers.set('Host', new URL(RAILWAY_ORIGIN).host);

    const modifiedRequest = new Request(targetUrl, {
      method: request.method,
      headers,
      body: request.method !== 'GET' && request.method !== 'HEAD' ? request.body : undefined,
      redirect: 'follow',
    });

    return fetch(modifiedRequest);
  },
};
