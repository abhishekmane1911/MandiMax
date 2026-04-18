/**
 * Netlify serverless proxy — adds Databricks auth server-side
 * Browser → /api/* → this function → Databricks App (with Bearer token) → response
 * Token never exposed to browser. No CORS issues.
 */

const DATABRICKS_APP = 'https://mandimax-bot-7474653808917244.aws.databricksapps.com';

exports.handler = async (event) => {
  // Handle CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: {
        'Access-Control-Allow-Origin':  '*',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      },
      body: '',
    };
  }

  const TOKEN = process.env.DATABRICKS_TOKEN;
  if (!TOKEN) {
    return {
      statusCode: 503,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'DATABRICKS_TOKEN not set in Netlify env vars' }),
    };
  }

  // Extract the /api/* path from the original request URL
  const reqUrl   = new URL(event.rawUrl);
  const apiPath  = reqUrl.pathname; // e.g. /api/chat, /api/stt
  const targetUrl = `${DATABRICKS_APP}${apiPath}${reqUrl.search}`;

  const reqHeaders = {
    'Authorization': `Bearer ${TOKEN}`,
  };

  // Forward Content-Type for JSON and multipart requests
  if (event.headers['content-type']) {
    reqHeaders['Content-Type'] = event.headers['content-type'];
  }

  try {
    const body = event.body
      ? (event.isBase64Encoded ? Buffer.from(event.body, 'base64') : event.body)
      : undefined;

    const response = await fetch(targetUrl, {
      method:  event.httpMethod,
      headers: reqHeaders,
      body:    event.httpMethod !== 'GET' ? body : undefined,
    });

    const responseBody = await response.text();

    return {
      statusCode: response.status,
      headers: {
        'Content-Type':                 response.headers.get('content-type') || 'application/json',
        'Access-Control-Allow-Origin':  '*',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      },
      body: responseBody,
    };
  } catch (error) {
    console.error('[Proxy Error]', error.message);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: error.message }),
    };
  }
};
