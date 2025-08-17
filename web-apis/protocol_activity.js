import { json } from '@sveltejs/kit';
import { getDb } from '$lib/server/pgdb.js';




export async function GET({ url }) {
  let client;
  try {
    const requestId = Math.random().toString(36).slice(2);
    console.time(`Protocol Activity API ${requestId}`);

    const timeFrame = url.searchParams.get('timeframe') || '24h';
    const customStartIso = url.searchParams.get('start');
    const customEndIso = url.searchParams.get('end');

    const db = getDb();
    client = await db.connect();

    let sql;
    let params = [];
    
    if (timeFrame === 'custom' && customStartIso && customEndIso) {

      const customStart = new Date(customStartIso);
      const customEnd = new Date(customEndIso);
      
      sql = `
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_per_minute
        WHERE minute_bucket >= $1::timestamptz AND minute_bucket < $2::timestamptz
        GROUP BY protocol_id
        ORDER BY transaction_count DESC;
      `;
      params = [customStart.toISOString(), customEnd.toISOString()];
    } else if (timeFrame === 'today') {
      sql = `
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_per_minute
        WHERE minute_bucket >= date_trunc('day', now() at time zone 'utc')
          AND minute_bucket < date_trunc('day', now() at time zone 'utc') + INTERVAL '1 day'
          AND protocol_id != 3  -- temporarily remove kasplex l2
        GROUP BY protocol_id
        ORDER BY transaction_count DESC;
      `;
    } else if (timeFrame === 'yesterday') {
      sql = `
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_per_minute
        WHERE minute_bucket >= (date_trunc('day', now() at time zone 'utc') - INTERVAL '1 day')
          AND minute_bucket < date_trunc('day', now() at time zone 'utc')
          AND protocol_id != 3 -- temporarily remove kasplex l2
        GROUP BY protocol_id
        ORDER BY transaction_count DESC;
      `;
    } else {

      const timeFrameMap = {
        '15m': "INTERVAL '15 MINUTES'",
        '1h': "INTERVAL '1 HOUR'",
        '24h': "INTERVAL '24 HOURS'",
        '7d': "INTERVAL '7 DAYS'"
      };
      const intervalExpression = timeFrameMap[timeFrame] || "INTERVAL '24 HOURS'";
      
      sql = `
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_per_minute
        WHERE minute_bucket >= (now() at time zone 'utc' - ${intervalExpression})
          AND minute_bucket <= (now() at time zone 'utc')
          AND protocol_id != 3  -- temporarily remove kasplex l2
        GROUP BY protocol_id
        ORDER BY transaction_count DESC;
      `;
    }

    const result = await client.query(sql, params);

    console.timeEnd(`Protocol Activity API ${requestId}`);
    console.log(`[Protocol Activity] ${requestId}: timeframe: ${timeFrame}`);
    
    const payload = {
      status: 'success',
      protocols: result.rows,
      timeFrame,
      start: customStartIso ?? undefined,
      end: customEndIso ?? undefined,
      optimization: 'database_aggregate'
    };

    return json(payload, {
      headers: {
        // Allow CDN/browser to reuse within a few seconds
        'Cache-Control': 'public, max-age=5, s-maxage=5'
      }
    });
  } catch (error) {
    console.error('Protocol Activity API error:', error);
    return json({ status: 'error', error: error.message, protocols: [] }, { status: 500 });
  } finally {
    if (client) client.release();
  }
}


