// Scheduled sync: read a work list from the database, fetch public metrics
// for each item, aggregate, and write results back. Paced and concurrent.

import { createClient } from "@supabase/supabase-js";

const DB_URL = process.env.SUPABASE_URL;
const DB_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!DB_URL || !DB_KEY) {
  console.error("missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
const db = createClient(DB_URL, DB_KEY);

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";

// Static conversion table to a base unit, so multi-unit totals can be summed.
const RATE = {
  JPY: 1, USD: 155, EUR: 168, GBP: 197, KRW: 0.11, TWD: 4.8,
  HKD: 20, CAD: 114, AUD: 103, PHP: 2.7, BRL: 28, INR: 1.85,
  SGD: 115, THB: 4.3, MXN: 8.5, IDR: 0.0096, MYR: 33, VND: 0.0061,
};
const SYMBOL = {
  "¥": "JPY", "￥": "JPY", "$": "USD", "€": "EUR", "£": "GBP",
  "₩": "KRW", "NT$": "TWD", "HK$": "HKD", "CA$": "CAD", "A$": "AUD",
  "₱": "PHP", "R$": "BRL", "₹": "INR", "₫": "VND", "RM": "MYR",
  "Rp": "IDR", "฿": "THB",
};

function parseAmount(text) {
  if (!text) return null;
  const t = text.trim().replace(/ /g, " ");
  const symbols = Object.keys(SYMBOL).sort((a, b) => b.length - a.length);
  let unit = null;
  for (const s of symbols) {
    if (t.includes(s)) { unit = SYMBOL[s]; break; }
  }
  if (!unit) {
    const code = t.match(/\b([A-Z]{3})\b/);
    if (code && RATE[code[1]] !== undefined) unit = code[1];
  }
  if (!unit) return null;
  const num = t.replace(/[^\d.,]/g, "").replace(/,/g, "");
  const amount = Number.parseFloat(num);
  if (!Number.isFinite(amount)) return null;
  return { unit, amount };
}

const CLIENT = { clientName: "WEB", clientVersion: "2.20250701.01.00", hl: "ja" };
// 1配信のページ上限。同接の多い配信ほどチャットが長くページ数も多いので、
// 高価値配信を過小集計しないよう長め（~30時間相当）にする。到達時はtruncatedで記録。
const MAX_PAGES = 1500;
const PAGE_PAUSE_MS = 120;

async function processItem(itemId) {
  const page = await fetch(`https://www.youtube.com/watch?v=${itemId}`, {
    headers: { "user-agent": UA, "accept-language": "ja" },
  });
  // ページ取得の一過性失敗は翌日リトライ（null）。
  if (!page.ok) return null;
  const html = await page.text();
  const key = (html.match(/"INNERTUBE_API_KEY":"([^"]+)"/) || [])[1];
  const first = html.match(
    /"liveChatRenderer":\{"continuations":\[\{"reloadContinuationData":\{"continuation":"([^"]+)"/,
  );
  // チャット無効・メンバー限定・リプレイ無しは恒久的。0で確定記録し二度と再取得しない
  // （毎日リトライで無駄なリクエストが累積するのを防ぐ）。
  if (!key || !first) return { empty: true };
  let continuation = first[1];

  const breakdown = {};
  let count = 0;
  let memberJoins = 0;
  let giftMemberships = 0;
  let pages = 0;
  for (let i = 0; i < MAX_PAGES && continuation; i++) {
    const res = await fetch(
      `https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay?key=${key}&prettyPrint=false`,
      {
        method: "POST",
        headers: { "content-type": "application/json", "user-agent": UA },
        body: JSON.stringify({ context: { client: CLIENT }, continuation }),
      },
    );
    if (!res.ok) break;
    const data = await res.json();
    const cont = data.continuationContents?.liveChatContinuation;
    if (!cont) break;
    pages++;
    for (const a of cont.actions ?? []) {
      const item =
        a.replayChatItemAction?.actions?.[0]?.addChatItemAction?.item;
      if (!item) continue;
      const paid =
        item.liveChatPaidMessageRenderer || item.liveChatPaidStickerRenderer;
      if (paid) {
        const parsed = parseAmount(paid.purchaseAmountText?.simpleText);
        if (parsed) {
          breakdown[parsed.unit] = (breakdown[parsed.unit] ?? 0) + parsed.amount;
          count++;
        }
        continue;
      }
      // 新規メンバー加入（マイルストーン継続は「New member」ヘッダのみカウント）
      if (item.liveChatMembershipItemRenderer) {
        const header = item.liveChatMembershipItemRenderer.headerSubtext?.runs
          ?.map((r) => r.text)
          .join("");
        // マイルストーン（"Member for N months"）は加入ではないので除外
        if (!header || !/month|か月|ヶ月/i.test(header)) memberJoins++;
        continue;
      }
      // ギフトメンバーシップ購入告知（「Nギフト」の件数を合算）
      const gift = item.liveChatSponsorshipsGiftPurchaseAnnouncementRenderer;
      if (gift) {
        const text = gift.header?.liveChatSponsorshipsHeaderRenderer?.primaryText?.runs
          ?.map((r) => r.text)
          .join("");
        const n = text?.match(/(\d+)/);
        giftMemberships += n ? Number(n[1]) : 1;
      }
    }
    continuation =
      cont.continuations?.[0]?.liveChatReplayContinuationData?.continuation ??
      null;
    await new Promise((r) => setTimeout(r, PAGE_PAUSE_MS));
  }

  let total = 0;
  for (const [unit, amt] of Object.entries(breakdown)) {
    total += amt * (RATE[unit] ?? 0);
  }
  return {
    total: Math.round(total),
    count,
    breakdown,
    memberJoins,
    giftMemberships,
    pages,
    truncated: pages >= MAX_PAGES, // 上限到達＝集計が途中で切れている可能性
  };
}

async function mapPool(items, concurrency, fn) {
  let idx = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (idx < items.length) {
      const my = idx++;
      await fn(items[my], my).catch(() => null);
    }
  });
  await Promise.all(workers);
}

async function main() {
  const windowDays = Number(process.env.SYNC_WINDOW_DAYS ?? 30);
  const since = new Date(Date.now() - windowDays * 864e5).toISOString();

  // MEMBER_BACKFILL=1: 旧コードで集計され会員データが無い「スパチャ有り」の行だけ再処理する
  // （列追加後の一度きり。全件は3.5万本で長すぎるため、価値ある行に限定）。
  // このモードでは全アーカイブ/already の取得は不要なので早期に組み立てて抜ける。
  let targets;
  if (process.env.MEMBER_BACKFILL === "1") {
    const need = [];
    for (let from = 0; ; from += 1000) {
      const { data } = await db
        .from("video_superchats")
        .select("video_id, channel_id")
        .gt("total_yen", 0)
        .eq("member_joins", 0)
        .eq("gift_memberships", 0)
        .order("total_yen", { ascending: false })
        .range(from, from + 999);
      for (const r of data ?? []) need.push({ ...r, published_at: null });
      if (!data || data.length < 1000) break;
    }
    targets = need;
    console.log(`member backfill targets: ${targets.length}`);
  } else {
    // 対象の全アーカイブ（窓内）を1000行上限を跨いで全件取得。
    // スパチャは同接の多い配信にほぼ限られるため peak_concurrent降順で優先処理する
    // （窓内は約3.5万本と多く、価値ある配信のカバーを前倒しするため）。
    const rows = [];
    for (let from = 0; from < 40000; from += 1000) {
      const { data, error } = await db
        .from("tracked_videos")
        .select("video_id, channel_id, published_at, peak_concurrent")
        .eq("live_status", "archive")
        .gte("published_at", since)
        .order("peak_concurrent", { ascending: false, nullsFirst: false })
        .order("video_id")
        .range(from, from + 999);
      if (error) throw error;
      rows.push(...(data ?? []));
      if (!data || data.length < 1000) break;
    }
    // 一度集めた配信（emptyの0記録含む）は二度と処理しない。未集計だけを毎日消化して
    // 数日で全配信をカバーし、以後は新規配信のみになる。
    const already = new Set();
    for (let from = 0; ; from += 1000) {
      const { data } = await db
        .from("video_superchats")
        .select("video_id")
        .order("video_id")
        .range(from, from + 999);
      for (const r of data ?? []) already.add(r.video_id);
      if (!data || data.length < 1000) break;
    }
    targets = rows.filter((r) => !already.has(r.video_id));
    console.log(
      `archives: ${rows.length} / already: ${already.size} / todo: ${targets.length}`,
    );
  }

  let written = 0;
  let empties = 0;
  let truncated = 0;
  const t0 = Date.now();
  const budgetMs = 300 * 60_000; // 実行時間の安全上限（次回に持ち越す）
  await mapPool(targets, 8, async (row) => {
    if (Date.now() - t0 > budgetMs) return;
    const result = await processItem(row.video_id);
    if (!result) return; // 一過性失敗。翌日リトライ（行を書かない）
    if (result.truncated) truncated++;
    if (result.empty) empties++;
    // emptyは0で確定記録し、already入りさせて恒久リトライを止める
    const { error: upErr } = await db.from("video_superchats").upsert(
      {
        video_id: row.video_id,
        channel_id: row.channel_id,
        total_yen: result.empty ? 0 : result.total,
        superchat_count: result.empty ? 0 : result.count,
        currency_breakdown: result.empty ? {} : result.breakdown,
        member_joins: result.empty ? 0 : result.memberJoins,
        gift_memberships: result.empty ? 0 : result.giftMemberships,
        harvested_at: new Date().toISOString(),
      },
      { onConflict: "video_id" },
    );
    if (!upErr) written++;
  });
  console.log(
    `wrote ${written} (empty ${empties}, truncated ${truncated}) / ${((Date.now() - t0) / 60000).toFixed(1)}min`,
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
