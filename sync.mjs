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

// 発言者バッジからメンバー歴（月）を返す。メンバーでなければ null。
// メンバーバッジは customThumbnail を持つ（MOD/OWNER/VERIFIED は icon.iconType）。
// tooltip 例(hl=ja): "新規メンバー" / "メンバー（2 か月）" / "メンバー（3 年）" / "メンバー（2 年 1 か月）"。
// 「年」は×12して月換算する（月だけ拾うと 3年→3か月 と誤るため。実データで確認済み）。
function memberTenure(badges) {
  if (!Array.isArray(badges)) return null;
  for (const b of badges) {
    const r = b.liveChatAuthorBadgeRenderer;
    if (!r || !r.customThumbnail) continue;
    const tip = r.tooltip || "";
    if (/新規|new member/i.test(tip)) return 0;
    const y = tip.match(/(\d+)\s*(?:年|years?)/);
    const mo = tip.match(/(\d+)\s*(?:か月|ヶ月|months?)/);
    return (y ? Number(y[1]) * 12 : 0) + (mo ? Number(mo[1]) : 0);
  }
  return null;
}

const CLIENT = { clientName: "WEB", clientVersion: "2.20250701.01.00", hl: "ja" };
// 1配信のページ上限。同接の多い配信ほどチャットが長くページ数も多いので、
// 高価値配信を過小集計しないよう長め（~30時間相当）にする。到達時はtruncatedで記録。
const MAX_PAGES = 1500;
const PAGE_PAUSE_MS = 120;
// 429/5xx/ネットワーク失敗のリトライ回数と初期バックオフ。YouTubeはデータセンター
// （GitHub Actions）IPからのチャット取得を強く絞るため、握り潰さず指数バックオフで粘る。
const FETCH_RETRIES = Number(process.env.SYNC_FETCH_RETRIES ?? 4);
const FETCH_BACKOFF_MS = Number(process.env.SYNC_FETCH_BACKOFF_MS ?? 800);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 429/5xx/ネットワーク失敗を指数バックオフ+ジッタで再試行する。恒久エラー（404等）や
// リトライ尽きはそのままresを返し（nullもあり得る）、呼び出し側で一過性扱いを判断する。
async function fetchRetry(url, opts, tries = FETCH_RETRIES) {
  let delay = FETCH_BACKOFF_MS;
  for (let i = 0; i < tries; i++) {
    let res = null;
    try {
      res = await fetch(url, opts);
    } catch {
      res = null;
    }
    if (res && res.ok) return res;
    const status = res ? res.status : 0;
    const retryable = status === 429 || status >= 500 || status === 0;
    if (i < tries - 1 && retryable) {
      await sleep(delay + Math.floor(Math.random() * 400));
      delay *= 2;
      continue;
    }
    return res;
  }
  return null;
}

async function processItem(itemId) {
  const page = await fetchRetry(`https://www.youtube.com/watch?v=${itemId}`, {
    headers: { "user-agent": UA, "accept-language": "ja" },
  });
  // ページ取得の一過性失敗は翌日リトライ（null）。
  if (!page || !page.ok) return null;
  const html = await page.text();
  const key = (html.match(/"INNERTUBE_API_KEY":"([^"]+)"/) || [])[1];
  const first = html.match(
    /"liveChatRenderer":\{"continuations":\[\{"reloadContinuationData":\{"continuation":"([^"]+)"/,
  );
  // INNERTUBE_API_KEY は正常なwatchページには必ず入っている。無い＝同意/ボット判定の
  // 壁ページを掴まされた可能性が高いので、0確定せず一過性扱い（null）で翌日リトライに回す
  // （データセンターIPでこれを0確定すると、実際にはスパチャのある配信を取りこぼす）。
  if (!key) return null;
  // key はあるがチャット継続が無い＝チャット無効・メンバー限定・リプレイ未生成。
  // 配信直後の生成ラグで一時的にこうなるので、0の確定は main 側で公開経過を見て判断する。
  if (!first) return { empty: true };
  let continuation = first[1];

  const breakdown = {};
  let count = 0;
  let memberJoins = 0;
  let giftMemberships = 0;
  // メンバーバッジ付きで発言したユニークaccount → 歴(月, 最大)。配信内で重複排除。
  const members = new Map();
  let pages = 0;
  // ページループが「自然終了（次のcontinuationが無い）」で終わったかを追跡する。
  // 429等で途中中断すると部分集計になるので、その場合はretryで返して確定させない。
  let interrupted = false;
  for (let i = 0; i < MAX_PAGES && continuation; i++) {
    const res = await fetchRetry(
      `https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay?key=${key}&prettyPrint=false`,
      {
        method: "POST",
        headers: { "content-type": "application/json", "user-agent": UA },
        body: JSON.stringify({ context: { client: CLIENT }, continuation }),
      },
    );
    if (!res || !res.ok) {
      interrupted = true; // バックオフ再試行後も失敗。部分集計を確定させない
      break;
    }
    const data = await res.json();
    const cont = data.continuationContents?.liveChatContinuation;
    if (!cont) {
      interrupted = true; // 応答異常。同上
      break;
    }
    pages++;
    for (const a of cont.actions ?? []) {
      const item =
        a.replayChatItemAction?.actions?.[0]?.addChatItemAction?.item;
      if (!item) continue;
      // メンバー観測: 通常/スパチャ/ステッカーの発言者バッジからメンバー歴を拾う。
      // コメントしたメンバーを1人ずつ確認できる（実測の下限。ROMメンバーは拾えない）。
      const msg =
        item.liveChatTextMessageRenderer ||
        item.liveChatPaidMessageRenderer ||
        item.liveChatPaidStickerRenderer;
      if (msg) {
        const aid = msg.authorExternalChannelId;
        const tenure = memberTenure(msg.authorBadges);
        if (aid && tenure !== null) {
          members.set(aid, Math.max(members.get(aid) ?? 0, tenure));
        }
      }
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

  // 途中中断（429等）は部分集計なので確定させず翌日リトライに回す。
  if (interrupted) return null;

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
    members: [...members.entries()], // [accountId, 歴(月)][]
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
    // 対象の全アーカイブ（窓内）を1000行上限を跨いで全件取得（上限なし）。
    // スパチャは同接の多い配信にほぼ限られるため peak_concurrent降順で優先処理する
    // （価値ある配信のカバーを前倒しし、末尾の0円配信は後回しでも埋まる）。
    const rows = [];
    for (let from = 0; ; from += 1000) {
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

  // リプレイ生成ラグの猶予: 公開からこの時間を過ぎてもチャットが無ければ恒久的とみなす。
  const EMPTY_CONFIRM_MS = 48 * 3600_000;

  let written = 0;
  let empties = 0;
  let truncated = 0;
  let deferred = 0;
  let membersWritten = 0;
  const t0 = Date.now();
  const budgetMs = 300 * 60_000; // 実行時間の安全上限（次回に持ち越す）
  // 並列数。データセンターIPでは高並列がYouTubeの絞りを誘発し成功率を落とすため、
  // バックオフと併せて控えめ（既定4）にして純増を最大化する。SYNC_CONCURRENCYで調整可。
  const concurrency = Math.max(1, Number(process.env.SYNC_CONCURRENCY ?? 4));
  await mapPool(targets, concurrency, async (row) => {
    if (Date.now() - t0 > budgetMs) return;
    const result = await processItem(row.video_id);
    if (!result) return; // 一過性失敗・途中中断。翌日リトライ（行を書かない）
    if (result.empty) {
      // 公開が新しい配信はリプレイ未生成の可能性があるので0確定を保留（翌日リトライ）。
      // published_atが不明（backfillターゲット）なら確定してよい。
      const pubMs = row.published_at ? Date.parse(row.published_at) : 0;
      if (pubMs && Date.now() - pubMs < EMPTY_CONFIRM_MS) {
        deferred++;
        return;
      }
      empties++;
    }
    if (result.truncated) truncated++;
    // emptyは0で確定記録し、already入りさせて恒久リトライを止める
    const patch = {
      video_id: row.video_id,
      channel_id: row.channel_id,
      total_yen: result.empty ? 0 : result.total,
      superchat_count: result.empty ? 0 : result.count,
      currency_breakdown: result.empty ? {} : result.breakdown,
      member_joins: result.empty ? 0 : result.memberJoins,
      gift_memberships: result.empty ? 0 : result.giftMemberships,
      harvested_at: new Date().toISOString(),
    };
    // published_atは持っている時だけ書く（member-backfillはnullなので既存値を壊さない）
    if (row.published_at) patch.published_at = row.published_at;
    const { error: upErr } = await db
      .from("video_superchats")
      .upsert(patch, { onConflict: "video_id" });
    if (!upErr) written++;

    // 観測メンバーを channel_members へ記録（歴・観測日時はGREATESTで更新）。
    // last_seen_at には配信の公開日時を渡す（30日窓＝直近30日の配信で観測、の意味）。
    // published_atが無い（member-backfill）ときは窓の意味が壊れるのでスキップする。
    if (!result.empty && result.members.length > 0 && row.published_at) {
      const rows = result.members.map(([m, t]) => ({
        c: row.channel_id,
        m,
        t,
        s: row.published_at,
      }));
      const { error: mErr } = await db.rpc("record_channel_members", {
        p_rows: rows,
      });
      if (!mErr) membersWritten += rows.length;
    }
  });
  console.log(
    `wrote ${written} (empty ${empties}, truncated ${truncated}, deferred ${deferred}, members ${membersWritten}) / ${((Date.now() - t0) / 60000).toFixed(1)}min`,
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
