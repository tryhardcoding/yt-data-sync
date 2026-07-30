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
// チャットのページ送りだけは粘りを強くする。ここで諦めると、それまでに歩いた
// 数百ページが丸ごと無駄になる（実測: 同接13万の配信が740ページ目の503で落ち、
// 181秒とメンバー2,000人規模の観測を捨てていた）。待つコストの方が桁で安い。
const CHAT_RETRIES = Number(process.env.SYNC_CHAT_RETRIES ?? 8);
// 指数バックオフの上限。8回×青天井だと1ページで数分待ちうる。
const BACKOFF_CAP_MS = 20_000;
// 1リクエストの上限。node の fetch は既定でタイムアウトを持たないので、応答が
// 返らない接続を掴んだワーカーは永久に待ち続ける。並列4のうち4本が掴めば実行が
// 丸ごと無音になる（実測 2026-07-29: 定期実行が45分以上、書き込みゼロのまま
// in_progress。同時刻に別枠で回した診断ジョブは retries 0 で正常だった）。
// ハングを一過性エラーに落として、バックオフ再試行に載せる。
const REQUEST_TIMEOUT_MS = Number(process.env.SYNC_REQUEST_TIMEOUT_MS ?? 30_000);
// 429を食らったら全ワーカーを揃って休ませる。個々のリトライだけだと、絞られている
// 最中に4本が別々に叩き続けて回復を遅らせるうえ、予算を空回りで溶かす
// （実測 2026-07-29: 50分で attempted 1581 のうち 1421 が429、書き込みは119で頭打ち）。
const COOLDOWN_MS = Number(process.env.SYNC_COOLDOWN_MS ?? 60_000);
let cooldownUntil = 0;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 失敗の内訳。fetchRetry も mapPool も失敗を握り潰すので、これまでログからは
// 「YouTubeに絞られて落ちたのか、単に予算切れで手が回らなかったのか」を区別できなかった。
// 実行毎に出して、並列や頻度を上げてよいかを実測で決められるようにする。
const stats = {
  attempted: 0, // 実際に着手した配信
  nextFail: 0, // nextエンドポイントが取れない（一過性扱い）
  botWall: 0, // 応答が壊れている＝同意/ボット判定の壁
  cooldowns: 0, // 429で全体を止めた回数
  interrupted: 0, // チャットのページ送り中に落ちた（部分集計なので破棄）
  retries: 0, // バックオフ再試行の回数
  give429: 0, // 再試行を使い切った内訳
  give5xx: 0,
  giveNet: 0,
  giveOther: 0,
  thrown: 0, // 予期しない例外
  salvagedMembers: 0, // 中断した配信から救出したメンバー観測の数
};

// 429/5xx/ネットワーク失敗を指数バックオフ+ジッタで再試行する。恒久エラー（404等）や
// リトライ尽きはそのままresを返し（nullもあり得る）、呼び出し側で一過性扱いを判断する。
async function fetchRetry(url, opts, tries = FETCH_RETRIES) {
  // 誰かが429を踏んでいる間は全員待つ
  const wait = cooldownUntil - Date.now();
  if (wait > 0) await sleep(wait);
  let delay = FETCH_BACKOFF_MS;
  for (let i = 0; i < tries; i++) {
    let res = null;
    try {
      res = await fetch(url, {
        ...opts,
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch {
      // タイムアウト・接続断はどちらもここに来る。status 0 扱いで再試行に載せる
      res = null;
    }
    if (res && res.ok) return res;
    const status = res ? res.status : 0;
    const retryable = status === 429 || status >= 500 || status === 0;
    if (i < tries - 1 && retryable) {
      stats.retries++;
      await sleep(delay + Math.floor(Math.random() * 400));
      delay = Math.min(delay * 2, BACKOFF_CAP_MS);
      continue;
    }
    if (status === 429) {
      stats.give429++;
      if (Date.now() >= cooldownUntil) stats.cooldowns++;
      cooldownUntil = Math.max(cooldownUntil, Date.now() + COOLDOWN_MS);
    }
    else if (status >= 500) stats.give5xx++;
    else if (status === 0) stats.giveNet++;
    else stats.giveOther++;
    return res;
  }
  return null;
}

// nextの応答からチャットリプレイの開始continuationを探す。
function findChatContinuation(obj, depth = 0) {
  if (!obj || typeof obj !== "object" || depth > 8) return null;
  const c = obj.liveChatRenderer?.continuations?.[0]?.reloadContinuationData?.continuation;
  if (c) return c;
  for (const v of Object.values(obj)) {
    if (v && typeof v === "object") {
      const hit = findChatContinuation(v, depth + 1);
      if (hit) return hit;
    }
  }
  return null;
}

async function processItem(itemId, key) {
  // watchページのHTML(1本1MB超)は取らない。InnerTubeのnextで同じ情報が取れる。
  // 実測(2026-07-29): 5本すべてで watch と判定が一致し、応答は約1/3のサイズ。
  // 決定的なのは 429 が watchページにだけ来ていたこと（give429とwatchFailが同数で、
  // youtubei系のチャット取得は1件も失敗していなかった）。
  const res = await fetchRetry(
    `https://www.youtube.com/youtubei/v1/next?key=${key}&prettyPrint=false`,
    {
      method: "POST",
      headers: { "content-type": "application/json", "user-agent": UA },
      body: JSON.stringify({ context: { client: CLIENT }, videoId: itemId }),
    },
  );
  if (!res || !res.ok) {
    stats.nextFail++;
    return null;
  }
  let data;
  try {
    data = await res.json();
  } catch {
    stats.botWall++;
    return null;
  }
  // チャット継続が無い＝チャット無効・メンバー限定・リプレイ未生成。
  // 配信直後の生成ラグで一時的にこうなるので、0の確定は main 側で公開経過を見て判断する。
  const first = findChatContinuation(data);
  if (!first) return { empty: true };
  let continuation = first;

  const breakdown = {};
  let count = 0;
  let memberJoins = 0;
  let giftMemberships = 0;
  // メンバーバッジ付きで発言したユニークaccount → 歴(月, 最大)。配信内で重複排除。
  const members = new Map();
  // ギフトメンバーシップを受け取ったaccount。その月ぶんは贈り主が払っているので、
  // 収益推定では観測メンバーから除外する（membershipYenとgiftYenの二重計上を止める）。
  const giftRecipients = new Set();
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
      CHAT_RETRIES,
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
      // ギフトメンバーシップ購入告知（「ギフトを N 個贈りました」の口数を合算）。
      // 文面はチャンネル名で始まるので、先頭の数字を拾うと名前の数字を口数と誤る
      // （"【876プロ】... ギフトを 1 個贈りました" が876口になっていた）。「個」直前を取る。
      const gift = item.liveChatSponsorshipsGiftPurchaseAnnouncementRenderer;
      if (gift) {
        const text = gift.header?.liveChatSponsorshipsHeaderRenderer?.primaryText?.runs
          ?.map((r) => r.text)
          .join("");
        const n = text?.match(/([\d,]+)\s*(?:個|gift)/i);
        giftMemberships += n ? Number(n[1].replace(/,/g, "")) : 1;
        continue;
      }
      // ギフト受贈告知。この renderer の author が受贈者本人。
      const redeem = item.liveChatSponsorshipsGiftRedemptionAnnouncementRenderer;
      if (redeem?.authorExternalChannelId) {
        giftRecipients.add(redeem.authorExternalChannelId);
      }
    }
    continuation =
      cont.continuations?.[0]?.liveChatReplayContinuationData?.continuation ??
      null;
    await new Promise((r) => setTimeout(r, PAGE_PAUSE_MS));
  }

  // 途中中断（429等）は部分集計。金額・口数は過小になるので確定させず次回に回すが、
  // 観測したメンバーだけは救出する。channel_members は「コメントしたメンバー」の
  // 積み上げ＝実測の下限として持っており、途中まででも下限としては正しい。
  // 長い配信ほど途中で落ちやすく、かつメンバー観測の価値が最も高いので、
  // ここを捨てると一番欲しいデータだけが構造的に落ち続ける。
  if (interrupted) {
    stats.interrupted++;
    stats.salvagedMembers += members.size;
    return { partial: true, members: [...members.entries()], pages };
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
    members: [...members.entries()], // [accountId, 歴(月)][]
    giftRecipients: [...giftRecipients], // accountId[]
    pages,
    truncated: pages >= MAX_PAGES, // 上限到達＝集計が途中で切れている可能性
  };
}

async function mapPool(items, concurrency, fn) {
  let idx = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (idx < items.length) {
      const my = idx++;
      await fn(items[my], my).catch(() => {
        stats.thrown++;
        return null;
      });
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
  } else if (process.env.GIFT_BACKFILL === "1") {
    // GIFT_BACKFILL=1: ギフトのあった窓内の配信だけ再処理する。目的は2つ。
    //  1) 受贈者(redemption)は今回から拾うので、既存行には受贈者が1人も入っていない
    //  2) 旧集計はチャンネル名の数字を口数と誤っていた（"【876プロ】...1個" が876口）
    // published_at を持ったまま渡すので、channel_members の30日窓の意味は保たれる。
    const need = [];
    for (let from = 0; ; from += 1000) {
      const { data } = await db
        .from("video_superchats")
        .select("video_id, channel_id, published_at")
        .gt("gift_memberships", 0)
        .gte("published_at", since)
        .order("gift_memberships", { ascending: false })
        .range(from, from + 999);
      need.push(...(data ?? []));
      if (!data || data.length < 1000) break;
    }
    targets = need;
    console.log(`gift backfill targets: ${targets.length}`);
  } else {
    // 対象の全アーカイブ（窓内）を1000行上限を跨いで全件取得（上限なし）。
    //
    // 公開が新しい順に処理する。以前は peak_concurrent降順だったが、これは
    // 「1本あたりのコストが最も高い配信から順に処理する」という意味になっていた。
    // 実測（2026-07-29）:
    //   同接未観測(null)     2〜22ページ / 1.2〜7.1秒
    //   同接50〜500          2〜8ページ / 1.2〜3.0秒
    //   同接13万             1500ページ（上限）/ 362秒
    // 巨大配信は小規模配信の100倍以上かかるため、300分の予算がそこで溶けて
    // 1日243本しか進まず、新規発生（2,554本/日）に永久に追いつけなかった。
    // 新しい順なら、その日に終わった配信をその日のうちに拾える。帯別の必要量は
    // 合計およそ342ワーカー分/日で、いまの枠（4回×300分×並列4＝4,800）の7%に収まる。
    // 大手を落とすわけではない（新規の大手も同じ日に処理される）。後回しになるのは
    // 過去の未処理分で、これは余った予算で古い方へ順に消化される。
    const rows = [];
    for (let from = 0; ; from += 1000) {
      const { data, error } = await db
        .from("tracked_videos")
        .select("video_id, channel_id, published_at, peak_concurrent, duration_sec")
        .eq("live_status", "archive")
        .gte("published_at", since)
        .order("published_at", { ascending: false })
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
    // 配信終了（公開+尺）から1時間経っていないものは、リプレイが生成されておらず
    // 空振り（deferred）になるだけなので今回は見送る。次の実行（6時間後）で拾う。
    // 実測(2026-07-29): 1時間の実行で attempted 1872 のうち 473 が deferred だった。
    const READY_LAG_MS = 60 * 60_000;
    const now = Date.now();
    let notReady = 0;
    targets = rows.filter((r) => {
      if (already.has(r.video_id)) return false;
      const end = Date.parse(r.published_at ?? "") + (r.duration_sec ?? 0) * 1000;
      // 日時が壊れている行は落とさず対象に入れる（判断できないものを捨てない）
      if (!Number.isFinite(end)) return true;
      if (end + READY_LAG_MS >= now) {
        notReady++;
        return false;
      }
      return true;
    });
    console.log(
      `archives: ${rows.length} / already: ${already.size} / notReady: ${notReady} / todo: ${targets.length}`,
    );
  }

  // リプレイ生成ラグの猶予: 公開からこの時間を過ぎてもチャットが無ければ恒久的とみなす。
  const EMPTY_CONFIRM_MS = 48 * 3600_000;

  // 進捗のハートビート。stats は実行終了時にしか出ないので、詰まったときに
  // 外から様子が分からなかった。5分毎に出して、止まっていることを検知できるようにする。
  // InnerTubeのキーは実行中ずっと使い回せる。1本ごとにwatchページを取っていたのを
  // ここ1回に減らす（絞られていたのはこの経路）。
  const home = await fetchRetry("https://www.youtube.com/", {
    headers: { "user-agent": UA, "accept-language": "ja" },
  });
  const apiKey = home && home.ok
    ? (((await home.text()).match(/"INNERTUBE_API_KEY":"([^"]+)"/) || [])[1] ?? null)
    : null;
  if (!apiKey) {
    console.log("INNERTUBE_API_KEY が取れなかった。次回に持ち越す");
    return;
  }

  let written = 0;
  let empties = 0;
  let truncated = 0;
  let deferred = 0;
  let membersWritten = 0;
  let giftsWritten = 0;
  const t0 = Date.now();
  // 実行時間の安全上限（超えたぶんは次回に持ち越す）。手元で短く回して
  // 動作を確かめられるよう env で上書きできる。
  const budgetMs = Number(process.env.SYNC_BUDGET_MIN ?? 300) * 60_000;
  // 並列数。データセンターIPでは高並列がYouTubeの絞りを誘発し成功率を落とすため、
  // バックオフと併せて控えめ（既定4）にして純増を最大化する。SYNC_CONCURRENCYで調整可。
  const concurrency = Math.max(1, Number(process.env.SYNC_CONCURRENCY ?? 4));
  // 観測メンバーを channel_members へ記録（歴・観測日時はGREATESTで更新）。
  // last_seen_at には配信の公開日時を渡す（30日窓＝直近30日の配信で観測、の意味）。
  // published_atが無い（member-backfill）ときは窓の意味が壊れるのでスキップする。
  const writeMembers = async (row, members) => {
    if (!members || members.length === 0 || !row.published_at) return;
    const rows = members.map(([m, t]) => ({
      c: row.channel_id,
      m,
      t,
      s: row.published_at,
    }));
    const { error } = await db.rpc("record_channel_members", { p_rows: rows });
    if (!error) membersWritten += rows.length;
  };

  // ハートビートはログとDBの両方に出す。GitHubは実行中のログをAPIから返さないので、
  // ログだけだと詰まっている最中に外から様子が分からない（実測 2026-07-29: 書き込みが
  // 90分止まったが、attempted が進んでいるのかどうかを終了まで確認できなかった）。
  const beat = async () => {
    const line = `[${((Date.now() - t0) / 60000).toFixed(0)}min] attempted=${stats.attempted} wrote=${written} deferred=${deferred} members=${membersWritten} interrupted=${stats.interrupted} retries=${stats.retries}`;
    console.log(line);
    await db
      .from("insight_snapshots")
      .upsert(
        {
          id: "chat-sync:heartbeat",
          captured_at: new Date().toISOString(),
          payload: {
            elapsedMin: Number(((Date.now() - t0) / 60000).toFixed(1)),
            written,
            empties,
            deferred,
            membersWritten,
            targets: targets.length,
            ...stats,
          },
        },
        { onConflict: "id" },
      )
      .then(() => {}, () => {});
  };
  const heartbeat = setInterval(beat, 120_000);
  heartbeat.unref?.();

  await mapPool(targets, concurrency, async (row) => {
    if (Date.now() - t0 > budgetMs) return;
    stats.attempted++;
    const result = await processItem(row.video_id, apiKey);
    if (!result) return; // 一過性失敗。次回リトライ（行を書かない）
    if (result.partial) {
      // 途中で落ちた配信。メンバーだけ記録し、video_superchats は書かない
      // ＝ already に入らないので、次回もう一度フルで取りに行く。
      await writeMembers(row, result.members);
      return;
    }
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

    if (!result.empty) await writeMembers(row, result.members);

    // ギフト受贈者を記録する（収益推定で観測メンバーから外すため）。
    // members と同じく published_at を窓の基準に使うので、無いときはスキップ。
    if (!result.empty && result.giftRecipients.length > 0 && row.published_at) {
      const rows = result.giftRecipients.map((m) => ({
        c: row.channel_id,
        m,
        s: row.published_at,
      }));
      const { error: gErr } = await db.rpc("record_gift_recipients", {
        p_rows: rows,
      });
      if (!gErr) giftsWritten += rows.length;
    }
  });
  clearInterval(heartbeat);

  // メンバー観測が増えたら、その場で重なりを作り直す。retention cron（1日1回）任せだと
  // 反映が最大24時間遅れる。実測5秒で終わる集計なので実行ごとに回してよい。
  // （このRPCは pg_safeupdate の DELETE 制約で長らく全件失敗していた。2026-07-30に修正済み）
  if (membersWritten > 0) {
    const { data: overlapRows, error: ovErr } = await db.rpc(
      "refresh_channel_audience_overlap",
      {},
    );
    console.log(
      `overlap refresh: ${ovErr ? `失敗 ${ovErr.message}` : `${overlapRows}行`}`,
    );
  }

  console.log(
    `wrote ${written} (empty ${empties}, truncated ${truncated}, deferred ${deferred}, members ${membersWritten}, gifted ${giftsWritten}) / ${((Date.now() - t0) / 60000).toFixed(1)}min`,
  );
  // 着手したのに書けなかったぶんの内訳。give429が多ければ絞られている＝頻度や並列を
  // 上げてはいけない。0のままなら詰まりは予算側なので増やしてよい、と読む。
  console.log(`stats ${JSON.stringify(stats)}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
