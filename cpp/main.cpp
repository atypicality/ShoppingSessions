#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

namespace fs = std::filesystem;

constexpr int64_t MS_PER_SEC = 1000LL, MS_PER_MIN = 60LL * MS_PER_SEC,
                  MS_PER_HOUR = 60LL * MS_PER_MIN, MS_PER_DAY = 24LL * MS_PER_HOUR,
                  SESSION_GAP_MS = 30LL * MS_PER_MIN, SEVEN_DAYS_MS = 7LL * MS_PER_DAY;
constexpr size_t N_SHARDS = 64;
constexpr size_t SHARD_MASK = N_SHARDS - 1;
constexpr size_t BATCH_LIMIT = 2048;

inline bool is_digit(char c) { return c >= '0' && c <= '9'; }

struct SVHash {
    using is_transparent = void;
    size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
};
struct SVEqual {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
};
template <typename V>
using SVMap = std::unordered_map<std::string, V, SVHash, SVEqual>;

bool parse_csv(std::string_view line, size_t n, std::vector<std::string_view>& out) {
    if (line.empty()) return false;
    out.clear();
    size_t start = 0;
    while (true) {
        size_t comma = line.find(',', start);
        std::string_view f = (comma == std::string_view::npos) ? line.substr(start) : line.substr(start, comma - start);
        if (f.size() >= 2 && f.front() == '"' && f.back() == '"') f = f.substr(1, f.size() - 2);
        if (f.find('"') != std::string_view::npos) return false;
        out.push_back(f);
        if (comma == std::string_view::npos) break;
        start = comma + 1;
    }
    return out.size() == n;
}

int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = static_cast<unsigned>(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

bool parse_2d(std::string_view s, size_t p, int& out) {
    if (!is_digit(s[p]) || !is_digit(s[p + 1])) return false;
    out = (s[p] - '0') * 10 + (s[p + 1] - '0');
    return true;
}

bool parse_ts(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    bool all_d = true;
    for (char c : s) if (!is_digit(c)) { all_d = false; break; }
    if (all_d) {
        size_t i = 0;
        while (i < s.size() && s[i] == '0') ++i;
        if (i == s.size()) { out = 0; return true; }
        if (s.size() - i > 18) return false;
        int64_t v = 0;
        for (; i < s.size(); ++i) v = v * 10 + (s[i] - '0');
        out = v;
        return true;
    }
    if (s.size() != 20 && s.size() != 25) return false;
    if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;
    int y1, y2, mo, d, h, mi, sec;
    if (!parse_2d(s, 0, y1) || !parse_2d(s, 2, y2) || !parse_2d(s, 5, mo) || !parse_2d(s, 8, d) ||
        !parse_2d(s, 11, h) || !parse_2d(s, 14, mi) || !parse_2d(s, 17, sec)) return false;
    int y = y1 * 100 + y2;
    static const int mdays[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int dim = (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) ? 29 : (mo >= 1 && mo <= 12 ? mdays[mo] : 0);
    if (mo < 1 || mo > 12 || d < 1 || d > dim || h > 23 || mi > 59 || sec > 59) return false;

    int64_t off = 0;
    if (s.size() == 20) {
        if (s[19] != 'Z') return false;
    } else {
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return false;
        int oh, om;
        if (!parse_2d(s, 20, oh) || !parse_2d(s, 23, om) || oh > 23 || om > 59) return false;
        off = (oh * MS_PER_HOUR + om * MS_PER_MIN) * (s[19] == '+' ? 1 : -1);
    }
    out = days_from_civil(y, mo, d) * MS_PER_DAY + h * MS_PER_HOUR + mi * MS_PER_MIN + sec * MS_PER_SEC - off;
    return true;
}

bool parse_price(std::string_view s, int32_t& out) {
    if (s.empty()) return false;
    for (char c : s) if (!is_digit(c)) return false;
    size_t i = 0;
    while (i < s.size() && s[i] == '0') ++i;
    if (i == s.size() || s.size() - i > 8) return false;
    int64_t v = 0;
    for (; i < s.size(); ++i) v = v * 10 + (s[i] - '0');
    if (v < 1 || v > 10000000LL) return false;
    out = static_cast<int32_t>(v);
    return true;
}

std::string extract_json_str(const std::string& json, std::string_view key) {
    size_t kpos = json.find(key);
    if (kpos == std::string::npos) return "";
    size_t colon = json.find(':', kpos + key.size());
    size_t q1 = json.find('"', colon + 1);
    size_t q2 = json.find('"', q1 + 1);
    return json.substr(q1 + 1, q2 - q1 - 1);
}

struct StringTable {
    SVMap<uint32_t> ids;
    std::vector<std::string> values;
    uint32_t get(std::string_view sv) {
        auto it = ids.find(sv);
        if (it != ids.end()) return it->second;
        uint32_t id = static_cast<uint32_t>(values.size());
        values.emplace_back(sv);
        ids.emplace(values.back(), id);
        return id;
    }
};

struct UnionFind {
    SVMap<uint32_t> id_to_idx;
    std::vector<uint32_t> parent;
    std::vector<std::string> min_id;

    uint32_t get_or_add(std::string_view sv) {
        auto it = id_to_idx.find(sv);
        if (it != id_to_idx.end()) return it->second;
        uint32_t idx = static_cast<uint32_t>(parent.size());
        parent.push_back(idx);
        min_id.emplace_back(sv);
        id_to_idx.emplace(min_id.back(), idx);
        return idx;
    }
    uint32_t find(uint32_t x) {
        uint32_t r = x;
        while (parent[r] != r) r = parent[r];
        while (parent[x] != r) { uint32_t n = parent[x]; parent[x] = r; x = n; }
        return r;
    }
    void unite(std::string_view a, std::string_view b) {
        uint32_t ra = find(get_or_add(a)), rb = find(get_or_add(b));
        if (ra == rb) return;
        parent[rb] = ra;
        if (min_id[rb] < min_id[ra]) min_id[ra] = min_id[rb];
    }
    void freeze() {
        for (uint32_t i = 0; i < parent.size(); ++i) parent[i] = find(i);
    }
    std::string_view canonical(std::string_view sv) const {
        auto it = id_to_idx.find(sv);
        return (it == id_to_idx.end()) ? sv : std::string_view(min_id[parent[it->second]]);
    }
};

struct DedupEvent {
    int64_t ingest_ts;
    int64_t ts;
    std::string person; // fits in SSO for typical IDs
    uint32_t category_id;
    int32_t price_cents;
    uint8_t flags; // bit 0: valid, bit 1: is_purchase
};

struct ValidEvent {
    int64_t ts;
    int32_t price_cents;
    uint32_t category_id;
    bool is_purchase;
};

struct ValidClick {
    int64_t ts;
    std::string click_id;
    uint32_t campaign_id;
};

struct PersonShard {
    std::mutex mu;
    StringTable people;
    std::vector<std::vector<ValidEvent>> events;
    std::vector<std::vector<ValidClick>> clicks;
};

struct UserRow {
    uint32_t shard;
    uint32_t pid;
    int32_t day;
    int64_t sessions;
    int64_t events;
    int64_t revenue_cents;
    uint32_t top_cat; // UINT32_MAX => null
};

struct CampAgg { int64_t sessions = 0, purchasing = 0, rev = 0, assisted = 0; };

std::vector<fs::path> list_csvs_sorted(const fs::path& dir) {
    std::vector<std::pair<uint64_t, fs::path>> sized;
    std::error_code ec;
    if (fs::exists(dir)) {
        for (const auto& e : fs::directory_iterator(dir)) {
            if (e.is_regular_file() && e.path().extension() == ".csv")
                sized.emplace_back(fs::file_size(e.path(), ec), e.path());
        }
    }
    std::sort(sized.begin(), sized.end(), [](const auto& a, const auto& b) { return a.first > b.first; });
    std::vector<fs::path> out;
    out.reserve(sized.size());
    for (auto& [sz, p] : sized) out.push_back(std::move(p));
    return out;
}

void write_parquet_table(const fs::path& path, const std::shared_ptr<arrow::Table>& table) {
    auto out_res = arrow::io::FileOutputStream::Open(path.string());
    if (!out_res.ok()) { std::cerr << out_res.status().ToString() << "\n"; std::exit(1); }
    auto st = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *out_res, 1 << 20);
    if (!st.ok()) { std::cerr << st.ToString() << "\n"; std::exit(1); }
}

int main(int argc, char** argv) {
    fs::path data_dir = (argc >= 3) ? fs::path(argv[1]) : fs::path(std::getenv("DATA_DIR") ? std::getenv("DATA_DIR") : "/data");
    fs::path output_dir = (argc >= 3) ? fs::path(argv[2]) : fs::path(std::getenv("OUTPUT_DIR") ? std::getenv("OUTPUT_DIR") : "/output");

    unsigned n_workers = std::thread::hardware_concurrency();
    if (const char* env = std::getenv("NCPU")) if (auto v = std::atoi(env); v > 0) n_workers = v;
    n_workers = std::max(1u, n_workers);

    // 0. Read manifest.json
    int64_t win_start = 0, win_end = 0;
    {
        std::ifstream mf(data_dir / "manifest.json");
        std::string content((std::istreambuf_iterator<char>(mf)), std::istreambuf_iterator<char>());
        parse_ts(extract_json_str(content, "\"window_start\""), win_start);
        parse_ts(extract_json_str(content, "\"window_end\""), win_end);
    }
    int64_t click_win_start = win_start - SEVEN_DAYS_MS;

    std::vector<char> io_buf(1 << 20);
    std::vector<std::string_view> cols;
    std::string line;

    // 1. Products & Categories
    StringTable categories;
    uint32_t unknown_cat = categories.get("unknown");
    SVMap<uint32_t> products;
    {
        std::ifstream in(data_dir / "products.csv");
        in.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        std::getline(in, line, '\n');
        while (std::getline(in, line, '\n'))
            if (parse_csv(line, 2, cols)) products.emplace(std::string(cols[0]), categories.get(cols[1]));
    }

    // 2. Campaigns
    StringTable camp_ids;
    std::vector<std::string> camp_channels;
    {
        std::ifstream in(data_dir / "campaigns.csv");
        in.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        std::getline(in, line, '\n');
        while (std::getline(in, line, '\n')) {
            if (parse_csv(line, 2, cols)) {
                uint32_t id = camp_ids.get(cols[0]);
                if (id >= camp_channels.size()) camp_channels.resize(id + 1);
                camp_channels[id] = std::string(cols[1]);
            }
        }
    }

    // 3. Links -> Union-Find (frozen for lock-free reads)
    UnionFind uf;
    for (const auto& path : list_csvs_sorted(data_dir / "links")) {
        std::ifstream in(path);
        in.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        std::getline(in, line, '\n');
        while (std::getline(in, line, '\n'))
            if (parse_csv(line, 2, cols) && !cols[0].empty() && !cols[1].empty()) uf.unite(cols[0], cols[1]);
    }
    uf.freeze();

    // 4. Parallel Events Parse + Sharded Dedup
    auto event_files = list_csvs_sorted(data_dir / "events");
    std::vector<std::mutex> ev_mu(N_SHARDS);
    std::vector<SVMap<DedupEvent>> ev_shards(N_SHARDS);
    for (auto& m : ev_shards) m.max_load_factor(0.75f);

    {
        std::atomic<size_t> next_file{0};
        auto worker = [&]() {
            std::vector<char> buf(1 << 20);
            std::vector<std::string_view> c;
            std::string ln;
            std::vector<std::vector<std::pair<std::string, DedupEvent>>> batches(N_SHARDS);

            auto flush_shard = [&](size_t s) {
                if (batches[s].empty()) return;
                std::lock_guard<std::mutex> lk(ev_mu[s]);
                auto& map = ev_shards[s];
                for (auto& [eid, ev] : batches[s]) {
                    auto it = map.find(eid);
                    if (it == map.end()) map.emplace(std::move(eid), std::move(ev));
                    else if (ev.ingest_ts < it->second.ingest_ts) it->second = std::move(ev);
                }
                batches[s].clear();
            };

            while (true) {
                size_t idx = next_file.fetch_add(1, std::memory_order_relaxed);
                if (idx >= event_files.size()) break;
                std::ifstream in(event_files[idx]);
                in.rdbuf()->pubsetbuf(buf.data(), buf.size());
                std::getline(in, ln, '\n');
                while (std::getline(in, ln, '\n')) {
                    if (!parse_csv(ln, 7, c) || c[0].empty()) continue;
                    int64_t ingest_ts = 0;
                    if (!parse_ts(c[3], ingest_ts)) continue;

                    DedupEvent ev{ingest_ts, 0, "", 0, 0, 0};
                    int64_t ts = 0;
                    if (!c[1].empty() && !c[5].empty() && parse_ts(c[2], ts) &&
                        ts >= win_start && ts < win_end && (ingest_ts - ts) <= MS_PER_DAY) {
                        bool ok = true, is_purch = false;
                        int32_t price = 0;
                        if (c[4] == "purchase") { is_purch = true; ok = parse_price(c[6], price); }
                        else if (c[4] != "view" && c[4] != "cart") ok = false;

                        if (ok) {
                            auto pit = products.find(c[5]);
                            ev.ts = ts;
                            ev.person = std::string(uf.canonical(c[1]));
                            ev.category_id = (pit != products.end()) ? pit->second : unknown_cat;
                            ev.price_cents = price;
                            ev.flags = 1 | (is_purch ? 2 : 0);
                        }
                    }
                    size_t s = SVHash{}(c[0]) & SHARD_MASK;
                    batches[s].emplace_back(std::string(c[0]), std::move(ev));
                    if (batches[s].size() >= BATCH_LIMIT) flush_shard(s);
                }
            }
            for (size_t s = 0; s < N_SHARDS; ++s) flush_shard(s);
        };

        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker);
        for (auto& t : ts) t.join();
    }

    // 4b. Route surviving events into 64 PersonShards in parallel
    std::vector<PersonShard> pshards(N_SHARDS);
    {
        std::atomic<size_t> next_s{0};
        auto worker = [&]() {
            std::vector<std::vector<std::pair<std::string, ValidEvent>>> batches(N_SHARDS);
            auto flush = [&](size_t ps) {
                if (batches[ps].empty()) return;
                std::lock_guard<std::mutex> lk(pshards[ps].mu);
                auto& sh = pshards[ps];
                for (auto& [person, ve] : batches[ps]) {
                    uint32_t pid = sh.people.get(person);
                    if (pid >= sh.events.size()) sh.events.resize(pid + 1);
                    sh.events[pid].push_back(ve);
                }
                batches[ps].clear();
            };

            while (true) {
                size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
                if (s >= N_SHARDS) break;
                for (auto& [eid, ev] : ev_shards[s]) {
                    if (!(ev.flags & 1)) continue;
                    size_t ps = SVHash{}(ev.person) & SHARD_MASK;
                    batches[ps].emplace_back(std::move(ev.person),
                                             ValidEvent{ev.ts, ev.price_cents, ev.category_id, (ev.flags & 2) != 0});
                    if (batches[ps].size() >= BATCH_LIMIT) flush(ps);
                }
                ev_shards[s].clear();
                ev_shards[s].rehash(0);
            }
            for (size_t ps = 0; ps < N_SHARDS; ++ps) flush(ps);
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker);
        for (auto& t : ts) t.join();
    }
    for (auto& sh : pshards) sh.clicks.resize(sh.events.size());

    // 5. Parallel Clicks Parse + Sharded Dedup -> Route to PersonShards
    auto click_files = list_csvs_sorted(data_dir / "clicks");
    std::vector<std::mutex> ck_mu(N_SHARDS);
    std::vector<std::unordered_set<std::string, SVHash, SVEqual>> ck_seen(N_SHARDS);
    for (auto& s : ck_seen) s.max_load_factor(0.75f);

    {
        std::atomic<size_t> next_file{0};
        auto worker = [&]() {
            std::vector<char> buf(1 << 20);
            std::vector<std::string_view> c;
            std::string ln;
            struct RawCk { std::string cid; std::string person; int64_t ts; uint32_t camp; };
            std::vector<std::vector<RawCk>> batches(N_SHARDS);

            auto flush_ck = [&](size_t s) {
                if (batches[s].empty()) return;
                std::vector<RawCk> unique_cks;
                {
                    std::lock_guard<std::mutex> lk(ck_mu[s]);
                    auto& seen = ck_seen[s];
                    for (auto& r : batches[s]) {
                        if (seen.find(r.cid) == seen.end()) {
                            seen.insert(r.cid);
                            unique_cks.push_back(std::move(r));
                        }
                    }
                }
                batches[s].clear();
                for (auto& r : unique_cks) {
                    size_t ps = SVHash{}(r.person) & SHARD_MASK;
                    auto& sh = pshards[ps];
                    auto pit = sh.people.ids.find(r.person);
                    if (pit == sh.people.ids.end()) continue; // no valid events for this person
                    std::lock_guard<std::mutex> lk(sh.mu);
                    sh.clicks[pit->second].push_back({r.ts, std::move(r.cid), r.camp});
                }
            };

            while (true) {
                size_t idx = next_file.fetch_add(1, std::memory_order_relaxed);
                if (idx >= click_files.size()) break;
                std::ifstream in(click_files[idx]);
                in.rdbuf()->pubsetbuf(buf.data(), buf.size());
                std::getline(in, ln, '\n');
                while (std::getline(in, ln, '\n')) {
                    if (!parse_csv(ln, 4, c) || c[0].empty() || c[1].empty()) continue;
                    auto cit = camp_ids.ids.find(c[2]);
                    if (cit == camp_ids.ids.end()) continue;
                    int64_t ts = 0;
                    if (!parse_ts(c[3], ts) || ts < click_win_start || ts >= win_end) continue;
                    size_t s = SVHash{}(c[0]) & SHARD_MASK;
                    batches[s].push_back({std::string(c[0]), std::string(uf.canonical(c[1])), ts, cit->second});
                    if (batches[s].size() >= BATCH_LIMIT) flush_ck(s);
                }
            }
            for (size_t s = 0; s < N_SHARDS; ++s) flush_ck(s);
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker);
        for (auto& t : ts) t.join();
    }
    ck_seen.clear();

    // 6. Parallel Sessionize & Attribute across PersonShards
    std::atomic<size_t> next_ps{0};
    std::vector<std::vector<UserRow>> user_locals(n_workers);
    std::vector<std::unordered_map<uint64_t, CampAgg>> camp_locals(n_workers);
    const size_t num_cats = categories.values.size();

    auto agg_worker = [&](unsigned tid) {
        auto& urows = user_locals[tid];
        auto& cmap = camp_locals[tid];
        std::vector<int64_t> cat_rev(num_cats, 0);
        std::vector<uint32_t> touched_cats;
        std::unordered_map<uint32_t, uint32_t> active;

        while (true) {
            size_t ps = next_ps.fetch_add(1, std::memory_order_relaxed);
            if (ps >= N_SHARDS) break;
            auto& sh = pshards[ps];

            for (uint32_t pid = 0; pid < sh.events.size(); ++pid) {
                auto& evs = sh.events[pid];
                if (evs.empty()) continue;

                std::sort(evs.begin(), evs.end(), [](const ValidEvent& a, const ValidEvent& b) { return a.ts < b.ts; });
                auto& clicks = sh.clicks[pid];
                std::sort(clicks.begin(), clicks.end(), [](const ValidClick& a, const ValidClick& b) {
                    return a.ts != b.ts ? a.ts < b.ts : a.click_id < b.click_id;
                });

                active.clear();
                size_t L = 0, R = 0, i = 0;
                int32_t cur_day = INT32_MIN;
                int64_t d_sess = 0, d_evs = 0, d_rev = 0;

                auto flush_day = [&]() {
                    if (d_sess == 0) return;
                    uint32_t best_cat = UINT32_MAX;
                    int64_t best_rev = -1;
                    for (uint32_t c : touched_cats) {
                        int64_t r = cat_rev[c];
                        if (r > best_rev || (r == best_rev && categories.values[c] < categories.values[best_cat])) {
                            best_rev = r; best_cat = c;
                        }
                        cat_rev[c] = 0;
                    }
                    touched_cats.clear();
                    urows.push_back({static_cast<uint32_t>(ps), pid, cur_day, d_sess, d_evs, d_rev, best_cat});
                    d_sess = d_evs = d_rev = 0;
                };

                while (i < evs.size()) {
                    int64_t s_start = evs[i].ts;
                    int32_t day = static_cast<int32_t>(s_start >= 0 ? s_start / MS_PER_DAY : (s_start - MS_PER_DAY + 1) / MS_PER_DAY);
                    if (day != cur_day) { flush_day(); cur_day = day; }

                    d_sess += 1;
                    int64_t s_rev = 0, prev_ts = s_start;
                    bool has_purch = false;
                    while (i < evs.size() && evs[i].ts - prev_ts <= SESSION_GAP_MS) {
                        prev_ts = evs[i].ts;
                        d_evs += 1;
                        if (evs[i].is_purchase) {
                            has_purch = true;
                            s_rev += evs[i].price_cents;
                            d_rev += evs[i].price_cents;
                            uint32_t c = evs[i].category_id;
                            if (cat_rev[c] == 0) touched_cats.push_back(c);
                            cat_rev[c] += evs[i].price_cents;
                        }
                        ++i;
                    }

                    while (R < clicks.size() && clicks[R].ts <= s_start) { ++active[clicks[R].campaign_id]; ++R; }
                    while (L < R && clicks[L].ts < s_start - SEVEN_DAYS_MS) {
                        if (--active[clicks[L].campaign_id] == 0) active.erase(clicks[L].campaign_id);
                        ++L;
                    }

                    auto pack = [&](uint32_t cid) { return (static_cast<uint64_t>(cid) << 32) | static_cast<uint32_t>(day); };
                    if (L == R) {
                        auto& ca = cmap[pack(UINT32_MAX)];
                        ca.sessions += 1; ca.purchasing += has_purch; ca.rev += s_rev;
                    } else {
                        size_t best = R - 1;
                        while (best > L && clicks[best - 1].ts == clicks[R - 1].ts) --best;
                        auto& ca = cmap[pack(clicks[best].campaign_id)];
                        ca.sessions += 1; ca.purchasing += has_purch; ca.rev += s_rev;
                        for (const auto& [cid, cnt] : active) cmap[pack(cid)].assisted += 1;
                    }
                }
                flush_day();
            }
        }
    };

    {
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(agg_worker, t);
        for (auto& t : ts) t.join();
    }

    // 7. Write Parquet Directly (No CSV / DuckDB)
    fs::create_directories(output_dir / "user_daily");
    fs::create_directories(output_dir / "campaign_daily");

    {
        arrow::StringBuilder b_uid, b_cat;
        arrow::Date32Builder b_day;
        arrow::Int64Builder b_sess, b_evs, b_rev;

        for (const auto& vec : user_locals) {
            for (const auto& r : vec) {
                (void)b_uid.Append(pshards[r.shard].people.values[r.pid]);
                (void)b_day.Append(r.day);
                (void)b_sess.Append(r.sessions);
                (void)b_evs.Append(r.events);
                (void)b_rev.Append(r.revenue_cents);
                if (r.top_cat == UINT32_MAX) (void)b_cat.AppendNull();
                else (void)b_cat.Append(categories.values[r.top_cat]);
            }
        }

        std::shared_ptr<arrow::Array> a_uid, a_day, a_sess, a_evs, a_rev, a_cat;
        (void)b_uid.Finish(&a_uid); (void)b_day.Finish(&a_day); (void)b_sess.Finish(&a_sess);
        (void)b_evs.Finish(&a_evs); (void)b_rev.Finish(&a_rev); (void)b_cat.Finish(&a_cat);

        auto schema = arrow::schema({
            arrow::field("user_id", arrow::utf8()),
            arrow::field("day", arrow::date32()),
            arrow::field("sessions", arrow::int64()),
            arrow::field("events", arrow::int64()),
            arrow::field("revenue_cents", arrow::int64()),
            arrow::field("top_category", arrow::utf8(), true)
        });
        auto table = arrow::Table::Make(schema, {a_uid, a_day, a_sess, a_evs, a_rev, a_cat});
        write_parquet_table(output_dir / "user_daily" / "part-0.parquet", table);
    }

    {
        std::unordered_map<uint64_t, CampAgg> camp_daily;
        for (const auto& m : camp_locals) {
            for (const auto& [k, v] : m) {
                auto& t = camp_daily[k];
                t.sessions += v.sessions; t.purchasing += v.purchasing; t.rev += v.rev; t.assisted += v.assisted;
            }
        }

        arrow::StringBuilder b_cid, b_ch;
        arrow::Date32Builder b_day;
        arrow::Int64Builder b_sess, b_purch, b_rev, b_ast;

        for (const auto& [k, a] : camp_daily) {
            uint32_t cid = static_cast<uint32_t>(k >> 32);
            int32_t day = static_cast<int32_t>(static_cast<uint32_t>(k));
            bool org = (cid == UINT32_MAX);
            (void)b_cid.Append(org ? "organic" : camp_ids.values[cid]);
            (void)b_ch.Append(org ? "organic" : camp_channels[cid]);
            (void)b_day.Append(day);
            (void)b_sess.Append(a.sessions);
            (void)b_purch.Append(a.purchasing);
            (void)b_rev.Append(a.rev);
            (void)b_ast.Append(a.assisted);
        }

        std::shared_ptr<arrow::Array> a_cid, a_ch, a_day, a_sess, a_purch, a_rev, a_ast;
        (void)b_cid.Finish(&a_cid); (void)b_ch.Finish(&a_ch); (void)b_day.Finish(&a_day);
        (void)b_sess.Finish(&a_sess); (void)b_purch.Finish(&a_purch); (void)b_rev.Finish(&a_rev); (void)b_ast.Finish(&a_ast);

        auto schema = arrow::schema({
            arrow::field("campaign_id", arrow::utf8()),
            arrow::field("channel", arrow::utf8()),
            arrow::field("day", arrow::date32()),
            arrow::field("sessions", arrow::int64()),
            arrow::field("purchasing_sessions", arrow::int64()),
            arrow::field("revenue_cents", arrow::int64()),
            arrow::field("assisted_sessions", arrow::int64())
        });
        auto table = arrow::Table::Make(schema, {a_cid, a_ch, a_day, a_sess, a_purch, a_rev, a_ast});
        write_parquet_table(output_dir / "campaign_daily" / "part-0.parquet", table);
    }

    return 0;
}