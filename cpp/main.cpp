#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;

constexpr int64_t MS_PER_SEC = 1000LL, MS_PER_MIN = 60LL * MS_PER_SEC,
                  MS_PER_HOUR = 60LL * MS_PER_MIN, MS_PER_DAY = 24LL * MS_PER_HOUR,
                  SESSION_GAP_MS = 30LL * MS_PER_MIN, SEVEN_DAYS_MS = 7LL * MS_PER_DAY;

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

std::string day_to_date(int32_t day) {
    int64_t z = static_cast<int64_t>(day) + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y = static_cast<int64_t>(yoe) + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned d = doy - (153 * mp + 2) / 5 + 1;
    unsigned m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02u", static_cast<long long>(y), m, d);
    return buf;
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
    std::string_view canonical(std::string_view sv) {
        auto it = id_to_idx.find(sv);
        return (it == id_to_idx.end()) ? sv : std::string_view(min_id[find(it->second)]);
    }
};

// 32 bytes, zero heap allocations per event
struct DedupEvent {
    int64_t ingest_ts;
    int64_t ts;
    uint32_t person_id;
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

struct CampAgg { int64_t sessions = 0, purchasing = 0, rev = 0, assisted = 0; };

std::vector<fs::path> list_csvs(const fs::path& dir) {
    std::vector<fs::path> files;
    if (fs::exists(dir))
        for (const auto& e : fs::directory_iterator(dir))
            if (e.is_regular_file() && e.path().extension() == ".csv") files.push_back(e.path());
    return files;
}

size_t est_rows(const std::vector<fs::path>& files, size_t avg) {
    size_t b = 0;
    std::error_code ec;
    for (const auto& p : files) b += static_cast<size_t>(fs::file_size(p, ec));
    return b / avg + 16;
}

int main(int argc, char** argv) {
    if (argc < 6) return 1;
    fs::path data_dir = argv[1];
    int64_t win_start = 0, win_end = 0;
    parse_ts(argv[2], win_start);
    parse_ts(argv[3], win_end);
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

    // 3. Links -> Union-Find
    UnionFind uf;
    auto link_files = list_csvs(data_dir / "links");
    uf.id_to_idx.reserve(est_rows(link_files, 32));
    for (const auto& path : link_files) {
        std::ifstream in(path);
        in.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        std::getline(in, line, '\n');
        while (std::getline(in, line, '\n'))
            if (parse_csv(line, 2, cols) && !cols[0].empty() && !cols[1].empty()) uf.unite(cols[0], cols[1]);
    }

    // 4. Events: Dedup + Eager Validation into compact 32-byte struct
    StringTable people;
    auto event_files = list_csvs(data_dir / "events");
    SVMap<DedupEvent> dedup;
    dedup.max_load_factor(0.75f);
    dedup.reserve(est_rows(event_files, 90));

    for (const auto& path : event_files) {
        std::ifstream in(path);
        in.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        std::getline(in, line, '\n');
        while (std::getline(in, line, '\n')) {
            if (!parse_csv(line, 7, cols) || cols[0].empty()) continue;
            int64_t ingest_ts = 0;
            if (!parse_ts(cols[3], ingest_ts)) continue;

            auto it = dedup.find(cols[0]);
            if (it != dedup.end() && ingest_ts >= it->second.ingest_ts) continue;

            DedupEvent ev{ingest_ts, 0, 0, 0, 0, 0};
            int64_t ts = 0;
            if (!cols[1].empty() && !cols[5].empty() && parse_ts(cols[2], ts) &&
                ts >= win_start && ts < win_end && (ingest_ts - ts) <= MS_PER_DAY) {
                bool ok = true, is_purch = false;
                int32_t price = 0;
                if (cols[4] == "purchase") { is_purch = true; ok = parse_price(cols[6], price); }
                else if (cols[4] != "view" && cols[4] != "cart") ok = false;

                if (ok) {
                    auto pit = products.find(cols[5]);
                    ev.ts = ts;
                    ev.person_id = people.get(uf.canonical(cols[1]));
                    ev.category_id = (pit != products.end()) ? pit->second : unknown_cat;
                    ev.price_cents = price;
                    ev.flags = 1 | (is_purch ? 2 : 0);
                }
            }
            if (it == dedup.end()) dedup.emplace(std::string(cols[0]), ev);
            else it->second = ev;
        }
    }

    std::vector<std::vector<ValidEvent>> person_events(people.values.size());
    for (const auto& [k, ev] : dedup) {
        if (ev.flags & 1)
            person_events[ev.person_id].push_back({ev.ts, ev.price_cents, ev.category_id, (ev.flags & 2) != 0});
    }
    dedup.clear();
    dedup.rehash(0);

    // 5. Clicks
    auto click_files = list_csvs(data_dir / "clicks");
    std::unordered_set<std::string, SVHash, SVEqual> seen_clicks;
    seen_clicks.max_load_factor(0.75f);
    seen_clicks.reserve(est_rows(click_files, 70));
    std::vector<std::vector<ValidClick>> person_clicks(people.values.size());

    for (const auto& path : click_files) {
        std::ifstream in(path);
        in.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        std::getline(in, line, '\n');
        while (std::getline(in, line, '\n')) {
            if (!parse_csv(line, 4, cols) || cols[0].empty()) continue;
            if (seen_clicks.find(cols[0]) != seen_clicks.end()) continue;
            seen_clicks.emplace(cols[0]);
            if (cols[1].empty()) continue;
            auto cit = camp_ids.ids.find(cols[2]);
            if (cit == camp_ids.ids.end()) continue;
            int64_t ts = 0;
            if (!parse_ts(cols[3], ts) || ts < click_win_start || ts >= win_end) continue;

            auto pit = people.ids.find(uf.canonical(cols[1]));
            if (pit == people.ids.end()) continue; // person has no valid events -> can't have sessions
            person_clicks[pit->second].push_back({ts, std::string(cols[0]), cit->second});
        }
    }
    seen_clicks.clear();
    seen_clicks.rehash(0);

    // 6. Parallel Sessionize, Attribute & Stream user_daily directly to buffers
    unsigned n_workers = std::thread::hardware_concurrency();
    if (const char* env = std::getenv("NCPU")) if (auto v = std::atoi(env); v > 0) n_workers = v;
    n_workers = std::max(1u, std::min<unsigned>(n_workers, std::max<size_t>(1, person_events.size())));

    std::atomic<size_t> next_person{0};
    std::vector<std::string> user_bufs(n_workers);
    std::vector<std::unordered_map<uint64_t, CampAgg>> camp_locals(n_workers);
    const size_t num_cats = categories.values.size();

    auto worker_fn = [&](unsigned tid) {
        auto& ubuf = user_bufs[tid];
        ubuf.reserve(1 << 20);
        auto& cmap = camp_locals[tid];
        std::vector<int64_t> cat_rev(num_cats, 0);
        std::vector<uint32_t> touched_cats;
        std::unordered_map<uint32_t, uint32_t> active;

        while (true) {
            size_t pid = next_person.fetch_add(1, std::memory_order_relaxed);
            if (pid >= person_events.size()) break;
            auto& evs = person_events[pid];
            if (evs.empty()) continue;

            std::sort(evs.begin(), evs.end(), [](const ValidEvent& a, const ValidEvent& b) { return a.ts < b.ts; });
            auto& clicks = person_clicks[pid];
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
                ubuf.push_back('"'); ubuf.append(people.values[pid]); ubuf.append("\",");
                ubuf.append(day_to_date(cur_day)); ubuf.push_back(',');
                ubuf.append(std::to_string(d_sess)); ubuf.push_back(',');
                ubuf.append(std::to_string(d_evs)); ubuf.push_back(',');
                ubuf.append(std::to_string(d_rev)); ubuf.push_back(',');
                if (best_cat != UINT32_MAX) { ubuf.push_back('"'); ubuf.append(categories.values[best_cat]); ubuf.push_back('"'); }
                ubuf.push_back('\n');
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
    };

    std::vector<std::thread> threads;
    for (unsigned t = 0; t < n_workers; ++t) threads.emplace_back(worker_fn, t);
    for (auto& t : threads) t.join();

    // 7. Write user_daily & merged campaign_daily
    {
        std::ofstream out(argv[4], std::ios::binary);
        out.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        out << "user_id,day,sessions,events,revenue_cents,top_category\n";
        for (const auto& b : user_bufs) out.write(b.data(), b.size());
    }

    std::unordered_map<uint64_t, CampAgg> camp_daily;
    for (const auto& m : camp_locals) {
        for (const auto& [k, v] : m) {
            auto& t = camp_daily[k];
            t.sessions += v.sessions; t.purchasing += v.purchasing; t.rev += v.rev; t.assisted += v.assisted;
        }
    }
    {
        std::ofstream out(argv[5], std::ios::binary);
        out.rdbuf()->pubsetbuf(io_buf.data(), io_buf.size());
        out << "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n";
        for (const auto& [k, a] : camp_daily) {
            uint32_t cid = static_cast<uint32_t>(k >> 32);
            int32_t day = static_cast<int32_t>(static_cast<uint32_t>(k));
            bool org = (cid == UINT32_MAX);
            out << '"' << (org ? "organic" : camp_ids.values[cid]) << "\",\""
                << (org ? "organic" : camp_channels[cid]) << "\"," << day_to_date(day) << ','
                << a.sessions << ',' << a.purchasing << ',' << a.rev << ',' << a.assisted << '\n';
        }
    }
    return 0;
}