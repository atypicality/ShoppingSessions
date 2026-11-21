#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;

constexpr int64_t MS_PER_SEC  = 1000LL;
constexpr int64_t MS_PER_MIN  = 60LL * MS_PER_SEC;
constexpr int64_t MS_PER_HOUR = 60LL * MS_PER_MIN;
constexpr int64_t MS_PER_DAY  = 24LL * MS_PER_HOUR;
constexpr int64_t SESSION_GAP_MS = 30LL * MS_PER_MIN;
constexpr int64_t SEVEN_DAYS_MS  = 7LL * MS_PER_DAY;

// -------------------- Strict CSV Parser --------------------

inline bool is_ascii_digit(char c) { return c >= '0' && c <= '9'; }

// Parses a single line (without trailing '\n') into `expected_cols` fields.
// Returns false if malformed.
bool parse_csv_line(std::string_view line, size_t expected_cols, std::vector<std::string_view>& out) {
    if (line.empty()) return false;
    out.clear();
    size_t start = 0;
    while (true) {
        size_t comma = line.find(',', start);
        std::string_view field = (comma == std::string_view::npos)
            ? line.substr(start)
            : line.substr(start, comma - start);

        // Quote rules: only allowed wrapping the entire field
        if (field.size() >= 2 && field.front() == '"' && field.back() == '"') {
            field = field.substr(1, field.size() - 2);
            if (field.find('"') != std::string_view::npos) return false;
        } else {
            if (field.find('"') != std::string_view::npos) return false;
        }

        out.push_back(field);
        if (comma == std::string_view::npos) break;
        start = comma + 1;
    }
    return out.size() == expected_cols;
}

// -------------------- Exact Date & Timestamp Parsing --------------------

bool is_leap(int y) {
    return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
}

int days_in_month(int y, int m) {
    static const int d[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (m == 2 && is_leap(y)) return 29;
    return d[m];
}

// Howard Hinnant's days_from_civil: days since 1970-01-01
int64_t days_from_civil(int64_t y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

// Converts UTC epoch milliseconds to "YYYY-MM-DD"
std::string epoch_ms_to_date(int64_t ms) {
    int64_t z = (ms >= 0 ? ms / MS_PER_DAY : (ms - MS_PER_DAY + 1) / MS_PER_DAY) + 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y = static_cast<int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    const unsigned d = doy - (153 * mp + 2) / 5 + 1;
    const unsigned m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);

    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04lld-%02u-%02u", static_cast<long long>(y), m, d);
    return std::string(buf);
}

bool parse_2digits(std::string_view s, size_t pos, int& out) {
    if (!is_ascii_digit(s[pos]) || !is_ascii_digit(s[pos + 1])) return false;
    out = (s[pos] - '0') * 10 + (s[pos + 1] - '0');
    return true;
}

bool parse_4digits(std::string_view s, size_t pos, int& out) {
    int a, b;
    if (!parse_2digits(s, pos, a) || !parse_2digits(s, pos + 2, b)) return false;
    out = a * 100 + b;
    return true;
}

// Parses F1, F2, or F3 timestamp into UTC epoch ms.
// Returns false if malformed or overflows int64 (which is outside any valid window).
bool parse_timestamp(std::string_view s, int64_t& out_ms) {
    if (s.empty()) return false;

    // Check F3: ASCII digits only
    bool all_digits = true;
    for (char c : s) {
        if (!is_ascii_digit(c)) { all_digits = false; break; }
    }
    if (all_digits) {
        size_t i = 0;
        while (i < s.size() && s[i] == '0') ++i;
        if (i == s.size()) { out_ms = 0; return true; }
        if (s.size() - i > 18) return false; // safely outside any 2025-2028 window
        int64_t val = 0;
        for (; i < s.size(); ++i) val = val * 10 + (s[i] - '0');
        out_ms = val;
        return true;
    }

    // F1 (len 20): YYYY-MM-DDTHH:MM:SSZ
    // F2 (len 25): YYYY-MM-DDTHH:MM:SS+HH:MM
    if (s.size() != 20 && s.size() != 25) return false;
    if (s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;

    int y, mo, d, h, mi, sec;
    if (!parse_4digits(s, 0, y) || !parse_2digits(s, 5, mo) || !parse_2digits(s, 8, d) ||
        !parse_2digits(s, 11, h) || !parse_2digits(s, 14, mi) || !parse_2digits(s, 17, sec)) {
        return false;
    }
    if (mo < 1 || mo > 12 || d < 1 || d > days_in_month(y, mo)) return false;
    if (h < 0 || h > 23 || mi < 0 || mi > 59 || sec < 0 || sec > 59) return false;

    int64_t offset_ms = 0;
    if (s.size() == 20) {
        if (s[19] != 'Z') return false;
    } else {
        char sign = s[19];
        if ((sign != '+' && sign != '-') || s[22] != ':') return false;
        int oh, om;
        if (!parse_2digits(s, 20, oh) || !parse_2digits(s, 23, om)) return false;
        if (oh < 0 || oh > 23 || om < 0 || om > 59) return false;
        offset_ms = (oh * MS_PER_HOUR + om * MS_PER_MIN) * (sign == '+' ? 1 : -1);
    }

    int64_t days = days_from_civil(y, mo, d);
    out_ms = days * MS_PER_DAY + h * MS_PER_HOUR + mi * MS_PER_MIN + sec * MS_PER_SEC - offset_ms;
    return true;
}

bool parse_price(std::string_view s, int64_t& out_price) {
    if (s.empty()) return false;
    for (char c : s) if (!is_ascii_digit(c)) return false;
    size_t i = 0;
    while (i < s.size() && s[i] == '0') ++i;
    if (i == s.size() || s.size() - i > 8) return false;
    int64_t val = 0;
    for (; i < s.size(); ++i) val = val * 10 + (s[i] - '0');
    if (val < 1 || val > 10000000LL) return false;
    out_price = val;
    return true;
}

// -------------------- Union-Find for Linked IDs --------------------

struct UnionFind {
    std::unordered_map<std::string, uint32_t> id_to_idx;
    std::vector<uint32_t> parent;
    std::vector<std::string> min_id;

    uint32_t get_or_add(const std::string& id) {
        auto it = id_to_idx.find(id);
        if (it != id_to_idx.end()) return it->second;
        uint32_t idx = static_cast<uint32_t>(parent.size());
        id_to_idx.emplace(id, idx);
        parent.push_back(idx);
        min_id.push_back(id);
        return idx;
    }

    uint32_t find(uint32_t x) {
        uint32_t root = x;
        while (parent[root] != root) root = parent[root];
        while (parent[x] != root) {
            uint32_t next = parent[x];
            parent[x] = root;
            x = next;
        }
        return root;
    }

    void unite(const std::string& a, const std::string& b) {
        uint32_t ra = find(get_or_add(a));
        uint32_t rb = find(get_or_add(b));
        if (ra == rb) return;
        parent[rb] = ra;
        if (min_id[rb] < min_id[ra]) min_id[ra] = min_id[rb];
    }

    std::string canonical(const std::string& id) {
        auto it = id_to_idx.find(id);
        if (it == id_to_idx.end()) return id;
        return min_id[find(it->second)];
    }
};

// -------------------- Data Structures --------------------

struct RawEvent {
    std::string user_id;
    std::string ts_str;
    int64_t ingest_ts;
    std::string event_type;
    std::string product_id;
    std::string price_str;
};

struct ValidEvent {
    int64_t ts;
    int64_t price_cents; // 0 for view/cart
    bool is_purchase;
    std::string category;
};

struct ValidClick {
    int64_t ts;
    std::string click_id;
    std::string campaign_id;
};

struct UserDailyAgg {
    int64_t sessions = 0;
    int64_t events = 0;
    int64_t revenue_cents = 0;
    std::unordered_map<std::string, int64_t> cat_rev;
};

struct CampaignDailyAgg {
    int64_t sessions = 0;
    int64_t purchasing_sessions = 0;
    int64_t revenue_cents = 0;
    int64_t assisted_sessions = 0;
};

// Output order is explicitly not significant. Hashing the daily keys avoids
// the pointer chasing and log(N) tree operations of std::map while the large
// event stream is being aggregated.
struct DailyKeyHash {
    size_t operator()(const std::pair<std::string, std::string>& key) const noexcept {
        const size_t h1 = std::hash<std::string>{}(key.first);
        const size_t h2 = std::hash<std::string>{}(key.second);
        return h1 ^ (h2 + static_cast<size_t>(0x9e3779b97f4a7c15ULL) +
                     (h1 << 6) + (h1 >> 2));
    }
};

std::vector<fs::path> list_csvs(const fs::path& dir) {
    std::vector<fs::path> files;
    if (!fs::exists(dir)) return files;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            files.push_back(entry.path());
        }
    }
    return files;
}

int main(int argc, char** argv) {
    if (argc < 6) {
        std::cerr << "Usage: pipeline <data_dir> <window_start> <window_end> <user_csv> <campaign_csv>\n";
        return 1;
    }

    fs::path data_dir = argv[1];
    int64_t window_start = 0, window_end = 0;
    parse_timestamp(argv[2], window_start);
    parse_timestamp(argv[3], window_end);
    int64_t click_window_start = window_start - SEVEN_DAYS_MS;

    std::string user_out_path = argv[4];
    std::string campaign_out_path = argv[5];

    std::vector<std::string_view> cols;
    std::string line;

    // 1. Load products.csv
    std::unordered_map<std::string, std::string> products;
    {
        std::ifstream in(data_dir / "products.csv");
        std::getline(in, line, '\n'); // skip header
        while (std::getline(in, line, '\n')) {
            if (parse_csv_line(line, 2, cols)) {
                products.emplace(std::string(cols[0]), std::string(cols[1]));
            }
        }
    }

    // 2. Load campaigns.csv
    std::unordered_map<std::string, std::string> campaigns;
    {
        std::ifstream in(data_dir / "campaigns.csv");
        std::getline(in, line, '\n'); // skip header
        while (std::getline(in, line, '\n')) {
            if (parse_csv_line(line, 2, cols)) {
                campaigns.emplace(std::string(cols[0]), std::string(cols[1]));
            }
        }
    }

    // 3. Load links/*.csv -> Union-Find
    UnionFind uf;
    for (const auto& path : list_csvs(data_dir / "links")) {
        std::ifstream in(path);
        std::getline(in, line, '\n'); // skip header
        while (std::getline(in, line, '\n')) {
            if (!parse_csv_line(line, 2, cols)) continue;
            if (cols[0].empty() || cols[1].empty()) continue;
            uf.unite(std::string(cols[0]), std::string(cols[1]));
        }
    }

    // 4. Parse & Dedup events/*.csv (dedup BEFORE validation!)
    std::unordered_map<std::string, RawEvent> dedup_events;
    for (const auto& path : list_csvs(data_dir / "events")) {
        std::ifstream in(path);
        std::getline(in, line, '\n'); // skip header
        while (std::getline(in, line, '\n')) {
            if (!parse_csv_line(line, 7, cols)) continue;
            if (cols[0].empty()) continue; // event_id empty

            int64_t ingest_ts = 0;
            if (!parse_timestamp(cols[3], ingest_ts)) continue;

            std::string eid(cols[0]);
            auto it = dedup_events.find(eid);
            if (it == dedup_events.end() || ingest_ts < it->second.ingest_ts) {
                dedup_events[eid] = RawEvent{
                    std::string(cols[1]),
                    std::string(cols[2]),
                    ingest_ts,
                    std::string(cols[4]),
                    std::string(cols[5]),
                    std::string(cols[6])
                };
            }
        }
    }

    // 5. Validate surviving events & group by canonical person
    std::unordered_map<std::string, std::vector<ValidEvent>> person_events;
    for (const auto& [eid, ev] : dedup_events) {
        if (ev.user_id.empty() || ev.product_id.empty()) continue;

        int64_t ts = 0;
        if (!parse_timestamp(ev.ts_str, ts)) continue;
        if (ts < window_start || ts >= window_end) continue;
        if (ev.ingest_ts - ts > MS_PER_DAY) continue; // drop late events (> 24h)

        bool is_purchase = false;
        int64_t price_cents = 0;
        if (ev.event_type == "purchase") {
            is_purchase = true;
            if (!parse_price(ev.price_str, price_cents)) continue;
        } else if (ev.event_type != "view" && ev.event_type != "cart") {
            continue;
        }

        std::string cat = "unknown";
        auto pit = products.find(ev.product_id);
        if (pit != products.end()) cat = pit->second;

        person_events[uf.canonical(ev.user_id)].push_back(ValidEvent{ts, price_cents, is_purchase, std::move(cat)});
    }
    dedup_events.clear(); // free memory

    // 6. Parse, Dedup & Validate clicks/*.csv
    std::unordered_set<std::string> seen_clicks;
    std::unordered_map<std::string, std::vector<ValidClick>> person_clicks;
    for (const auto& path : list_csvs(data_dir / "clicks")) {
        std::ifstream in(path);
        std::getline(in, line, '\n'); // skip header
        while (std::getline(in, line, '\n')) {
            if (!parse_csv_line(line, 4, cols)) continue;
            if (cols[0].empty()) continue; // click_id empty
            std::string cid(cols[0]);
            if (!seen_clicks.insert(cid).second) continue; // dedup

            if (cols[1].empty()) continue; // user_id empty
            std::string camp_id(cols[2]);
            if (campaigns.find(camp_id) == campaigns.end()) continue;

            int64_t ts = 0;
            if (!parse_timestamp(cols[3], ts)) continue;
            if (ts < click_window_start || ts >= window_end) continue;

            person_clicks[uf.canonical(std::string(cols[1]))].push_back(ValidClick{ts, std::move(cid), std::move(camp_id)});
        }
    }
    seen_clicks.clear();

    // 7. Sessionize, Attribute Clicks, and Aggregate
    std::unordered_map<std::pair<std::string, std::string>, UserDailyAgg, DailyKeyHash> user_daily;
    std::unordered_map<std::pair<std::string, std::string>, CampaignDailyAgg, DailyKeyHash> campaign_daily;
    user_daily.reserve(person_events.size());
    campaign_daily.reserve(person_events.size());

    for (auto& [person, evs] : person_events) {
        std::sort(evs.begin(), evs.end(), [](const ValidEvent& a, const ValidEvent& b) {
            return a.ts < b.ts;
        });

        auto& clicks = person_clicks[person];
        std::sort(clicks.begin(), clicks.end(), [](const ValidClick& a, const ValidClick& b) {
            if (a.ts != b.ts) return a.ts < b.ts;
            return a.click_id < b.click_id;
        });

        size_t L = 0, R = 0;
        // Counts of campaigns currently represented in [L, R). This lets us
        // enumerate distinct active campaigns instead of rescanning every
        // click in the seven-day window for every session.
        std::unordered_map<std::string, uint32_t> active_campaigns;
        active_campaigns.reserve(std::min<size_t>(clicks.size(), 64));
        size_t i = 0;
        while (i < evs.size()) {
            int64_t session_start = evs[i].ts;
            std::string day = epoch_ms_to_date(session_start);

            int64_t sess_rev = 0;
            bool has_purchase = false;
            auto& u_agg = user_daily[{person, day}];
            u_agg.sessions += 1;

            // Consume all events in this session
            int64_t prev_ts = session_start;
            while (i < evs.size() && (evs[i].ts - prev_ts <= SESSION_GAP_MS)) {
                prev_ts = evs[i].ts;
                u_agg.events += 1;
                if (evs[i].is_purchase) {
                    has_purchase = true;
                    sess_rev += evs[i].price_cents;
                    u_agg.revenue_cents += evs[i].price_cents;
                    u_agg.cat_rev[evs[i].category] += evs[i].price_cents;
                }
                ++i;
            }

            // Sliding window for clicks in [session_start - 7d, session_start]
            while (R < clicks.size() && clicks[R].ts <= session_start) {
                ++active_campaigns[clicks[R].campaign_id];
                ++R;
            }
            while (L < R && clicks[L].ts < session_start - SEVEN_DAYS_MS) {
                auto active = active_campaigns.find(clicks[L].campaign_id);
                if (active != active_campaigns.end()) {
                    if (--active->second == 0) active_campaigns.erase(active);
                }
                ++L;
            }

            if (L == R) {
                auto& c_agg = campaign_daily[{"organic", day}];
                c_agg.sessions += 1;
                if (has_purchase) c_agg.purchasing_sessions += 1;
                c_agg.revenue_cents += sess_rev;
            } else {
                // Latest ts is clicks[R-1].ts; scan back to find smallest click_id with that ts
                size_t best = R - 1;
                while (best > L && clicks[best - 1].ts == clicks[R - 1].ts) --best;

                auto& c_agg = campaign_daily[{clicks[best].campaign_id, day}];
                c_agg.sessions += 1;
                if (has_purchase) c_agg.purchasing_sessions += 1;
                c_agg.revenue_cents += sess_rev;

                // Assists: active_campaigns already contains each campaign
                // once, regardless of how many clicks it contributed.
                for (const auto& [campaign_id, count] : active_campaigns) {
                    (void)count;
                    campaign_daily[{campaign_id, day}].assisted_sessions += 1;
                }
            }
        }
    }

    // 8. Write intermediate CSVs (quoting strings preserves leading/trailing spaces)
    {
        std::ofstream out(user_out_path);
        out << "user_id,day,sessions,events,revenue_cents,top_category\n";
        for (const auto& [key, agg] : user_daily) {
            std::string top_cat;
            int64_t best_rev = -1;
            for (const auto& [cat, rev] : agg.cat_rev) {
                if (rev > best_rev || (rev == best_rev && cat < top_cat)) {
                    best_rev = rev;
                    top_cat = cat;
                }
            }
            out << '"' << key.first << "\"," << key.second << ','
                << agg.sessions << ',' << agg.events << ',' << agg.revenue_cents << ',';
            if (!top_cat.empty()) out << '"' << top_cat << '"';
            out << '\n';
        }
    }

    {
        std::ofstream out(campaign_out_path);
        out << "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n";
        for (const auto& [key, agg] : campaign_daily) {
            const std::string& camp_id = key.first;
            std::string channel = (camp_id == "organic") ? "organic" : campaigns[camp_id];
            out << '"' << camp_id << "\",\"" << channel << "\"," << key.second << ','
                << agg.sessions << ',' << agg.purchasing_sessions << ','
                << agg.revenue_cents << ',' << agg.assisted_sessions << '\n';
        }
    }

    return 0;
}
