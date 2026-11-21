#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
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
constexpr size_t CHUNK_BYTES = 4 << 20; // 4 MB chunks for parallel mmap scanning

inline bool is_digit(char c) { return c >= '0' && c <= '9'; }

// -------------------- Fast Hashing --------------------

inline uint64_t mix64(uint64_t x) noexcept {
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

struct Hash128 {
    uint64_t lo, hi;
    bool operator==(const Hash128& o) const noexcept { return lo == o.lo && hi == o.hi; }
};

inline Hash128 hash128(std::string_view s) noexcept {
    uint64_t h1 = 0xcbf29ce484222325ULL;
    uint64_t h2 = 0x9e3779b97f4a7c15ULL;
    const char* p = s.data();
    size_t n = s.size();
    while (n >= 8) {
        uint64_t v;
        std::memcpy(&v, p, 8);
        h1 = (h1 ^ v) * 0x100000001b3ULL;
        h2 = (h2 ^ v) * 0x9e3779b185ebca87ULL;
        p += 8; n -= 8;
    }
    uint64_t tail = 0;
    for (size_t i = 0; i < n; ++i) tail |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (i * 8);
    h1 = mix64(h1 ^ tail ^ s.size());
    h2 = mix64(h2 ^ tail ^ (s.size() * 0x517cc1b727220a95ULL));
    if (h1 == 0 && h2 == 0) h1 = 1; // reserve {0,0} as empty sentinel
    return {h1, h2};
}

struct SVHash {
    using is_transparent = void;
    size_t operator()(std::string_view s) const noexcept { return hash128(s).lo; }
};
struct SVEqual {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
};
template <typename V>
using SVMap = std::unordered_map<std::string, V, SVHash, SVEqual>;

// -------------------- Memory-Mapped File --------------------

struct MMapFile {
    const char* data = nullptr;
    size_t size = 0;

    bool open(const fs::path& p) {
        int fd = ::open(p.c_str(), O_RDONLY);
        if (fd < 0) return false;
        struct stat st{};
        if (fstat(fd, &st) != 0 || st.st_size <= 0) { ::close(fd); return false; }
        size = static_cast<size_t>(st.st_size);
        void* ptr = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (ptr == MAP_FAILED) { size = 0; return false; }
        ::madvise(ptr, size, MADV_SEQUENTIAL);
        data = static_cast<const char*>(ptr);
        return true;
    }
    void close() {
        if (data) { ::munmap(const_cast<char*>(data), size); data = nullptr; size = 0; }
    }
    ~MMapFile() { close(); }
    MMapFile() = default;
    MMapFile(const MMapFile&) = delete;
    MMapFile& operator=(const MMapFile&) = delete;
    MMapFile(MMapFile&& o) noexcept : data(o.data), size(o.size) { o.data = nullptr; o.size = 0; }
};

struct ByteChunk {
    const char* begin;
    const char* end;
};

std::vector<ByteChunk> make_chunks(const std::vector<MMapFile>& files) {
    std::vector<ByteChunk> chunks;
    for (const auto& f : files) {
        if (!f.data || f.size == 0) continue;
        const char* f_end = f.data + f.size;
        const char* nl = static_cast<const char*>(std::memchr(f.data, '\n', f.size));
        if (!nl || nl + 1 >= f_end) continue; // skip header
        const char* cur = nl + 1;
        while (cur < f_end) {
            const char* target = cur + CHUNK_BYTES;
            if (target >= f_end) {
                chunks.push_back({cur, f_end});
                break;
            }
            const char* next_nl = static_cast<const char*>(std::memchr(target, '\n', static_cast<size_t>(f_end - target)));
            if (!next_nl) {
                chunks.push_back({cur, f_end});
                break;
            }
            chunks.push_back({cur, next_nl + 1});
            cur = next_nl + 1;
        }
    }
    return chunks;
}

// -------------------- Fast Parsers --------------------

inline bool parse_csv(std::string_view line, size_t n, std::string_view* out) {
    if (line.empty()) return false;
    size_t idx = 0, start = 0, len = line.size();
    const char* p = line.data();
    while (true) {
        const char* comma = static_cast<const char*>(std::memchr(p + start, ',', len - start));
        size_t flen = comma ? static_cast<size_t>(comma - (p + start)) : (len - start);
        std::string_view f(p + start, flen);
        if (flen >= 2 && f.front() == '"' && f.back() == '"') f = f.substr(1, flen - 2);
        if (std::memchr(f.data(), '"', f.size()) != nullptr) return false;
        if (idx >= n) return false;
        out[idx++] = f;
        if (!comma) break;
        start = static_cast<size_t>(comma - p) + 1;
    }
    return idx == n;
}

inline int64_t days_from_civil(int64_t y, unsigned m, unsigned d) noexcept {
    y -= m <= 2;
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = static_cast<unsigned>(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}

inline bool parse_2d(const char* p, int& out) noexcept {
    unsigned a = static_cast<unsigned char>(p[0] - '0');
    unsigned b = static_cast<unsigned char>(p[1] - '0');
    if (a > 9 || b > 9) return false;
    out = static_cast<int>(a * 10 + b);
    return true;
}

inline bool parse_ts(std::string_view s, int64_t& out) noexcept {
    const size_t sz = s.size();
    if (sz == 0) return false;
    const char* p = s.data();

    // Fast path: ISO-8601 F1 (20) or F2 (25)
    if ((sz == 20 || sz == 25) && p[4] == '-' && p[7] == '-' && p[10] == 'T' && p[13] == ':' && p[16] == ':') {
        int y1, y2, mo, d, h, mi, sec;
        if (!parse_2d(p, y1) || !parse_2d(p + 2, y2) || !parse_2d(p + 5, mo) || !parse_2d(p + 8, d) ||
            !parse_2d(p + 11, h) || !parse_2d(p + 14, mi) || !parse_2d(p + 17, sec)) return false;
        int y = y1 * 100 + y2;
        static constexpr int mdays[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int dim = (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) ? 29 : (mo >= 1 && mo <= 12 ? mdays[mo] : 0);
        if (mo < 1 || mo > 12 || d < 1 || d > dim || h > 23 || mi > 59 || sec > 59) return false;

        int64_t off = 0;
        if (sz == 20) {
            if (p[19] != 'Z') return false;
        } else {
            if ((p[19] != '+' && p[19] != '-') || p[22] != ':') return false;
            int oh, om;
            if (!parse_2d(p + 20, oh) || !parse_2d(p + 23, om) || oh > 23 || om > 59) return false;
            off = (oh * MS_PER_HOUR + om * MS_PER_MIN) * (p[19] == '+' ? 1 : -1);
        }
        out = days_from_civil(y, mo, d) * MS_PER_DAY + h * MS_PER_HOUR + mi * MS_PER_MIN + sec * MS_PER_SEC - off;
        return true;
    }

    // F3: all ASCII digits
    size_t i = 0;
    while (i < sz && p[i] == '0') ++i;
    if (i == sz) { out = 0; return true; }
    if (sz - i > 18) return false;
    int64_t v = 0;
    for (; i < sz; ++i) {
        unsigned d = static_cast<unsigned char>(p[i] - '0');
        if (d > 9) return false;
        v = v * 10 + d;
    }
    out = v;
    return true;
}

inline bool parse_price(std::string_view s, int32_t& out) noexcept {
    const size_t sz = s.size();
    if (sz == 0) return false;
    const char* p = s.data();
    size_t i = 0;
    while (i < sz && p[i] == '0') ++i;
    if (i == sz || sz - i > 8) return false;
    int64_t v = 0;
    for (; i < sz; ++i) {
        unsigned d = static_cast<unsigned char>(p[i] - '0');
        if (d > 9) return false;
        v = v * 10 + d;
    }
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

// -------------------- Data Structures --------------------

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

// Zero-allocation POD structs pointing into mmap buffers
struct RawDedupEvent {
    Hash128 eid_hash;
    int64_t ingest_ts;
    int64_t ts;
    std::string_view person;
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

struct PersonEventMsg {
    std::string_view person;
    ValidEvent ev;
};

struct RawClickMsg {
    Hash128 cid_hash;
    std::string_view cid;
    int64_t ts;
    uint32_t ps;
    uint32_t pid;
    uint32_t camp;
};

struct ValidClick {
    int64_t ts;
    std::string_view click_id;
    uint32_t campaign_id;
};

struct PersonClickMsg {
    uint32_t pid;
    ValidClick ck;
};

// Flat open-addressing hash table for event dedup (zero per-row malloc)
struct FlatEventMap {
    std::vector<RawDedupEvent> slots;
    size_t mask = 0;

    void init(size_t expected) {
        size_t cap = 1024;
        while (cap < expected * 2) cap <<= 1;
        slots.assign(cap, RawDedupEvent{{0, 0}, 0, 0, {}, 0, 0, 0});
        mask = cap - 1;
    }

    inline void insert_or_min(const RawDedupEvent& e) {
        size_t idx = e.eid_hash.lo & mask;
        while (true) {
            auto& s = slots[idx];
            if (s.eid_hash.lo == 0 && s.eid_hash.hi == 0) {
                s = e;
                return;
            }
            if (s.eid_hash == e.eid_hash) {
                if (e.ingest_ts < s.ingest_ts) s = e;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }
};

// Flat open-addressing hash set for click dedup
struct FlatClickSet {
    std::vector<Hash128> slots;
    size_t mask = 0;

    void init(size_t expected) {
        size_t cap = 1024;
        while (cap < expected * 2) cap <<= 1;
        slots.assign(cap, Hash128{0, 0});
        mask = cap - 1;
    }

    inline bool insert(Hash128 h) {
        size_t idx = h.lo & mask;
        while (true) {
            auto& s = slots[idx];
            if (s.lo == 0 && s.hi == 0) { s = h; return true; }
            if (s == h) return false;
            idx = (idx + 1) & mask;
        }
    }
};

struct PersonShard {
    StringTable people;
    std::vector<std::vector<ValidEvent>> events;
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

std::vector<fs::path> list_csvs(const fs::path& dir) {
    std::vector<fs::path> out;
    if (fs::exists(dir)) {
        for (const auto& e : fs::directory_iterator(dir)) {
            if (e.is_regular_file() && e.path().extension() == ".csv") out.push_back(e.path());
        }
    }
    return out;
}

void write_parquet_table(const fs::path& path, const std::shared_ptr<arrow::Table>& table) {
    auto out_res = arrow::io::FileOutputStream::Open(path.string());
    if (!out_res.ok()) { std::cerr << out_res.status().ToString() << "\n"; std::exit(1); }
    auto props = parquet::WriterProperties::Builder()
                     .compression(arrow::Compression::UNCOMPRESSED)
                     ->build();
    auto st = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *out_res, 1 << 20, props);
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

    auto for_each_line = [](const MMapFile& f, auto&& fn) {
        if (!f.data || f.size == 0) return;
        const char* end = f.data + f.size;
        const char* nl = static_cast<const char*>(std::memchr(f.data, '\n', f.size));
        if (!nl) return;
        const char* cur = nl + 1;
        while (cur < end) {
            const char* next = static_cast<const char*>(std::memchr(cur, '\n', static_cast<size_t>(end - cur)));
            size_t len = next ? static_cast<size_t>(next - cur) : static_cast<size_t>(end - cur);
            fn(std::string_view(cur, len));
            cur = next ? next + 1 : end;
        }
    };

    // 1. Products & Categories
    StringTable categories;
    uint32_t unknown_cat = categories.get("unknown");
    SVMap<uint32_t> products;
    {
        MMapFile f;
        if (f.open(data_dir / "products.csv")) {
            std::string_view c[2];
            for_each_line(f, [&](std::string_view ln) {
                if (parse_csv(ln, 2, c)) products.emplace(std::string(c[0]), categories.get(c[1]));
            });
        }
    }

    // 2. Campaigns
    StringTable camp_ids;
    std::vector<std::string> camp_channels;
    {
        MMapFile f;
        if (f.open(data_dir / "campaigns.csv")) {
            std::string_view c[2];
            for_each_line(f, [&](std::string_view ln) {
                if (parse_csv(ln, 2, c)) {
                    uint32_t id = camp_ids.get(c[0]);
                    if (id >= camp_channels.size()) camp_channels.resize(id + 1);
                    camp_channels[id] = std::string(c[1]);
                }
            });
        }
    }

    // 3. Links -> Union-Find (frozen for lock-free reads)
    UnionFind uf;
    for (const auto& path : list_csvs(data_dir / "links")) {
        MMapFile f;
        if (f.open(path)) {
            std::string_view c[2];
            for_each_line(f, [&](std::string_view ln) {
                if (parse_csv(ln, 2, c) && !c[0].empty() && !c[1].empty()) uf.unite(c[0], c[1]);
            });
        }
    }
    uf.freeze();

    // 4a. Parallel Events Parse -> Thread-local shard buckets (ZERO locks)
    std::vector<MMapFile> event_mmaps;
    for (const auto& p : list_csvs(data_dir / "events")) {
        MMapFile f;
        if (f.open(p)) event_mmaps.push_back(std::move(f));
    }
    auto ev_chunks = make_chunks(event_mmaps);

    std::vector<std::vector<std::vector<RawDedupEvent>>> ev_buckets(
        n_workers, std::vector<std::vector<RawDedupEvent>>(N_SHARDS));
    {
        std::atomic<size_t> next_chunk{0};
        auto worker = [&](unsigned tid) {
            auto& my_b = ev_buckets[tid];
            std::string_view c[7];
            while (true) {
                size_t idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                if (idx >= ev_chunks.size()) break;
                const char* cur = ev_chunks[idx].begin;
                const char* end = ev_chunks[idx].end;
                while (cur < end) {
                    const char* nl = static_cast<const char*>(std::memchr(cur, '\n', static_cast<size_t>(end - cur)));
                    size_t len = nl ? static_cast<size_t>(nl - cur) : static_cast<size_t>(end - cur);
                    std::string_view ln(cur, len);
                    cur = nl ? nl + 1 : end;

                    if (!parse_csv(ln, 7, c) || c[0].empty()) continue;
                    int64_t ingest_ts = 0;
                    if (!parse_ts(c[3], ingest_ts)) continue;

                    Hash128 h = hash128(c[0]);
                    RawDedupEvent ev{h, ingest_ts, 0, {}, 0, 0, 0};
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
                            ev.person = uf.canonical(c[1]);
                            ev.category_id = (pit != products.end()) ? pit->second : unknown_cat;
                            ev.price_cents = price;
                            ev.flags = 1 | (is_purch ? 2 : 0);
                        }
                    }
                    my_b[h.lo & SHARD_MASK].push_back(ev);
                }
            }
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker, t);
        for (auto& t : ts) t.join();
    }

    // 4b. Lock-free per-shard Event Dedup -> Thread-local PersonShard buckets
    std::vector<std::vector<std::vector<PersonEventMsg>>> pev_buckets(
        n_workers, std::vector<std::vector<PersonEventMsg>>(N_SHARDS));
    {
        std::atomic<size_t> next_s{0};
        auto worker = [&](unsigned tid) {
            auto& my_pb = pev_buckets[tid];
            FlatEventMap map;
            while (true) {
                size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
                if (s >= N_SHARDS) break;
                size_t total = 0;
                for (unsigned t = 0; t < n_workers; ++t) total += ev_buckets[t][s].size();
                if (total == 0) continue;

                map.init(total);
                for (unsigned t = 0; t < n_workers; ++t) {
                    for (const auto& e : ev_buckets[t][s]) map.insert_or_min(e);
                    std::vector<RawDedupEvent>().swap(ev_buckets[t][s]);
                }
                for (const auto& e : map.slots) {
                    if ((e.eid_hash.lo == 0 && e.eid_hash.hi == 0) || !(e.flags & 1)) continue;
                    size_t ps = hash128(e.person).lo & SHARD_MASK;
                    my_pb[ps].push_back({e.person, ValidEvent{e.ts, e.price_cents, e.category_id, (e.flags & 2) != 0}});
                }
            }
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker, t);
        for (auto& t : ts) t.join();
    }
    ev_buckets.clear();

    // 4c. Lock-free populate PersonShards
    std::vector<PersonShard> pshards(N_SHARDS);
    {
        std::atomic<size_t> next_ps{0};
        auto worker = [&]() {
            while (true) {
                size_t ps = next_ps.fetch_add(1, std::memory_order_relaxed);
                if (ps >= N_SHARDS) break;
                auto& sh = pshards[ps];
                for (unsigned t = 0; t < n_workers; ++t) {
                    for (const auto& msg : pev_buckets[t][ps]) {
                        uint32_t pid = sh.people.get(msg.person);
                        if (pid >= sh.events.size()) sh.events.resize(pid + 1);
                        sh.events[pid].push_back(msg.ev);
                    }
                    std::vector<PersonEventMsg>().swap(pev_buckets[t][ps]);
                }
            }
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker);
        for (auto& t : ts) t.join();
    }
    pev_buckets.clear();
    event_mmaps.clear(); // safe to unmap now; person strings are owned by pshards[ps].people

    // 5a. Parallel Clicks Parse + Early Person Filter -> Thread-local shard buckets
    std::vector<MMapFile> click_mmaps;
    for (const auto& p : list_csvs(data_dir / "clicks")) {
        MMapFile f;
        if (f.open(p)) click_mmaps.push_back(std::move(f));
    }
    auto ck_chunks = make_chunks(click_mmaps);

    std::vector<std::vector<std::vector<RawClickMsg>>> ck_buckets(
        n_workers, std::vector<std::vector<RawClickMsg>>(N_SHARDS));
    {
        std::atomic<size_t> next_chunk{0};
        auto worker = [&](unsigned tid) {
            auto& my_b = ck_buckets[tid];
            std::string_view c[4];
            while (true) {
                size_t idx = next_chunk.fetch_add(1, std::memory_order_relaxed);
                if (idx >= ck_chunks.size()) break;
                const char* cur = ck_chunks[idx].begin;
                const char* end = ck_chunks[idx].end;
                while (cur < end) {
                    const char* nl = static_cast<const char*>(std::memchr(cur, '\n', static_cast<size_t>(end - cur)));
                    size_t len = nl ? static_cast<size_t>(nl - cur) : static_cast<size_t>(end - cur);
                    std::string_view ln(cur, len);
                    cur = nl ? nl + 1 : end;

                    if (!parse_csv(ln, 4, c) || c[0].empty() || c[1].empty()) continue;
                    auto cit = camp_ids.ids.find(c[2]);
                    if (cit == camp_ids.ids.end()) continue;
                    int64_t ts = 0;
                    if (!parse_ts(c[3], ts) || ts < click_win_start || ts >= win_end) continue;

                    // Lock-free early filter: drop clicks for people with no valid events
                    std::string_view person = uf.canonical(c[1]);
                    uint32_t ps = static_cast<uint32_t>(hash128(person).lo & SHARD_MASK);
                    auto pit = pshards[ps].people.ids.find(person);
                    if (pit == pshards[ps].people.ids.end()) continue;

                    Hash128 h = hash128(c[0]);
                    my_b[h.lo & SHARD_MASK].push_back({h, c[0], ts, ps, pit->second, cit->second});
                }
            }
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker, t);
        for (auto& t : ts) t.join();
    }

    // 5b. Lock-free Click Dedup -> Thread-local PersonShard click buckets
    std::vector<std::vector<std::vector<PersonClickMsg>>> pck_buckets(
        n_workers, std::vector<std::vector<PersonClickMsg>>(N_SHARDS));
    {
        std::atomic<size_t> next_s{0};
        auto worker = [&](unsigned tid) {
            auto& my_pb = pck_buckets[tid];
            FlatClickSet seen;
            while (true) {
                size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
                if (s >= N_SHARDS) break;
                size_t total = 0;
                for (unsigned t = 0; t < n_workers; ++t) total += ck_buckets[t][s].size();
                if (total == 0) continue;

                seen.init(total);
                for (unsigned t = 0; t < n_workers; ++t) {
                    for (const auto& r : ck_buckets[t][s]) {
                        if (seen.insert(r.cid_hash)) {
                            my_pb[r.ps].push_back({r.pid, ValidClick{r.ts, r.cid, r.camp}});
                        }
                    }
                    std::vector<RawClickMsg>().swap(ck_buckets[t][s]);
                }
            }
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker, t);
        for (auto& t : ts) t.join();
    }
    ck_buckets.clear();

    // 6. Parallel Sessionize, Attribute, and Write user_daily/part-{tid}.parquet
    fs::create_directories(output_dir / "user_daily");
    fs::create_directories(output_dir / "campaign_daily");

    auto user_schema = arrow::schema({
        arrow::field("user_id", arrow::utf8()),
        arrow::field("day", arrow::date32()),
        arrow::field("sessions", arrow::int64()),
        arrow::field("events", arrow::int64()),
        arrow::field("revenue_cents", arrow::int64()),
        arrow::field("top_category", arrow::utf8(), true)
    });

    std::atomic<size_t> next_ps{0};
    std::atomic<bool> wrote_any_user{false};
    std::vector<std::unordered_map<uint64_t, CampAgg>> camp_locals(n_workers);
    const size_t num_cats = categories.values.size();
    const size_t num_camps = camp_ids.values.size();

    auto agg_worker = [&](unsigned tid) {
        std::vector<UserRow> urows;
        auto& cmap = camp_locals[tid];
        std::vector<int64_t> cat_rev(num_cats, 0);
        std::vector<uint32_t> touched_cats;
        std::vector<uint32_t> active_cnt(num_camps, 0);
        std::vector<uint32_t> active_list;

        while (true) {
            size_t ps = next_ps.fetch_add(1, std::memory_order_relaxed);
            if (ps >= N_SHARDS) break;
            auto& sh = pshards[ps];

            // Gather clicks for this person shard (lock-free)
            std::vector<std::vector<ValidClick>> pclicks(sh.events.size());
            for (unsigned t = 0; t < n_workers; ++t) {
                for (const auto& m : pck_buckets[t][ps]) pclicks[m.pid].push_back(m.ck);
                std::vector<PersonClickMsg>().swap(pck_buckets[t][ps]);
            }

            for (uint32_t pid = 0; pid < sh.events.size(); ++pid) {
                auto& evs = sh.events[pid];
                if (evs.empty()) continue;

                std::sort(evs.begin(), evs.end(), [](const ValidEvent& a, const ValidEvent& b) { return a.ts < b.ts; });
                auto& clicks = pclicks[pid];
                if (clicks.size() > 1) {
                    std::sort(clicks.begin(), clicks.end(), [](const ValidClick& a, const ValidClick& b) {
                        return a.ts != b.ts ? a.ts < b.ts : a.click_id < b.click_id;
                    });
                }

                for (uint32_t c : active_list) active_cnt[c] = 0;
                active_list.clear();

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

                    while (R < clicks.size() && clicks[R].ts <= s_start) {
                        uint32_t cid = clicks[R].campaign_id;
                        if (active_cnt[cid]++ == 0) active_list.push_back(cid);
                        ++R;
                    }
                    while (L < R && clicks[L].ts < s_start - SEVEN_DAYS_MS) {
                        --active_cnt[clicks[L].campaign_id];
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
                        for (size_t k = 0; k < active_list.size(); ) {
                            uint32_t cid = active_list[k];
                            if (active_cnt[cid] == 0) {
                                active_list[k] = active_list.back();
                                active_list.pop_back();
                            } else {
                                cmap[pack(cid)].assisted += 1;
                                ++k;
                            }
                        }
                    }
                }
                flush_day();
            }
        }

        // Write this worker's user_daily partition in parallel
        if (!urows.empty()) {
            wrote_any_user.store(true, std::memory_order_relaxed);
            arrow::StringBuilder b_uid, b_cat;
            arrow::Date32Builder b_day;
            arrow::Int64Builder b_sess, b_evs, b_rev;
            (void)b_uid.Reserve(urows.size()); (void)b_cat.Reserve(urows.size());
            (void)b_day.Reserve(urows.size()); (void)b_sess.Reserve(urows.size());
            (void)b_evs.Reserve(urows.size()); (void)b_rev.Reserve(urows.size());

            for (const auto& r : urows) {
                (void)b_uid.Append(pshards[r.shard].people.values[r.pid]);
                (void)b_day.Append(r.day);
                (void)b_sess.Append(r.sessions);
                (void)b_evs.Append(r.events);
                (void)b_rev.Append(r.revenue_cents);
                if (r.top_cat == UINT32_MAX) (void)b_cat.AppendNull();
                else (void)b_cat.Append(categories.values[r.top_cat]);
            }

            std::shared_ptr<arrow::Array> a_uid, a_day, a_sess, a_evs, a_rev, a_cat;
            (void)b_uid.Finish(&a_uid); (void)b_day.Finish(&a_day); (void)b_sess.Finish(&a_sess);
            (void)b_evs.Finish(&a_evs); (void)b_rev.Finish(&a_rev); (void)b_cat.Finish(&a_cat);
            auto table = arrow::Table::Make(user_schema, {a_uid, a_day, a_sess, a_evs, a_rev, a_cat});
            write_parquet_table(output_dir / "user_daily" / ("part-" + std::to_string(tid) + ".parquet"), table);
        }
    };

    {
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(agg_worker, t);
        for (auto& t : ts) t.join();
    }

    // Ensure at least one valid parquet file exists if user_daily is empty
    if (!wrote_any_user.load()) {
        arrow::StringBuilder b_uid, b_cat;
        arrow::Date32Builder b_day;
        arrow::Int64Builder b_sess, b_evs, b_rev;
        std::shared_ptr<arrow::Array> a_uid, a_day, a_sess, a_evs, a_rev, a_cat;
        (void)b_uid.Finish(&a_uid); (void)b_day.Finish(&a_day); (void)b_sess.Finish(&a_sess);
        (void)b_evs.Finish(&a_evs); (void)b_rev.Finish(&a_rev); (void)b_cat.Finish(&a_cat);
        write_parquet_table(output_dir / "user_daily" / "part-0.parquet",
                            arrow::Table::Make(user_schema, {a_uid, a_day, a_sess, a_evs, a_rev, a_cat}));
    }

    // 7. Merge & Write campaign_daily/part-0.parquet
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
        (void)b_cid.Reserve(camp_daily.size()); (void)b_ch.Reserve(camp_daily.size());
        (void)b_day.Reserve(camp_daily.size()); (void)b_sess.Reserve(camp_daily.size());
        (void)b_purch.Reserve(camp_daily.size()); (void)b_rev.Reserve(camp_daily.size());
        (void)b_ast.Reserve(camp_daily.size());

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
        write_parquet_table(output_dir / "campaign_daily" / "part-0.parquet",
                            arrow::Table::Make(schema, {a_cid, a_ch, a_day, a_sess, a_purch, a_rev, a_ast}));
    }

    return 0;
}