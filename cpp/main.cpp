#include <immintrin.h>
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
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

namespace fs = std::filesystem;

constexpr int64_t MS_PER_SEC = 1000LL, MS_PER_MIN = 60LL * MS_PER_SEC,
                  MS_PER_HOUR = 60LL * MS_PER_MIN, MS_PER_DAY = 24LL * MS_PER_HOUR,
                  SESSION_GAP_MS = 30LL * MS_PER_MIN, SEVEN_DAYS_MS = 7LL * MS_PER_DAY;
// Benchmark-verified default: 64 shards and 8 threads (NCPU=8).
constexpr size_t N_SHARDS = 64;
constexpr size_t SHARD_MASK = N_SHARDS - 1;
constexpr size_t CHUNK_BYTES = 4 << 20;
constexpr uint32_t ORGANIC_CID = UINT32_MAX - 1;

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
    return {h1, h2};
}

inline uint64_t hash64(std::string_view s) noexcept {
    uint64_t h = 0xcbf29ce484222325ULL;
    const char* p = s.data();
    size_t n = s.size();
    while (n >= 8) {
        uint64_t v;
        std::memcpy(&v, p, 8);
        h = (h ^ v) * 0x100000001b3ULL;
        p += 8; n -= 8;
    }
    uint64_t tail = 0;
    for (size_t i = 0; i < n; ++i) tail |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (i * 8);
    return mix64(h ^ tail ^ s.size());
}

struct FlatStrMap {
    struct Slot {
        uint64_t hash;
        uint32_t off;
        uint32_t len;
        uint32_t val;
    };
    std::vector<char> arena;
    std::vector<Slot> slots;
    std::vector<std::pair<uint32_t, uint32_t>> id_slices;
    size_t count = 0;
    size_t mask = 0;

    void init(size_t expected = 64, size_t expected_bytes = 0) {
        size_t cap = 64;
        while (cap < expected * 2) cap <<= 1;
        slots.assign(cap, Slot{0, 0, UINT32_MAX, 0});
        mask = cap - 1;
        count = 0;
        if (expected_bytes > 0) arena.reserve(expected_bytes);
        if (expected > 0) id_slices.reserve(expected);
    }

    void rehash(size_t new_cap) {
        std::vector<Slot> nslots(new_cap, Slot{0, 0, UINT32_MAX, 0});
        size_t nmask = new_cap - 1;
        for (const auto& s : slots) {
            if (s.len == UINT32_MAX) continue;
            size_t idx = s.hash & nmask;
            while (nslots[idx].len != UINT32_MAX) idx = (idx + 1) & nmask;
            nslots[idx] = s;
        }
        slots.swap(nslots);
        mask = nmask;
    }

    inline uint32_t find(std::string_view sv, uint64_t h) const noexcept {
        if (slots.empty()) return UINT32_MAX;
        size_t idx = h & mask;
        const char* base = arena.data();
        const size_t sz = sv.size();
        while (true) {
            const auto& s = slots[idx];
            if (s.len == UINT32_MAX) return UINT32_MAX;
            if (s.hash == h && s.len == sz && (sz == 0 || std::memcmp(base + s.off, sv.data(), sz) == 0)) return s.val;
            idx = (idx + 1) & mask;
        }
    }

    inline uint32_t find(std::string_view sv) const noexcept { return find(sv, hash64(sv)); }

    inline uint32_t emplace_val(std::string_view sv, uint32_t val, uint64_t h) {
        if (slots.empty()) init(64);
        if ((count + 1) * 2 >= slots.size()) rehash(slots.size() << 1);
        size_t idx = h & mask;
        const char* base = arena.data();
        const size_t sz = sv.size();
        while (true) {
            auto& s = slots[idx];
            if (s.len == UINT32_MAX) {
                uint32_t off = static_cast<uint32_t>(arena.size());
                arena.insert(arena.end(), sv.data(), sv.data() + sz);
                s = Slot{h, off, static_cast<uint32_t>(sz), val};
                ++count;
                return val;
            }
            if (s.hash == h && s.len == sz && (sz == 0 || std::memcmp(base + s.off, sv.data(), sz) == 0)) return s.val;
            idx = (idx + 1) & mask;
        }
    }

    inline uint32_t get_id(std::string_view sv, uint64_t h) {
        if (slots.empty()) init(64);
        if ((count + 1) * 2 >= slots.size()) rehash(slots.size() << 1);
        size_t idx = h & mask;
        const char* base = arena.data();
        const size_t sz = sv.size();
        while (true) {
            auto& s = slots[idx];
            if (s.len == UINT32_MAX) {
                uint32_t off = static_cast<uint32_t>(arena.size());
                arena.insert(arena.end(), sv.data(), sv.data() + sz);
                uint32_t id = static_cast<uint32_t>(id_slices.size());
                id_slices.emplace_back(off, static_cast<uint32_t>(sz));
                s = Slot{h, off, static_cast<uint32_t>(sz), id};
                ++count;
                return id;
            }
            if (s.hash == h && s.len == sz && (sz == 0 || std::memcmp(base + s.off, sv.data(), sz) == 0)) return s.val;
            idx = (idx + 1) & mask;
        }
    }

    inline uint32_t get_id(std::string_view sv) { return get_id(sv, hash64(sv)); }

    inline std::string_view view(uint32_t id) const noexcept {
        auto [off, len] = id_slices[id];
        return std::string_view(arena.data() + off, len);
    }
};

struct ProductTable {
    static constexpr uint32_t MAX_FAST_ID = 2000000;
    std::vector<uint32_t> fast;
    FlatStrMap fallback;

    static inline bool parse_p_id(std::string_view s, uint32_t& out) noexcept {
        const size_t sz = s.size();
        if (sz < 2 || sz > 8 || s[0] != 'p') return false;
        if (sz > 2 && s[1] == '0') return false;
        uint32_t v = 0;
        for (size_t i = 1; i < sz; ++i) {
            unsigned d = static_cast<unsigned char>(s[i] - '0');
            if (d > 9) return false;
            v = v * 10 + d;
        }
        if (v >= MAX_FAST_ID) return false;
        out = v;
        return true;
    }

    void insert(std::string_view pid, uint32_t cat_id) {
        uint32_t idx;
        if (parse_p_id(pid, idx)) {
            if (idx >= fast.size()) fast.resize(idx + 1, UINT32_MAX);
            if (fast[idx] == UINT32_MAX) fast[idx] = cat_id;
        } else {
            fallback.emplace_val(pid, cat_id, hash64(pid));
        }
    }

    inline uint32_t find(std::string_view pid) const noexcept {
        uint32_t idx;
        if (parse_p_id(pid, idx)) {
            return (idx < fast.size()) ? fast[idx] : UINT32_MAX;
        }
        return fallback.find(pid);
    }
};

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
        if (!nl || nl + 1 >= f_end) continue;
        const char* cur = nl + 1;
        while (cur < f_end) {
            if (static_cast<size_t>(f_end - cur) <= CHUNK_BYTES) {
                chunks.push_back({cur, f_end});
                break;
            }
            const char* target = cur + CHUNK_BYTES;
            const char* next_nl = static_cast<const char*>(std::memchr(target, '\n', static_cast<size_t>(f_end - target)));
            if (!next_nl) { chunks.push_back({cur, f_end}); break; }
            chunks.push_back({cur, next_nl + 1});
            cur = next_nl + 1;
        }
    }
    return chunks;
}

inline bool parse_csv_slow(std::string_view line, size_t n, std::string_view* out) noexcept {
    const size_t len = line.size();
    if (len == 0) return false;
    const char* p = line.data();
    size_t idx = 0, start = 0;
    while (true) {
        const char* comma = static_cast<const char*>(std::memchr(p + start, ',', len - start));
        size_t flen = comma ? static_cast<size_t>(comma - (p + start)) : (len - start);
        const char* fp = p + start;
        if (flen >= 2 && fp[0] == '"' && fp[flen - 1] == '"') {
            fp += 1;
            flen -= 2;
        }
        if (flen > 0 && __builtin_expect(std::memchr(fp, '"', flen) != nullptr, 0)) return false;
        if (idx >= n) return false;
        out[idx++] = std::string_view(fp, flen);
        if (!comma) return idx == n;
        start = static_cast<size_t>(comma - p) + 1;
    }
}

inline bool parse_csv(std::string_view line, size_t n, std::string_view* out) noexcept {
    const size_t len = line.size();
    if (len == 0) return false;
    const char* p = line.data();
    const __m256i v_comma = _mm256_set1_epi8(',');
    const __m256i v_quote = _mm256_set1_epi8('"');

    size_t idx = 0, start = 0, pos = 0;
    while (pos + 32 <= len) {
        const __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + pos));
        const uint32_t qmask = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, v_quote)));
        if (__builtin_expect(qmask != 0, 0)) return parse_csv_slow(line, n, out);

        uint32_t cmask = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(chunk, v_comma)));
        while (cmask) {
            const size_t cpos = pos + static_cast<size_t>(__builtin_ctz(cmask));
            if (__builtin_expect(idx >= n, 0)) return false;
            out[idx++] = std::string_view(p + start, cpos - start);
            start = cpos + 1;
            cmask &= cmask - 1;
        }
        pos += 32;
    }

    for (; pos < len; ++pos) {
        const char ch = p[pos];
        if (__builtin_expect(ch == '"', 0)) return parse_csv_slow(line, n, out);
        if (ch == ',') {
            if (__builtin_expect(idx >= n, 0)) return false;
            out[idx++] = std::string_view(p + start, pos - start);
            start = pos + 1;
        }
    }
    if (idx + 1 != n) return false;
    out[idx] = std::string_view(p + start, len - start);
    return true;
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

struct DateCache {
    char date[10] = {};
    int64_t day_ms = 0;
};

inline bool parse_ts(std::string_view s, int64_t& out, DateCache& dc) noexcept {
    const size_t sz = s.size();
    if (sz == 0) return false;
    const char* p = s.data();

    if ((sz == 20 || sz == 25) && p[4] == '-' && p[7] == '-' && p[10] == 'T' && p[13] == ':' && p[16] == ':') {
        int64_t day_ms;
        if (std::memcmp(p, dc.date, 10) == 0) {
            day_ms = dc.day_ms;
        } else {
            int y1, y2, mo, d;
            if (!parse_2d(p, y1) || !parse_2d(p + 2, y2) || !parse_2d(p + 5, mo) || !parse_2d(p + 8, d)) return false;
            int y = y1 * 100 + y2;
            static constexpr int mdays[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
            int dim = (mo == 2 && ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0)) ? 29 : (mo >= 1 && mo <= 12 ? mdays[mo] : 0);
            if (mo < 1 || mo > 12 || d < 1 || d > dim) return false;
            day_ms = days_from_civil(y, mo, d) * MS_PER_DAY;
            std::memcpy(dc.date, p, 10);
            dc.day_ms = day_ms;
        }

        int h, mi, sec;
        if (!parse_2d(p + 11, h) || !parse_2d(p + 14, mi) || !parse_2d(p + 17, sec)) return false;
        if (h > 23 || mi > 59 || sec > 59) return false;

        int64_t off = 0;
        if (sz == 20) {
            if (p[19] != 'Z') return false;
        } else {
            if ((p[19] != '+' && p[19] != '-') || p[22] != ':') return false;
            int oh, om;
            if (!parse_2d(p + 20, oh) || !parse_2d(p + 23, om) || oh > 23 || om > 59) return false;
            off = (oh * MS_PER_HOUR + om * MS_PER_MIN) * (p[19] == '+' ? 1 : -1);
        }
        out = day_ms + h * MS_PER_HOUR + mi * MS_PER_MIN + sec * MS_PER_SEC - off;
        return true;
    }

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

inline bool parse_ts(std::string_view s, int64_t& out) noexcept {
    DateCache dc;
    return parse_ts(s, out, dc);
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

struct UnionFind {
    FlatStrMap map;
    std::vector<uint32_t> parent;
    std::vector<uint32_t> min_idx;
    std::vector<uint64_t> root_hash;

    uint32_t get_or_add(std::string_view sv, uint64_t h) {
        uint32_t old_n = static_cast<uint32_t>(map.id_slices.size());
        uint32_t idx = map.get_id(sv, h);
        if (idx == old_n) {
            parent.push_back(idx);
            min_idx.push_back(idx);
        }
        return idx;
    }
    uint32_t find(uint32_t x) {
        uint32_t r = x;
        while (parent[r] != r) r = parent[r];
        while (parent[x] != r) { uint32_t n = parent[x]; parent[x] = r; x = n; }
        return r;
    }
    void unite(std::string_view a, uint64_t ha, std::string_view b, uint64_t hb) {
        uint32_t ra = find(get_or_add(a, ha)), rb = find(get_or_add(b, hb));
        if (ra == rb) return;
        parent[rb] = ra;
        if (map.view(min_idx[rb]) < map.view(min_idx[ra])) min_idx[ra] = min_idx[rb];
    }
    void freeze() {
        root_hash.resize(parent.size());
        for (uint32_t i = 0; i < parent.size(); ++i) parent[i] = find(i);
        for (uint32_t i = 0; i < parent.size(); ++i) {
            if (parent[i] == i) root_hash[i] = hash64(map.view(min_idx[i]));
        }
    }
    inline std::pair<std::string_view, uint64_t> canonical_with_hash_prehashed(std::string_view sv, uint64_t h) const noexcept {
        if (map.count == 0) return {sv, h};
        uint32_t idx = map.find(sv, h);
        if (idx == UINT32_MAX) return {sv, h};
        uint32_t r = parent[idx];
        return {map.view(min_idx[r]), root_hash[r]};
    }
};

struct RawDedupEvent {
    Hash128 eid_hash;
    int64_t ingest_ts;
    int64_t ts;
    const char* user_ptr;
    uint32_t ulen_cat;
    uint32_t price_flags;
};

struct ValidEvent {
    int64_t ts;
    int32_t price_cents;
    uint32_t category_id;
    bool is_purchase;
};

struct PersonEventMsg {
    uint64_t phash;
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

// Hybrid sorting: comparison sort for small per-user collections, stable radix
// sort for larger collections. The passes with constant timestamp bytes are
// skipped, so nearby timestamps generally require only 3-5 radix passes.
// Timestamps are signed int64; flipping the sign bit makes their unsigned
// representation order-preserving.
#ifndef SHOPPING_RADIX_THRESHOLD
#define SHOPPING_RADIX_THRESHOLD 1024
#endif
#ifndef SHOPPING_RADIX_EVENTS
#define SHOPPING_RADIX_EVENTS 1
#endif
#ifndef SHOPPING_RADIX_CLICKS
#define SHOPPING_RADIX_CLICKS 1
#endif
static_assert(SHOPPING_RADIX_THRESHOLD >= 2, "radix threshold must be at least 2");

template <typename T>
inline void radix_sort_timestamp(T* data, size_t n, std::vector<T>& scratch) {
    if (n < 2) return;
    constexpr uint64_t SIGN_BIT = 1ULL << 63;
    const uint64_t first_key = static_cast<uint64_t>(data[0].ts) ^ SIGN_BIT;
    uint64_t differing_bits = 0;
    for (size_t i = 1; i < n; ++i)
        differing_bits |= (static_cast<uint64_t>(data[i].ts) ^ SIGN_BIT) ^ first_key;
    if (!differing_bits) return; // already sorted: all timestamps equal

    scratch.resize(n);
    T* src = data;
    T* dst = scratch.data();
    for (unsigned shift = 0; shift < 64; shift += 8) {
        if (((differing_bits >> shift) & 0xFFu) == 0) continue;
        size_t offsets[256] = {};
        for (size_t i = 0; i < n; ++i) {
            const uint64_t k = static_cast<uint64_t>(src[i].ts) ^ SIGN_BIT;
            ++offsets[(k >> shift) & 0xFFu];
        }
        size_t total = 0;
        for (size_t b = 0; b < 256; ++b) {
            const size_t count = offsets[b];
            offsets[b] = total;
            total += count;
        }
        for (size_t i = 0; i < n; ++i) {
            const uint64_t k = static_cast<uint64_t>(src[i].ts) ^ SIGN_BIT;
            dst[offsets[(k >> shift) & 0xFFu]++] = src[i];
        }
        std::swap(src, dst);
    }
    if (src != data) std::copy(src, src + n, data);
}

// After timestamp-only sorting of clicks, the attribution code expects the
// FIRST click of the final tied timestamp group to have the lexicographically
// smallest click ID. Selecting that record once per tie group preserves this
// rule without repeatedly comparing strings during sort or attribution.
inline void promote_smallest_click_id_in_ties(ValidClick* clicks, size_t n) {
    for (size_t begin = 0; begin < n;) {
        size_t end = begin + 1;
        size_t min_id = begin;
        while (end < n && clicks[end].ts == clicks[begin].ts) {
            if (clicks[end].click_id < clicks[min_id].click_id) min_id = end;
            ++end;
        }
        if (min_id != begin) std::swap(clicks[begin], clicks[min_id]);
        begin = end;
    }
}

// Use a more compact open-addressing table than 50% default occupancy.
// Change SHOPPING_DEDUP_LOAD and A/B benchmark if different input distributions favor it.
#ifndef SHOPPING_DEDUP_LOAD
#define SHOPPING_DEDUP_LOAD 70
#endif
static_assert(SHOPPING_DEDUP_LOAD >= 40 && SHOPPING_DEDUP_LOAD <= 80);

struct FlatEventMap {
    struct Slot {
        uint64_t h_lo;
        uint32_t idx;
        uint32_t gen;
    };
    std::vector<Slot> slots;
    std::vector<RawDedupEvent> payloads;
    size_t mask = 0;
    uint32_t gen = 0;

    void reset(size_t expected) {
        size_t cap = 1024;
        while (cap * SHOPPING_DEDUP_LOAD < expected * 100) cap <<= 1;
        if (slots.size() < cap) {
            slots.assign(cap, Slot{0, 0, 0});
            gen = 1;
        } else if (++gen == 0) {
            for (auto& s : slots) s.gen = 0;
            gen = 1;
        }
        mask = slots.size() - 1;
        payloads.clear();
        if (payloads.capacity() < expected) payloads.reserve(expected);
    }

    inline void insert_or_min(const RawDedupEvent& e) noexcept {
        size_t pos = e.eid_hash.lo & mask;
        while (true) {
            auto& s = slots[pos];
            if (s.gen != gen) {
                s.h_lo = e.eid_hash.lo;
                s.idx = static_cast<uint32_t>(payloads.size());
                s.gen = gen;
                payloads.push_back(e);
                return;
            }
            if (s.h_lo == e.eid_hash.lo && payloads[s.idx].eid_hash == e.eid_hash) {
                if (e.ingest_ts < payloads[s.idx].ingest_ts) payloads[s.idx] = e;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }
};

struct FlatClickSet {
    struct Slot {
        Hash128 h;
        uint32_t gen;
    };
    std::vector<Slot> slots;
    size_t mask = 0;
    uint32_t gen = 0;

    void reset(size_t expected) {
        size_t cap = 1024;
        while (cap < expected * 2) cap <<= 1;
        if (slots.size() < cap) {
            slots.assign(cap, Slot{{0, 0}, 0});
            gen = 1;
        } else if (++gen == 0) {
            for (auto& s : slots) s.gen = 0;
            gen = 1;
        }
        mask = slots.size() - 1;
    }

    inline bool insert(Hash128 h) noexcept {
        size_t idx = h.lo & mask;
        while (true) {
            auto& s = slots[idx];
            if (s.gen != gen) { s.h = h; s.gen = gen; return true; }
            if (s.h == h) return false;
            idx = (idx + 1) & mask;
        }
    }
};

struct CampAgg { int64_t sessions = 0, purchasing = 0, rev = 0, assisted = 0; };

struct FlatCampMap {
    struct Slot {
        uint64_t key;
        CampAgg val;
    };
    std::vector<Slot> slots;
    size_t count = 0;
    size_t mask = 0;

    void init(size_t cap = 2048) {
        slots.assign(cap, Slot{UINT64_MAX, {}});
        mask = cap - 1;
        count = 0;
    }

    void rehash(size_t new_cap) {
        std::vector<Slot> nslots(new_cap, Slot{UINT64_MAX, {}});
        size_t nmask = new_cap - 1;
        for (const auto& s : slots) {
            if (s.key == UINT64_MAX) continue;
            size_t idx = mix64(s.key) & nmask;
            while (nslots[idx].key != UINT64_MAX) idx = (idx + 1) & nmask;
            nslots[idx] = s;
        }
        slots.swap(nslots);
        mask = nmask;
    }

    inline CampAgg& operator[](uint64_t key) {
        if (slots.empty()) init(2048);
        if ((count + 1) * 2 >= slots.size()) rehash(slots.size() << 1);
        size_t idx = mix64(key) & mask;
        while (true) {
            auto& s = slots[idx];
            if (s.key == key) return s.val;
            if (s.key == UINT64_MAX) {
                s.key = key;
                s.val = {};
                ++count;
                return s.val;
            }
            idx = (idx + 1) & mask;
        }
    }
};

struct PersonShard {
    FlatStrMap people;
    std::vector<uint32_t> ev_offsets;
    std::vector<ValidEvent> flat_events;
};

struct UserRow {
    uint32_t shard;
    uint32_t pid;
    int32_t day;
    int64_t sessions;
    int64_t events;
    int64_t revenue_cents;
    uint32_t top_cat;
};

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
    auto cst = (*out_res)->Close();
    if (!cst.ok()) { std::cerr << cst.ToString() << "\n"; std::exit(1); }
}

int main(int argc, char** argv) {
    fs::path data_dir = (argc >= 3) ? fs::path(argv[1]) : fs::path(std::getenv("DATA_DIR") ? std::getenv("DATA_DIR") : "/data");
    fs::path output_dir = (argc >= 3) ? fs::path(argv[2]) : fs::path(std::getenv("OUTPUT_DIR") ? std::getenv("OUTPUT_DIR") : "/output");

    unsigned n_workers = std::thread::hardware_concurrency();
    if (const char* env = std::getenv("NCPU")) if (auto v = std::atoi(env); v > 0) n_workers = v;
    n_workers = std::max(1u, n_workers);

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

    FlatStrMap categories;
    uint32_t unknown_cat = categories.get_id("unknown");
    ProductTable products;
    FlatStrMap camp_ids;
    std::vector<std::string> camp_channels;

    std::thread t_prod([&]() {
        MMapFile f;
        if (f.open(data_dir / "products.csv")) {
            std::string_view c[2];
            for_each_line(f, [&](std::string_view ln) {
                if (parse_csv(ln, 2, c)) products.insert(c[0], categories.get_id(c[1]));
            });
        }
    });

    std::thread t_camp([&]() {
        MMapFile f;
        if (f.open(data_dir / "campaigns.csv")) {
            camp_ids.init(f.size / 24 + 64, f.size / 2 + 256);
            std::string_view c[2];
            for_each_line(f, [&](std::string_view ln) {
                if (parse_csv(ln, 2, c)) {
                    uint32_t id = camp_ids.get_id(c[0]);
                    if (id >= camp_channels.size()) camp_channels.resize(id + 1);
                    camp_channels[id] = std::string(c[1]);
                }
            });
        }
    });

    UnionFind uf;
    // Main optimization: build identity links concurrently with event ingestion.
    // Event parsing needs the products table but does not read UnionFind.
    // Joining t_links before dedup establishes safe read-after-write synchronization.
    std::thread t_links([&]() {
    {
        auto link_files = list_csvs(data_dir / "links");
        size_t lbytes = 0;
        std::vector<MMapFile> lmmaps;
        for (const auto& p : link_files) {
            MMapFile f;
            if (f.open(p)) { lbytes += f.size; lmmaps.push_back(std::move(f)); }
        }
        auto lchunks = make_chunks(lmmaps);
        if (!lchunks.empty()) {
            struct LinkPair { std::string_view a, b; uint64_t ha, hb; };
            std::vector<std::vector<LinkPair>> l_locals(n_workers);
            std::atomic<size_t> next_lc{0};
            auto lworker = [&](unsigned tid) {
                auto& out = l_locals[tid];
                out.reserve(lbytes / (24 * n_workers) + 64);
                std::string_view c[2];
                while (true) {
                    size_t idx = next_lc.fetch_add(1, std::memory_order_relaxed);
                    if (idx >= lchunks.size()) break;
                    const char* cur = lchunks[idx].begin;
                    const char* end = lchunks[idx].end;
                    while (cur < end) {
                        const char* nl = static_cast<const char*>(std::memchr(cur, '\n', static_cast<size_t>(end - cur)));
                        size_t len = nl ? static_cast<size_t>(nl - cur) : static_cast<size_t>(end - cur);
                        std::string_view ln(cur, len);
                        cur = nl ? nl + 1 : end;
                        if (parse_csv(ln, 2, c) && !c[0].empty() && !c[1].empty()) {
                            out.push_back({c[0], c[1], hash64(c[0]), hash64(c[1])});
                        }
                    }
                }
            };
            std::vector<std::thread> ts;
            for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(lworker, t);
            for (auto& t : ts) t.join();

            uf.map.init(lbytes / 20 + 64, lbytes);
            for (const auto& vec : l_locals) {
                for (const auto& lp : vec) uf.unite(lp.a, lp.ha, lp.b, lp.hb);
            }
        }
    }
    uf.freeze();
    });
    t_prod.join(); // needed for category lookups during event parsing

    std::vector<MMapFile> event_mmaps;
    size_t total_ev_bytes = 0;
    for (const auto& p : list_csvs(data_dir / "events")) {
        MMapFile f;
        if (f.open(p)) { total_ev_bytes += f.size; event_mmaps.push_back(std::move(f)); }
    }
    auto ev_chunks = make_chunks(event_mmaps);
    const size_t est_ev_per_bucket = total_ev_bytes / (100 * n_workers * N_SHARDS) + 32;

    std::vector<std::vector<std::vector<RawDedupEvent>>> ev_buckets(
        n_workers, std::vector<std::vector<RawDedupEvent>>(N_SHARDS));
    {
        std::atomic<size_t> next_chunk{0};
        auto worker = [&](unsigned tid) {
            auto& my_b = ev_buckets[tid];
            for (auto& b : my_b) b.reserve(est_ev_per_bucket);
            DateCache dc;
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
                    if (!parse_ts(c[3], ingest_ts, dc)) continue;

                    Hash128 h = hash128(c[0]);
                    RawDedupEvent ev{h, ingest_ts, 0, nullptr, 0, 0};
                    int64_t ts = 0;
                    if (!c[1].empty() && !c[5].empty() && parse_ts(c[2], ts, dc) &&
                        ts >= win_start && ts < win_end && (ingest_ts - ts) <= MS_PER_DAY) {
                        bool ok = true, is_purch = false;
                        int32_t price = 0;
                        if (c[4] == "purchase") { is_purch = true; ok = parse_price(c[6], price); }
                        else if (c[4] != "view" && c[4] != "cart") ok = false;

                        if (ok) {
                            uint32_t cat_id = unknown_cat;
                            if (is_purch) {
                                uint32_t found = products.find(c[5]);
                                if (found != UINT32_MAX) cat_id = found;
                            }
                            ev.ts = ts;
                            ev.user_ptr = c[1].data();
                            ev.ulen_cat = (static_cast<uint32_t>(c[1].size()) << 16) | (cat_id & 0xFFFFu);
                            ev.price_flags = (static_cast<uint32_t>(price) << 2) | 1u | (is_purch ? 2u : 0u);
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
    t_links.join();
    t_camp.join();

    std::vector<std::vector<std::vector<PersonEventMsg>>> pev_buckets(
        n_workers, std::vector<std::vector<PersonEventMsg>>(N_SHARDS));
    {
        const size_t est_pev_per_bucket = total_ev_bytes / (120 * n_workers * N_SHARDS) + 32;
        std::atomic<size_t> next_s{0};
        const bool has_uf = (uf.map.count > 0);
        const size_t uf_mask = uf.map.mask;
        auto worker = [&](unsigned tid) {
            auto& my_pb = pev_buckets[tid];
            for (auto& b : my_pb) b.reserve(est_pev_per_bucket);
            FlatEventMap map;
            constexpr size_t B = 16;
            uint64_t uhashes[B];
            while (true) {
                size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
                if (s >= N_SHARDS) break;
                size_t total = 0;
                for (unsigned t = 0; t < n_workers; ++t) total += ev_buckets[t][s].size();
                if (total == 0) continue;

                map.reset(total);
                const size_t mask = map.mask;
                for (unsigned t = 0; t < n_workers; ++t) {
                    const auto& vec = ev_buckets[t][s];
                    const size_t n = vec.size();
                    for (size_t i = 0; i < n; ++i) {
                        if (i + 16 < n) __builtin_prefetch(&map.slots[vec[i + 16].eid_hash.lo & mask], 1, 1);
                        map.insert_or_min(vec[i]);
                    }
                    std::vector<RawDedupEvent>().swap(ev_buckets[t][s]);
                }

                const size_t np = map.payloads.size();
                for (size_t base = 0; base < np; base += B) {
                    const size_t lim = std::min(np, base + B);
                    for (size_t i = base; i < lim; ++i) {
                        const auto& e = map.payloads[i];
                        if (!(e.price_flags & 1u)) continue;
                        uint64_t h = hash64(std::string_view(e.user_ptr, e.ulen_cat >> 16));
                        uhashes[i - base] = h;
                        if (has_uf) __builtin_prefetch(&uf.map.slots[h & uf_mask], 0, 1);
                    }
                    for (size_t i = base; i < lim; ++i) {
                        const auto& e = map.payloads[i];
                        if (!(e.price_flags & 1u)) continue;
                        std::string_view u(e.user_ptr, e.ulen_cat >> 16);
                        auto [person, ph] = uf.canonical_with_hash_prehashed(u, uhashes[i - base]);
                        my_pb[ph & SHARD_MASK].push_back({
                            ph, person,
                            ValidEvent{e.ts, static_cast<int32_t>(e.price_flags >> 2), e.ulen_cat & 0xFFFFu, (e.price_flags & 2u) != 0}
                        });
                    }
                }
            }
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker, t);
        for (auto& t : ts) t.join();
    }
    ev_buckets.clear();

    std::vector<PersonShard> pshards(N_SHARDS);
    {
        std::atomic<size_t> next_ps{0};
        auto worker = [&]() {
            std::vector<uint32_t> pids;
            std::vector<uint32_t> counts;
            while (true) {
                size_t ps = next_ps.fetch_add(1, std::memory_order_relaxed);
                if (ps >= N_SHARDS) break;
                auto& sh = pshards[ps];
                size_t total = 0;
                for (unsigned t = 0; t < n_workers; ++t) total += pev_buckets[t][ps].size();
                if (total == 0) continue;

                sh.people.init(total / 4 + 32, total * 4 + 256);
                pids.clear();
                pids.reserve(total);
                counts.clear();

                for (unsigned t = 0; t < n_workers; ++t) {
                    for (const auto& msg : pev_buckets[t][ps]) {
                        uint32_t pid = sh.people.get_id(msg.person, msg.phash);
                        if (pid >= counts.size()) counts.resize(pid + 1, 0);
                        ++counts[pid];
                        pids.push_back(pid);
                    }
                }

                const size_t n_people = counts.size();
                sh.ev_offsets.resize(n_people + 1);
                uint32_t sum = 0;
                for (size_t i = 0; i < n_people; ++i) {
                    sh.ev_offsets[i] = sum;
                    sum += counts[i];
                    counts[i] = sh.ev_offsets[i];
                }
                sh.ev_offsets[n_people] = sum;
                sh.flat_events.resize(sum);

                size_t idx = 0;
                for (unsigned t = 0; t < n_workers; ++t) {
                    for (const auto& msg : pev_buckets[t][ps]) {
                        sh.flat_events[counts[pids[idx++]]++] = msg.ev;
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
    event_mmaps.clear();

    std::vector<MMapFile> click_mmaps;
    size_t total_ck_bytes = 0;
    for (const auto& p : list_csvs(data_dir / "clicks")) {
        MMapFile f;
        if (f.open(p)) { total_ck_bytes += f.size; click_mmaps.push_back(std::move(f)); }
    }
    auto ck_chunks = make_chunks(click_mmaps);
    const size_t est_ck_per_bucket = total_ck_bytes / (80 * n_workers * N_SHARDS) + 32;

    std::vector<std::vector<std::vector<RawClickMsg>>> ck_buckets(
        n_workers, std::vector<std::vector<RawClickMsg>>(N_SHARDS));
    {
        std::atomic<size_t> next_chunk{0};
        const bool has_uf = (uf.map.count > 0);
        const size_t uf_mask = uf.map.mask;
        auto worker = [&](unsigned tid) {
            auto& my_b = ck_buckets[tid];
            for (auto& b : my_b) b.reserve(est_ck_per_bucket);
            DateCache dc;
            std::string_view c[4];

            constexpr size_t B = 32;
            struct PendingClick {
                std::string_view cid, user, person;
                uint64_t uhash, ph;
                int64_t ts;
                uint32_t camp_id, ps;
            };
            PendingClick pend[B];
            size_t n_pend = 0;

            auto flush_pending = [&]() {
                if (n_pend == 0) return;
                if (has_uf) {
                    for (size_t k = 0; k < n_pend; ++k) {
                        __builtin_prefetch(&uf.map.slots[pend[k].uhash & uf_mask], 0, 1);
                    }
                }
                for (size_t k = 0; k < n_pend; ++k) {
                    auto [person, ph] = uf.canonical_with_hash_prehashed(pend[k].user, pend[k].uhash);
                    pend[k].person = person;
                    pend[k].ph = ph;
                    uint32_t ps = static_cast<uint32_t>(ph & SHARD_MASK);
                    pend[k].ps = ps;
                    const auto& pm = pshards[ps].people;
                    if (!pm.slots.empty()) __builtin_prefetch(&pm.slots[ph & pm.mask], 0, 1);
                }
                for (size_t k = 0; k < n_pend; ++k) {
                    uint32_t pid = pshards[pend[k].ps].people.find(pend[k].person, pend[k].ph);
                    if (pid == UINT32_MAX) continue;
                    Hash128 h = hash128(pend[k].cid);
                    my_b[h.lo & SHARD_MASK].push_back({h, pend[k].cid, pend[k].ts, pend[k].ps, pid, pend[k].camp_id});
                }
                n_pend = 0;
            };

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
                    uint32_t camp_id = camp_ids.find(c[2]);
                    if (camp_id == UINT32_MAX) continue;
                    int64_t ts = 0;
                    if (!parse_ts(c[3], ts, dc) || ts < click_win_start || ts >= win_end) continue;

                    pend[n_pend++] = {c[0], c[1], {}, hash64(c[1]), 0, ts, camp_id, 0};
                    if (n_pend == B) flush_pending();
                }
            }
            flush_pending();
        };
        std::vector<std::thread> ts;
        for (unsigned t = 0; t < n_workers; ++t) ts.emplace_back(worker, t);
        for (auto& t : ts) t.join();
    }

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

                seen.reset(total);
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
    std::vector<FlatCampMap> camp_locals(n_workers);
    const size_t num_cats = categories.id_slices.size();
    const size_t num_camps = camp_ids.id_slices.size();

    auto agg_worker = [&](unsigned tid) {
        std::vector<UserRow> urows;
        auto& cmap = camp_locals[tid];
        cmap.init(2048);
        std::vector<int64_t> cat_rev(num_cats, 0);
        std::vector<uint32_t> touched_cats;
        std::vector<uint32_t> active_cnt(num_camps, 0);
        std::vector<uint32_t> active_list;
        std::vector<uint32_t> ck_offsets;
        std::vector<uint32_t> ck_pos;
        std::vector<ValidClick> flat_clicks;
        // Scratch is retained per aggregation worker, not reallocated per user.
        std::vector<ValidEvent> event_radix_scratch;
        std::vector<ValidClick> click_radix_scratch;

        while (true) {
            size_t ps = next_ps.fetch_add(1, std::memory_order_relaxed);
            if (ps >= N_SHARDS) break;
            auto& sh = pshards[ps];
            const size_t n_people = sh.people.id_slices.size();
            if (n_people == 0) continue;

            ck_offsets.assign(n_people + 1, 0);
            size_t total_cks = 0;
            for (unsigned t = 0; t < n_workers; ++t) {
                total_cks += pck_buckets[t][ps].size();
                for (const auto& m : pck_buckets[t][ps]) ++ck_offsets[m.pid + 1];
            }
            for (size_t i = 1; i <= n_people; ++i) ck_offsets[i] += ck_offsets[i - 1];
            ck_pos.assign(ck_offsets.begin(), ck_offsets.begin() + n_people);
            flat_clicks.resize(total_cks);
            for (unsigned t = 0; t < n_workers; ++t) {
                for (const auto& m : pck_buckets[t][ps]) flat_clicks[ck_pos[m.pid]++] = m.ck;
                std::vector<PersonClickMsg>().swap(pck_buckets[t][ps]);
            }

            for (uint32_t pid = 0; pid < n_people; ++pid) {
                uint32_t ev_b = sh.ev_offsets[pid], ev_e = sh.ev_offsets[pid + 1];
                if (ev_b == ev_e) continue;

                ValidEvent* evs = sh.flat_events.data() + ev_b;
                const size_t n_evs = ev_e - ev_b;
                if (n_evs > 1) {
#if SHOPPING_RADIX_EVENTS
                    if (n_evs >= SHOPPING_RADIX_THRESHOLD)
                        radix_sort_timestamp(evs, n_evs, event_radix_scratch);
                    else
#endif
                        std::sort(evs, evs + n_evs, [](const ValidEvent& a, const ValidEvent& b) {
                            return a.ts < b.ts;
                        });
                }

                uint32_t ck_b = ck_offsets[pid], ck_e = ck_offsets[pid + 1];
                ValidClick* clicks = flat_clicks.data() + ck_b;
                const size_t n_cks = ck_e - ck_b;
                if (n_cks > 1) {
#if SHOPPING_RADIX_CLICKS
                    if (n_cks >= SHOPPING_RADIX_THRESHOLD) {
                        radix_sort_timestamp(clicks, n_cks, click_radix_scratch);
                        // Attribution assumes the first record in each timestamp
                        // tie has the smallest ID, just like the old comparator.
                        promote_smallest_click_id_in_ties(clicks, n_cks);
                    } else
#endif
                    {
                        std::sort(clicks, clicks + n_cks, [](const ValidClick& a, const ValidClick& b) {
                            return a.ts != b.ts ? a.ts < b.ts : a.click_id < b.click_id;
                        });
                    }
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
                        if (r > best_rev || (r == best_rev && categories.view(c) < categories.view(best_cat))) {
                            best_rev = r; best_cat = c;
                        }
                        cat_rev[c] = 0;
                    }
                    touched_cats.clear();
                    urows.push_back({static_cast<uint32_t>(ps), pid, cur_day, d_sess, d_evs, d_rev, best_cat});
                    d_sess = d_evs = d_rev = 0;
                };

                while (i < n_evs) {
                    int64_t s_start = evs[i].ts;
                    int32_t day = static_cast<int32_t>(s_start >= 0 ? s_start / MS_PER_DAY : (s_start - MS_PER_DAY + 1) / MS_PER_DAY);
                    if (day != cur_day) { flush_day(); cur_day = day; }

                    d_sess += 1;
                    int64_t s_rev = 0, prev_ts = s_start;
                    bool has_purch = false;
                    while (i < n_evs && evs[i].ts - prev_ts <= SESSION_GAP_MS) {
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

                    while (R < n_cks && clicks[R].ts <= s_start) {
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
                        for (uint32_t cid : active_list) active_cnt[cid] = 0;
                        active_list.clear();
                        auto& ca = cmap[pack(ORGANIC_CID)];
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

        if (!urows.empty()) {
            wrote_any_user.store(true, std::memory_order_relaxed);
            size_t uid_bytes = 0, cat_bytes = 0;
            for (const auto& r : urows) {
                uid_bytes += pshards[r.shard].people.id_slices[r.pid].second;
                if (r.top_cat != UINT32_MAX) cat_bytes += categories.id_slices[r.top_cat].second;
            }

            arrow::StringBuilder b_uid, b_cat;
            arrow::Date32Builder b_day;
            arrow::Int64Builder b_sess, b_evs, b_rev;
            (void)b_uid.Reserve(urows.size()); (void)b_uid.ReserveData(uid_bytes);
            (void)b_cat.Reserve(urows.size()); (void)b_cat.ReserveData(cat_bytes);
            (void)b_day.Reserve(urows.size()); (void)b_sess.Reserve(urows.size());
            (void)b_evs.Reserve(urows.size()); (void)b_rev.Reserve(urows.size());

            for (const auto& r : urows) {
                b_uid.UnsafeAppend(pshards[r.shard].people.view(r.pid));
                b_day.UnsafeAppend(r.day);
                b_sess.UnsafeAppend(r.sessions);
                b_evs.UnsafeAppend(r.events);
                b_rev.UnsafeAppend(r.revenue_cents);
                if (r.top_cat == UINT32_MAX) b_cat.UnsafeAppendNull();
                else b_cat.UnsafeAppend(categories.view(r.top_cat));
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

    {
        FlatCampMap camp_daily;
        camp_daily.init(4096);
        for (const auto& m : camp_locals) {
            for (const auto& s : m.slots) {
                if (s.key == UINT64_MAX) continue;
                auto& t = camp_daily[s.key];
                t.sessions += s.val.sessions;
                t.purchasing += s.val.purchasing;
                t.rev += s.val.rev;
                t.assisted += s.val.assisted;
            }
        }

        size_t cid_bytes = 0, ch_bytes = 0;
        for (const auto& s : camp_daily.slots) {
            if (s.key == UINT64_MAX) continue;
            uint32_t cid = static_cast<uint32_t>(s.key >> 32);
            if (cid == ORGANIC_CID) { cid_bytes += 7; ch_bytes += 7; }
            else { cid_bytes += camp_ids.id_slices[cid].second; ch_bytes += camp_channels[cid].size(); }
        }

        arrow::StringBuilder b_cid, b_ch;
        arrow::Date32Builder b_day;
        arrow::Int64Builder b_sess, b_purch, b_rev, b_ast;
        (void)b_cid.Reserve(camp_daily.count); (void)b_cid.ReserveData(cid_bytes);
        (void)b_ch.Reserve(camp_daily.count); (void)b_ch.ReserveData(ch_bytes);
        (void)b_day.Reserve(camp_daily.count); (void)b_sess.Reserve(camp_daily.count);
        (void)b_purch.Reserve(camp_daily.count); (void)b_rev.Reserve(camp_daily.count);
        (void)b_ast.Reserve(camp_daily.count);

        for (const auto& s : camp_daily.slots) {
            if (s.key == UINT64_MAX) continue;
            uint32_t cid = static_cast<uint32_t>(s.key >> 32);
            int32_t day = static_cast<int32_t>(static_cast<uint32_t>(s.key));
            bool org = (cid == ORGANIC_CID);
            b_cid.UnsafeAppend(org ? std::string_view("organic") : camp_ids.view(cid));
            b_ch.UnsafeAppend(org ? std::string_view("organic") : std::string_view(camp_channels[cid]));
            b_day.UnsafeAppend(day);
            b_sess.UnsafeAppend(s.val.sessions);
            b_purch.UnsafeAppend(s.val.purchasing);
            b_rev.UnsafeAppend(s.val.rev);
            b_ast.UnsafeAppend(s.val.assisted);
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
    ::_exit(0);
}
