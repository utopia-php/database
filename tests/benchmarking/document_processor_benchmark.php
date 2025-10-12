<?php

require_once __DIR__ . "/../../vendor/autoload.php";

use Utopia\Database\Database;
use Utopia\Database\DateTime;
use Utopia\Database\Document;
use Utopia\Database\DocumentProcessor;

// LEVEL: LIGHT | MEDIUM | HEAVY
$level = strtoupper($argv[1] ?? "MEDIUM");

// Optional flags: --assert | --assert-parity, --repeat=N, --warmup=N
$args = array_slice($argv, 2);
$assertParity = false;
$repeat = 1;
$warmup = 0;
foreach ($args as $arg) {
    if ($arg === '--assert' || $arg === '--assert-parity') {
        $assertParity = true;
        continue;
    }
    if (preg_match('/^--repeat=(\d+)$/', $arg, $m)) {
        $repeat = max(1, (int)$m[1]);
        continue;
    }
    if (preg_match('/^--warmup=(\d+)$/', $arg, $m)) {
        $warmup = max(0, (int)$m[1]);
        continue;
    }
}
if (getenv('ASSERT_PARITY')) {
    $assertParity = true;
}

$levels = [
    "LIGHT" => ["docs" => 1000, "arrays" => 1, "array_size" => 10],
    "MEDIUM" => ["docs" => 5000, "arrays" => 1, "array_size" => 10],
    "HEAVY" => ["docs" => 10000, "arrays" => 2, "array_size" => 20],
    // Spatial-heavy scenario: adds spatial attributes with decode filters
    "SPATIAL" => ["docs" => 5000, "arrays" => 1, "array_size" => 10, "spatial" => true],
];

if (!isset($levels[$level])) {
    fwrite(STDERR, "Invalid level: {$level}\n");
    exit(1);
}

$cfg = $levels[$level];
$docs = $cfg["docs"];
$arraySize = $cfg["array_size"];

// Build a realistic collection schema with filters (optionally spatial)
function buildCollection(bool $spatial = false): Document
{
    $attributes = [];
    for ($i = 1; $i <= 3; $i++) {
        $attributes[] = ['$id' => "s{$i}", "type" => Database::VAR_STRING, "array" => false, "filters" => []];
    }
    $attributes[] = ['$id' => "jsonData", "type" => Database::VAR_STRING, "array" => false, "filters" => ["json"]];
    $attributes[] = ['$id' => "jsonArray", "type" => Database::VAR_STRING, "array" => true, "filters" => ["json"]];
    for ($i = 1; $i <= 2; $i++) {
        $attributes[] = ['$id' => "n{$i}", "type" => Database::VAR_INTEGER, "array" => false, "filters" => []];
    }
    $attributes[] = ['$id' => "b1", "type" => Database::VAR_BOOLEAN, "array" => false, "filters" => []];
    $attributes[] = ['$id' => "d1", "type" => Database::VAR_DATETIME, "array" => false, "filters" => ["datetime"]];
    $attributes[] = ['$id' => "d2", "type" => Database::VAR_DATETIME, "array" => false, "filters" => ["datetime"]];
    $attributes[] = ['$id' => "arr", "type" => Database::VAR_STRING, "array" => true, "filters" => []];

    if ($spatial) {
        $attributes[] = ['$id' => 'p1', 'type' => Database::VAR_POINT, 'array' => false, 'filters' => [Database::VAR_POINT]];
        $attributes[] = ['$id' => 'ls1', 'type' => Database::VAR_LINESTRING, 'array' => false, 'filters' => [Database::VAR_LINESTRING]];
        $attributes[] = ['$id' => 'pg1', 'type' => Database::VAR_POLYGON, 'array' => false, 'filters' => [Database::VAR_POLYGON]];
    }

    return new Document(["attributes" => $attributes]);
}

function makeDoc(int $i, int $arraySize, bool $spatial = false): Document
{
    $d = new Document([
        '$id' => "doc{$i}",
        "s1" => "alpha{$i}",
        "s2" => "beta{$i}",
        "s3" => "gamma{$i}",
        "jsonData" => ["nested" => "data", "count" => $i],
        "jsonArray" => [["id" => 1], ["id" => 2]],
        "n1" => $i,
        "n2" => $i * 2,
        "b1" => $i % 2 === 0,
        "d1" => "2024-01-15 10:30:00",
        "d2" => "2024-01-15 15:45:30",
        "arr" => array_map(fn ($k) => "it{$k}", range(1, $arraySize)),
    ]);

    if ($spatial) {
        // Encode spatial as JSON strings to simulate adapter-encoded values
        $lon = ($i % 180) - 90;
        $lat = (($i * 2) % 180) - 90;
        $d->setAttribute('p1', json_encode(['type' => 'Point', 'coordinates' => [$lon, $lat]]));
        $d->setAttribute('ls1', json_encode(['type' => 'LineString', 'coordinates' => [[$lon, $lat], [$lon + 1, $lat + 1], [$lon + 2, $lat + 2]]]));
        $d->setAttribute('pg1', json_encode(['type' => 'Polygon', 'coordinates' => [[[$lon, $lat], [$lon + 1, $lat], [$lon + 1, $lat + 1], [$lon, $lat + 1], [$lon, $lat]]]]));
    }

    return $d;
}

$collection = buildCollection((bool)($cfg['spatial'] ?? false));

/**
 * @return array<int, float>
 */
function measure(callable $fn, int $repeat = 1, int $warmup = 0): array
{
    for ($w = 0; $w < $warmup; $w++) {
        $fn();
    }
    $times = [];
    for ($r = 0; $r < $repeat; $r++) {
        $start = microtime(true);
        $fn();
        $times[] = (microtime(true) - $start) * 1000;
    }
    sort($times);
    return $times; // sorted ascending
}

// Baseline and optimized functions
$baseline = new BaselineProcessor();
$processor = new DocumentProcessorWithFilters();

$spatialEnabled = (bool)($cfg['spatial'] ?? false);

$baselineTimes = measure(function () use ($baseline, $collection, $docs, $arraySize, $spatialEnabled) {
    for ($i = 1; $i <= $docs; $i++) {
        $doc = makeDoc($i, $arraySize, $spatialEnabled);
        $doc = $baseline->decodeBaseline($collection, $doc);
        $doc = $baseline->castingBaseline($collection, $doc);
    }
}, $repeat, $warmup);

$optimizedTimes = measure(function () use ($processor, $collection, $docs, $arraySize, $spatialEnabled) {
    for ($i = 1; $i <= $docs; $i++) {
        $doc = makeDoc($i, $arraySize, $spatialEnabled);
        $doc = $processor->processRead($collection, $doc);
    }
}, $repeat, $warmup);

$baselineMs = (int) round($baselineTimes[(int) floor((count($baselineTimes) - 1) / 2)]);
$optMs = (int) round($optimizedTimes[(int) floor((count($optimizedTimes) - 1) / 2)]);

$gain = $baselineMs > 0 ? (($baselineMs - $optMs) / $baselineMs) * 100 : 0;

echo "\nDocument Processor Benchmark - {$level} (WITH FILTERS)\n";
echo "+---------+----------+----------+--------+\n";
echo "| Metric  | Baseline | Optimized|  Gain  |\n";
echo "+---------+----------+----------+--------+\n";
printf("| %-7s | %8d | %8d | %6.1f%% |\n", "time", (int) $baselineMs, (int) $optMs, $gain);
if ($repeat > 1) {
    echo "(median of {$repeat} runs, warmup={$warmup})\n";
}
echo "\n";

// Optional parity assert mode
if ($assertParity) {
    $checks = min($docs, 1000);
    for ($i = 1; $i <= $checks; $i++) {
        $docA = makeDoc($i, $arraySize, $spatialEnabled);
        $base = $baseline->decodeBaseline($collection, clone $docA);
        $base = $baseline->castingBaseline($collection, $base);

        $docB = makeDoc($i, $arraySize, $spatialEnabled);
        $opt = $processor->processRead($collection, $docB);

        $a = $base->getArrayCopy();
        $b = $opt->getArrayCopy();
        if ($a != $b) {
            fwrite(STDERR, "Parity mismatch on doc {$i}\n");
            // Find first differing key
            foreach ($a as $k => $v) {
                $va = $a[$k] ?? null;
                $vb = $b[$k] ?? null;
                if ($va != $vb) {
                    fwrite(STDERR, " - Attribute '{$k}' differs\n");
                    break;
                }
            }
            exit(1);
        }
    }
    echo "Parity assertion passed on {$checks} docs.\n\n";
}
echo "+---------+----------+----------+--------+\n\n";

/**
 * DocumentProcessor with proper filter support for fair comparison
 */
class DocumentProcessorWithFilters
{
    /**
     * @var array<string, array{decode: callable}>
     */
    private static array $filters = [];

    public function __construct()
    {
        // Register the same filters as Database class
        self::$filters["json"] = [
            "decode" => function (mixed $value) {
                if (!is_string($value)) {
                    return $value;
                }
                $value = json_decode($value, true) ?? [];
                if (array_key_exists('$id', $value)) {
                    return new Document($value);
                } else {
                    $value = array_map(function ($item) {
                        if (is_array($item) && array_key_exists('$id', $item)) {
                            return new Document($item);
                        }
                        return $item;
                    }, $value);
                }
                return $value;
            },
        ];

        self::$filters["datetime"] = [
            "decode" => function (?string $value) {
                return DateTime::formatTz($value);
            },
        ];

        // Spatial-like decoders for benchmark (decode JSON strings)
        self::$filters[Database::VAR_POINT] = [
            'decode' => function (?string $value) {
                return is_string($value) ? (json_decode($value, true) ?? $value) : $value;
            },
        ];
        self::$filters[Database::VAR_LINESTRING] = [
            'decode' => function (?string $value) {
                return is_string($value) ? (json_decode($value, true) ?? $value) : $value;
            },
        ];
        self::$filters[Database::VAR_POLYGON] = [
            'decode' => function (?string $value) {
                return is_string($value) ? (json_decode($value, true) ?? $value) : $value;
            },
        ];
    }

    public function processRead(Document $collection, Document $document): Document
    {
        $attributes = \array_filter(
            $collection->getAttribute("attributes", []),
            fn ($attribute) => $attribute["type"] !== Database::VAR_RELATIONSHIP,
        );

        foreach (Database::INTERNAL_ATTRIBUTES as $attribute) {
            $attributes[] = $attribute;
        }

        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? "";
            $type = $attribute["type"] ?? "";
            $array = $attribute["array"] ?? false;
            $filters = $attribute["filters"] ?? [];

            if ($key === '$permissions') {
                continue;
            }

            $value = $document->getAttribute($key);

            if ($array) {
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    $value = \is_array($decoded) ? $decoded : $value;
                }
                if (!\is_array($value)) {
                    $value = $value === null ? [] : [$value];
                }

                foreach ($value as $i => $node) {
                    // Apply filters in reverse order like Database::decode
                    foreach (array_reverse($filters) as $filter) {
                        $node = $this->decodeAttribute($filter, $node);
                    }
                    $value[$i] = $this->castNode($type, $node);
                }
                $document->setAttribute($key, $value);
            } else {
                // Apply filters
                foreach (array_reverse($filters) as $filter) {
                    $value = $this->decodeAttribute($filter, $value);
                }
                $document->setAttribute($key, $this->castNode($type, $value));
            }
        }

        return $document;
    }

    protected function decodeAttribute(string $filter, mixed $value): mixed
    {
        if (!array_key_exists($filter, self::$filters)) {
            return $value; // Unknown filter, pass through
        }
        return self::$filters[$filter]["decode"]($value);
    }

    private function castNode(string $type, mixed $node): mixed
    {
        switch ($type) {
            case Database::VAR_ID:
                return (string) $node;
            case Database::VAR_BOOLEAN:
                return (bool) $node;
            case Database::VAR_INTEGER:
                return (int) $node;
            case Database::VAR_FLOAT:
                return (float) $node;
            default:
                return $node;
        }
    }
}

/**
 * Baseline processor that properly handles filters for fair comparison
 */
class BaselineProcessor
{
    /**
     * @var array<string, array{decode: callable}>
     */
    private static array $filters = [];

    public function __construct()
    {
        // Register the same filters as Database class
        self::$filters["json"] = [
            "decode" => function (mixed $value) {
                if (!is_string($value)) {
                    return $value;
                }
                $value = json_decode($value, true) ?? [];
                if (array_key_exists('$id', $value)) {
                    return new Document($value);
                } else {
                    $value = array_map(function ($item) {
                        if (is_array($item) && array_key_exists('$id', $item)) {
                            return new Document($item);
                        }
                        return $item;
                    }, $value);
                }
                return $value;
            },
        ];

        self::$filters["datetime"] = [
            "decode" => function (?string $value) {
                return DateTime::formatTz($value);
            },
        ];

        // Spatial-like decoders for benchmark (decode JSON strings)
        self::$filters[Database::VAR_POINT] = [
            'decode' => function (?string $value) {
                return is_string($value) ? (json_decode($value, true) ?? $value) : $value;
            },
        ];
        self::$filters[Database::VAR_LINESTRING] = [
            'decode' => function (?string $value) {
                return is_string($value) ? (json_decode($value, true) ?? $value) : $value;
            },
        ];
        self::$filters[Database::VAR_POLYGON] = [
            'decode' => function (?string $value) {
                return is_string($value) ? (json_decode($value, true) ?? $value) : $value;
            },
        ];
    }

    public function decodeBaseline(Document $collection, Document $document): Document
    {
        $attributes = \array_filter(
            $collection->getAttribute("attributes", []),
            fn ($attribute) => $attribute["type"] !== Database::VAR_RELATIONSHIP,
        );
        foreach (Database::INTERNAL_ATTRIBUTES as $attribute) {
            $attributes[] = $attribute;
        }
        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? "";
            $array = $attribute["array"] ?? false;
            $filters = $attribute["filters"] ?? [];

            if ($key === '$permissions') {
                continue;
            }
            $value = $document->getAttribute($key);
            $value = $array ? $value : [$value];
            $value = is_null($value) ? [] : $value;

            // PROPERLY APPLY FILTERS like Database::decode does
            foreach ($value as $index => $node) {
                foreach (array_reverse($filters) as $filter) {
                    $node = $this->decodeAttribute($filter, $node);
                }
                $value[$index] = $node;
            }

            $document->setAttribute($key, $array ? $value : $value[0]);
        }
        return $document;
    }

    public function castingBaseline(Document $collection, Document $document): Document
    {
        $attributes = $collection->getAttribute("attributes", []);
        foreach (Database::INTERNAL_ATTRIBUTES as $attribute) {
            $attributes[] = $attribute;
        }
        foreach ($attributes as $attribute) {
            $key = $attribute['$id'] ?? "";
            $type = $attribute["type"] ?? "";
            $array = $attribute["array"] ?? false;
            if ($key === '$permissions') {
                continue;
            }
            $value = $document->getAttribute($key);
            if ($array) {
                if (is_string($value)) {
                    $decoded = json_decode($value, true);
                    $value = \is_array($decoded) ? $decoded : $value;
                }
                if (!\is_array($value)) {
                    $value = $value === null ? [] : [$value];
                }
                foreach ($value as $i => $node) {
                    $value[$i] = $this->castNode($type, $node);
                }
                $document->setAttribute($key, $value);
            } else {
                $document->setAttribute($key, $this->castNode($type, $value));
            }
        }
        return $document;
    }

    protected function decodeAttribute(string $filter, mixed $value): mixed
    {
        if (!array_key_exists($filter, self::$filters)) {
            return $value; // Unknown filter, pass through
        }
        return self::$filters[$filter]["decode"]($value);
    }

    private function castNode(string $type, mixed $node): mixed
    {
        switch ($type) {
            case Database::VAR_ID:
                return (string) $node;
            case Database::VAR_BOOLEAN:
                return (bool) $node;
            case Database::VAR_INTEGER:
                return (int) $node;
            case Database::VAR_FLOAT:
                return (float) $node;
            default:
                return $node;
        }
    }
}
