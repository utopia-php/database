<?php

namespace Utopia\Database\Replication;

/**
 * Decodes the binlog events we care about: TABLE_MAP (to learn a table's
 * column layout and names) and WRITE/UPDATE/DELETE_ROWS (the actual changes).
 *
 * Requires the source to run with `binlog_row_metadata=FULL` so column names
 * arrive in the TABLE_MAP optional metadata — this avoids any INFORMATION_SCHEMA
 * round-trips.
 *
 * Pure (operates on byte buffers), so it is unit-testable with fixtures.
 */
class EventParser
{
    /**
     * table_id => decoded table definition.
     *
     * @var array<int, array{schema: string, table: string, count: int, types: list<int>, metadata: list<int>, names: list<string>}>
     */
    private array $tables = [];

    private const array DIGITS_TO_BYTES = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];

    /**
     * Cache a table definition from a TABLE_MAP event body.
     */
    public function parseTableMap(string $body): void
    {
        $reader = new BinaryReader($body);

        $tableId = $reader->readUInt(6);
        $reader->skip(2); // flags

        $schema = $reader->read($reader->readUInt8());
        $reader->skip(1); // NUL
        $table = $reader->read($reader->readUInt8());
        $reader->skip(1); // NUL

        $count = $reader->readLengthEncodedInt() ?? 0;
        $types = \array_values(\unpack('C*', $reader->read($count)) ?: []);

        $metadataBlock = $reader->readLengthEncodedString() ?? '';
        $metadata = $this->parseMetadata($types, $metadataBlock);

        $reader->skip((int) \ceil($count / 8)); // null bitmap

        $names = $this->parseColumnNames($reader, $count);

        $this->tables[$tableId] = \compact('schema', 'table', 'count', 'types', 'metadata', 'names');
    }

    /**
     * Decode a ROWS event body into row maps.
     *
     * @return array{schema: string, table: string, rows: list<array<string, mixed>>}|null
     */
    public function parseRows(int $eventType, string $body): ?array
    {
        $reader = new BinaryReader($body);

        $tableId = $reader->readUInt(6);
        $reader->skip(2); // flags

        $table = $this->tables[$tableId] ?? null;
        if ($table === null) {
            return null; // TABLE_MAP not seen (e.g. mid-stream start) — skip
        }

        $isV2 = \in_array($eventType, [
            Constants::WRITE_ROWS_EVENT_V2,
            Constants::UPDATE_ROWS_EVENT_V2,
            Constants::DELETE_ROWS_EVENT_V2,
        ], true);
        if ($isV2) {
            $extraLength = $reader->readUInt16();
            $reader->skip($extraLength - 2);
        }

        $columnCount = $reader->readLengthEncodedInt() ?? 0;
        $bitmapSize = (int) \ceil($columnCount / 8);

        $present = $reader->read($bitmapSize);
        $isUpdate = \in_array($eventType, [Constants::UPDATE_ROWS_EVENT_V1, Constants::UPDATE_ROWS_EVENT_V2], true);
        $presentAfter = $isUpdate ? $reader->read($bitmapSize) : $present;

        $rows = [];
        while (!$reader->eof()) {
            if ($isUpdate) {
                $this->readRow($reader, $table, $present); // before-image (discarded)
            }
            $rows[] = $this->readRow($reader, $table, $presentAfter);
        }

        return ['schema' => $table['schema'], 'table' => $table['table'], 'rows' => $rows];
    }

    /**
     * @param array{count: int, types: list<int>, metadata: list<int>, names: list<string>} $table
     * @return array<string, mixed>
     */
    private function readRow(BinaryReader $reader, array $table, string $present): array
    {
        $presentCount = $this->countBits($present);
        $nullBitmap = $reader->read((int) \ceil($presentCount / 8));

        $row = [];
        $nullIndex = 0;
        for ($column = 0; $column < $table['count']; $column++) {
            if (!$this->bitAt($present, $column)) {
                continue;
            }

            $name = $table['names'][$column] ?? (string) $column;
            $isNull = $this->bitAt($nullBitmap, $nullIndex);
            $nullIndex++;

            $row[$name] = $isNull
                ? null
                : $this->decodeValue($reader, $table['types'][$column], $table['metadata'][$column]);
        }

        return $row;
    }

    private function decodeValue(BinaryReader $reader, int $type, int $metadata): mixed
    {
        switch ($type) {
            case Constants::TYPE_TINY:
                return $reader->readUInt(1);
            case Constants::TYPE_SHORT:
                return $reader->readUInt(2);
            case Constants::TYPE_INT24:
                return $reader->readUInt(3);
            case Constants::TYPE_LONG:
                return $reader->readUInt(4);
            case Constants::TYPE_LONGLONG:
                return $reader->readUInt(8);
            case Constants::TYPE_YEAR:
                return $reader->readUInt(1);
            case Constants::TYPE_FLOAT:
                return $this->unpackNumber('g', $reader->read(4));
            case Constants::TYPE_DOUBLE:
                return $this->unpackNumber('e', $reader->read(8));
            case Constants::TYPE_VARCHAR:
            case Constants::TYPE_VAR_STRING:
                $prefix = $metadata > 255 ? 2 : 1;
                return $reader->read($reader->readUInt($prefix));
            case Constants::TYPE_STRING:
                return $this->decodeString($reader, $metadata);
            case Constants::TYPE_BLOB:
            case Constants::TYPE_TINY_BLOB:
            case Constants::TYPE_MEDIUM_BLOB:
            case Constants::TYPE_LONG_BLOB:
            case Constants::TYPE_GEOMETRY:
            case Constants::TYPE_JSON:
                return $reader->read($reader->readUInt(\max(1, $metadata)));
            case Constants::TYPE_ENUM:
            case Constants::TYPE_SET:
                return $reader->readUInt(\max(1, $metadata));
            case Constants::TYPE_NEWDECIMAL:
            case Constants::TYPE_DECIMAL:
                return $reader->read($this->decimalLength($metadata >> 8, $metadata & 0xFF));
            case Constants::TYPE_DATE:
            case Constants::TYPE_TIME:
                return $reader->read(3);
            case Constants::TYPE_TIMESTAMP:
                return $reader->read(4);
            case Constants::TYPE_DATETIME:
                return $reader->read(8);
            case Constants::TYPE_TIMESTAMP2:
                return $reader->read(4 + \intdiv($metadata + 1, 2));
            case Constants::TYPE_DATETIME2:
                return $reader->read(5 + \intdiv($metadata + 1, 2));
            case Constants::TYPE_TIME2:
                return $reader->read(3 + \intdiv($metadata + 1, 2));
            case Constants::TYPE_BIT:
                $bits = (($metadata >> 8) * 8) + ($metadata & 0xFF);
                return $reader->read((int) \ceil($bits / 8));
            case Constants::TYPE_NULL:
                return null;
            default:
                throw new \RuntimeException("Unsupported binlog column type: {$type}");
        }
    }

    private function unpackNumber(string $format, string $bytes): float
    {
        $value = \unpack($format, $bytes);

        return \is_array($value) ? (float) ($value[1] ?? 0) : 0.0;
    }

    private function decodeString(BinaryReader $reader, int $metadata): mixed
    {
        $realType = $metadata >> 8;
        $low = $metadata & 0xFF;

        if ($realType === Constants::TYPE_ENUM || $realType === Constants::TYPE_SET) {
            return $reader->readUInt(\max(1, $low));
        }

        // Packed CHAR length: high bits live in the real-type byte.
        $maxLength = ((($realType & 0x30) ^ 0x30) << 4) | $low;
        $prefix = $maxLength > 255 ? 2 : 1;

        return $reader->read($reader->readUInt($prefix));
    }

    /**
     * @param list<int> $types
     * @return list<int>
     */
    private function parseMetadata(array $types, string $block): array
    {
        $reader = new BinaryReader($block);
        $metadata = [];

        foreach ($types as $type) {
            $metadata[] = match ($type) {
                Constants::TYPE_FLOAT,
                Constants::TYPE_DOUBLE,
                Constants::TYPE_BLOB,
                Constants::TYPE_TINY_BLOB,
                Constants::TYPE_MEDIUM_BLOB,
                Constants::TYPE_LONG_BLOB,
                Constants::TYPE_GEOMETRY,
                Constants::TYPE_JSON,
                Constants::TYPE_TIMESTAMP2,
                Constants::TYPE_DATETIME2,
                Constants::TYPE_TIME2 => $reader->readUInt8(),
                Constants::TYPE_VARCHAR,
                Constants::TYPE_VAR_STRING,
                Constants::TYPE_BIT => $reader->readUInt16(),
                Constants::TYPE_NEWDECIMAL,
                Constants::TYPE_DECIMAL => ($reader->readUInt8() << 8) | $reader->readUInt8(),
                Constants::TYPE_STRING,
                Constants::TYPE_ENUM,
                Constants::TYPE_SET => ($reader->readUInt8() << 8) | $reader->readUInt8(),
                default => 0,
            };
        }

        return $metadata;
    }

    /**
     * Read column names from the TABLE_MAP optional metadata (COLUMN_NAME field).
     *
     * @return list<string>
     */
    private function parseColumnNames(BinaryReader $reader, int $count): array
    {
        $names = [];

        while (!$reader->eof()) {
            $fieldType = $reader->readUInt8();
            $fieldLength = $reader->readLengthEncodedInt() ?? 0;
            $field = $reader->read($fieldLength);

            if ($fieldType === Constants::METADATA_COLUMN_NAME) {
                $fieldReader = new BinaryReader($field);
                while (!$fieldReader->eof()) {
                    $names[] = $fieldReader->readLengthEncodedString() ?? '';
                }
            }
        }

        // Fall back to positional names if FULL metadata is unavailable.
        if ($names === []) {
            $names = \array_map('strval', \range(0, \max(0, $count - 1)));
        }

        return $names;
    }

    private function decimalLength(int $precision, int $scale): int
    {
        $integer = $precision - $scale;
        $integerFull = \intdiv($integer, 9);
        $fractionFull = \intdiv($scale, 9);

        return $integerFull * 4 + self::DIGITS_TO_BYTES[$integer - $integerFull * 9]
            + $fractionFull * 4 + self::DIGITS_TO_BYTES[$scale - $fractionFull * 9];
    }

    private function bitAt(string $bitmap, int $index): bool
    {
        return (\ord($bitmap[$index >> 3]) >> ($index & 7) & 1) === 1;
    }

    private function countBits(string $bitmap): int
    {
        $count = 0;
        for ($i = 0, $len = \strlen($bitmap); $i < $len; $i++) {
            $count += \substr_count(\decbin(\ord($bitmap[$i])), '1');
        }

        return $count;
    }
}
