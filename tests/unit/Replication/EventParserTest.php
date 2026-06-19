<?php

namespace Tests\Unit\Replication;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Replication\Constants;
use Utopia\Database\Replication\EventParser;

class EventParserTest extends TestCase
{
    private const int TABLE_ID = 42;
    private const string SCHEMA = 'appwrite';
    private const string TABLE = 'console15x_projects';

    /**
     * A two-column table: `_id` BIGINT, `_uid` VARCHAR (utf8mb4(255) => 1020 max bytes).
     */
    private function tableMapBody(): string
    {
        $body = $this->uint(self::TABLE_ID, 6)
            . "\x00\x00" // flags
            . \chr(\strlen(self::SCHEMA)) . self::SCHEMA . "\x00"
            . \chr(\strlen(self::TABLE)) . self::TABLE . "\x00"
            . \chr(2) // column count
            . \chr(Constants::TYPE_LONGLONG) . \chr(Constants::TYPE_VAR_STRING);

        $metadata = \pack('v', 1020); // VAR_STRING max length; LONGLONG has none
        $body .= \chr(\strlen($metadata)) . $metadata;
        $body .= "\x00"; // null bitmap (ceil(2/8))

        // Optional metadata: SIGNEDNESS (skipped) then COLUMN_NAME.
        $body .= \chr(1) . \chr(1) . "\x00"; // SIGNEDNESS field, 1 byte payload
        $names = \chr(3) . '_id' . \chr(4) . '_uid';
        $body .= \chr(Constants::METADATA_COLUMN_NAME) . \chr(\strlen($names)) . $names;

        return $body;
    }

    private function rowsHeader(): string
    {
        return $this->uint(self::TABLE_ID, 6)
            . "\x00\x00"   // flags
            . "\x02\x00"   // v2 extra-data length = 2 (none)
            . \chr(2)      // column count
            . \chr(0b11);  // both columns present
    }

    private function cell(int $id, string $uid): string
    {
        // null bitmap (no nulls) + BIGINT + length-prefixed VARCHAR (2-byte prefix).
        return "\x00" . \pack('P', $id) . \pack('v', \strlen($uid)) . $uid;
    }

    private function uint(int $value, int $bytes): string
    {
        $out = '';
        for ($i = 0; $i < $bytes; $i++) {
            $out .= \chr(($value >> ($i * 8)) & 0xFF);
        }

        return $out;
    }

    public function testWriteRowsDecodesNamedColumns(): void
    {
        $parser = new EventParser();
        $parser->parseTableMap($this->tableMapBody());

        $body = $this->rowsHeader() . $this->cell(100, 'proj123');
        $decoded = $parser->parseRows(Constants::WRITE_ROWS_EVENT_V2, $body);

        $this->assertSame(self::SCHEMA, $decoded['schema']);
        $this->assertSame(self::TABLE, $decoded['table']);
        $this->assertCount(1, $decoded['rows']);
        $this->assertSame(100, $decoded['rows'][0]['_id']);
        $this->assertSame('proj123', $decoded['rows'][0]['_uid']);
    }

    public function testMultipleRowsInOneEvent(): void
    {
        $parser = new EventParser();
        $parser->parseTableMap($this->tableMapBody());

        $body = $this->rowsHeader() . $this->cell(1, 'aaa') . $this->cell(2, 'bbbb');
        $decoded = $parser->parseRows(Constants::WRITE_ROWS_EVENT_V2, $body);

        $this->assertCount(2, $decoded['rows']);
        $this->assertSame('aaa', $decoded['rows'][0]['_uid']);
        $this->assertSame('bbbb', $decoded['rows'][1]['_uid']);
    }

    public function testUpdateKeepsAfterImage(): void
    {
        $parser = new EventParser();
        $parser->parseTableMap($this->tableMapBody());

        // Update header carries two "columns present" bitmaps (before + after).
        $header = $this->uint(self::TABLE_ID, 6) . "\x00\x00" . "\x02\x00" . \chr(2) . \chr(0b11) . \chr(0b11);
        $body = $header . $this->cell(100, 'old_uid') . $this->cell(100, 'new_uid');

        $decoded = $parser->parseRows(Constants::UPDATE_ROWS_EVENT_V2, $body);

        $this->assertCount(1, $decoded['rows']);
        $this->assertSame('new_uid', $decoded['rows'][0]['_uid']);
    }

    public function testNullColumnIsDecodedAsNull(): void
    {
        $parser = new EventParser();
        $parser->parseTableMap($this->tableMapBody());

        // null bitmap marks column 1 (_uid) as null; only _id has a value.
        $row = \chr(0b10) . \pack('P', 7);
        $decoded = $parser->parseRows(Constants::WRITE_ROWS_EVENT_V2, $this->rowsHeader() . $row);

        $this->assertSame(7, $decoded['rows'][0]['_id']);
        $this->assertNull($decoded['rows'][0]['_uid']);
    }

    public function testUnknownTableIsSkipped(): void
    {
        $parser = new EventParser();
        // No TABLE_MAP cached for this id.
        $body = $this->rowsHeader() . $this->cell(1, 'x');

        $this->assertNull($parser->parseRows(Constants::WRITE_ROWS_EVENT_V2, $body));
    }
}
